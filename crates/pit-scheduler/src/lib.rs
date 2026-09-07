//! Bounded local scheduling for independent PitFast executions.

use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Result, anyhow, bail};

/// Monotonic invocation identifier local to one scheduler process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExecutionId(pub u64);

impl fmt::Display for ExecutionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:04}", self.0 + 1)
    }
}

/// Zero-based host execution-lane identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LaneId(pub usize);

impl fmt::Display for LaneId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A host CPU execution lane owned by a local scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionLane {
    /// Zero-based lane identifier.
    pub id: LaneId,
}

/// Logical state of one execution lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneState {
    /// The lane can accept another execution.
    Free,
    /// The lane is currently running the specified execution.
    Running { execution_id: ExecutionId },
}

/// Lightweight lifecycle events emitted by one scheduler run.
#[derive(Debug, Clone)]
pub enum ExecutionEvent {
    /// An invocation entered the scheduler's queue.
    Queued { execution_id: ExecutionId },
    /// An invocation was assigned to a lane and started.
    Started {
        execution_id: ExecutionId,
        lane_id: LaneId,
        queued_for: Duration,
    },
    /// An invocation completed successfully.
    Completed {
        execution_id: ExecutionId,
        lane_id: LaneId,
        duration: Duration,
    },
    /// An invocation failed and released its lane.
    Failed {
        execution_id: ExecutionId,
        lane_id: LaneId,
        duration: Duration,
        error: String,
    },
}

/// Basic lifecycle information for one scheduled execution.
#[derive(Debug, Clone)]
pub struct ExecutionReport {
    /// Monotonic invocation identifier.
    pub execution_id: ExecutionId,
    /// Lane that performed the invocation.
    pub lane_id: LaneId,
    /// Host timestamp immediately before the invocation started.
    pub started: SystemTime,
    /// Time spent waiting in the scheduler queue before starting.
    pub queued_for: Duration,
    /// Time between work becoming schedulable and a lane assignment. This is
    /// the scheduler's measured Scheduling Gap; with an immediately free lane
    /// it is zero.
    pub scheduling_gap: Duration,
    /// Wall-clock time spent in the invocation.
    pub duration: Duration,
    /// Whether the invocation returned successfully.
    pub success: bool,
    /// Human-readable failure, if the invocation failed.
    pub error: Option<String>,
}

/// The value and lifecycle report produced by one scheduled execution.
#[derive(Debug)]
pub struct ExecutionResult<T> {
    pub report: ExecutionReport,
    pub value: Option<T>,
}

/// Observable scheduler state after or during a run.
#[derive(Debug, Clone)]
pub struct SchedulerSnapshot {
    pub lane_count: usize,
    pub running: usize,
    pub queued: usize,
    pub completed: usize,
    pub failed: usize,
    pub peak_active: usize,
    pub lanes: Vec<LaneState>,
}

/// Results, events, and final state from one scheduler run.
#[derive(Debug)]
pub struct SchedulerRun<T> {
    pub results: Vec<ExecutionResult<T>>,
    pub events: Vec<ExecutionEvent>,
    pub snapshot: SchedulerSnapshot,
}

struct RunState {
    queued: AtomicUsize,
    running: AtomicUsize,
    completed: AtomicUsize,
    failed: AtomicUsize,
    peak_active: AtomicUsize,
    lanes: Vec<Mutex<LaneState>>,
    events: Mutex<Vec<ExecutionEvent>>,
}

impl RunState {
    fn new(lane_count: usize, queued: usize) -> Self {
        Self {
            queued: AtomicUsize::new(queued),
            running: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
            peak_active: AtomicUsize::new(0),
            lanes: (0..lane_count)
                .map(|_| Mutex::new(LaneState::Free))
                .collect(),
            events: Mutex::new(Vec::with_capacity(queued.saturating_mul(3))),
        }
    }

    fn push_event(&self, event: ExecutionEvent) {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(event);
    }

    fn set_lane(&self, lane_id: LaneId, state: LaneState) {
        *self.lanes[lane_id.0]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = state;
    }

    fn snapshot(&self, lane_count: usize) -> SchedulerSnapshot {
        SchedulerSnapshot {
            lane_count,
            running: self.running.load(Ordering::Acquire),
            queued: self.queued.load(Ordering::Acquire),
            completed: self.completed.load(Ordering::Acquire),
            failed: self.failed.load(Ordering::Acquire),
            peak_active: self.peak_active.load(Ordering::Acquire),
            lanes: self
                .lanes
                .iter()
                .map(|lane| *lane.lock().unwrap_or_else(|poisoned| poisoned.into_inner()))
                .collect(),
        }
    }
}

/// A bounded scheduler that uses one worker thread per execution lane.
#[derive(Debug, Clone)]
pub struct PitScheduler {
    lanes: Arc<[ExecutionLane]>,
    next_execution_id: Arc<AtomicU64>,
    run_lock: Arc<Mutex<()>>,
    shared_lanes: Arc<(Mutex<Vec<bool>>, std::sync::Condvar)>,
    shared_active: Arc<AtomicUsize>,
    shared_waiting: Arc<AtomicUsize>,
    shared_peak: Arc<AtomicUsize>,
    created_at: Arc<Instant>,
    busy_nanos: Arc<AtomicU64>,
}

impl PitScheduler {
    /// Creates a scheduler with exactly `lane_count` local execution lanes.
    pub fn new(lane_count: usize) -> Result<Self> {
        if lane_count == 0 {
            bail!("execution lane count must be greater than zero");
        }

        let lanes = (0..lane_count)
            .map(|id| ExecutionLane { id: LaneId(id) })
            .collect::<Vec<_>>()
            .into();
        Ok(Self {
            lanes,
            next_execution_id: Arc::new(AtomicU64::new(0)),
            run_lock: Arc::new(Mutex::new(())),
            shared_lanes: Arc::new((
                Mutex::new(vec![false; lane_count]),
                std::sync::Condvar::new(),
            )),
            shared_active: Arc::new(AtomicUsize::new(0)),
            shared_waiting: Arc::new(AtomicUsize::new(0)),
            shared_peak: Arc::new(AtomicUsize::new(0)),
            created_at: Arc::new(Instant::now()),
            busy_nanos: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Creates a scheduler using the host's detected parallelism.
    pub fn local() -> Self {
        let lane_count = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1);
        let lanes = (0..lane_count)
            .map(|id| ExecutionLane { id: LaneId(id) })
            .collect::<Vec<_>>()
            .into();
        Self {
            lanes,
            next_execution_id: Arc::new(AtomicU64::new(0)),
            run_lock: Arc::new(Mutex::new(())),
            shared_lanes: Arc::new((
                Mutex::new(vec![false; lane_count]),
                std::sync::Condvar::new(),
            )),
            shared_active: Arc::new(AtomicUsize::new(0)),
            shared_waiting: Arc::new(AtomicUsize::new(0)),
            shared_peak: Arc::new(AtomicUsize::new(0)),
            created_at: Arc::new(Instant::now()),
            busy_nanos: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns the number of simultaneously available execution lanes.
    pub fn lane_count(&self) -> usize {
        self.lanes.len()
    }

    /// Returns the scheduler's lane descriptors.
    pub fn lanes(&self) -> &[ExecutionLane] {
        &self.lanes
    }

    /// Peak occupancy observed by the shared single-task API.
    pub fn shared_peak_active(&self) -> usize {
        self.shared_peak.load(Ordering::Acquire)
    }

    /// Current number of executions occupying shared lanes.
    pub fn shared_active(&self) -> usize {
        self.shared_active.load(Ordering::Acquire)
    }

    /// Current number of callers waiting for a shared lane.
    pub fn shared_waiting(&self) -> usize {
        self.shared_waiting.load(Ordering::Acquire)
    }

    /// Logical execution-grid utilization since this scheduler was created.
    /// This is lane busy time divided by available lane time; it is not a
    /// physical CPU utilization measurement.
    pub fn grid_utilization(&self) -> f64 {
        let elapsed_nanos = self.created_at.elapsed().as_nanos();
        if elapsed_nanos == 0 || self.lane_count() == 0 {
            return 0.0;
        }
        let capacity = elapsed_nanos.saturating_mul(self.lane_count() as u128);
        (self.busy_nanos.load(Ordering::Acquire) as f64 / capacity as f64).min(1.0)
    }

    pub fn busy_time(&self) -> Duration {
        Duration::from_nanos(self.busy_nanos.load(Ordering::Acquire))
    }

    /// A lightweight live view used by Garage heartbeats. It intentionally
    /// does not pretend that a `run_one` caller has an execution id before the
    /// caller enters the lane.
    pub fn shared_snapshot(&self) -> SchedulerSnapshot {
        let (lock, _) = &*self.shared_lanes;
        let occupied = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        SchedulerSnapshot {
            lane_count: self.lane_count(),
            running: self.shared_active(),
            queued: self.shared_waiting(),
            completed: 0,
            failed: 0,
            peak_active: self.shared_peak_active(),
            lanes: occupied
                .iter()
                .map(|used| {
                    if *used {
                        LaneState::Running {
                            execution_id: ExecutionId(0),
                        }
                    } else {
                        LaneState::Free
                    }
                })
                .collect(),
        }
    }

    /// Runs one task using the scheduler's shared lane pool. Unlike a batch,
    /// concurrent callers can occupy different lanes; this is the ingress API
    /// used by PitLane.
    pub fn run_one<T, F>(&self, task: F) -> Result<ExecutionResult<T>>
    where
        T: Send + 'static,
        F: FnOnce(ExecutionId, LaneId) -> Result<T>,
    {
        let execution_id = ExecutionId(self.next_execution_id.fetch_add(1, Ordering::Relaxed));
        let queued_at = Instant::now();
        self.shared_waiting.fetch_add(1, Ordering::AcqRel);
        let (lock, wake) = &*self.shared_lanes;
        let mut occupied = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let lane = loop {
            if let Some((index, slot)) = occupied.iter_mut().enumerate().find(|(_, used)| !**used) {
                *slot = true;
                break LaneId(index);
            }
            occupied = wake
                .wait(occupied)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        };
        self.shared_waiting.fetch_sub(1, Ordering::AcqRel);
        drop(occupied);
        let active = self.shared_active.fetch_add(1, Ordering::AcqRel) + 1;
        self.shared_peak.fetch_max(active, Ordering::AcqRel);
        let queued_for = queued_at.elapsed();
        let started = SystemTime::now();
        let timer = Instant::now();
        let outcome = task(execution_id, lane);
        let duration = timer.elapsed();
        self.busy_nanos.fetch_add(
            duration.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::AcqRel,
        );
        let (success, error, value) = match outcome {
            Ok(value) => (true, None, Some(value)),
            Err(error) => (false, Some(error.to_string()), None),
        };
        let report = ExecutionReport {
            execution_id,
            lane_id: lane,
            started,
            queued_for,
            scheduling_gap: queued_for,
            duration,
            success,
            error,
        };
        let mut occupied = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        occupied[lane.0] = false;
        self.shared_active.fetch_sub(1, Ordering::AcqRel);
        wake.notify_one();
        Ok(ExecutionResult { report, value })
    }

    /// Runs `count` jobs and returns results plus lifecycle telemetry.
    ///
    /// Excess jobs remain queued behind the atomic work cursor. A worker takes
    /// another job as soon as its previous job finishes; no busy waiting or
    /// process creation is involved. At most one worker owns a lane, so a lane
    /// cannot contain two active executions.
    pub fn run_many_detailed<T, F>(&self, count: usize, task: F) -> Result<SchedulerRun<T>>
    where
        T: Send + 'static,
        F: Fn(ExecutionId, LaneId) -> Result<T> + Send + Sync + 'static,
    {
        self.run_many_classified(count, task, |_| None)
    }

    /// Runs jobs while allowing a caller to classify a returned value as a
    /// logical failure without losing that value from the result set.
    pub fn run_many_classified<T, F, C>(
        &self,
        count: usize,
        task: F,
        classify: C,
    ) -> Result<SchedulerRun<T>>
    where
        T: Send + 'static,
        F: Fn(ExecutionId, LaneId) -> Result<T> + Send + Sync + 'static,
        C: Fn(&T) -> Option<String> + Send + Sync + 'static,
    {
        if count == 0 {
            bail!("execution count must be greater than zero");
        }

        // A scheduler owns its lanes. Serializing batches on the same scheduler
        // prevents two callers from creating overlapping workers for one lane
        // set while preserving multicore execution inside each batch.
        let _run_guard = self
            .run_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let run_state = Arc::new(RunState::new(self.lane_count(), count));
        let execution_ids: Arc<[ExecutionId]> = (0..count)
            .map(|_| ExecutionId(self.next_execution_id.fetch_add(1, Ordering::Relaxed)))
            .collect::<Vec<_>>()
            .into();
        let queued_at: Arc<[Instant]> = (0..count)
            .map(|_| Instant::now())
            .collect::<Vec<_>>()
            .into();
        for execution_id in execution_ids.iter().copied() {
            run_state.push_event(ExecutionEvent::Queued { execution_id });
        }

        let next_execution = Arc::new(AtomicUsize::new(0));
        let task = Arc::new(task);
        let classify = Arc::new(classify);
        let results = Arc::new(Mutex::new(Vec::with_capacity(count)));

        thread::scope(|scope| {
            for lane in self.lanes.iter().copied() {
                let next_execution = Arc::clone(&next_execution);
                let task = Arc::clone(&task);
                let classify = Arc::clone(&classify);
                let results = Arc::clone(&results);
                let run_state = Arc::clone(&run_state);
                let execution_ids = Arc::clone(&execution_ids);
                let queued_at = Arc::clone(&queued_at);
                scope.spawn(move || {
                    loop {
                        let work_index = next_execution.fetch_add(1, Ordering::Relaxed);
                        if work_index >= count {
                            break;
                        }

                        let execution_id = execution_ids[work_index];
                        run_state.queued.fetch_sub(1, Ordering::AcqRel);
                        run_state.set_lane(lane.id, LaneState::Running { execution_id });
                        let active = run_state.running.fetch_add(1, Ordering::AcqRel) + 1;
                        run_state.peak_active.fetch_max(active, Ordering::AcqRel);
                        let queued_for = queued_at[work_index].elapsed();
                        run_state.push_event(ExecutionEvent::Started {
                            execution_id,
                            lane_id: lane.id,
                            queued_for,
                        });

                        let started = SystemTime::now();
                        let timer = Instant::now();
                        let outcome = task(execution_id, lane.id);
                        let duration = timer.elapsed();
                        // The batch scheduler assigns the next queued item
                        // directly to the lane that just became free, so its
                        // scheduling gap is the measured queue wait.
                        let (success, error, value) = match outcome {
                            Ok(value) => match classify(&value) {
                                Some(error) => (false, Some(error), Some(value)),
                                None => (true, None, Some(value)),
                            },
                            Err(error) => (false, Some(error.to_string()), None),
                        };

                        let report = ExecutionReport {
                            execution_id,
                            lane_id: lane.id,
                            started,
                            queued_for,
                            scheduling_gap: queued_for,
                            duration,
                            success,
                            error: error.clone(),
                        };
                        self.busy_nanos.fetch_add(
                            duration.as_nanos().min(u64::MAX as u128) as u64,
                            Ordering::AcqRel,
                        );
                        if success {
                            run_state.completed.fetch_add(1, Ordering::AcqRel);
                            run_state.push_event(ExecutionEvent::Completed {
                                execution_id,
                                lane_id: lane.id,
                                duration,
                            });
                        } else {
                            run_state.failed.fetch_add(1, Ordering::AcqRel);
                            run_state.push_event(ExecutionEvent::Failed {
                                execution_id,
                                lane_id: lane.id,
                                duration,
                                error: error.unwrap_or_else(|| "unknown error".to_owned()),
                            });
                        }
                        run_state.running.fetch_sub(1, Ordering::AcqRel);
                        run_state.set_lane(lane.id, LaneState::Free);
                        tracing::debug!(
                            execution_id = execution_id.0,
                            lane_id = lane.id.0,
                            success,
                            duration_ms = duration.as_secs_f64() * 1000.0,
                            "execution completed"
                        );
                        results
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .push(ExecutionResult { report, value });
                    }
                });
            }
        });

        let mut results = Arc::try_unwrap(results)
            .map_err(|_| anyhow!("scheduler workers did not release the result collector"))?
            .into_inner()
            .map_err(|_| anyhow!("scheduler result lock is poisoned"))?;
        results.sort_unstable_by_key(|result| result.report.execution_id);
        let snapshot = run_state.snapshot(self.lane_count());
        let events = Arc::try_unwrap(run_state)
            .map_err(|_| anyhow!("scheduler workers did not release run state"))?
            .events
            .into_inner()
            .map_err(|_| anyhow!("scheduler event lock is poisoned"))?;

        Ok(SchedulerRun {
            results,
            events,
            snapshot,
        })
    }

    /// Runs jobs and returns only their execution results.
    pub fn run_many<T, F>(&self, count: usize, task: F) -> Result<Vec<ExecutionResult<T>>>
    where
        T: Send + 'static,
        F: Fn(ExecutionId, LaneId) -> Result<T> + Send + Sync + 'static,
    {
        Ok(self.run_many_detailed(count, task)?.results)
    }
}

#[cfg(test)]
mod tests {
    use super::{ExecutionEvent, ExecutionId, LaneState, PitScheduler};
    use anyhow::anyhow;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn detects_at_least_one_lane() {
        assert!(PitScheduler::local().lane_count() >= 1);
    }

    #[test]
    fn rejects_invalid_counts() {
        assert!(PitScheduler::new(0).is_err());
        assert!(
            PitScheduler::new(1)
                .expect("one lane is valid")
                .run_many::<(), _>(0, |_, _| Ok(()))
                .is_err()
        );
    }

    #[test]
    fn completes_requested_task_count_and_emits_queue_events() {
        let scheduler = PitScheduler::new(2).expect("two lanes are valid");
        let run = scheduler
            .run_many_detailed(25, |execution_id, _| Ok(execution_id))
            .expect("jobs should complete");
        assert_eq!(run.results.len(), 25);
        assert_eq!(run.snapshot.completed, 25);
        assert_eq!(run.snapshot.failed, 0);
        assert_eq!(run.snapshot.queued, 0);
        assert_eq!(run.snapshot.running, 0);
        assert!(
            run.events
                .iter()
                .filter(|event| matches!(event, ExecutionEvent::Queued { .. }))
                .count()
                == 25
        );
        assert!(
            run.snapshot
                .lanes
                .iter()
                .all(|state| *state == LaneState::Free)
        );
    }

    #[test]
    fn never_exceeds_lane_count_and_reuses_lanes() {
        let scheduler = PitScheduler::new(3).expect("three lanes are valid");
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let active_for_task = Arc::clone(&active);
        let peak_for_task = Arc::clone(&peak);
        let run = scheduler
            .run_many_detailed(18, move |_, _| {
                let current = active_for_task.fetch_add(1, Ordering::SeqCst) + 1;
                peak_for_task.fetch_max(current, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(5));
                active_for_task.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
            .expect("jobs should complete");

        assert_eq!(run.results.len(), 18);
        assert!(peak.load(Ordering::SeqCst) <= scheduler.lane_count());
        assert_eq!(peak.load(Ordering::SeqCst), scheduler.lane_count());
        assert_eq!(run.snapshot.running, 0);
        assert!(
            run.snapshot
                .lanes
                .iter()
                .all(|state| *state == LaneState::Free)
        );
    }

    #[test]
    fn failure_releases_lane_and_allows_queued_work_to_finish() {
        let scheduler = PitScheduler::new(2).expect("two lanes are valid");
        let run = scheduler
            .run_many_detailed(12, |execution_id, _| {
                if execution_id == ExecutionId(0) {
                    Err(anyhow!("intentional failure"))
                } else {
                    Ok(())
                }
            })
            .expect("scheduler should return failed results");

        assert_eq!(run.results.len(), 12);
        assert_eq!(run.snapshot.completed, 11);
        assert_eq!(run.snapshot.failed, 1);
        assert_eq!(run.snapshot.running, 0);
        assert!(
            run.snapshot
                .lanes
                .iter()
                .all(|state| *state == LaneState::Free)
        );
        assert!(run.events.iter().any(|event| matches!(
            event,
            ExecutionEvent::Failed { error, .. } if error.contains("intentional failure")
        )));
    }

    #[test]
    fn execution_ids_are_monotonic_across_runs() {
        let scheduler = PitScheduler::new(1).expect("one lane is valid");
        let first = scheduler
            .run_many(1, |execution_id, _| Ok(execution_id))
            .expect("first run should complete");
        let second = scheduler
            .run_many(1, |execution_id, _| Ok(execution_id))
            .expect("second run should complete");
        assert!(second[0].report.execution_id > first[0].report.execution_id);
    }
}
