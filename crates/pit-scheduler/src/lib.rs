//! Bounded local scheduling for independent PitFast executions.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Result, anyhow, bail};

/// A host CPU execution lane owned by a local scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionLane {
    /// Zero-based lane identifier.
    pub id: usize,
}

/// Basic lifecycle information for one scheduled execution.
#[derive(Debug, Clone)]
pub struct ExecutionReport {
    /// Zero-based invocation identifier.
    pub execution_id: usize,
    /// Lane that performed the invocation.
    pub lane_id: usize,
    /// Host timestamp immediately before the invocation started.
    pub started: SystemTime,
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

/// A bounded scheduler that uses one worker thread per execution lane.
#[derive(Debug, Clone)]
pub struct PitScheduler {
    lanes: Arc<[ExecutionLane]>,
}

impl PitScheduler {
    /// Creates a scheduler with exactly `lane_count` local execution lanes.
    pub fn new(lane_count: usize) -> Result<Self> {
        if lane_count == 0 {
            bail!("execution lane count must be greater than zero");
        }

        let lanes = (0..lane_count)
            .map(|id| ExecutionLane { id })
            .collect::<Vec<_>>()
            .into();
        Ok(Self { lanes })
    }

    /// Creates a scheduler using the host's detected parallelism.
    pub fn local() -> Self {
        let lane_count = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1);
        let lanes = (0..lane_count)
            .map(|id| ExecutionLane { id })
            .collect::<Vec<_>>()
            .into();
        Self { lanes }
    }

    /// Returns the number of simultaneously available execution lanes.
    pub fn lane_count(&self) -> usize {
        self.lanes.len()
    }

    /// Returns the scheduler's lane descriptors.
    pub fn lanes(&self) -> &[ExecutionLane] {
        &self.lanes
    }

    /// Runs `count` independent jobs, bounded by the scheduler's lane count.
    ///
    /// Excess jobs remain queued behind the atomic work cursor. A worker takes
    /// another job as soon as its previous job finishes; no busy waiting or
    /// process creation is involved.
    pub fn run_many<T, F>(&self, count: usize, task: F) -> Result<Vec<ExecutionResult<T>>>
    where
        T: Send + 'static,
        F: Fn(usize, usize) -> Result<T> + Send + Sync + 'static,
    {
        if count == 0 {
            bail!("execution count must be greater than zero");
        }

        let next_execution = Arc::new(AtomicUsize::new(0));
        let task = Arc::new(task);
        let results = Arc::new(Mutex::new(Vec::with_capacity(count)));

        thread::scope(|scope| {
            for lane in self.lanes.iter().copied() {
                let next_execution = Arc::clone(&next_execution);
                let task = Arc::clone(&task);
                let results = Arc::clone(&results);
                scope.spawn(move || {
                    loop {
                        let execution_id = next_execution.fetch_add(1, Ordering::Relaxed);
                        if execution_id >= count {
                            break;
                        }

                        let started = SystemTime::now();
                        let timer = Instant::now();
                        let outcome = task(execution_id, lane.id);
                        let duration = timer.elapsed();
                        let (success, error, value) = match outcome {
                            Ok(value) => (true, None, Some(value)),
                            Err(error) => (false, Some(error.to_string()), None),
                        };

                        let report = ExecutionReport {
                            execution_id,
                            lane_id: lane.id,
                            started,
                            duration,
                            success,
                            error,
                        };
                        tracing::debug!(
                            execution_id,
                            lane_id = lane.id,
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
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::PitScheduler;
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
    fn completes_requested_task_count() {
        let scheduler = PitScheduler::new(2).expect("two lanes are valid");
        let results = scheduler
            .run_many(25, |execution_id, _| Ok(execution_id))
            .expect("jobs should complete");
        assert_eq!(results.len(), 25);
        assert!(results.iter().all(|result| result.report.success));
    }

    #[test]
    fn never_exceeds_lane_count() {
        let scheduler = PitScheduler::new(3).expect("three lanes are valid");
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let active_for_task = Arc::clone(&active);
        let peak_for_task = Arc::clone(&peak);
        let results = scheduler
            .run_many(18, move |_, _| {
                let current = active_for_task.fetch_add(1, Ordering::SeqCst) + 1;
                peak_for_task.fetch_max(current, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(5));
                active_for_task.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
            .expect("jobs should complete");

        assert_eq!(results.len(), 18);
        assert!(peak.load(Ordering::SeqCst) <= scheduler.lane_count());
        assert!(peak.load(Ordering::SeqCst) > 1);
    }
}
