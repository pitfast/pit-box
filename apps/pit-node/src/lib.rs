//! CLI-independent composition of the PitFast runtime and scheduler.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Result, anyhow, bail};
use pit_artifact::{ArtifactFormat, ComponentWorld, RuntimeSpec};
use pit_runtime::{PitRuntime, PreparedArtifact, RuntimeExecutionResult};
use pit_scheduler::{
    ExecutionEvent, ExecutionId, ExecutionReport, LaneId, PitScheduler, SchedulerRun,
    SchedulerSnapshot,
};

pub use pit_runtime::{
    ArtifactId, ArtifactSource, CancellationToken, ExecutionLimits, ExecutionRequest,
    ExecutionStatus, HttpRequest, HttpResponse, WasmArtifact, validate_env_entry,
};

/// Validate a project artifact's runtime contract against this local PitBox.
pub fn validate_runtime_spec(spec: &RuntimeSpec) -> Result<()> {
    if !spec.abi.is_supported() {
        bail!(
            "unsupported runtime ABI '{}'; this PitBox supports wasi-preview1 and wasi-preview2",
            spec.abi.as_str()
        );
    }
    let expected = match spec.abi.as_str() {
        "wasi-preview1" => (
            ArtifactFormat::CoreModule,
            pit_artifact::WASI_PREVIEW1_ENTRYPOINT,
        ),
        "wasi-preview2" => (
            ArtifactFormat::Component,
            match spec.world {
                Some(ComponentWorld::WasiHttpProxy) => pit_artifact::WASI_HTTP_PROXY_WORLD,
                _ => pit_artifact::WASI_PREVIEW2_ENTRYPOINT,
            },
        ),
        _ => unreachable!(),
    };
    if spec.format != expected.0 {
        bail!(
            "runtime ABI '{}' requires the {} artifact format",
            spec.abi.as_str(),
            expected.0
        );
    }
    if spec.entrypoint.as_str() != expected.1 {
        bail!(
            "unsupported entrypoint '{}'; expected {}",
            spec.entrypoint.as_str(),
            expected.1
        );
    }
    Ok(())
}

/// One isolated execution result with scheduler and runtime identity.
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub execution_id: ExecutionId,
    pub lane_id: LaneId,
    pub status: ExecutionStatus,
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub started: SystemTime,
    pub queued_for: Duration,
    pub duration: Duration,
    pub error: Option<String>,
}

/// Aggregate result from one node batch.
pub struct NodeRunReport {
    pub artifact_path: PathBuf,
    pub execution_lanes: usize,
    pub requested: usize,
    pub completed: usize,
    pub failed: usize,
    pub timed_out: usize,
    pub cancelled: usize,
    pub memory_limit_exceeded: usize,
    pub total_duration: Duration,
    pub peak_active: usize,
    pub snapshot: SchedulerSnapshot,
    pub events: Vec<ExecutionEvent>,
    pub executions: Vec<ExecutionResult>,
}

/// A local PitFast node with one prepared artifact and bounded scheduler.
pub struct PitNode {
    prepared: Arc<PreparedArtifact>,
    scheduler: PitScheduler,
}

/// Shared prepared-artifact HTTP dispatcher. Services registered by PitLane use
/// one scheduler, so all HTTP components consume the same execution grid.
pub struct PitHttpDispatcher {
    runtime: PitRuntime,
    scheduler: PitScheduler,
    artifacts: std::collections::HashMap<String, Arc<PreparedArtifact>>,
}

#[derive(Debug)]
pub struct HttpDispatchResult {
    pub execution_id: ExecutionId,
    pub lane_id: LaneId,
    pub response: Option<HttpResponse>,
    pub error: Option<String>,
    pub queued_for: Duration,
    pub duration: Duration,
}

impl PitHttpDispatcher {
    pub fn new() -> Result<Self> {
        Ok(Self {
            runtime: PitRuntime::new()?,
            scheduler: PitScheduler::local(),
            artifacts: std::collections::HashMap::new(),
        })
    }

    pub fn execution_lanes(&self) -> usize {
        self.scheduler.lane_count()
    }

    pub fn shared_peak_active(&self) -> usize {
        self.scheduler.shared_peak_active()
    }

    pub fn register(&mut self, key: impl Into<String>, artifact: WasmArtifact) -> Result<()> {
        let prepared = self.runtime.prepare(artifact)?;
        self.artifacts.insert(key.into(), Arc::new(prepared));
        Ok(())
    }

    pub fn execute_http(
        &self,
        key: &str,
        request: HttpRequest,
        limits: ExecutionLimits,
        cancellation: CancellationToken,
    ) -> Result<HttpDispatchResult> {
        let prepared = self
            .artifacts
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow!("unknown prepared HTTP artifact '{key}'"))?;
        let request = Arc::new(request);
        let scheduled = self.scheduler.run_one(move |_, _| {
            prepared.execute_http(&request, cancellation.clone(), limits.clone())
        })?;
        let report = scheduled.report;
        Ok(HttpDispatchResult {
            execution_id: report.execution_id,
            lane_id: report.lane_id,
            response: scheduled.value,
            error: report.error,
            queued_for: report.queued_for,
            duration: report.duration,
        })
    }
}

impl PitNode {
    /// Prepares an artifact once and detects local execution lanes.
    pub fn from_artifact(artifact: WasmArtifact) -> Result<Self> {
        let runtime = PitRuntime::new()?;
        let prepared = runtime.prepare(artifact)?;
        Ok(Self {
            prepared: Arc::new(prepared),
            scheduler: PitScheduler::local(),
        })
    }

    /// Convenience path-based constructor.
    pub fn from_file(wasm_path: impl AsRef<Path>) -> Result<Self> {
        Self::from_artifact(WasmArtifact::from_path(wasm_path))
    }

    pub fn artifact(&self) -> &WasmArtifact {
        self.prepared.artifact()
    }

    pub fn default_entrypoint(&self) -> &'static str {
        self.prepared.default_entrypoint()
    }

    pub fn measure_instantiation(&self, request: &ExecutionRequest) -> Result<Duration> {
        self.prepared.measure_instantiation(request)
    }

    pub fn execution_lanes(&self) -> usize {
        self.scheduler.lane_count()
    }

    /// Executes one request without exposing scheduler or Wasmtime internals.
    pub fn execute(&self, request: ExecutionRequest) -> Result<ExecutionResult> {
        self.execute_with_cancellation(request, CancellationToken::new())
    }

    /// Executes one request with a caller-controlled cancellation token.
    pub fn execute_with_cancellation(
        &self,
        request: ExecutionRequest,
        cancellation: CancellationToken,
    ) -> Result<ExecutionResult> {
        let report = self.execute_many_with_cancellation(request, 1, cancellation)?;
        report
            .executions
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("single execution returned no result"))
    }

    /// Schedules independent executions using a fresh cancellation token.
    pub fn execute_many(&self, request: ExecutionRequest, count: usize) -> Result<NodeRunReport> {
        self.execute_many_with_cancellation(request, count, CancellationToken::new())
    }

    /// Schedules independent executions sharing a cancellation token.
    pub fn execute_many_with_cancellation(
        &self,
        request: ExecutionRequest,
        count: usize,
        cancellation: CancellationToken,
    ) -> Result<NodeRunReport> {
        request.validate()?;
        if request.artifact.id() != self.prepared.artifact().id() {
            bail!(
                "execution request artifact '{}' does not match prepared artifact '{}'",
                request.artifact.id(),
                self.prepared.artifact().id()
            );
        }

        let started = Instant::now();
        let prepared = Arc::clone(&self.prepared);
        let request = Arc::new(request);
        let SchedulerRun {
            results,
            events,
            snapshot,
        } = self.scheduler.run_many_classified(
            count,
            move |_, _| prepared.execute(request.as_ref(), cancellation.clone()),
            |runtime| {
                (runtime.status != ExecutionStatus::Completed).then(|| {
                    runtime
                        .error
                        .clone()
                        .unwrap_or_else(|| runtime.status.to_string())
                })
            },
        )?;

        let executions = results
            .into_iter()
            .map(|scheduled| {
                let report = scheduled.report;
                match scheduled.value {
                    Some(runtime) => Self::execution_result(report, runtime),
                    None => ExecutionResult {
                        execution_id: report.execution_id,
                        lane_id: report.lane_id,
                        status: ExecutionStatus::RuntimeError,
                        exit_code: None,
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                        started: report.started,
                        queued_for: report.queued_for,
                        duration: report.duration,
                        error: report.error,
                    },
                }
            })
            .collect::<Vec<_>>();

        let completed = executions
            .iter()
            .filter(|execution| execution.status == ExecutionStatus::Completed)
            .count();
        let failed = executions
            .iter()
            .filter(|execution| {
                matches!(
                    execution.status,
                    ExecutionStatus::GuestExitNonZero
                        | ExecutionStatus::GuestTrap
                        | ExecutionStatus::MemoryLimitExceeded
                        | ExecutionStatus::RuntimeError
                )
            })
            .count();
        let timed_out = executions
            .iter()
            .filter(|execution| execution.status == ExecutionStatus::TimedOut)
            .count();
        let cancelled = executions
            .iter()
            .filter(|execution| execution.status == ExecutionStatus::Cancelled)
            .count();
        let memory_limit_exceeded = executions
            .iter()
            .filter(|execution| execution.status == ExecutionStatus::MemoryLimitExceeded)
            .count();
        let artifact_path = match self.prepared.artifact().source() {
            pit_runtime::ArtifactSource::Path(path) => path.clone(),
            pit_runtime::ArtifactSource::Bytes(_) => PathBuf::from("<in-memory-artifact>"),
        };

        Ok(NodeRunReport {
            artifact_path,
            execution_lanes: self.scheduler.lane_count(),
            requested: count,
            completed,
            failed,
            timed_out,
            cancelled,
            memory_limit_exceeded,
            total_duration: started.elapsed(),
            peak_active: snapshot.peak_active,
            snapshot,
            events,
            executions,
        })
    }

    /// Backward-compatible convenience for default requests.
    pub fn run(&self, concurrency: usize) -> Result<NodeRunReport> {
        self.execute_many(
            ExecutionRequest::new(self.prepared.artifact().clone()),
            concurrency,
        )
    }

    fn execution_result(
        report: ExecutionReport,
        runtime: RuntimeExecutionResult,
    ) -> ExecutionResult {
        ExecutionResult {
            execution_id: report.execution_id,
            lane_id: report.lane_id,
            status: runtime.status,
            exit_code: runtime.exit_code,
            stdout: runtime.stdout,
            stderr: runtime.stderr,
            started: runtime.started,
            queued_for: report.queued_for,
            duration: report.duration,
            error: runtime.error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CancellationToken, ExecutionRequest, ExecutionStatus, PitNode, WasmArtifact};
    use pit_scheduler::LaneState;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn cancellation_returns_status_and_releases_lane() {
        let bytes = wat::parse_str(r#"(module (func (export "_start") (loop br 0)))"#)
            .expect("test WAT must compile");
        let artifact = WasmArtifact::from_bytes(bytes);
        let node = Arc::new(PitNode::from_artifact(artifact.clone()).expect("node should prepare"));
        let token = CancellationToken::new();
        let worker_token = token.clone();
        let worker_node = Arc::clone(&node);
        let request = ExecutionRequest::new(artifact);
        let handle = thread::spawn(move || {
            worker_node.execute_many_with_cancellation(request, 1, worker_token)
        });

        thread::sleep(Duration::from_millis(25));
        token.cancel();
        let report = match handle.join() {
            Ok(Ok(report)) => report,
            Ok(Err(error)) => panic!("cancellation execution failed: {error}"),
            Err(_) => panic!("cancellation worker panicked"),
        };
        assert_eq!(report.executions[0].status, ExecutionStatus::Cancelled);
        assert_eq!(report.snapshot.failed, 1);
        assert_eq!(report.snapshot.running, 0);
        assert_eq!(report.snapshot.queued, 0);
        assert!(
            report
                .snapshot
                .lanes
                .iter()
                .all(|state| *state == LaneState::Free)
        );
    }
}
