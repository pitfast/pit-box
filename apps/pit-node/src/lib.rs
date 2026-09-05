//! Composition of the local PitFast runtime and scheduler.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use pit_runtime::PitRuntime;
use pit_scheduler::{ExecutionReport, PitScheduler};

/// A local PitFast node: one compiled artifact and a bounded execution scheduler.
pub struct PitNode {
    runtime: Arc<PitRuntime>,
    scheduler: PitScheduler,
}

/// Aggregate result from a node run.
pub struct NodeRunReport {
    pub artifact_path: PathBuf,
    pub execution_lanes: usize,
    pub requested: usize,
    pub completed: usize,
    pub failed: usize,
    pub total_duration: Duration,
    pub executions: Vec<ExecutionReport>,
}

impl PitNode {
    /// Loads and compiles `wasm_path`, and detects the local execution lanes.
    pub fn from_file(wasm_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            runtime: Arc::new(PitRuntime::from_file(wasm_path)?),
            scheduler: PitScheduler::local(),
        })
    }

    /// Returns the number of execution lanes available to this node.
    pub fn execution_lanes(&self) -> usize {
        self.scheduler.lane_count()
    }

    /// Schedules `concurrency` independent invocations of the compiled artifact.
    pub fn run(&self, concurrency: usize) -> Result<NodeRunReport> {
        let started = Instant::now();
        let runtime = Arc::clone(&self.runtime);
        let results = self
            .scheduler
            .run_many(concurrency, move |_, _| runtime.run_once())?;
        let completed = results
            .iter()
            .filter(|result| result.report.success)
            .count();
        let failed = results.len() - completed;
        let executions = results.into_iter().map(|result| result.report).collect();

        Ok(NodeRunReport {
            artifact_path: self.runtime.artifact_path().to_path_buf(),
            execution_lanes: self.scheduler.lane_count(),
            requested: concurrency,
            completed,
            failed,
            total_duration: started.elapsed(),
            executions,
        })
    }
}
