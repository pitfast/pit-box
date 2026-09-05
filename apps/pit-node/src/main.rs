use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::Parser;
use pit_node::{ExecutionStatus, PitNode};

#[derive(Debug, Parser)]
#[command(name = "pit-node", about = "Run a local PitFast execution node")]
struct Args {
    /// WASI Preview 1 WebAssembly module to execute.
    wasm_file: PathBuf,

    /// Number of independent invocations to schedule.
    #[arg(long, default_value_t = 1)]
    concurrency: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let node = PitNode::from_file(&args.wasm_file)?;
    let report = node.run(args.concurrency)?;
    println!(
        "pit-node completed {} of {} execution(s) on {} lane(s)",
        report.completed, report.requested, report.execution_lanes
    );
    if report.completed != report.requested {
        for execution in report
            .executions
            .iter()
            .filter(|execution| execution.status != ExecutionStatus::Completed)
        {
            eprintln!(
                "execution {} failed on lane {}: status={}, error={}",
                execution.execution_id,
                execution.lane_id,
                execution.status,
                execution.error.as_deref().unwrap_or("unknown error")
            );
        }
        bail!("one or more WASM executions failed");
    }
    Ok(())
}
