use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use pit_node::PitNode;
use pit_scheduler::PitScheduler;

#[derive(Debug, Parser)]
#[command(name = "pit", about = "PitFast local WebAssembly execution")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run independent invocations of a WASI Preview 1 WebAssembly module.
    Run {
        /// Path to the `.wasm` module.
        wasm_file: PathBuf,

        /// Number of independent invocations.
        #[arg(long, default_value_t = 1)]
        concurrency: usize,
    },
    /// Display local hardware information used by the scheduler.
    System,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_target(false)
        .without_time()
        .init();

    match Cli::parse().command {
        Command::System => print_system(),
        Command::Run {
            wasm_file,
            concurrency,
        } => run(wasm_file, concurrency),
    }
}

fn print_system() -> Result<()> {
    let lanes = PitScheduler::local().lane_count();
    println!("PitFast Local System");
    println!();
    println!("CPU execution lanes: {lanes}");
    println!("Architecture: {}", std::env::consts::ARCH);
    Ok(())
}

fn run(wasm_file: PathBuf, concurrency: usize) -> Result<()> {
    if concurrency == 0 {
        bail!("concurrency must be greater than zero");
    }

    let node = PitNode::from_file(&wasm_file)?;
    let report = node.run(concurrency)?;
    println!("PitFast Pit Box");
    println!();
    println!("WASM module: {}", report.artifact_path.display());
    println!("Executions: {}", report.requested);
    println!("Execution lanes: {}", report.execution_lanes);
    println!();
    println!("Completed: {}", report.completed);
    println!("Failed: {}", report.failed);
    println!("Total time: {:.3}s", report.total_duration.as_secs_f64());

    if report.failed > 0 {
        for execution in report
            .executions
            .iter()
            .filter(|execution| !execution.success)
        {
            eprintln!(
                "execution {} failed on lane {}: {}",
                execution.execution_id,
                execution.lane_id,
                execution.error.as_deref().unwrap_or("unknown error")
            );
        }
        bail!("one or more WASM executions failed");
    }
    Ok(())
}
