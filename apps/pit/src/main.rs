use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use pit_node::{NodeRunReport, PitNode};
use pit_scheduler::{ExecutionEvent, ExecutionReport, PitScheduler};

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
        /// Path to the WASM module.
        wasm_file: PathBuf,

        /// Number of independent invocations.
        #[arg(long, default_value_t = 1)]
        concurrency: usize,

        /// Print queued, started, and completed execution events.
        #[arg(short, long)]
        verbose: bool,
    },
    /// Run a release-oriented local concurrency benchmark.
    Bench {
        /// Path to the WASM module.
        wasm_file: PathBuf,
    },
    /// Display local hardware and scheduler information.
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
            verbose,
        } => run(wasm_file, concurrency, verbose),
        Command::Bench { wasm_file } => benchmark(wasm_file),
    }
}

fn print_system() -> Result<()> {
    let lanes = PitScheduler::local().lane_count();
    println!("PitFast Local System");
    println!();
    println!("Architecture: {}", std::env::consts::ARCH);
    println!("Execution lanes: {lanes}");
    println!("Parallelism source: std::thread::available_parallelism()");
    Ok(())
}

fn run(wasm_file: PathBuf, concurrency: usize, verbose: bool) -> Result<()> {
    if concurrency == 0 {
        bail!("concurrency must be greater than zero");
    }

    let node = PitNode::from_file(&wasm_file)?;
    let report = node.run(concurrency)?;
    if verbose {
        print_events(&report.events);
        println!();
    }
    print_run_summary(&report);
    if report.failed > 0 {
        print_failures(&report.executions);
        bail!("one or more WASM executions failed");
    }
    Ok(())
}

fn benchmark(wasm_file: PathBuf) -> Result<()> {
    let node = PitNode::from_file(&wasm_file)?;
    let levels = benchmark_levels(node.execution_lanes());

    println!("PitFast Benchmark");
    println!();
    println!("Component:");
    println!("  {}", wasm_file.display());
    println!();
    println!("Execution lanes:");
    println!("  {}", node.execution_lanes());
    println!();
    println!(
        "{:<12} {:>10} {:>14} {:>10} {:>10} {:>10} {:>10} {:>6}",
        "Concurrency", "Total", "Throughput", "Mean", "p50", "p95", "p99", "Peak"
    );

    for concurrency in levels {
        let report = node.run(concurrency)?;
        let timing = TimingSummary::from_reports(&report.executions);
        println!(
            "{:<12} {:>10} {:>13.2}/s {:>9} {:>9} {:>9} {:>9} {:>6}",
            concurrency,
            format_duration(report.total_duration),
            timing.throughput(concurrency, report.total_duration),
            format_duration(timing.mean),
            format_duration(timing.p50),
            format_duration(timing.p95),
            format_duration(timing.p99),
            report.peak_active,
        );
        if report.failed > 0 {
            print_failures(&report.executions);
            bail!("benchmark execution failed at concurrency {concurrency}");
        }
    }

    Ok(())
}

fn benchmark_levels(lanes: usize) -> Vec<usize> {
    let mut levels = vec![1];
    let mut level = 2;
    while level < lanes {
        levels.push(level);
        level = level.saturating_mul(2);
        if level == usize::MAX {
            break;
        }
    }
    if !levels.contains(&lanes) {
        levels.push(lanes);
    }
    let oversubscribed = lanes.saturating_mul(2).max(lanes.saturating_add(1));
    if !levels.contains(&oversubscribed) {
        levels.push(oversubscribed);
    }
    levels
}

fn print_run_summary(report: &NodeRunReport) {
    let timing = TimingSummary::from_reports(&report.executions);
    println!("PitFast Pit Box");
    println!();
    println!("Component: {}", report.artifact_path.display());
    println!();
    println!("Execution Grid");
    println!("  Lanes: {}", report.execution_lanes);
    println!();
    println!("Executions");
    println!("  Requested: {}", report.requested);
    println!("  Completed: {}", report.completed);
    println!("  Failed: {}", report.failed);
    println!("  Peak Active: {}", report.peak_active);
    println!();
    println!("Timing");
    println!("  Total: {}", format_duration(report.total_duration));
    println!("  Mean: {}", format_duration(timing.mean));
    println!("  p50: {}", format_duration(timing.p50));
    println!("  p95: {}", format_duration(timing.p95));
    println!("  p99: {}", format_duration(timing.p99));
}

fn print_events(events: &[ExecutionEvent]) {
    for event in events {
        match event {
            ExecutionEvent::Queued { execution_id } => {
                println!("BOX {execution_id}  QUEUED");
            }
            ExecutionEvent::Started {
                execution_id,
                lane_id,
                queued_for,
            } => {
                println!(
                    "BOX {execution_id}  LANE {lane_id}  START  queue {}",
                    format_duration(*queued_for)
                );
            }
            ExecutionEvent::Completed {
                execution_id,
                lane_id,
                duration,
            } => {
                println!(
                    "BOX {execution_id}  LANE {lane_id}  DONE   {}",
                    format_duration(*duration)
                );
            }
            ExecutionEvent::Failed {
                execution_id,
                lane_id,
                duration,
                error,
            } => {
                println!(
                    "BOX {execution_id}  LANE {lane_id}  FAIL   {}  {error}",
                    format_duration(*duration)
                );
            }
        }
    }
}

fn print_failures(executions: &[ExecutionReport]) {
    for execution in executions
        .iter()
        .filter(|execution| !execution.success)
        .take(5)
    {
        eprintln!(
            "execution {} failed on lane {}: {}",
            execution.execution_id,
            execution.lane_id,
            execution.error.as_deref().unwrap_or("unknown error")
        );
    }
}

#[derive(Debug, Clone, Copy)]
struct TimingSummary {
    mean: Duration,
    p50: Duration,
    p95: Duration,
    p99: Duration,
}

impl TimingSummary {
    fn from_reports(reports: &[ExecutionReport]) -> Self {
        let mut durations = reports
            .iter()
            .map(|report| report.duration)
            .collect::<Vec<_>>();
        durations.sort_unstable();
        let total_nanos = durations.iter().map(Duration::as_nanos).sum::<u128>();
        let mean = Duration::from_nanos(
            u64::try_from(total_nanos / durations.len() as u128).unwrap_or(u64::MAX),
        );
        Self {
            mean,
            p50: percentile(&durations, 50),
            p95: percentile(&durations, 95),
            p99: percentile(&durations, 99),
        }
    }

    fn throughput(self, executions: usize, total: Duration) -> f64 {
        executions as f64 / total.as_secs_f64()
    }
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = percentile.saturating_mul(sorted.len()).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn format_duration(duration: Duration) -> String {
    let millis = duration.as_secs_f64() * 1000.0;
    if millis < 1000.0 {
        format!("{millis:.1}ms")
    } else {
        format!("{:.3}s", duration.as_secs_f64())
    }
}
