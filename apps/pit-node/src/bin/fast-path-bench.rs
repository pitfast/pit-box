use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use pit_node::{CancellationToken, ExecutionLimits, HttpRequest, PitHttpDispatcher, WasmArtifact};
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Serialize)]
struct Sample {
    total_us: u128,
    guest_started_us: u128,
    prepared_lookup_us: u128,
    queue_us: u128,
    scheduling_gap_us: u128,
    guest_us: u128,
    burst_guest_started_us: Option<u128>,
    success: bool,
}

#[derive(Debug, Serialize)]
struct Scenario {
    metric: String,
    samples: Vec<Sample>,
    summary: Summary,
    total_summary: Summary,
    scheduling_gap_summary: Summary,
    grid_fill_time_us: Option<u128>,
}

#[derive(Debug, Serialize, Default)]
struct Summary {
    count: usize,
    min_us: u128,
    mean_us: f64,
    p50_us: u128,
    p90_us: u128,
    p95_us: u128,
    p99_us: u128,
    max_us: u128,
    stddev_us: f64,
}

#[derive(Debug, Serialize)]
struct Output {
    schema_version: u32,
    phase: String,
    timestamp_unix_ms: u128,
    artifact: String,
    digest: String,
    size_bytes: u64,
    lanes: usize,
    samples_per_scenario: usize,
    host: HostMetadata,
    grid_utilization_after_run: f64,
    scenarios: std::collections::BTreeMap<String, Scenario>,
}

#[derive(Debug, Serialize)]
struct HostMetadata {
    os: String,
    arch: String,
    logical_cpus: usize,
    wasmtime: String,
    build: String,
}

fn main() -> Result<()> {
    let mut artifact = None;
    let mut output = None;
    let mut phase = "before".to_owned();
    let mut lanes = 8usize;
    let mut samples = 100usize;
    let mut compiled_cache = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--artifact" => artifact = args.next(),
            "--output" => output = args.next(),
            "--phase" => phase = args.next().unwrap_or_else(|| "before".to_owned()),
            "--lanes" => lanes = args.next().context("--lanes requires a value")?.parse()?,
            "--samples" => samples = args.next().context("--samples requires a value")?.parse()?,
            "--compiled-cache" => compiled_cache = args.next().map(PathBuf::from),
            "--help" => {
                println!(
                    "fast-path-bench --artifact FILE --output FILE [--phase before|after] [--lanes N] [--samples N] [--compiled-cache DIR]"
                );
                return Ok(());
            }
            other => bail!("unknown argument {other}"),
        }
    }
    let artifact = PathBuf::from(artifact.context("--artifact is required")?);
    let output = PathBuf::from(output.context("--output is required")?);
    let bytes = fs::read(&artifact)?;
    let digest = format!("sha256:{:x}", Sha256::digest(&bytes));
    let artifact_source = WasmArtifact::from_path(&artifact);
    let request = HttpRequest {
        method: "GET".to_owned(),
        path_and_query: "/hello".to_owned(),
        headers: Vec::new(),
        body: Vec::new(),
        env: Vec::new(),
        allowed_tcp: Vec::new(),
        source_garage_id: None,
        visited_garages: Vec::new(),
    };
    let dispatcher = Arc::new(make_dispatcher(lanes, compiled_cache.as_deref())?);
    dispatcher.register(digest.clone(), artifact_source.clone())?;
    let hot = collect_samples(Arc::clone(&dispatcher), &digest, &request, samples, 1)?;
    let burst = collect_samples(
        Arc::clone(&dispatcher),
        &digest,
        &request,
        samples.max(lanes * 4),
        lanes,
    )?;
    let cold_count = samples.clamp(10, 30);
    let mut cold = Vec::with_capacity(cold_count);
    for trial_index in 0..cold_count {
        let trial_started = Instant::now();
        let trial_cache = compiled_cache
            .as_ref()
            .map(|root| root.join(format!("cold-{trial_index}")));
        let trial = make_dispatcher(lanes, trial_cache.as_deref())?;
        trial.register(digest.clone(), artifact_source.clone())?;
        let result = trial.execute_http(
            &digest,
            request.clone(),
            ExecutionLimits::default(),
            CancellationToken::new(),
        );
        if let Some(cache) = &trial_cache {
            // Cold trials deliberately use isolated cache roots. They are
            // measurement fixtures, not retained benchmark state.
            let _ = fs::remove_dir_all(cache);
        }
        cold.push(sample_from_result(trial_started.elapsed(), result?));
    }
    let mut scenarios = std::collections::BTreeMap::new();
    scenarios.insert("hot_memory".to_owned(), scenario(hot));
    scenarios.insert("cold_local_wasm".to_owned(), scenario_total(cold));
    scenarios.insert("burst_hot".to_owned(), scenario_burst(burst, lanes));
    if let Some(cache) = compiled_cache.as_deref() {
        let restart_started = Instant::now();
        let restarted = make_dispatcher(lanes, Some(cache))?;
        restarted.register(digest.clone(), artifact_source.clone())?;
        let result = restarted.execute_http(
            &digest,
            request,
            ExecutionLimits::default(),
            CancellationToken::new(),
        )?;
        scenarios.insert(
            "warm_restore".to_owned(),
            scenario_total(vec![sample_from_result(restart_started.elapsed(), result)]),
        );
    }
    let output_value = Output {
        schema_version: 1,
        phase,
        timestamp_unix_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        artifact: artifact.display().to_string(),
        digest,
        size_bytes: bytes.len() as u64,
        lanes,
        samples_per_scenario: samples,
        host: HostMetadata {
            os: env::consts::OS.to_owned(),
            arch: env::consts::ARCH.to_owned(),
            logical_cpus: thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get),
            wasmtime: "39.0.2".to_owned(),
            build: if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            }
            .to_owned(),
        },
        grid_utilization_after_run: dispatcher.grid_utilization(),
        scenarios,
    };
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output, serde_json::to_vec_pretty(&output_value)?)?;
    println!("wrote {}", output.display());
    Ok(())
}

fn make_dispatcher(lanes: usize, cache: Option<&std::path::Path>) -> Result<PitHttpDispatcher> {
    match cache {
        Some(path) => PitHttpDispatcher::with_compiled_cache(lanes, path),
        None => PitHttpDispatcher::with_lanes(lanes),
    }
}

fn collect_samples(
    dispatcher: Arc<PitHttpDispatcher>,
    key: &str,
    request: &HttpRequest,
    count: usize,
    concurrency: usize,
) -> Result<Vec<Sample>> {
    if concurrency <= 1 {
        return (0..count)
            .map(|_| {
                let started = Instant::now();
                let result = dispatcher.execute_http(
                    key,
                    request.clone(),
                    ExecutionLimits::default(),
                    CancellationToken::new(),
                )?;
                Ok(sample_from_result(started.elapsed(), result))
            })
            .collect();
    }
    let mut results = Vec::with_capacity(count);
    for batch_start in (0..count).step_by(concurrency) {
        let batch_size = (count - batch_start).min(concurrency);
        let barrier = Arc::new(Barrier::new(batch_size));
        let burst_started = Instant::now();
        let mut handles = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            let dispatcher = Arc::clone(&dispatcher);
            let request = request.clone();
            let key = key.to_owned();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || -> Result<Sample> {
                barrier.wait();
                let request_offset = burst_started.elapsed();
                let started = Instant::now();
                let result = dispatcher.execute_http(
                    &key,
                    request,
                    ExecutionLimits::default(),
                    CancellationToken::new(),
                )?;
                let mut sample = sample_from_result(started.elapsed(), result);
                sample.burst_guest_started_us =
                    Some(request_offset.as_micros() + sample.guest_started_us);
                Ok(sample)
            }));
        }
        results.extend(
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .map_err(|_| anyhow::anyhow!("benchmark worker panicked"))?
                })
                .collect::<Result<Vec<_>>>()?,
        );
    }
    Ok(results)
}

fn sample_from_result(total: Duration, result: pit_node::HttpDispatchResult) -> Sample {
    Sample {
        total_us: total.as_micros(),
        guest_started_us: result.guest_started_after_request.as_micros(),
        prepared_lookup_us: result.prepared_lookup.as_micros(),
        queue_us: result.queued_for.as_micros(),
        scheduling_gap_us: result.scheduling_gap.as_micros(),
        guest_us: result.duration.as_micros(),
        burst_guest_started_us: None,
        success: result.response.is_some(),
    }
}

fn scenario(samples: Vec<Sample>) -> Scenario {
    scenario_with_metric(samples, "guest_started_us", None)
}

fn scenario_burst(samples: Vec<Sample>, lanes: usize) -> Scenario {
    let mut starts = samples
        .iter()
        .filter_map(|sample| sample.burst_guest_started_us)
        .collect::<Vec<_>>();
    starts.sort_unstable();
    let grid_fill_time_us = (starts.len() >= lanes).then(|| starts[lanes - 1]);
    scenario_with_metric(samples, "guest_started_us", grid_fill_time_us)
}

fn scenario_with_metric(
    samples: Vec<Sample>,
    metric: &str,
    grid_fill_time_us: Option<u128>,
) -> Scenario {
    let values = samples
        .iter()
        .map(|sample| sample.guest_started_us)
        .collect::<Vec<_>>();
    let total_values = samples
        .iter()
        .map(|sample| sample.total_us)
        .collect::<Vec<_>>();
    let scheduling_gap_values = samples
        .iter()
        .map(|sample| sample.scheduling_gap_us)
        .collect::<Vec<_>>();
    Scenario {
        metric: metric.to_owned(),
        summary: summary(&values),
        total_summary: summary(&total_values),
        scheduling_gap_summary: summary(&scheduling_gap_values),
        grid_fill_time_us,
        samples,
    }
}

fn scenario_total(samples: Vec<Sample>) -> Scenario {
    scenario_with_metric(samples, "total_us", None)
}

fn summary(values: &[u128]) -> Summary {
    if values.is_empty() {
        return Summary::default();
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let mean = sorted.iter().sum::<u128>() as f64 / sorted.len() as f64;
    let variance = sorted
        .iter()
        .map(|value| (*value as f64 - mean).powi(2))
        .sum::<f64>()
        / sorted.len() as f64;
    Summary {
        count: sorted.len(),
        min_us: sorted[0],
        mean_us: mean,
        p50_us: percentile(&sorted, 50),
        p90_us: percentile(&sorted, 90),
        p95_us: percentile(&sorted, 95),
        p99_us: percentile(&sorted, 99),
        max_us: *sorted.last().unwrap(),
        stddev_us: variance.sqrt(),
    }
}

fn percentile(values: &[u128], percentile: usize) -> u128 {
    let index = (values.len() * percentile).div_ceil(100).saturating_sub(1);
    values[index.min(values.len() - 1)]
}
