use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use pit_node::{
    CancellationToken, CompiledCacheRestoreMode, ExecutionLimits, HttpRequest, PitHttpDispatcher,
    PitRuntimeConfig, RuntimeAllocationMode, WasmArtifact,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Serialize)]
struct Sample {
    total_ns: u128,
    guest_started_ns: u128,
    prepared_lookup_ns: u128,
    queue_ns: u128,
    scheduling_gap_ns: u128,
    guest_ns: u128,
    total_us: u128,
    guest_started_us: u128,
    prepared_lookup_us: u128,
    queue_us: u128,
    scheduling_gap_us: u128,
    guest_us: u128,
    burst_guest_started_ns: Option<u128>,
    burst_guest_started_us: Option<u128>,
    request_setup_ns: u128,
    wasi_setup_ns: u128,
    store_setup_ns: u128,
    instance_setup_ns: u128,
    guest_execution_ns: u128,
    error: Option<String>,
    success: bool,
}

#[derive(Debug, Serialize)]
struct Scenario {
    metric: String,
    samples: Vec<Sample>,
    summary: Summary,
    total_summary: Summary,
    scheduling_gap_summary: Summary,
    grid_fill_time_ns: Option<u128>,
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
    dispatcher_construction_ns: u128,
    restore_mode: String,
    allocation: String,
    pooling_slots: Option<usize>,
    memory_init_cow: Option<bool>,
    pre_resolved_imports: bool,
    host: HostMetadata,
    memory_after_dispatcher: Option<MemoryObservation>,
    memory_after_preparation: Option<MemoryObservation>,
    memory_at_output: Option<MemoryObservation>,
    grid_utilization_after_run: f64,
    preparation: Option<PreparationObservation>,
    scenarios: std::collections::BTreeMap<String, Scenario>,
}

#[derive(Debug, Serialize)]
struct PreparationObservation {
    readiness: String,
    source: String,
    compile_ns: u128,
    restore_ns: u128,
    cache_publication_ns: u128,
    metadata_lookup_ns: u128,
    compile_fingerprint_ns: u128,
    artifact_digest_ns: u128,
    cache_file_read_ns: u128,
    cache_deserialize_ns: u128,
    http_linker_setup_ns: u128,
    linker_preparation_ns: u128,
    prepared_object_construction_ns: u128,
}

#[derive(Debug, Serialize)]
struct HostMetadata {
    os: String,
    kernel: Option<String>,
    arch: String,
    cpu_model: Option<String>,
    logical_cpus: usize,
    memory_total_bytes: Option<u64>,
    filesystem: Option<String>,
    pit_box_commit: Option<String>,
    wasmtime: String,
    build: String,
}

#[derive(Debug, Serialize, Clone, Copy)]
struct MemoryObservation {
    rss_bytes: u64,
    virtual_bytes: u64,
}

fn main() -> Result<()> {
    let mut artifact = None;
    let mut output = None;
    let mut phase = "before".to_owned();
    let mut lanes = 8usize;
    let mut samples = 100usize;
    let mut compiled_cache = None;
    let mut restore_mode = CompiledCacheRestoreMode::FileBacked;
    let mut pooling = false;
    let mut pooling_slots = None;
    let mut memory_init_cow = None;
    let mut pre_resolved_imports = true;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--artifact" => artifact = args.next(),
            "--output" => output = args.next(),
            "--phase" => phase = args.next().unwrap_or_else(|| "before".to_owned()),
            "--lanes" => lanes = args.next().context("--lanes requires a value")?.parse()?,
            "--samples" => samples = args.next().context("--samples requires a value")?.parse()?,
            "--compiled-cache" => compiled_cache = args.next().map(PathBuf::from),
            "--restore" => {
                restore_mode = match args.next().as_deref() {
                    Some("bytes") => CompiledCacheRestoreMode::Bytes,
                    Some("file") | Some("file-backed") => CompiledCacheRestoreMode::FileBacked,
                    Some(value) => bail!("unsupported --restore mode {value}"),
                    None => bail!("--restore requires bytes or file"),
                }
            }
            "--pooling" => pooling = true,
            "--no-pre-resolved" => pre_resolved_imports = false,
            "--pooling-slots" => {
                pooling_slots = Some(
                    args.next()
                        .context("--pooling-slots requires a value")?
                        .parse()?,
                )
            }
            "--cow" => {
                memory_init_cow = Some(match args.next().as_deref() {
                    Some("on") => true,
                    Some("off") => false,
                    Some(value) => bail!("unsupported --cow value {value}"),
                    None => bail!("--cow requires on or off"),
                })
            }
            "--help" => {
                println!(
                    "fast-path-bench --artifact FILE --output FILE [--phase before|after] [--lanes N] [--samples N] [--compiled-cache DIR] [--restore bytes|file] [--no-pre-resolved] [--pooling --pooling-slots N] [--cow on|off]"
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
    let artifact_source = WasmArtifact::from_path_with_digest(&artifact, digest.clone());
    let request = HttpRequest {
        method: "GET".to_owned(),
        path_and_query: "/hello".to_owned(),
        headers: vec![("host".to_owned(), "pitfast".to_owned())],
        body: Vec::new(),
        env: Vec::new(),
        allowed_tcp: Vec::new(),
        source_garage_id: None,
        visited_garages: Vec::new(),
        application_id: None,
        release_id: None,
        paddock: None,
        paddock_grants: Vec::new(),
        paddock_auth: None,
    };
    let runtime_config = PitRuntimeConfig {
        compiled_cache_restore: restore_mode,
        allocation: pooling_slots
            .or_else(|| pooling.then_some(lanes.saturating_add(2)))
            .map_or(RuntimeAllocationMode::OnDemand, |slots| {
                RuntimeAllocationMode::Pooling { slots }
            }),
        memory_init_cow,
        pre_resolved_imports,
    };
    let dispatcher_started = Instant::now();
    let dispatcher = Arc::new(make_dispatcher(
        lanes,
        compiled_cache.as_deref(),
        runtime_config.clone(),
    )?);
    let dispatcher_construction_ns = dispatcher_started.elapsed().as_nanos();
    let memory_after_dispatcher = process_memory();
    let initial_preparation =
        dispatcher.register_with_report(digest.clone(), artifact_source.clone())?;
    let memory_after_preparation = process_memory();
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
        let trial = make_dispatcher(lanes, trial_cache.as_deref(), runtime_config.clone())?;
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
        let restarted = make_dispatcher(lanes, Some(cache), runtime_config.clone())?;
        let restarted_preparation =
            restarted.register_with_report(digest.clone(), artifact_source.clone())?;
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
        let preparation = Some(preparation_observation(&restarted_preparation));
        let output_value = Output {
            schema_version: 2,
            phase,
            timestamp_unix_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
            artifact: artifact.display().to_string(),
            digest,
            size_bytes: bytes.len() as u64,
            lanes,
            samples_per_scenario: samples,
            dispatcher_construction_ns,
            restore_mode: format!("{restore_mode:?}"),
            allocation: format!("{:?}", runtime_config.allocation),
            pooling_slots: match runtime_config.allocation {
                RuntimeAllocationMode::Pooling { slots } => Some(slots),
                RuntimeAllocationMode::OnDemand => None,
            },
            memory_init_cow: runtime_config.memory_init_cow,
            pre_resolved_imports: runtime_config.pre_resolved_imports,
            host: host_metadata(&artifact),
            memory_after_dispatcher,
            memory_after_preparation,
            memory_at_output: process_memory(),
            grid_utilization_after_run: dispatcher.grid_utilization(),
            preparation,
            scenarios,
        };
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&output, serde_json::to_vec_pretty(&output_value)?)?;
        println!("wrote {}", output.display());
        return Ok(());
    }
    let output_value = Output {
        schema_version: 2,
        phase,
        timestamp_unix_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        artifact: artifact.display().to_string(),
        digest,
        size_bytes: bytes.len() as u64,
        lanes,
        samples_per_scenario: samples,
        dispatcher_construction_ns,
        restore_mode: format!("{restore_mode:?}"),
        allocation: format!("{:?}", runtime_config.allocation),
        pooling_slots: match runtime_config.allocation {
            RuntimeAllocationMode::Pooling { slots } => Some(slots),
            RuntimeAllocationMode::OnDemand => None,
        },
        memory_init_cow: runtime_config.memory_init_cow,
        pre_resolved_imports: runtime_config.pre_resolved_imports,
        host: host_metadata(&artifact),
        memory_after_dispatcher,
        memory_after_preparation,
        memory_at_output: process_memory(),
        grid_utilization_after_run: dispatcher.grid_utilization(),
        preparation: Some(preparation_observation(&initial_preparation)),
        scenarios,
    };
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output, serde_json::to_vec_pretty(&output_value)?)?;
    println!("wrote {}", output.display());
    Ok(())
}

fn make_dispatcher(
    lanes: usize,
    cache: Option<&std::path::Path>,
    runtime_config: PitRuntimeConfig,
) -> Result<PitHttpDispatcher> {
    match cache {
        Some(path) => PitHttpDispatcher::with_compiled_cache_options(lanes, path, runtime_config),
        None => PitHttpDispatcher::with_options(lanes, runtime_config),
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
                sample.burst_guest_started_ns =
                    Some(request_offset.as_nanos() + sample.guest_started_ns);
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
        total_ns: total.as_nanos(),
        guest_started_ns: result.guest_started_after_request.as_nanos(),
        prepared_lookup_ns: result.prepared_lookup.as_nanos(),
        queue_ns: result.queued_for.as_nanos(),
        scheduling_gap_ns: result.scheduling_gap.as_nanos(),
        guest_ns: result.duration.as_nanos(),
        total_us: total.as_micros(),
        guest_started_us: result.guest_started_after_request.as_micros(),
        prepared_lookup_us: result.prepared_lookup.as_micros(),
        queue_us: result.queued_for.as_micros(),
        scheduling_gap_us: result.scheduling_gap.as_micros(),
        guest_us: result.duration.as_micros(),
        burst_guest_started_ns: None,
        burst_guest_started_us: None,
        request_setup_ns: result.runtime_timings.request_setup.as_nanos(),
        wasi_setup_ns: result.runtime_timings.wasi_setup.as_nanos(),
        store_setup_ns: result.runtime_timings.store_setup.as_nanos(),
        instance_setup_ns: result.runtime_timings.instance_setup.as_nanos(),
        guest_execution_ns: result.runtime_timings.guest_execution.as_nanos(),
        error: result.error,
        success: result.response.is_some(),
    }
}

fn preparation_observation(report: &pit_node::PreparationReport) -> PreparationObservation {
    PreparationObservation {
        readiness: format!("{:?}", report.readiness),
        source: format!("{:?}", report.source),
        compile_ns: report.compile_duration.as_nanos(),
        restore_ns: report.restore_duration.as_nanos(),
        cache_publication_ns: report.cache_publication_duration.as_nanos(),
        metadata_lookup_ns: report.cache_metadata_lookup_duration.as_nanos(),
        compile_fingerprint_ns: report.compile_fingerprint_duration.as_nanos(),
        artifact_digest_ns: report.artifact_digest_duration.as_nanos(),
        cache_file_read_ns: report.cache_file_read_duration.as_nanos(),
        cache_deserialize_ns: report.cache_deserialize_duration.as_nanos(),
        http_linker_setup_ns: report.http_linker_setup_duration.as_nanos(),
        linker_preparation_ns: report.linker_preparation_duration.as_nanos(),
        prepared_object_construction_ns: report.prepared_object_construction_duration.as_nanos(),
    }
}

fn host_metadata(artifact: &std::path::Path) -> HostMetadata {
    let kernel = fs::read_to_string("/proc/sys/kernel/osrelease")
        .ok()
        .map(|value| value.trim().to_owned());
    let cpu_model = fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                line.contains("model name")
                    .then(|| {
                        line.split_once(':')
                            .map(|(_, value)| value.trim().to_owned())
                    })
                    .flatten()
            })
        });
    let memory_total_bytes = fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                let mut fields = line.split_whitespace();
                (fields.next() == Some("MemTotal:"))
                    .then(|| fields.next()?.parse::<u64>().ok())
                    .flatten()
                    .map(|value| value * 1024)
            })
        });
    let filesystem = std::process::Command::new("stat")
        .args(["-f", "-c", "%T"])
        .arg(artifact)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_owned());
    let pit_box_commit = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_owned());
    HostMetadata {
        os: env::consts::OS.to_owned(),
        kernel,
        arch: env::consts::ARCH.to_owned(),
        cpu_model,
        logical_cpus: thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get),
        memory_total_bytes,
        filesystem,
        pit_box_commit,
        wasmtime: "39.0.2".to_owned(),
        build: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }
        .to_owned(),
    }
}

fn process_memory() -> Option<MemoryObservation> {
    let contents = fs::read_to_string("/proc/self/status").ok()?;
    let mut rss_bytes = None;
    let mut virtual_bytes = None;
    for line in contents.lines() {
        let mut fields = line.split_whitespace();
        match fields.next() {
            Some("VmRSS:") => {
                rss_bytes = fields
                    .next()
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(|value| value * 1024)
            }
            Some("VmSize:") => {
                virtual_bytes = fields
                    .next()
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(|value| value * 1024)
            }
            _ => {}
        }
    }
    Some(MemoryObservation {
        rss_bytes: rss_bytes?,
        virtual_bytes: virtual_bytes?,
    })
}

fn scenario(samples: Vec<Sample>) -> Scenario {
    scenario_with_metric(samples, "guest_started_us", None, None)
}

fn scenario_burst(samples: Vec<Sample>, lanes: usize) -> Scenario {
    let mut starts_ns = samples
        .iter()
        .filter_map(|sample| sample.burst_guest_started_ns)
        .collect::<Vec<_>>();
    starts_ns.sort_unstable();
    let grid_fill_time_ns = (starts_ns.len() >= lanes).then(|| starts_ns[lanes - 1]);
    let grid_fill_time_us = grid_fill_time_ns.map(|value| value / 1_000);
    scenario_with_metric(
        samples,
        "guest_started_us",
        grid_fill_time_ns,
        grid_fill_time_us,
    )
}

fn scenario_with_metric(
    samples: Vec<Sample>,
    metric: &str,
    grid_fill_time_ns: Option<u128>,
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
        grid_fill_time_ns,
        grid_fill_time_us,
        samples,
    }
}

fn scenario_total(samples: Vec<Sample>) -> Scenario {
    scenario_with_metric(samples, "total_us", None, None)
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
