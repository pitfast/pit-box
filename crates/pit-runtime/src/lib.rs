//! Reusable, constrained WASI Preview 1 and Preview 2 execution for PitFast.
//!
//! The runtime owns one shared Wasmtime engine, prepares artifacts once, and
//! creates a fresh Store and WASI context for every invocation.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context as TaskContext, Poll};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use pit_artifact::ArtifactFormat;
use pit_lane_core::{PitUri, ServiceInvocationRequest, ServiceInvoker};
use pit_paddock_core::{
    NamespaceId, ObjectKey, ObjectMetadata, ObjectRef, ObjectWriter, PaddockGatewayAuth,
    PaddockGrant, PaddockObjectBackend,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::{Date, Month, Time as ClockTime};
use tokio::io::AsyncWrite;
use wasmparser::{Encoding, Parser, Payload};
use wasmtime::component::{Component, Linker as ComponentLinker, ResourceTable};
use wasmtime::{
    Config, Engine, InstanceAllocationStrategy, Linker as CoreLinker, Module,
    PoolingAllocationConfig, ResourceLimiter, Store, UpdateDeadline,
};
use wasmtime_wasi::cli::{IsTerminal, StdoutStream};
use wasmtime_wasi::p2::bindings::sync::Command;
use wasmtime_wasi::{I32Exit, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView, p1::WasiP1Ctx};
use wasmtime_wasi_http::bindings::http::types::Scheme;
use wasmtime_wasi_http::body::{HyperIncomingBody, HyperOutgoingBody};
use wasmtime_wasi_http::types::IncomingResponse;
use wasmtime_wasi_http::types::{HostFutureIncomingResponse, OutgoingRequestConfig};
use wasmtime_wasi_http::{HttpError, HttpResult, WasiHttpCtx, WasiHttpView};

pub struct PaddockWriterState {
    writer: Option<Box<dyn ObjectWriter>>,
}

mod service_bindings {
    wasmtime::component::bindgen!({
        path: "../../../pit-lane/crates/pit-lane-core/wit",
        world: "service-consumer",
        imports: { default: async },
        require_store_data_send: true,
    });
}

mod paddock_bindings {
    wasmtime::component::bindgen!({
        path: "../../../pit-lane/crates/pit-lane-core/wit/paddock",
        world: "paddock-consumer",
        imports: { default: async },
        with: { "pitfast:paddock/store.writer": crate::PaddockWriterState },
        require_store_data_send: true,
    });
}

/// Stable identity for an artifact source.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArtifactId(String);

impl ArtifactId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ArtifactId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Supported local artifact sources. Remote sources can be added later.
#[derive(Debug, Clone)]
pub enum ArtifactSource {
    Path(PathBuf),
    Bytes(Arc<[u8]>),
}

/// A raw WebAssembly artifact independent of CLI path and manifest handling.
#[derive(Debug, Clone)]
pub struct WasmArtifact {
    id: ArtifactId,
    source: ArtifactSource,
    digest: Arc<OnceLock<String>>,
}

impl WasmArtifact {
    pub fn from_path(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        Self {
            id: ArtifactId(format!("path:{}", path.display())),
            source: ArtifactSource::Path(path),
            digest: Arc::new(OnceLock::new()),
        }
    }

    /// Construct an artifact from a path when the control plane already
    /// verified its immutable digest. This avoids re-hashing a large artifact
    /// merely to locate its derived compiled cache entry.
    pub fn from_path_with_digest(path: impl AsRef<Path>, digest: impl Into<String>) -> Self {
        let digest = digest.into();
        let digest_cache = Arc::new(OnceLock::new());
        let _ = digest_cache.set(digest.clone());
        Self {
            id: ArtifactId(digest),
            source: ArtifactSource::Path(path.as_ref().to_path_buf()),
            digest: digest_cache,
        }
    }

    pub fn from_bytes(bytes: impl Into<Arc<[u8]>>) -> Self {
        let bytes = bytes.into();
        let digest = Sha256::digest(&bytes);
        let digest_cache = Arc::new(OnceLock::new());
        let digest_string = format!("sha256:{digest:x}");
        let _ = digest_cache.set(digest_string.clone());
        Self {
            id: ArtifactId(digest_string),
            source: ArtifactSource::Bytes(bytes),
            digest: digest_cache,
        }
    }

    pub fn id(&self) -> &ArtifactId {
        &self.id
    }

    pub fn source(&self) -> &ArtifactSource {
        &self.source
    }

    fn digest(&self, bytes: &[u8]) -> &str {
        self.digest.get_or_init(|| artifact_digest(bytes)).as_str()
    }

    fn program_name(&self) -> String {
        match &self.source {
            ArtifactSource::Path(path) => path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("pit-wasm")
                .to_owned(),
            ArtifactSource::Bytes(_) => "pit-wasm".to_owned(),
        }
    }
}

/// Per-invocation limits.
#[derive(Debug, Clone, Default)]
pub struct ExecutionLimits {
    pub timeout: Option<Duration>,
    pub memory_bytes: Option<usize>,
}

/// Explicit input contract for one WASI execution.
#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    pub artifact: WasmArtifact,
    /// WASI entrypoint to invoke. Raw artifacts default to `_start`.
    pub entrypoint: String,
    /// Arguments after argv[0]. The runtime adds the artifact name as argv[0].
    pub args: Vec<String>,
    /// Only these variables are exposed. Host environment is never inherited.
    pub env: Vec<(String, String)>,
    pub limits: ExecutionLimits,
}

impl ExecutionRequest {
    pub fn new(artifact: WasmArtifact) -> Self {
        Self {
            artifact,
            entrypoint: "_start".to_owned(),
            args: Vec::new(),
            env: Vec::new(),
            limits: ExecutionLimits::default(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.entrypoint.is_empty() || self.entrypoint.contains('\0') {
            bail!("entrypoint must be a non-empty string without NUL bytes");
        }
        for (index, arg) in self.args.iter().enumerate() {
            if arg.contains('\0') {
                bail!("argument {index} contains a NUL byte");
            }
        }
        for (key, value) in &self.env {
            validate_env_entry(key, value)?;
        }
        Ok(())
    }

    pub fn with_args(mut self, args: impl IntoIterator<Item = String>) -> Self {
        self.args = args.into_iter().collect();
        self
    }

    pub fn with_entrypoint(mut self, entrypoint: impl Into<String>) -> Self {
        self.entrypoint = entrypoint.into();
        self
    }

    pub fn with_env(mut self, env: impl IntoIterator<Item = (String, String)>) -> Self {
        self.env = env.into_iter().collect();
        self
    }

    pub fn with_limits(mut self, limits: ExecutionLimits) -> Self {
        self.limits = limits;
        self
    }
}

pub fn validate_env_entry(key: &str, value: &str) -> Result<()> {
    if key.is_empty() {
        bail!("environment variable name must not be empty");
    }
    if key.contains('=') {
        bail!("environment variable name must not contain '=': {key}");
    }
    if key.contains('\0') || value.contains('\0') {
        bail!("environment variable entries must not contain NUL bytes");
    }
    Ok(())
}

/// Structured result status for a guest invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStatus {
    Completed,
    GuestExitNonZero,
    GuestTrap,
    TimedOut,
    Cancelled,
    MemoryLimitExceeded,
    RuntimeError,
}

impl fmt::Display for ExecutionStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Completed => "Completed",
            Self::GuestExitNonZero => "GuestExitNonZero",
            Self::GuestTrap => "GuestTrap",
            Self::TimedOut => "TimedOut",
            Self::Cancelled => "Cancelled",
            Self::MemoryLimitExceeded => "MemoryLimitExceeded",
            Self::RuntimeError => "RuntimeError",
        };
        formatter.write_str(name)
    }
}

/// Shareable cancellation request.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Runtime-level result before scheduler identity is attached.
#[derive(Debug, Clone)]
pub struct RuntimeExecutionResult {
    pub status: ExecutionStatus,
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub duration: Duration,
    pub started: SystemTime,
    pub error: Option<String>,
}

/// How a derived Wasmtime cache entry is restored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompiledCacheRestoreMode {
    /// Read the complete serialized artifact into host memory before loading it.
    Bytes,
    /// Let Wasmtime map the serialized artifact directly from its immutable file.
    #[default]
    FileBacked,
}

/// Instance allocation strategy used by a PitBox engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RuntimeAllocationMode {
    #[default]
    OnDemand,
    Pooling {
        slots: usize,
    },
}

/// Experimental runtime controls used by the fast-path benchmark and by the
/// eventual production default. They remain runtime-contract controls: no
/// source-language or service-specific behavior is represented here.
#[derive(Debug, Clone)]
pub struct PitRuntimeConfig {
    pub compiled_cache_restore: CompiledCacheRestoreMode,
    pub allocation: RuntimeAllocationMode,
    pub memory_init_cow: Option<bool>,
    pub pre_resolved_imports: bool,
}

impl Default for PitRuntimeConfig {
    fn default() -> Self {
        Self {
            compiled_cache_restore: CompiledCacheRestoreMode::FileBacked,
            allocation: RuntimeAllocationMode::OnDemand,
            memory_init_cow: None,
            pre_resolved_imports: true,
        }
    }
}

#[derive(Clone)]
pub struct HttpRequest {
    pub method: String,
    pub path_and_query: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Explicit guest environment for this request. The host environment is
    /// never inherited implicitly.
    pub env: Vec<(String, String)>,
    /// Exact host-owned TCP endpoints the guest may connect to. Empty means
    /// that TCP, UDP, and name lookup remain unavailable.
    pub allowed_tcp: Vec<std::net::SocketAddr>,
    /// Garage identity for distributed nested-invocation correlation.
    pub source_garage_id: Option<String>,
    /// Garages already traversed by this logical invocation path.
    pub visited_garages: Vec<String>,
    /// Immutable application release context captured at ingress.
    pub application_id: Option<String>,
    pub release_id: Option<String>,
    /// Optional host-owned generic Paddock capability for this invocation.
    /// The guest never receives backend credentials or filesystem paths.
    pub paddock: Option<Arc<dyn PaddockObjectBackend>>,
    pub paddock_grants: Vec<PaddockGrant>,
    pub paddock_auth: Option<PaddockGatewayAuth>,
}

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// High-resolution stages that are safe to measure inside one isolated HTTP
/// execution. The Store, WASI, and HTTP context remain fresh per invocation.
#[derive(Debug, Clone, Default)]
pub struct HttpExecutionTimings {
    pub request_setup: Duration,
    pub wasi_setup: Duration,
    pub store_setup: Duration,
    pub instance_setup: Duration,
    pub guest_execution: Duration,
}

struct EpochTicker {
    stop: Arc<AtomicBool>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl EpochTicker {
    fn start(engine: Engine) -> Result<Arc<Self>> {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = thread::Builder::new()
            .name("pit-wasmtime-epoch".to_owned())
            .spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    thread::sleep(Duration::from_millis(1));
                    engine.increment_epoch();
                }
            })
            .context("failed to start Wasmtime epoch ticker")?;
        Ok(Arc::new(Self {
            stop,
            handle: Mutex::new(Some(handle)),
        }))
    }
}

impl Drop for EpochTicker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self
            .handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = handle.join();
        }
    }
}

/// Shared Wasmtime engine and preparation service.
pub struct PitRuntime {
    engine: Engine,
    http_engine: Engine,
    http_component_fingerprint: String,
    ticker: Arc<EpochTicker>,
    http_ticker: Arc<EpochTicker>,
    config: PitRuntimeConfig,
}

/// Persistent, machine-specific acceleration cache for compiled Wasm.
///
/// The cache is never authoritative: the portable artifact remains the source
/// of truth and every cache entry is disposable.
#[derive(Debug, Clone)]
pub struct CompiledCacheConfig {
    pub root: PathBuf,
    pub max_bytes: u64,
}

impl CompiledCacheConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            max_bytes: 4 * 1024 * 1024 * 1024,
        }
    }

    pub fn default_root() -> Result<PathBuf> {
        if let Some(value) = std::env::var_os("XDG_CACHE_HOME") {
            return Ok(PathBuf::from(value).join("pit/compiled"));
        }
        let home = std::env::var_os("HOME")
            .ok_or_else(|| anyhow!("HOME is unavailable; set XDG_CACHE_HOME"))?;
        Ok(PathBuf::from(home).join(".cache/pit/compiled"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadinessState {
    Cold,
    Warm,
    Hot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompiledCacheSource {
    ColdCompiled,
    WarmRestored,
}

#[derive(Debug, Clone)]
pub struct PreparationReport {
    pub readiness: ReadinessState,
    pub source: CompiledCacheSource,
    pub compile_duration: Duration,
    pub restore_duration: Duration,
    pub cache_publication_duration: Duration,
    pub cache_metadata_lookup_duration: Duration,
    pub compile_fingerprint_duration: Duration,
    pub artifact_digest_duration: Duration,
    pub cache_file_read_duration: Duration,
    pub cache_deserialize_duration: Duration,
    pub http_linker_setup_duration: Duration,
    pub linker_preparation_duration: Duration,
    pub prepared_object_construction_duration: Duration,
}

impl Default for PreparationReport {
    fn default() -> Self {
        Self {
            readiness: ReadinessState::Cold,
            source: CompiledCacheSource::ColdCompiled,
            compile_duration: Duration::ZERO,
            restore_duration: Duration::ZERO,
            cache_publication_duration: Duration::ZERO,
            cache_metadata_lookup_duration: Duration::ZERO,
            compile_fingerprint_duration: Duration::ZERO,
            artifact_digest_duration: Duration::ZERO,
            cache_file_read_duration: Duration::ZERO,
            cache_deserialize_duration: Duration::ZERO,
            http_linker_setup_duration: Duration::ZERO,
            linker_preparation_duration: Duration::ZERO,
            prepared_object_construction_duration: Duration::ZERO,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompiledCacheMetadata {
    schema_version: u32,
    artifact_digest: String,
    fingerprint: String,
    wasmtime: String,
    target: String,
    architecture: String,
    format: String,
    world: String,
    wasm_size_bytes: u64,
    compiled_size_bytes: u64,
}

const COMPILED_CACHE_SCHEMA: u32 = 2;
const WASMTIME_VERSION: &str = "39.0.2";
static CACHE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Exact compatibility inputs used to distinguish machine-ready cache entries.
/// The canonical portable `.wasm` digest remains deployment authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompileFingerprint {
    pub schema_version: u32,
    pub wasmtime: String,
    pub target: String,
    pub architecture: String,
    pub format: String,
    pub world: String,
    pub engine_compatibility_hash: String,
}

impl CompileFingerprint {
    fn for_engine(engine: &Engine, format: ArtifactFormat, world: &str) -> Self {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        engine.precompile_compatibility_hash().hash(&mut hasher);
        Self {
            schema_version: COMPILED_CACHE_SCHEMA,
            wasmtime: WASMTIME_VERSION.to_owned(),
            target: format!(
                "{}-{}-{}",
                std::env::consts::ARCH,
                std::env::consts::OS,
                std::env::consts::FAMILY
            ),
            architecture: std::env::consts::ARCH.to_owned(),
            format: format.to_string(),
            world: world.to_owned(),
            engine_compatibility_hash: format!("{:016x}", hasher.finish()),
        }
    }

    pub fn value(&self) -> String {
        let encoded = serde_json::to_vec(self).expect("CompileFingerprint is serializable");
        format!("v{}-{:x}", self.schema_version, Sha256::digest(encoded))
    }
}

struct CompiledCacheEntry {
    path: PathBuf,
    source: CompiledCacheSource,
    compile_duration: Duration,
    restore_duration: Duration,
    publication_duration: Duration,
    metadata_lookup_duration: Duration,
    fingerprint_duration: Duration,
    file_read_duration: Duration,
}

fn artifact_digest(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn engine_fingerprint(engine: &Engine, format: ArtifactFormat, world: &str) -> String {
    CompileFingerprint::for_engine(engine, format, world).value()
}

fn cache_entry_paths(
    config: &CompiledCacheConfig,
    digest: &str,
    fingerprint: &str,
) -> (PathBuf, PathBuf) {
    let hex = digest.strip_prefix("sha256:").unwrap_or(digest);
    let prefix = &hex[..2.min(hex.len())];
    let directory = config
        .root
        .join("sha256")
        .join(prefix)
        .join(hex)
        .join(fingerprint);
    (
        directory.join("artifact.cwasm"),
        directory.join("metadata.json"),
    )
}

fn remove_cache_entry(cache_path: &Path, metadata_path: &Path) {
    if let Some(parent) = cache_path.parent() {
        let _ = std::fs::remove_dir_all(parent);
    }
    let _ = std::fs::remove_file(metadata_path);
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("cache path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let temp = parent.join(format!(
        ".{}.tmp-{}-{}",
        path.file_name().unwrap().to_string_lossy(),
        std::process::id(),
        CACHE_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&temp, bytes)?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&temp)?;
    file.sync_all()?;
    std::fs::rename(temp, path)?;
    Ok(())
}

fn evict_compiled_cache(config: &CompiledCacheConfig) {
    let root = config.root.join("sha256");
    let mut entries = Vec::new();
    let Ok(prefixes) = std::fs::read_dir(root) else {
        return;
    };
    for prefix in prefixes.flatten() {
        let Ok(digests) = std::fs::read_dir(prefix.path()) else {
            continue;
        };
        for digest in digests.flatten() {
            let Ok(fingerprints) = std::fs::read_dir(digest.path()) else {
                continue;
            };
            for fingerprint in fingerprints.flatten() {
                let artifact = fingerprint.path().join("artifact.cwasm");
                if let Ok(metadata) = std::fs::metadata(&artifact) {
                    entries.push((
                        metadata
                            .modified()
                            .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                        metadata.len(),
                        fingerprint.path(),
                    ));
                }
            }
        }
    }
    let mut total = entries.iter().map(|(_, size, _)| *size).sum::<u64>();
    entries.sort_by_key(|(modified, _, _)| *modified);
    for (_, size, path) in entries {
        if total <= config.max_bytes {
            break;
        }
        let _ = std::fs::remove_dir_all(path);
        total = total.saturating_sub(size);
    }
}

#[allow(clippy::too_many_arguments)]
fn compiled_cache_entry(
    config: &CompiledCacheConfig,
    engine: &Engine,
    fingerprint: &str,
    digest: &str,
    source: &[u8],
    format: ArtifactFormat,
    world: &str,
    restore_mode: CompiledCacheRestoreMode,
) -> Result<CompiledCacheEntry> {
    let fingerprint_duration = Duration::ZERO;
    let (cache_path, metadata_path) = cache_entry_paths(config, digest, fingerprint);
    let restore_started = Instant::now();
    let metadata_lookup_started = Instant::now();
    let metadata_result = std::fs::read(&metadata_path);
    let metadata_lookup_duration = metadata_lookup_started.elapsed();
    if let Ok(metadata_bytes) = metadata_result {
        let file_read_started = Instant::now();
        let compiled_size = match restore_mode {
            CompiledCacheRestoreMode::Bytes => std::fs::metadata(&cache_path).map(|m| m.len()),
            CompiledCacheRestoreMode::FileBacked => std::fs::metadata(&cache_path).map(|m| m.len()),
        };
        let file_read_duration = file_read_started.elapsed();
        let valid = serde_json::from_slice::<CompiledCacheMetadata>(&metadata_bytes)
            .map(|metadata| {
                metadata.schema_version == COMPILED_CACHE_SCHEMA
                    && metadata.artifact_digest == digest
                    && metadata.fingerprint == fingerprint
                    && metadata.wasmtime == WASMTIME_VERSION
                    && metadata.target
                        == format!(
                            "{}-{}-{}",
                            std::env::consts::ARCH,
                            std::env::consts::OS,
                            std::env::consts::FAMILY
                        )
                    && metadata.architecture == std::env::consts::ARCH
                    && metadata.format == format.to_string()
                    && metadata.world == world
                    && metadata.wasm_size_bytes == source.len() as u64
                    && compiled_size.as_ref().ok() == Some(&metadata.compiled_size_bytes)
            })
            .unwrap_or(false);
        if valid && compiled_size.unwrap_or_default() != 0 {
            return Ok(CompiledCacheEntry {
                path: cache_path,
                source: CompiledCacheSource::WarmRestored,
                compile_duration: Duration::ZERO,
                restore_duration: restore_started.elapsed(),
                publication_duration: Duration::ZERO,
                metadata_lookup_duration,
                fingerprint_duration,
                file_read_duration,
            });
        }
        remove_cache_entry(&cache_path, &metadata_path);
    }

    let compile_started = Instant::now();
    let bytes = match format {
        ArtifactFormat::Component => engine.precompile_component(source)?,
        ArtifactFormat::CoreModule => engine.precompile_module(source)?,
    };
    let compile_duration = compile_started.elapsed();
    let publication_started = Instant::now();
    let metadata = CompiledCacheMetadata {
        schema_version: COMPILED_CACHE_SCHEMA,
        artifact_digest: digest.to_owned(),
        fingerprint: fingerprint.to_owned(),
        wasmtime: WASMTIME_VERSION.to_owned(),
        target: format!(
            "{}-{}-{}",
            std::env::consts::ARCH,
            std::env::consts::OS,
            std::env::consts::FAMILY
        ),
        architecture: std::env::consts::ARCH.to_owned(),
        format: format.to_string(),
        world: world.to_owned(),
        wasm_size_bytes: source.len() as u64,
        compiled_size_bytes: bytes.len() as u64,
    };
    write_atomic(&cache_path, &bytes)?;
    write_atomic(&metadata_path, &serde_json::to_vec_pretty(&metadata)?)?;
    evict_compiled_cache(config);
    Ok(CompiledCacheEntry {
        path: cache_path,
        source: CompiledCacheSource::ColdCompiled,
        compile_duration,
        restore_duration: Duration::ZERO,
        publication_duration: publication_started.elapsed(),
        metadata_lookup_duration,
        fingerprint_duration,
        file_read_duration: Duration::ZERO,
    })
}

fn configure_engine(config: &mut Config, runtime_config: &PitRuntimeConfig) -> Result<()> {
    if let Some(memory_init_cow) = runtime_config.memory_init_cow {
        config.memory_init_cow(memory_init_cow);
    }
    if let RuntimeAllocationMode::Pooling { slots } = runtime_config.allocation {
        let slots = slots.max(1);
        let slots_u32 = u32::try_from(slots).context("pooling slot count exceeds u32")?;
        let resource_slots = slots
            .checked_mul(8)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| anyhow!("pooling resource slot count is too large"))?;
        let mut pooling = PoolingAllocationConfig::new();
        pooling
            .total_component_instances(slots_u32)
            .total_core_instances(resource_slots)
            .total_memories(resource_slots)
            .total_tables(resource_slots)
            .total_stacks(slots_u32)
            .max_unused_warm_slots(slots_u32);
        config.allocation_strategy(InstanceAllocationStrategy::Pooling(pooling));
    }
    Ok(())
}

impl PitRuntime {
    pub fn new() -> Result<Self> {
        Self::with_config(PitRuntimeConfig::default())
    }

    pub fn with_config(runtime_config: PitRuntimeConfig) -> Result<Self> {
        let mut config = Config::new();
        config.epoch_interruption(true);
        configure_engine(&mut config, &runtime_config)?;
        let engine = Engine::new(&config).context("failed to create Wasmtime engine")?;
        let ticker = EpochTicker::start(engine.clone())?;
        let mut http_config = Config::new();
        http_config.epoch_interruption(true);
        http_config.async_support(true);
        configure_engine(&mut http_config, &runtime_config)?;
        let http_engine =
            Engine::new(&http_config).context("failed to create async HTTP engine")?;
        let http_component_fingerprint =
            engine_fingerprint(&http_engine, ArtifactFormat::Component, "wasi:http/proxy");
        let http_ticker = EpochTicker::start(http_engine.clone())?;
        Ok(Self {
            engine,
            http_engine,
            http_component_fingerprint,
            ticker,
            http_ticker,
            config: runtime_config,
        })
    }

    pub fn prepare(&self, artifact: WasmArtifact) -> Result<PreparedArtifact> {
        self.prepare_with_world(artifact, None, None)
            .map(|(prepared, _)| prepared)
    }

    pub fn prepare_http(&self, artifact: WasmArtifact) -> Result<PreparedArtifact> {
        self.prepare_with_world(artifact, Some("wasi:http/proxy"), None)
            .map(|(prepared, _)| prepared)
    }

    pub fn prepare_http_cached(
        &self,
        artifact: WasmArtifact,
        cache: &CompiledCacheConfig,
    ) -> Result<(PreparedArtifact, PreparationReport)> {
        self.prepare_with_world(artifact, Some("wasi:http/proxy"), Some(cache))
    }

    pub fn warm_digests(cache: &CompiledCacheConfig) -> Vec<String> {
        let mut values = Vec::new();
        let Ok(prefixes) = std::fs::read_dir(cache.root.join("sha256")) else {
            return values;
        };
        for prefix in prefixes.flatten() {
            let Ok(digests) = std::fs::read_dir(prefix.path()) else {
                continue;
            };
            for digest in digests.flatten() {
                let Ok(fingerprints) = std::fs::read_dir(digest.path()) else {
                    continue;
                };
                if fingerprints.flatten().any(|entry| {
                    let directory = entry.path();
                    let cache_path = directory.join("artifact.cwasm");
                    let metadata_path = directory.join("metadata.json");
                    let (Ok(bytes), Ok(metadata_bytes)) =
                        (std::fs::read(&cache_path), std::fs::read(&metadata_path))
                    else {
                        return false;
                    };
                    let Ok(metadata) =
                        serde_json::from_slice::<CompiledCacheMetadata>(&metadata_bytes)
                    else {
                        return false;
                    };
                    let digest_name = digest.file_name();
                    let Some(name) = digest_name.to_str() else {
                        return false;
                    };
                    metadata.schema_version == COMPILED_CACHE_SCHEMA
                        && metadata.artifact_digest == format!("sha256:{name}")
                        && directory.file_name().and_then(|value| value.to_str())
                            == Some(metadata.fingerprint.as_str())
                        && metadata.wasmtime == WASMTIME_VERSION
                        && metadata.target
                            == format!(
                                "{}-{}-{}",
                                std::env::consts::ARCH,
                                std::env::consts::OS,
                                std::env::consts::FAMILY
                            )
                        && metadata.architecture == std::env::consts::ARCH
                        && metadata.compiled_size_bytes == bytes.len() as u64
                        && !bytes.is_empty()
                }) && let Some(name) = digest.file_name().to_str()
                {
                    values.push(format!("sha256:{name}"));
                }
            }
        }
        values.sort();
        values.dedup();
        values
    }

    fn prepare_with_world(
        &self,
        artifact: WasmArtifact,
        requested_world: Option<&str>,
        cache: Option<&CompiledCacheConfig>,
    ) -> Result<(PreparedArtifact, PreparationReport)> {
        let bytes = match artifact.source() {
            ArtifactSource::Path(path) => {
                if !path.exists() {
                    bail!("WASM file does not exist: {}", path.display());
                }
                if !path.is_file() {
                    bail!("WASM path is not a file: {}", path.display());
                }
                std::fs::read(path)
                    .with_context(|| format!("failed to read WASM artifact {}", path.display()))?
            }
            ArtifactSource::Bytes(bytes) => bytes.to_vec(),
        };
        let digest_started = Instant::now();
        let artifact_digest = artifact.digest(&bytes).to_owned();
        let artifact_digest_duration = digest_started.elapsed();
        let format = detect_format(&bytes)?;
        let (module, p1_linker, component, p2_linker, http_pre, http_engine, http_ticker, report) =
            match format {
                ArtifactFormat::CoreModule => (
                    Some(
                        Module::from_binary(&self.engine, &bytes)
                            .context("WASM module preparation failed")?,
                    ),
                    Some(core_linker(&self.engine)?),
                    None,
                    None,
                    None,
                    None,
                    None,
                    PreparationReport::default(),
                ),
                ArtifactFormat::Component => {
                    let (linker, component) = if cache.is_none() {
                        let mut linker = ComponentLinker::new(&self.engine);
                        let mut sync_wasi_options =
                            wasmtime_wasi::p2::bindings::sync::LinkOptions::default();
                        sync_wasi_options.cli_exit_with_code(true);
                        wasmtime_wasi::p2::add_to_linker_with_options_sync(
                            &mut linker,
                            &sync_wasi_options,
                        )
                        .context("failed to link WASI Preview 2")?;
                        let component = Component::new(&self.engine, &bytes)
                            .context("WASM component preparation failed")?;
                        (Some(linker), Some(component))
                    } else {
                        (None, None)
                    };
                    let http_component = if let Some(cache) = cache {
                        let restore_started = Instant::now();
                        let entry = compiled_cache_entry(
                            cache,
                            &self.http_engine,
                            &self.http_component_fingerprint,
                            &artifact_digest,
                            &bytes,
                            format,
                            requested_world.unwrap_or("component"),
                            self.config.compiled_cache_restore,
                        )?;
                        let mut report = PreparationReport {
                            readiness: match entry.source {
                                CompiledCacheSource::WarmRestored => ReadinessState::Warm,
                                CompiledCacheSource::ColdCompiled => ReadinessState::Cold,
                            },
                            source: entry.source,
                            compile_duration: entry.compile_duration,
                            restore_duration: entry.restore_duration,
                            cache_publication_duration: entry.publication_duration,
                            cache_metadata_lookup_duration: entry.metadata_lookup_duration,
                            compile_fingerprint_duration: entry.fingerprint_duration,
                            artifact_digest_duration,
                            cache_file_read_duration: entry.file_read_duration,
                            ..PreparationReport::default()
                        };
                        // The deserializer is unsafe because Wasmtime trusts the
                        // bytes as compiled code. The cache entry was created by
                        // this exact Wasmtime engine fingerprint, was validated
                        // against the canonical digest and metadata, and is
                        // immutable after atomic publication.
                        let deserialize_started = Instant::now();
                        let component_result = match self.config.compiled_cache_restore {
                            CompiledCacheRestoreMode::Bytes => {
                                let read_started = Instant::now();
                                let serialized = std::fs::read(&entry.path).with_context(|| {
                                    format!(
                                        "failed to read compiled cache {}",
                                        entry.path.display()
                                    )
                                })?;
                                report.cache_file_read_duration += read_started.elapsed();
                                // SAFETY: the cache entry is atomically produced by PitFast and
                                // is accepted only after its digest/fingerprint metadata matches
                                // the canonical artifact and this exact Wasmtime engine.
                                unsafe { Component::deserialize(&self.http_engine, serialized) }
                            }
                            CompiledCacheRestoreMode::FileBacked => {
                                // SAFETY: the cache entry is atomically produced by PitFast and
                                // is accepted only after its digest/fingerprint metadata matches
                                // the canonical artifact and this exact Wasmtime engine. The file
                                // remains immutable for the lifetime of the mapped Component.
                                unsafe {
                                    Component::deserialize_file(&self.http_engine, &entry.path)
                                }
                            }
                        };
                        report.cache_deserialize_duration = deserialize_started.elapsed();
                        let component = match component_result {
                            Ok(component) => component,
                            Err(error) if entry.source == CompiledCacheSource::WarmRestored => {
                                let fingerprint = &self.http_component_fingerprint;
                                let (cache_path, metadata_path) =
                                    cache_entry_paths(cache, &artifact_digest, fingerprint);
                                remove_cache_entry(&cache_path, &metadata_path);
                                let rebuilt = compiled_cache_entry(
                                    cache,
                                    &self.http_engine,
                                    fingerprint,
                                    &artifact_digest,
                                    &bytes,
                                    format,
                                    requested_world.unwrap_or("component"),
                                    self.config.compiled_cache_restore,
                                )?;
                                report = PreparationReport {
                                    readiness: ReadinessState::Cold,
                                    source: rebuilt.source,
                                    compile_duration: rebuilt.compile_duration,
                                    restore_duration: rebuilt.restore_duration,
                                    cache_publication_duration: rebuilt.publication_duration,
                                    cache_metadata_lookup_duration: rebuilt
                                        .metadata_lookup_duration,
                                    compile_fingerprint_duration: rebuilt.fingerprint_duration,
                                    artifact_digest_duration,
                                    cache_file_read_duration: rebuilt.file_read_duration,
                                    ..PreparationReport::default()
                                };
                                let rebuild_deserialize_started = Instant::now();
                                let component = match self.config.compiled_cache_restore {
                                    CompiledCacheRestoreMode::Bytes => {
                                        let serialized = std::fs::read(&rebuilt.path)?;
                                        // SAFETY: the rebuilt entry was produced by this engine
                                        // from the canonical artifact immediately above.
                                        unsafe {
                                            Component::deserialize(&self.http_engine, serialized)
                                        }
                                    }
                                    CompiledCacheRestoreMode::FileBacked => {
                                        // SAFETY: the rebuilt entry was atomically published by
                                        // this engine and remains immutable while mapped.
                                        unsafe {
                                            Component::deserialize_file(
                                                &self.http_engine,
                                                &rebuilt.path,
                                            )
                                        }
                                    }
                                }
                                .with_context(|| {
                                    format!(
                                        "compiled HTTP component deserialization failed after cache rebuild: {error}"
                                    )
                                })?;
                                report.cache_deserialize_duration =
                                    rebuild_deserialize_started.elapsed();
                                component
                            }
                            Err(error) => {
                                return Err(error)
                                    .context("compiled HTTP component deserialization failed");
                            }
                        };
                        let http_linker_started = Instant::now();
                        let mut http_linker = ComponentLinker::new(&self.http_engine);
                        let mut async_wasi_options =
                            wasmtime_wasi::p2::bindings::LinkOptions::default();
                        async_wasi_options.cli_exit_with_code(true);
                        wasmtime_wasi::p2::add_to_linker_with_options_async(
                            &mut http_linker,
                            &async_wasi_options,
                        )
                        .context("failed to link WASI Preview 2 HTTP base")?;
                        service_bindings::ServiceConsumer::add_to_linker::<
                            _,
                            wasmtime::component::HasSelf<_>,
                        >(&mut http_linker, |state| state)
                        .context("failed to link PitFast service capability")?;
                        paddock_bindings::PaddockConsumer::add_to_linker::<
                            _,
                            wasmtime::component::HasSelf<_>,
                        >(&mut http_linker, |state| state)
                        .context("failed to link generic Paddock capability")?;
                        wasmtime_wasi_http::add_only_http_to_linker_async(&mut http_linker)
                            .context("failed to link WASI HTTP")?;
                        report.http_linker_setup_duration = http_linker_started.elapsed();
                        let (http_pre, http_component, http_linker) =
                            if self.config.pre_resolved_imports {
                                let linker_started = Instant::now();
                                let http_pre = wasmtime_wasi_http::bindings::ProxyPre::new(
                                    http_linker.instantiate_pre(&component)?,
                                )?;
                                report.linker_preparation_duration = linker_started.elapsed();
                                (Some(http_pre), None, None)
                            } else {
                                (None, Some(component.clone()), Some(http_linker.clone()))
                            };
                        let prepared_object_started = Instant::now();
                        let prepared = PreparedArtifact {
                            engine: self.engine.clone(),
                            module: None,
                            p1_linker: None,
                            component: None,
                            p2_linker: None,
                            http_pre,
                            http_component,
                            http_linker,
                            http_engine: Some(self.http_engine.clone()),
                            http_ticker: Some(Arc::clone(&self.http_ticker)),
                            artifact,
                            ticker: Arc::clone(&self.ticker),
                        };
                        report.prepared_object_construction_duration =
                            prepared_object_started.elapsed();
                        report.restore_duration = restore_started.elapsed();
                        return Ok((prepared, report));
                    } else {
                        Component::new(&self.http_engine, &bytes)
                            .context("WASM HTTP component preparation failed")?
                    };
                    let mut http_linker = ComponentLinker::new(&self.http_engine);
                    let mut async_wasi_options =
                        wasmtime_wasi::p2::bindings::LinkOptions::default();
                    async_wasi_options.cli_exit_with_code(true);
                    wasmtime_wasi::p2::add_to_linker_with_options_async(
                        &mut http_linker,
                        &async_wasi_options,
                    )
                    .context("failed to link WASI Preview 2 HTTP base")?;
                    service_bindings::ServiceConsumer::add_to_linker::<
                        _,
                        wasmtime::component::HasSelf<_>,
                    >(&mut http_linker, |state| state)
                    .context("failed to link PitFast service capability")?;
                    paddock_bindings::PaddockConsumer::add_to_linker::<
                        _,
                        wasmtime::component::HasSelf<_>,
                    >(&mut http_linker, |state| state)
                    .context("failed to link generic Paddock capability")?;
                    wasmtime_wasi_http::add_only_http_to_linker_async(&mut http_linker)
                        .context("failed to link WASI HTTP")?;
                    let http_pre = requested_world
                        .filter(|world| *world == "wasi:http/proxy")
                        .map(|_| {
                            wasmtime_wasi_http::bindings::ProxyPre::new(
                                http_linker.instantiate_pre(&http_component)?,
                            )
                        })
                        .transpose()?;
                    (
                        None,
                        None,
                        Some(component.expect("uncached component was prepared")),
                        Some(linker.expect("uncached linker was prepared")),
                        http_pre,
                        Some(self.http_engine.clone()),
                        Some(Arc::clone(&self.http_ticker)),
                        PreparationReport::default(),
                    )
                }
            };
        Ok((
            PreparedArtifact {
                engine: self.engine.clone(),
                module,
                p1_linker,
                component,
                p2_linker,
                http_pre,
                http_component: None,
                http_linker: None,
                http_engine,
                http_ticker,
                artifact,
                ticker: Arc::clone(&self.ticker),
            },
            report,
        ))
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<PreparedArtifact> {
        Self::new()?.prepare(WasmArtifact::from_path(path))
    }
}

/// Compiled artifact ready for isolated Store creation.
#[derive(Clone)]
pub struct PreparedArtifact {
    engine: Engine,
    module: Option<Module>,
    p1_linker: Option<CoreLinker<HostState>>,
    component: Option<Component>,
    p2_linker: Option<ComponentLinker<P2HostState>>,
    http_pre: Option<wasmtime_wasi_http::bindings::ProxyPre<HttpHostState>>,
    http_component: Option<Component>,
    http_linker: Option<ComponentLinker<HttpHostState>>,
    http_engine: Option<Engine>,
    http_ticker: Option<Arc<EpochTicker>>,
    artifact: WasmArtifact,
    ticker: Arc<EpochTicker>,
}

impl PreparedArtifact {
    pub fn artifact(&self) -> &WasmArtifact {
        &self.artifact
    }

    pub fn default_entrypoint(&self) -> &'static str {
        if self.component.is_some() || self.http_pre.is_some() || self.http_component.is_some() {
            pit_artifact::WASI_PREVIEW2_ENTRYPOINT
        } else {
            pit_artifact::WASI_PREVIEW1_ENTRYPOINT
        }
    }

    /// Measures isolated Store/instance creation without invoking the guest.
    /// The result intentionally includes linker lookup work for the P1 path.
    pub fn measure_instantiation(&self, request: &ExecutionRequest) -> Result<Duration> {
        request.validate()?;
        if request.artifact.id() != self.artifact.id() {
            bail!("execution request artifact does not match prepared artifact");
        }
        let mut args = Vec::with_capacity(request.args.len() + 1);
        args.push(request.artifact.program_name());
        args.extend(request.args.iter().cloned());
        let started = Instant::now();
        if let (Some(component), Some(linker)) = (&self.component, &self.p2_linker) {
            let mut builder = WasiCtxBuilder::new();
            builder.args(&args).envs(&request.env);
            let mut store = Store::new(
                &self.engine,
                P2HostState {
                    wasi: builder.build(),
                    table: ResourceTable::new(),
                    limits: RuntimeLimiter::new(request.limits.memory_bytes),
                },
            );
            store.limiter(|state| &mut state.limits);
            let _command = Command::instantiate(&mut store, component, linker)?;
        } else {
            let stdout = CapturedOutput::new();
            let stderr = CapturedOutput::new();
            let mut builder = WasiCtxBuilder::new();
            builder.args(&args).envs(&request.env);
            builder.stdout(stdout).stderr(stderr);
            let mut store = Store::new(
                &self.engine,
                HostState {
                    wasi: builder.build_p1(),
                    limits: RuntimeLimiter::new(request.limits.memory_bytes),
                },
            );
            store.limiter(|state| &mut state.limits);
            let _instance = self
                .p1_linker
                .as_ref()
                .unwrap()
                .instantiate(&mut store, self.module.as_ref().unwrap())?;
        }
        Ok(started.elapsed())
    }

    pub fn execute(
        &self,
        request: &ExecutionRequest,
        cancellation: CancellationToken,
    ) -> Result<RuntimeExecutionResult> {
        request.validate()?;
        if request.artifact.id() != self.artifact.id() {
            bail!(
                "execution request artifact '{}' does not match prepared artifact '{}'",
                request.artifact.id(),
                self.artifact.id()
            );
        }
        if self.component.is_some() {
            if request.entrypoint != pit_artifact::WASI_PREVIEW2_ENTRYPOINT {
                bail!(
                    "unsupported WASI Preview 2 entrypoint '{}'; expected {}",
                    request.entrypoint,
                    pit_artifact::WASI_PREVIEW2_ENTRYPOINT
                );
            }
            return self.execute_p2(request, cancellation);
        }
        // Keep the shared epoch ticker alive for every prepared artifact.
        let _ = &self.ticker;

        let started = SystemTime::now();
        let timer = Instant::now();
        let stdout = CapturedOutput::new();
        let stderr = CapturedOutput::new();
        let mut args = Vec::with_capacity(request.args.len() + 1);
        args.push(request.artifact.program_name());
        args.extend(request.args.iter().cloned());

        let mut wasi_builder = WasiCtxBuilder::new();
        wasi_builder.args(&args).envs(&request.env);
        wasi_builder.stdout(stdout.clone()).stderr(stderr.clone());
        let wasi = wasi_builder.build_p1();
        let limits = RuntimeLimiter::new(request.limits.memory_bytes);
        let mut store = Store::new(&self.engine, HostState { wasi, limits });
        store.limiter(|state| &mut state.limits);

        let control = Arc::new(ExecutionControl::new(cancellation, request.limits.timeout));
        let callback_control = Arc::clone(&control);
        store.set_epoch_deadline(1);
        store.epoch_deadline_callback(move |_| {
            if callback_control.token.is_cancelled() {
                callback_control
                    .signal
                    .store(ControlSignal::Cancelled as u8, Ordering::Release);
                return Ok(UpdateDeadline::Interrupt);
            }
            if callback_control.timed_out() {
                callback_control
                    .signal
                    .store(ControlSignal::TimedOut as u8, Ordering::Release);
                return Ok(UpdateDeadline::Interrupt);
            }
            Ok(UpdateDeadline::Continue(1))
        });

        let call_result = match self
            .p1_linker
            .as_ref()
            .unwrap()
            .instantiate(&mut store, self.module.as_ref().unwrap())
        {
            Ok(instance) => {
                match instance.get_typed_func::<(), ()>(&mut store, &request.entrypoint) {
                    Ok(start) => start.call(&mut store, ()),
                    Err(error) => {
                        return Ok(Self::result(
                            status_for_error(&store, &control),
                            None,
                            &stdout,
                            &stderr,
                            started,
                            timer.elapsed(),
                            Some(error.to_string()),
                        ));
                    }
                }
            }
            Err(error) => {
                return Ok(Self::result(
                    status_for_error(&store, &control),
                    None,
                    &stdout,
                    &stderr,
                    started,
                    timer.elapsed(),
                    Some(error.to_string()),
                ));
            }
        };

        let status = control.signal();
        let memory_exceeded = store.data().limits.memory_exceeded;
        let result = match status {
            ControlSignal::TimedOut => Self::result(
                ExecutionStatus::TimedOut,
                None,
                &stdout,
                &stderr,
                started,
                timer.elapsed(),
                Some("execution exceeded its timeout".to_owned()),
            ),
            ControlSignal::Cancelled => Self::result(
                ExecutionStatus::Cancelled,
                None,
                &stdout,
                &stderr,
                started,
                timer.elapsed(),
                Some("execution was cancelled".to_owned()),
            ),
            ControlSignal::None if memory_exceeded => Self::result(
                ExecutionStatus::MemoryLimitExceeded,
                None,
                &stdout,
                &stderr,
                started,
                timer.elapsed(),
                Some("execution exceeded its memory limit".to_owned()),
            ),
            ControlSignal::None => match call_result {
                Ok(()) => Self::result(
                    ExecutionStatus::Completed,
                    Some(0),
                    &stdout,
                    &stderr,
                    started,
                    timer.elapsed(),
                    None,
                ),
                Err(error) => {
                    let exit_code = error
                        .chain()
                        .find_map(|cause| cause.downcast_ref::<I32Exit>().map(|exit| exit.0));
                    if let Some(exit_code) = exit_code {
                        let status = if exit_code == 0 {
                            ExecutionStatus::Completed
                        } else {
                            ExecutionStatus::GuestExitNonZero
                        };
                        Self::result(
                            status,
                            Some(exit_code),
                            &stdout,
                            &stderr,
                            started,
                            timer.elapsed(),
                            (exit_code != 0).then(|| error.to_string()),
                        )
                    } else {
                        Self::result(
                            ExecutionStatus::GuestTrap,
                            None,
                            &stdout,
                            &stderr,
                            started,
                            timer.elapsed(),
                            Some(error.to_string()),
                        )
                    }
                }
            },
        };
        Ok(result)
    }

    /// Executes one request against a prepared `wasi:http/proxy` component.
    /// The caller is responsible for scheduling this operation on a PitBox lane.
    pub fn execute_http(
        &self,
        request: &HttpRequest,
        cancellation: CancellationToken,
        limits: ExecutionLimits,
    ) -> Result<HttpResponse> {
        self.execute_http_with_invoker(request, cancellation, limits, None, None, 0)
    }

    /// Executes an HTTP component with an optional direct logical service
    /// invoker. Nested calls stay inside the current host execution instead of
    /// waiting for another scheduler lane, which prevents caller/callee
    /// deadlocks when every lane is occupied.
    pub fn execute_http_with_invoker(
        &self,
        request: &HttpRequest,
        cancellation: CancellationToken,
        limits: ExecutionLimits,
        service_invoker: Option<Arc<dyn ServiceInvoker>>,
        parent_execution_id: Option<String>,
        invocation_depth: u16,
    ) -> Result<HttpResponse> {
        self.execute_http_with_invoker_timed(
            request,
            cancellation,
            limits,
            service_invoker,
            parent_execution_id,
            invocation_depth,
        )
        .map(|(response, _)| response)
    }

    /// HTTP execution with isolated runtime setup timings. The returned
    /// timings deliberately stop at the guest handler boundary; response
    /// business work is reported separately as `guest_execution`.
    pub fn execute_http_with_invoker_timed(
        &self,
        request: &HttpRequest,
        cancellation: CancellationToken,
        limits: ExecutionLimits,
        service_invoker: Option<Arc<dyn ServiceInvoker>>,
        parent_execution_id: Option<String>,
        invocation_depth: u16,
    ) -> Result<(HttpResponse, HttpExecutionTimings)> {
        wasmtime_wasi::runtime::in_tokio(async {
            let http_engine = self
                .http_engine
                .as_ref()
                .ok_or_else(|| anyhow!("HTTP engine was not prepared"))?;
            let _http_ticker = self
                .http_ticker
                .as_ref()
                .ok_or_else(|| anyhow!("HTTP epoch ticker was not prepared"))?;
            let request_setup_started = Instant::now();
            let body = Full::new(Bytes::from(request.body.clone()))
                .map_err(|never| match never {})
                .boxed();
            let mut builder = hyper::Request::builder()
                .method(request.method.as_str())
                .uri(request.path_and_query.as_str());
            for (name, value) in &request.headers {
                builder = builder.header(name, value);
            }
            let req = builder.body(body).context("invalid HTTP request")?;
            let request_setup = request_setup_started.elapsed();
            let wasi_setup_started = Instant::now();
            let mut wasi = WasiCtxBuilder::new();
            wasi.args(&["pit-http"]);
            wasi.envs(&request.env);
            let allowed_tcp = request.allowed_tcp.clone();
            wasi.allow_tcp(!allowed_tcp.is_empty())
                .allow_udp(false)
                .allow_ip_name_lookup(false)
                .socket_addr_check(move |address, use_kind| {
                    let allowed_tcp = allowed_tcp.clone();
                    Box::pin(async move {
                        matches!(use_kind, wasmtime_wasi::sockets::SocketAddrUse::TcpConnect)
                            && allowed_tcp.contains(&address)
                    })
                });
            let wasi = wasi.build();
            let wasi_setup = wasi_setup_started.elapsed();
            let store_setup_started = Instant::now();
            let mut store = Store::new(
                http_engine,
                HttpHostState {
                    wasi,
                    table: ResourceTable::new(),
                    http: WasiHttpCtx::new(),
                    limits: RuntimeLimiter::new(limits.memory_bytes),
                    service_invoker,
                    parent_execution_id,
                    invocation_depth,
                    source_garage_id: request.source_garage_id.clone(),
                    visited_garages: request.visited_garages.clone(),
                    application_id: request.application_id.clone(),
                    release_id: request.release_id.clone(),
                    paddock: request.paddock.clone(),
                    paddock_grants: request.paddock_grants.clone(),
                    paddock_auth: request.paddock_auth.clone(),
                    request_method: request.method.clone(),
                    request_path: request.path_and_query.clone(),
                    request_headers: request.headers.clone(),
                    request_payload_hash: hex_digest(Sha256::digest(&request.body)),
                },
            );
            store.limiter(|state| &mut state.limits);
            let store_setup = store_setup_started.elapsed();
            let control = Arc::new(ExecutionControl::new(cancellation, limits.timeout));
            let callback_control = Arc::clone(&control);
            store.set_epoch_deadline(1);
            store.epoch_deadline_callback(move |_| {
                if callback_control.token.is_cancelled() {
                    callback_control
                        .signal
                        .store(ControlSignal::Cancelled as u8, Ordering::Release);
                    return Ok(UpdateDeadline::Interrupt);
                }
                if callback_control.timed_out() {
                    callback_control
                        .signal
                        .store(ControlSignal::TimedOut as u8, Ordering::Release);
                    return Ok(UpdateDeadline::Interrupt);
                }
                Ok(UpdateDeadline::Continue(1))
            });
            let req = store.data_mut().new_incoming_request(Scheme::Http, req)?;
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let output = store.data_mut().new_response_outparam(sender)?;
            let instance_setup_started = Instant::now();
            let proxy = if let Some(http_pre) = self.http_pre.as_ref() {
                http_pre.instantiate_async(&mut store).await?
            } else {
                let component = self
                    .http_component
                    .as_ref()
                    .ok_or_else(|| anyhow!("HTTP component was not prepared"))?;
                let linker = self
                    .http_linker
                    .as_ref()
                    .ok_or_else(|| anyhow!("HTTP linker was not prepared"))?;
                let pre = wasmtime_wasi_http::bindings::ProxyPre::new(
                    linker.instantiate_pre(component)?,
                )?;
                pre.instantiate_async(&mut store).await?
            };
            let instance_setup = instance_setup_started.elapsed();
            let guest_started = Instant::now();
            let handler = wasmtime_wasi::runtime::spawn(async move {
                let result = proxy
                    .wasi_http_incoming_handler()
                    .call_handle(&mut store, req, output)
                    .await;
                (store, result)
            });
            // Consume the response body while the guest is still running. The
            // WASI HTTP outgoing stream is bounded; waiting for the guest
            // before draining it deadlocks any legitimate response larger than
            // the stream buffer (for example a real frontend JavaScript asset).
            let response_task = tokio::spawn(async move {
                let response = receiver
                    .await
                    .map_err(|_| anyhow!("HTTP guest dropped response output"))??;
                let status = response.status().as_u16();
                let headers = response
                    .headers()
                    .iter()
                    .map(|(name, value)| {
                        Ok::<_, anyhow::Error>((name.to_string(), value.to_str()?.to_owned()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let body = response.into_body().collect().await?.to_bytes().to_vec();
                Ok::<_, anyhow::Error>(HttpResponse {
                    status,
                    headers,
                    body,
                })
            });
            let (store, call_result) = handler.await;
            if let Err(error) = call_result {
                response_task.abort();
                return Err(anyhow!("WASI HTTP incoming handler failed: {error:#}"));
            }
            let response = response_task.await??;
            match control.signal() {
                ControlSignal::Cancelled => bail!("HTTP execution was cancelled"),
                ControlSignal::TimedOut => bail!("HTTP execution exceeded its timeout"),
                ControlSignal::None if store.data().limits.memory_exceeded => {
                    bail!("HTTP execution exceeded its memory limit")
                }
                ControlSignal::None => {}
            }
            Ok((
                response,
                HttpExecutionTimings {
                    request_setup,
                    wasi_setup,
                    store_setup,
                    instance_setup,
                    guest_execution: guest_started.elapsed(),
                },
            ))
        })
    }

    fn result(
        status: ExecutionStatus,
        exit_code: Option<i32>,
        stdout: &CapturedOutput,
        stderr: &CapturedOutput,
        started: SystemTime,
        duration: Duration,
        error: Option<String>,
    ) -> RuntimeExecutionResult {
        RuntimeExecutionResult {
            status,
            exit_code,
            stdout: stdout.contents(),
            stderr: stderr.contents(),
            duration,
            started,
            error,
        }
    }

    fn execute_p2(
        &self,
        request: &ExecutionRequest,
        cancellation: CancellationToken,
    ) -> Result<RuntimeExecutionResult> {
        let started = SystemTime::now();
        let timer = Instant::now();
        let stdout = wasmtime_wasi::p2::pipe::MemoryOutputPipe::new(usize::MAX);
        let stderr = wasmtime_wasi::p2::pipe::MemoryOutputPipe::new(usize::MAX);
        let mut args = Vec::with_capacity(request.args.len() + 1);
        args.push(request.artifact.program_name());
        args.extend(request.args.iter().cloned());
        let mut wasi_builder = WasiCtxBuilder::new();
        wasi_builder.args(&args).envs(&request.env);
        wasi_builder.stdout(stdout.clone()).stderr(stderr.clone());
        let mut store = Store::new(
            &self.engine,
            P2HostState {
                wasi: wasi_builder.build(),
                table: ResourceTable::new(),
                limits: RuntimeLimiter::new(request.limits.memory_bytes),
            },
        );
        store.limiter(|state| &mut state.limits);
        let control = Arc::new(ExecutionControl::new(cancellation, request.limits.timeout));
        let callback_control = Arc::clone(&control);
        store.set_epoch_deadline(1);
        store.epoch_deadline_callback(move |_| {
            if callback_control.token.is_cancelled() {
                callback_control
                    .signal
                    .store(ControlSignal::Cancelled as u8, Ordering::Release);
                return Ok(UpdateDeadline::Interrupt);
            }
            if callback_control.timed_out() {
                callback_control
                    .signal
                    .store(ControlSignal::TimedOut as u8, Ordering::Release);
                return Ok(UpdateDeadline::Interrupt);
            }
            Ok(UpdateDeadline::Continue(1))
        });

        let call_result = match Command::instantiate(
            &mut store,
            self.component.as_ref().unwrap(),
            self.p2_linker.as_ref().unwrap(),
        ) {
            Ok(command) => command.wasi_cli_run().call_run(&mut store),
            Err(error) => {
                return Ok(Self::result_p2(
                    status_for_p2_error(&store, &control),
                    None,
                    &stdout.contents(),
                    &stderr.contents(),
                    started,
                    timer.elapsed(),
                    Some(error.to_string()),
                ));
            }
        };
        let stdout_bytes = stdout.contents();
        let stderr_bytes = stderr.contents();
        let status = control.signal();
        let memory_exceeded = store.data().limits.memory_exceeded;
        let result = match status {
            ControlSignal::TimedOut => Self::result_p2(
                ExecutionStatus::TimedOut,
                None,
                &stdout_bytes,
                &stderr_bytes,
                started,
                timer.elapsed(),
                Some("execution exceeded its timeout".to_owned()),
            ),
            ControlSignal::Cancelled => Self::result_p2(
                ExecutionStatus::Cancelled,
                None,
                &stdout_bytes,
                &stderr_bytes,
                started,
                timer.elapsed(),
                Some("execution was cancelled".to_owned()),
            ),
            ControlSignal::None if memory_exceeded => Self::result_p2(
                ExecutionStatus::MemoryLimitExceeded,
                None,
                &stdout_bytes,
                &stderr_bytes,
                started,
                timer.elapsed(),
                Some("execution exceeded its memory limit".to_owned()),
            ),
            ControlSignal::None => match call_result {
                Ok(Ok(())) => Self::result_p2(
                    ExecutionStatus::Completed,
                    Some(0),
                    &stdout_bytes,
                    &stderr_bytes,
                    started,
                    timer.elapsed(),
                    None,
                ),
                Ok(Err(())) => Self::result_p2(
                    ExecutionStatus::GuestExitNonZero,
                    Some(1),
                    &stdout_bytes,
                    &stderr_bytes,
                    started,
                    timer.elapsed(),
                    Some("guest exited with a non-zero status".to_owned()),
                ),
                Err(error) => {
                    if let Some(exit_code) = error
                        .chain()
                        .find_map(|cause| cause.downcast_ref::<I32Exit>().map(|exit| exit.0))
                    {
                        Self::result_p2(
                            if exit_code == 0 {
                                ExecutionStatus::Completed
                            } else {
                                ExecutionStatus::GuestExitNonZero
                            },
                            Some(exit_code),
                            &stdout_bytes,
                            &stderr_bytes,
                            started,
                            timer.elapsed(),
                            (exit_code != 0).then(|| error.to_string()),
                        )
                    } else {
                        Self::result_p2(
                            ExecutionStatus::GuestTrap,
                            None,
                            &stdout_bytes,
                            &stderr_bytes,
                            started,
                            timer.elapsed(),
                            Some(error.to_string()),
                        )
                    }
                }
            },
        };
        Ok(result)
    }

    fn result_p2(
        status: ExecutionStatus,
        exit_code: Option<i32>,
        stdout: &[u8],
        stderr: &[u8],
        started: SystemTime,
        duration: Duration,
        error: Option<String>,
    ) -> RuntimeExecutionResult {
        RuntimeExecutionResult {
            status,
            exit_code,
            stdout: stdout.to_vec(),
            stderr: stderr.to_vec(),
            duration,
            started,
            error,
        }
    }
}

struct P2HostState {
    wasi: WasiCtx,
    table: ResourceTable,
    limits: RuntimeLimiter,
}

struct HttpHostState {
    wasi: WasiCtx,
    table: ResourceTable,
    http: WasiHttpCtx,
    limits: RuntimeLimiter,
    service_invoker: Option<Arc<dyn ServiceInvoker>>,
    parent_execution_id: Option<String>,
    invocation_depth: u16,
    source_garage_id: Option<String>,
    visited_garages: Vec<String>,
    application_id: Option<String>,
    release_id: Option<String>,
    paddock: Option<Arc<dyn PaddockObjectBackend>>,
    paddock_grants: Vec<PaddockGrant>,
    paddock_auth: Option<PaddockGatewayAuth>,
    request_method: String,
    request_path: String,
    request_headers: Vec<(String, String)>,
    request_payload_hash: String,
}

impl WasiView for P2HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl WasiView for HttpHostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl WasiHttpView for HttpHostState {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.http
    }
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }

    fn outgoing_body_buffer_chunks(&mut self) -> usize {
        // Component handlers publish the response outparam only after they
        // finish writing the response body. Keep enough bounded capacity for
        // the host to accept ordinary frontend assets before that publication;
        // the PitLane response limit still bounds total response memory.
        4096
    }

    fn send_request(
        &mut self,
        request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<HostFutureIncomingResponse> {
        use wasmtime_wasi_http::bindings::http::types::ErrorCode;
        tracing::debug!(uri = %request.uri(), "WASI HTTP outgoing request");
        let authority = request
            .uri()
            .authority()
            .map(|authority| authority.as_str().to_owned())
            .ok_or_else(|| HttpError::from(ErrorCode::HttpRequestUriInvalid))?;
        let path = request
            .uri()
            .path_and_query()
            .map_or("/", |path| path.as_str());
        let target = format!("pit://{authority}{path}")
            .parse::<PitUri>()
            .map_err(|_| ErrorCode::HttpRequestDenied)?;
        let invoker = self
            .service_invoker
            .clone()
            .ok_or_else(|| HttpError::from(ErrorCode::HttpRequestDenied))?;
        let method = request.method().clone();
        let headers = request
            .headers()
            .iter()
            .map(|(name, value)| {
                Ok::<_, anyhow::Error>((name.to_string(), value.to_str()?.to_owned()))
            })
            .collect::<Result<Vec<_>>>()
            .map_err(|_| ErrorCode::HttpRequestDenied)?;
        let body =
            collect_http_body(request.into_body()).map_err(|_| ErrorCode::HttpRequestDenied)?;
        let invocation = ServiceInvocationRequest {
            target,
            method,
            headers: headers
                .into_iter()
                .filter_map(|(name, value)| {
                    Some((
                        http::header::HeaderName::try_from(name).ok()?,
                        http::HeaderValue::try_from(value).ok()?,
                    ))
                })
                .collect(),
            body,
            parent_execution_id: self.parent_execution_id.clone(),
            depth: self.invocation_depth.saturating_add(1),
            source_garage_id: self.source_garage_id.clone(),
            visited_garages: self.visited_garages.clone(),
            application_id: self.application_id.clone(),
            release_id: self.release_id.clone(),
        };
        // The synchronous v39 binding can call this hook while a Tokio bridge
        // is active. Move the direct child execution off that bridge so the
        // child can safely use its own WASI HTTP response bridge. It remains
        // inline with respect to PitBox scheduling and consumes no new lane.
        let response = std::thread::spawn(move || invoker.invoke(invocation))
            .join()
            .map_err(|_| ErrorCode::HttpRequestDenied)?
            .map_err(|_| ErrorCode::HttpRequestDenied)?;
        tracing::debug!(
            status = response.status.as_u16(),
            "WASI HTTP logical request completed"
        );
        let response_body_len = response.body.len();
        let mut builder = hyper::Response::builder().status(response.status);
        builder = builder.header(http::header::CONTENT_LENGTH, response_body_len);
        for (name, value) in response.headers {
            if let Some(name) = name {
                builder = builder.header(name, value);
            }
        }
        let body: HyperIncomingBody = Full::new(response.body)
            .map_err(|never| match never {})
            .boxed();
        let response = builder
            .body(body)
            .map_err(|_| ErrorCode::HttpRequestDenied)?;
        let result = IncomingResponse {
            resp: response,
            worker: None,
            between_bytes_timeout: config.between_bytes_timeout,
        };
        tracing::debug!("WASI HTTP outgoing response ready");
        Ok(HostFutureIncomingResponse::ready(Ok(Ok(result))))
    }
}

fn collect_http_body(body: HyperOutgoingBody) -> Result<Bytes> {
    let collected = match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(body.collect())),
        Err(_) => wasmtime_wasi::runtime::in_tokio(body.collect()),
    }
    .map_err(|error| anyhow!("HTTP request body collection failed: {error}"))?;
    Ok(collected.to_bytes())
}

impl service_bindings::pitfast::service::invoke::Host for HttpHostState {
    async fn call(
        &mut self,
        request: service_bindings::pitfast::service::invoke::Request,
    ) -> std::result::Result<
        service_bindings::pitfast::service::invoke::Response,
        service_bindings::pitfast::service::invoke::ServiceError,
    > {
        use service_bindings::pitfast::service::invoke::ServiceError;
        let target = request
            .uri
            .parse::<PitUri>()
            .map_err(|error| ServiceError::InvalidUri(error.to_string()))?;
        let invoker = self
            .service_invoker
            .as_ref()
            .ok_or_else(|| ServiceError::Unavailable("service invocation is not enabled".into()))?;
        let method = http::Method::from_bytes(request.method.as_bytes())
            .map_err(|error| ServiceError::Unavailable(format!("invalid method: {error}")))?;
        let mut headers = http::HeaderMap::new();
        for (name, value) in request.headers {
            let name = http::header::HeaderName::try_from(name.as_str())
                .map_err(|error| ServiceError::Unavailable(format!("invalid header: {error}")))?;
            let value = http::HeaderValue::try_from(value.as_str())
                .map_err(|error| ServiceError::Unavailable(format!("invalid header: {error}")))?;
            headers.append(name, value);
        }
        let parent_execution_id = self.parent_execution_id.clone();
        let depth = self.invocation_depth.saturating_add(1);
        let source_garage_id = self.source_garage_id.clone();
        let visited_garages = self.visited_garages.clone();
        let application_id = self.application_id.clone();
        let release_id = self.release_id.clone();
        let invoker = Arc::clone(invoker);
        let response = std::thread::spawn(move || {
            invoker.invoke(ServiceInvocationRequest {
                target,
                method,
                headers,
                body: Bytes::from(request.body),
                parent_execution_id,
                depth,
                source_garage_id,
                visited_garages,
                application_id,
                release_id,
            })
        })
        .join()
        .map_err(|_| ServiceError::Unavailable("service invocation thread panicked".into()))?
        .map_err(|error| ServiceError::Unavailable(error.to_string()))?;
        Ok(service_bindings::pitfast::service::invoke::Response {
            status: response.status.as_u16(),
            headers: response
                .headers
                .iter()
                .map(|(name, value)| {
                    (
                        name.to_string(),
                        value.to_str().unwrap_or_default().to_owned(),
                    )
                })
                .collect(),
            body: response.body.to_vec(),
        })
    }
}

fn paddock_error(error: anyhow::Error) -> paddock_bindings::pitfast::paddock::store::Error {
    paddock_bindings::pitfast::paddock::store::Error::Unavailable(error.to_string())
}

fn paddock_granted(state: &HttpHostState, namespace: &NamespaceId, write: bool) -> bool {
    state
        .paddock_grants
        .iter()
        .any(|grant| grant.allows(namespace, write))
}

fn to_paddock_object(object: ObjectRef) -> paddock_bindings::pitfast::paddock::store::ObjectRef {
    paddock_bindings::pitfast::paddock::store::ObjectRef {
        namespace: object.namespace.to_string(),
        key: object.key.to_string(),
        version: object.version.get(),
        digest: object.digest.to_string(),
        size: object.size.get(),
        content_type: object.metadata.content_type,
        deleted: object.deleted,
    }
}

impl paddock_bindings::pitfast::paddock::store::Host for HttpHostState {
    async fn begin_write(
        &mut self,
        namespace: String,
        key: String,
        content_type: Option<String>,
    ) -> std::result::Result<
        wasmtime::component::Resource<paddock_bindings::pitfast::paddock::store::Writer>,
        paddock_bindings::pitfast::paddock::store::Error,
    > {
        let namespace = NamespaceId::new(namespace).map_err(paddock_error)?;
        let key = ObjectKey::new(key).map_err(paddock_error)?;
        if !paddock_granted(self, &namespace, true) {
            return Err(paddock_bindings::pitfast::paddock::store::Error::Denied(
                format!("write access denied for namespace {namespace}"),
            ));
        }
        let backend = self.paddock.as_ref().ok_or_else(|| {
            paddock_bindings::pitfast::paddock::store::Error::Unavailable(
                "Paddock capability is not configured".into(),
            )
        })?;
        let metadata = ObjectMetadata {
            content_type,
            ..Default::default()
        };
        let writer = backend
            .begin_object_write(namespace, key, metadata, None)
            .await
            .map_err(paddock_error)?;
        self.table
            .push(PaddockWriterState {
                writer: Some(writer),
            })
            .map_err(|error| paddock_error(anyhow!(error.to_string())))
    }

    async fn get(
        &mut self,
        namespace: String,
        key: String,
    ) -> std::result::Result<
        paddock_bindings::pitfast::paddock::store::ObjectRef,
        paddock_bindings::pitfast::paddock::store::Error,
    > {
        let namespace = NamespaceId::new(namespace).map_err(paddock_error)?;
        let key = ObjectKey::new(key).map_err(paddock_error)?;
        if !paddock_granted(self, &namespace, false) {
            return Err(paddock_bindings::pitfast::paddock::store::Error::Denied(
                format!("read access denied for namespace {namespace}"),
            ));
        }
        let backend = self.paddock.as_ref().ok_or_else(|| {
            paddock_bindings::pitfast::paddock::store::Error::Unavailable(
                "Paddock capability is not configured".into(),
            )
        })?;
        backend
            .get_object(&namespace, &key, None)
            .await
            .map(to_paddock_object)
            .map_err(paddock_error)
    }

    async fn read_range(
        &mut self,
        namespace: String,
        key: String,
        offset: u64,
        length: u64,
    ) -> std::result::Result<Vec<u8>, paddock_bindings::pitfast::paddock::store::Error> {
        let namespace = NamespaceId::new(namespace).map_err(paddock_error)?;
        let key = ObjectKey::new(key).map_err(paddock_error)?;
        if !paddock_granted(self, &namespace, false) {
            return Err(paddock_bindings::pitfast::paddock::store::Error::Denied(
                format!("read access denied for namespace {namespace}"),
            ));
        }
        let backend = self.paddock.as_ref().ok_or_else(|| {
            paddock_bindings::pitfast::paddock::store::Error::Unavailable(
                "Paddock capability is not configured".into(),
            )
        })?;
        let object = backend
            .get_object(&namespace, &key, None)
            .await
            .map_err(paddock_error)?;
        backend
            .read_blob_range(&object.digest, offset, length)
            .await
            .map_err(paddock_error)
    }

    async fn delete(
        &mut self,
        namespace: String,
        key: String,
    ) -> std::result::Result<(), paddock_bindings::pitfast::paddock::store::Error> {
        let namespace = NamespaceId::new(namespace).map_err(paddock_error)?;
        let key = ObjectKey::new(key).map_err(paddock_error)?;
        let allowed = self
            .paddock_grants
            .iter()
            .any(|grant| grant.namespace == namespace && grant.delete);
        if !allowed {
            return Err(paddock_bindings::pitfast::paddock::store::Error::Denied(
                format!("delete access denied for namespace {namespace}"),
            ));
        }
        let backend = self.paddock.as_ref().ok_or_else(|| {
            paddock_bindings::pitfast::paddock::store::Error::Unavailable(
                "Paddock capability is not configured".into(),
            )
        })?;
        backend
            .delete_object(&namespace, &key, None)
            .await
            .map_err(paddock_error)
    }

    async fn list_objects(
        &mut self,
        namespace: String,
        prefix: Option<String>,
    ) -> std::result::Result<
        Vec<paddock_bindings::pitfast::paddock::store::ObjectRef>,
        paddock_bindings::pitfast::paddock::store::Error,
    > {
        let namespace = NamespaceId::new(namespace).map_err(paddock_error)?;
        let allowed = self
            .paddock_grants
            .iter()
            .any(|grant| grant.namespace == namespace && grant.list);
        if !allowed {
            return Err(paddock_bindings::pitfast::paddock::store::Error::Denied(
                format!("list access denied for namespace {namespace}"),
            ));
        }
        let backend = self.paddock.as_ref().ok_or_else(|| {
            paddock_bindings::pitfast::paddock::store::Error::Unavailable(
                "Paddock capability is not configured".into(),
            )
        })?;
        backend
            .list_objects(&namespace, prefix.as_deref())
            .await
            .map(|objects| objects.into_iter().map(to_paddock_object).collect())
            .map_err(paddock_error)
    }
}

impl paddock_bindings::pitfast::paddock::store::HostWriter for HttpHostState {
    async fn write(
        &mut self,
        resource: wasmtime::component::Resource<paddock_bindings::pitfast::paddock::store::Writer>,
        chunk: Vec<u8>,
    ) -> std::result::Result<(), paddock_bindings::pitfast::paddock::store::Error> {
        let writer = self
            .table
            .get_mut(&resource)
            .map_err(|error| paddock_error(anyhow!(error.to_string())))?
            .writer
            .as_mut()
            .ok_or_else(|| {
                paddock_bindings::pitfast::paddock::store::Error::Conflict(
                    "writer finalized".into(),
                )
            })?;
        writer.write_chunk(&chunk).await.map_err(paddock_error)
    }

    async fn commit(
        &mut self,
        resource: wasmtime::component::Resource<paddock_bindings::pitfast::paddock::store::Writer>,
    ) -> std::result::Result<
        paddock_bindings::pitfast::paddock::store::ObjectRef,
        paddock_bindings::pitfast::paddock::store::Error,
    > {
        let writer = self
            .table
            .get_mut(&resource)
            .map_err(|error| paddock_error(anyhow!(error.to_string())))?
            .writer
            .take()
            .ok_or_else(|| {
                paddock_bindings::pitfast::paddock::store::Error::Conflict(
                    "writer finalized".into(),
                )
            })?;
        writer
            .commit()
            .await
            .map(to_paddock_object)
            .map_err(paddock_error)
    }

    async fn abort(
        &mut self,
        resource: wasmtime::component::Resource<paddock_bindings::pitfast::paddock::store::Writer>,
    ) -> std::result::Result<(), paddock_bindings::pitfast::paddock::store::Error> {
        let writer = self
            .table
            .get_mut(&resource)
            .map_err(|error| paddock_error(anyhow!(error.to_string())))?
            .writer
            .take();
        if let Some(writer) = writer {
            writer.abort().await.map_err(paddock_error)?;
        }
        Ok(())
    }

    async fn drop(
        &mut self,
        resource: wasmtime::component::Resource<paddock_bindings::pitfast::paddock::store::Writer>,
    ) -> wasmtime::Result<()> {
        let _ = self.table.delete(resource)?;
        Ok(())
    }
}

impl paddock_bindings::pitfast::paddock::auth::Host for HttpHostState {
    async fn verify(&mut self) -> std::result::Result<(), String> {
        verify_gateway_signature(self)
    }
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut padded = [0_u8; 64];
    if key.len() > padded.len() {
        padded.copy_from_slice(&Sha256::digest(key));
    } else {
        padded[..key.len()].copy_from_slice(key);
    }
    let mut inner = [0_u8; 64];
    let mut outer = [0_u8; 64];
    for (index, value) in padded.iter().enumerate() {
        inner[index] = *value ^ 0x36;
        outer[index] = *value ^ 0x5c;
    }
    let mut inner_input = Vec::with_capacity(64 + message.len());
    inner_input.extend_from_slice(&inner);
    inner_input.extend_from_slice(message);
    let inner_digest = Sha256::digest(inner_input);
    let mut outer_input = Vec::with_capacity(64 + inner_digest.len());
    outer_input.extend_from_slice(&outer);
    outer_input.extend_from_slice(&inner_digest);
    Sha256::digest(outer_input).into()
}

fn constant_time_hex_eq(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.bytes()
        .zip(right.bytes())
        .fold(0_u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}

fn header_value(state: &HttpHostState, name: &str) -> Option<String> {
    state
        .request_headers
        .iter()
        .find(|(header, _)| header.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.trim().to_owned())
}

fn aws_percent_encode(value: &[u8]) -> String {
    value
        .iter()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                (*byte as char).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect()
}

fn canonical_path_and_query(path: &str) -> Result<(String, String), String> {
    let (raw_path, raw_query) = path.split_once('?').unwrap_or((path, ""));
    let canonical_path = raw_path
        .split('/')
        .map(|part| aws_percent_encode(part.as_bytes()))
        .collect::<Vec<_>>()
        .join("/");
    let mut query = raw_query
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            (
                aws_percent_encode(key.as_bytes()),
                aws_percent_encode(value.as_bytes()),
            )
        })
        .collect::<Vec<_>>();
    query.sort();
    Ok((
        if canonical_path.is_empty() {
            "/".into()
        } else {
            canonical_path
        },
        query
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&"),
    ))
}

fn parse_amz_timestamp(value: &str) -> Result<i64, String> {
    if value.len() != 16
        || value.as_bytes()[8] != b'T'
        || value.as_bytes()[15] != b'Z'
        || !value[..8].bytes().all(|byte| byte.is_ascii_digit())
        || !value[9..15].bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("x-amz-date must use YYYYMMDDTHHMMSSZ".into());
    }
    let number = |start: usize, end: usize| {
        value[start..end]
            .parse::<u8>()
            .map_err(|_| "x-amz-date contains an invalid number".to_owned())
    };
    let year = value[..4]
        .parse::<i32>()
        .map_err(|_| "x-amz-date contains an invalid year".to_owned())?;
    let month = Month::try_from(number(4, 6)?)
        .map_err(|_| "x-amz-date contains an invalid month".to_owned())?;
    let date = Date::from_calendar_date(year, month, number(6, 8)?)
        .map_err(|_| "x-amz-date contains an invalid day".to_owned())?;
    let clock = ClockTime::from_hms(number(9, 11)?, number(11, 13)?, number(13, 15)?)
        .map_err(|_| "x-amz-date contains an invalid time".to_owned())?;
    Ok(date.with_time(clock).assume_utc().unix_timestamp())
}

fn verify_gateway_signature(state: &HttpHostState) -> std::result::Result<(), String> {
    let Some(auth) = state.paddock_auth.as_ref() else {
        return Ok(());
    };
    let authorization = header_value(state, "authorization")
        .ok_or_else(|| "SigV4 Authorization header is required".to_owned())?;
    let amz_date = header_value(state, "x-amz-date")
        .ok_or_else(|| "x-amz-date header is required".to_owned())?;
    let signed_timestamp = parse_amz_timestamp(&amz_date)?;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch".to_owned())?
        .as_secs() as i64;
    if (now - signed_timestamp).abs() > 900 {
        return Err("request timestamp is outside the 15 minute SigV4 window".into());
    }
    let parts = authorization
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .ok_or_else(|| "unsupported authorization scheme".to_owned())?
        .split(", ")
        .filter_map(|part| part.split_once('='))
        .collect::<std::collections::HashMap<_, _>>();
    let credential = parts
        .get("Credential")
        .ok_or_else(|| "SigV4 Credential is missing".to_owned())?;
    let credential_parts = credential.split('/').collect::<Vec<_>>();
    if credential_parts.len() != 5 || credential_parts[0] != auth.access_key {
        return Err("invalid SigV4 credential".into());
    }
    if credential_parts[1] != &amz_date[..8]
        || credential_parts[2] != auth.region
        || credential_parts[3] != "s3"
        || credential_parts[4] != "aws4_request"
    {
        return Err("invalid SigV4 credential scope".into());
    }
    let signed_headers = parts
        .get("SignedHeaders")
        .ok_or_else(|| "SigV4 SignedHeaders is missing".to_owned())?;
    let signature = parts
        .get("Signature")
        .ok_or_else(|| "SigV4 Signature is missing".to_owned())?;
    if signature.len() != 64 || !signature.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("invalid SigV4 signature".into());
    }
    let signed = signed_headers.split(';').collect::<Vec<_>>();
    if signed.windows(2).any(|window| window[0] >= window[1]) {
        return Err("SigV4 SignedHeaders must be sorted".into());
    }
    let mut canonical_headers = String::new();
    for name in &signed {
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err("invalid signed header name".into());
        }
        let value =
            header_value(state, name).ok_or_else(|| format!("signed header is missing: {name}"))?;
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        canonical_headers.push_str(&value.split_whitespace().collect::<Vec<_>>().join(" "));
        canonical_headers.push('\n');
    }
    if !signed.contains(&"host") || !signed.contains(&"x-amz-date") {
        return Err("host and x-amz-date must be signed".into());
    }
    let payload_hash = header_value(state, "x-amz-content-sha256")
        .unwrap_or_else(|| state.request_payload_hash.clone());
    if payload_hash != "UNSIGNED-PAYLOAD" && payload_hash != state.request_payload_hash {
        return Err("x-amz-content-sha256 does not match the request body".into());
    }
    let (canonical_uri, canonical_query) = canonical_path_and_query(&state.request_path)?;
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        state.request_method.to_uppercase(),
        canonical_uri,
        canonical_query,
        canonical_headers,
        signed_headers,
        payload_hash
    );
    let scope = format!("{}/{}/{}/aws4_request", &amz_date[..8], auth.region, "s3");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex_digest(Sha256::digest(canonical_request.as_bytes()))
    );
    let date_key = hmac_sha256(
        format!("AWS4{}", auth.secret_key).as_bytes(),
        &amz_date.as_bytes()[..8],
    );
    let region_key = hmac_sha256(&date_key, auth.region.as_bytes());
    let service_key = hmac_sha256(&region_key, b"s3");
    let signing_key = hmac_sha256(&service_key, b"aws4_request");
    let expected = hex_digest(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
    if !constant_time_hex_eq(&expected, signature) {
        return Err("signature does not match".into());
    }
    Ok(())
}

struct HostState {
    wasi: WasiP1Ctx,
    limits: RuntimeLimiter,
}

struct RuntimeLimiter {
    memory_limit: Option<usize>,
    memory_exceeded: bool,
}

impl RuntimeLimiter {
    fn new(memory_limit: Option<usize>) -> Self {
        Self {
            memory_limit,
            memory_exceeded: false,
        }
    }
}

impl ResourceLimiter for RuntimeLimiter {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> Result<bool> {
        if let Some(limit) = self.memory_limit
            && desired > limit
        {
            self.memory_exceeded = true;
            return Err(anyhow!(
                "linear memory growth to {desired} bytes exceeds configured limit of {limit} bytes"
            ));
        }
        Ok(true)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        _desired: usize,
        _maximum: Option<usize>,
    ) -> Result<bool> {
        Ok(true)
    }
}

fn status_for_error(store: &Store<HostState>, control: &ExecutionControl) -> ExecutionStatus {
    match control.signal() {
        ControlSignal::TimedOut => ExecutionStatus::TimedOut,
        ControlSignal::Cancelled => ExecutionStatus::Cancelled,
        ControlSignal::None if store.data().limits.memory_exceeded => {
            ExecutionStatus::MemoryLimitExceeded
        }
        ControlSignal::None => ExecutionStatus::RuntimeError,
    }
}

fn status_for_p2_error(store: &Store<P2HostState>, control: &ExecutionControl) -> ExecutionStatus {
    match control.signal() {
        ControlSignal::TimedOut => ExecutionStatus::TimedOut,
        ControlSignal::Cancelled => ExecutionStatus::Cancelled,
        ControlSignal::None if store.data().limits.memory_exceeded => {
            ExecutionStatus::MemoryLimitExceeded
        }
        ControlSignal::None => ExecutionStatus::RuntimeError,
    }
}

fn detect_format(bytes: &[u8]) -> Result<ArtifactFormat> {
    for payload in Parser::new(0).parse_all(bytes) {
        if let Payload::Version { encoding, .. } = payload? {
            return Ok(match encoding {
                Encoding::Module => ArtifactFormat::CoreModule,
                Encoding::Component => ArtifactFormat::Component,
            });
        }
    }
    bail!("invalid WebAssembly artifact")
}

fn core_linker(engine: &Engine) -> Result<CoreLinker<HostState>> {
    let mut linker = CoreLinker::new(engine);
    wasmtime_wasi::p1::add_to_linker_sync(&mut linker, |state: &mut HostState| &mut state.wasi)
        .context("failed to link WASI Preview 1 imports")?;
    Ok(linker)
}

struct ExecutionControl {
    token: CancellationToken,
    deadline: Option<Instant>,
    signal: AtomicU8,
}

impl ExecutionControl {
    fn new(token: CancellationToken, timeout: Option<Duration>) -> Self {
        Self {
            token,
            deadline: timeout.map(|duration| Instant::now() + duration),
            signal: AtomicU8::new(ControlSignal::None as u8),
        }
    }

    fn timed_out(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    fn signal(&self) -> ControlSignal {
        match self.signal.load(Ordering::Acquire) {
            value if value == ControlSignal::TimedOut as u8 => ControlSignal::TimedOut,
            value if value == ControlSignal::Cancelled as u8 => ControlSignal::Cancelled,
            _ => ControlSignal::None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ControlSignal {
    None = 0,
    TimedOut = 1,
    Cancelled = 2,
}

#[derive(Clone)]
struct CapturedOutput {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl CapturedOutput {
    fn new() -> Self {
        Self {
            bytes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn contents(&self) -> Vec<u8> {
        self.bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

struct CaptureWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl AsyncWrite for CaptureWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _context: &mut TaskContext<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend_from_slice(buffer);
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl IsTerminal for CapturedOutput {
    fn is_terminal(&self) -> bool {
        false
    }
}

impl StdoutStream for CapturedOutput {
    fn async_stream(&self) -> Box<dyn AsyncWrite + Send + Sync> {
        Box::new(CaptureWriter {
            bytes: Arc::clone(&self.bytes),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ArtifactFormat, ArtifactSource, CancellationToken, CompiledCacheConfig,
        CompiledCacheRestoreMode, CompiledCacheSource, ExecutionLimits, ExecutionRequest,
        ExecutionStatus, PitRuntime, PitRuntimeConfig, PreparedArtifact, RuntimeAllocationMode,
        WasmArtifact, cache_entry_paths, canonical_path_and_query, compiled_cache_entry,
        parse_amz_timestamp,
    };
    use std::time::Duration;
    use wasmtime::{Config, Engine};

    fn prepared_request(source: &str) -> (PreparedArtifact, ExecutionRequest) {
        let bytes = wat::parse_str(source).expect("test WAT must compile");
        let artifact = WasmArtifact::from_bytes(bytes);
        let prepared = PitRuntime::new()
            .expect("test engine should initialize")
            .prepare(artifact.clone())
            .expect("test module should compile");
        (prepared, ExecutionRequest::new(artifact))
    }

    #[test]
    fn path_artifact_and_request_defaults_are_valid() {
        let request = ExecutionRequest::new(WasmArtifact::from_path("hello.wasm"));
        assert!(request.args.is_empty());
        assert!(request.env.is_empty());
        assert!(request.limits.timeout.is_none());
        assert!(request.limits.memory_bytes.is_none());
        assert!(request.validate().is_ok());
    }

    #[test]
    fn sigv4_timestamp_requires_valid_utc_datetime() {
        assert!(parse_amz_timestamp("20260908T120000Z").is_ok());
        assert!(parse_amz_timestamp("20260230T120000Z").is_err());
        assert!(parse_amz_timestamp("20260908T120000+00").is_err());
    }

    #[test]
    fn sigv4_path_and_query_are_sorted_and_encoded() {
        let (path, query) = canonical_path_and_query("/a space/utf8?q=z value&b=2&a=1")
            .expect("canonical path should be valid");
        assert_eq!(path, "/a%20space/utf8");
        assert_eq!(query, "a=1&b=2&q=z%20value");
    }

    #[test]
    fn missing_path_artifact_fails_cleanly() {
        let runtime = PitRuntime::new().expect("test engine should initialize");
        let error = match runtime.prepare(WasmArtifact::from_path(
            "/pitfast/path-that-does-not-exist.wasm",
        )) {
            Ok(_) => panic!("missing artifact should fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("WASM file does not exist"));
    }

    #[test]
    fn bytes_artifacts_have_content_identity() {
        let first = WasmArtifact::from_bytes([1_u8, 2, 3]);
        let second = WasmArtifact::from_bytes([1_u8, 2, 3]);
        let third = WasmArtifact::from_bytes([1_u8, 2, 4]);
        assert_eq!(first.id(), second.id());
        assert_ne!(first.id(), third.id());
    }

    #[test]
    fn verified_path_artifact_reuses_supplied_digest_identity() {
        let artifact = WasmArtifact::from_path_with_digest(
            "/tmp/verified-artifact.wasm",
            "sha256:0123456789abcdef",
        );
        assert_eq!(artifact.id().as_str(), "sha256:0123456789abcdef");
        assert!(matches!(artifact.source(), ArtifactSource::Path(_)));
    }

    #[test]
    fn file_backed_restore_is_the_runtime_default() {
        assert_eq!(
            PitRuntimeConfig::default().compiled_cache_restore,
            CompiledCacheRestoreMode::FileBacked
        );
        assert_eq!(
            PitRuntimeConfig::default().allocation,
            RuntimeAllocationMode::OnDemand
        );
    }

    #[test]
    fn request_rejects_invalid_guest_strings() {
        let request = ExecutionRequest::new(WasmArtifact::from_bytes([]))
            .with_args(vec!["bad\0arg".to_owned()])
            .with_limits(ExecutionLimits {
                timeout: Some(Duration::from_millis(1)),
                memory_bytes: Some(1024),
            });
        assert!(request.validate().is_err());
    }

    #[test]
    fn request_rejects_malformed_environment_entries() {
        let artifact = WasmArtifact::from_bytes([]);
        assert!(
            ExecutionRequest::new(artifact.clone())
                .with_env(vec![(String::new(), "value".to_owned())])
                .validate()
                .is_err()
        );
        assert!(
            ExecutionRequest::new(artifact)
                .with_env(vec![("KEY".to_owned(), "bad\0value".to_owned())])
                .validate()
                .is_err()
        );
    }

    #[test]
    fn cancellation_token_is_shareable() {
        let token = CancellationToken::new();
        let clone = token.clone();
        assert!(!clone.is_cancelled());
        token.cancel();
        assert!(clone.is_cancelled());
    }

    #[test]
    fn compiled_cache_rejects_metadata_and_size_mismatches() {
        let root = std::env::temp_dir().join(format!(
            "pitfast-compiled-cache-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let cache = CompiledCacheConfig::new(&root);
        let bytes =
            wat::parse_str("(module (func (export \"f\")))").expect("test module should compile");
        let mut config = Config::new();
        config.cranelift_opt_level(wasmtime::OptLevel::Speed);
        let engine = Engine::new(&config).expect("test engine should initialize");
        let digest = super::artifact_digest(&bytes);
        let fingerprint =
            super::engine_fingerprint(&engine, ArtifactFormat::CoreModule, "wasi:cli/command");

        let first = compiled_cache_entry(
            &cache,
            &engine,
            &fingerprint,
            &digest,
            &bytes,
            ArtifactFormat::CoreModule,
            "wasi:cli/command",
            CompiledCacheRestoreMode::Bytes,
        )
        .expect("first compilation should succeed");
        assert_eq!(first.source, CompiledCacheSource::ColdCompiled);
        assert_eq!(
            PitRuntime::warm_digests(&cache),
            vec![super::artifact_digest(&bytes)]
        );
        let second = compiled_cache_entry(
            &cache,
            &engine,
            &fingerprint,
            &digest,
            &bytes,
            ArtifactFormat::CoreModule,
            "wasi:cli/command",
            CompiledCacheRestoreMode::Bytes,
        )
        .expect("cache restore should succeed");
        assert_eq!(second.source, CompiledCacheSource::WarmRestored);

        let (cache_path, metadata_path) = cache_entry_paths(&cache, &digest, &fingerprint);
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&metadata_path).expect("metadata should exist"))
                .expect("metadata should be JSON");
        metadata["fingerprint"] = serde_json::Value::String("wrong".to_owned());
        std::fs::write(
            &metadata_path,
            serde_json::to_vec(&metadata).expect("metadata should serialize"),
        )
        .expect("metadata should be writable");
        let third = compiled_cache_entry(
            &cache,
            &engine,
            &fingerprint,
            &digest,
            &bytes,
            ArtifactFormat::CoreModule,
            "wasi:cli/command",
            CompiledCacheRestoreMode::Bytes,
        )
        .expect("metadata mismatch should rebuild");
        assert_eq!(third.source, CompiledCacheSource::ColdCompiled);

        std::fs::write(&cache_path, [0_u8]).expect("compiled cache should be writable");
        let fourth = compiled_cache_entry(
            &cache,
            &engine,
            &fingerprint,
            &digest,
            &bytes,
            ArtifactFormat::CoreModule,
            "wasi:cli/command",
            CompiledCacheRestoreMode::Bytes,
        )
        .expect("size mismatch should rebuild");
        assert_eq!(fourth.source, CompiledCacheSource::ColdCompiled);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn corrupted_file_backed_component_cache_recompiles_from_wasm() {
        const HTTP_COMPONENT: &[u8] =
            include_bytes!("../../../../pit-crew/fixtures/c-http/.pit/build/c_http.wasm");
        let root = std::env::temp_dir().join(format!(
            "pitfast-component-cache-corruption-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let cache = CompiledCacheConfig::new(&root);
        let runtime = PitRuntime::with_config(PitRuntimeConfig {
            compiled_cache_restore: CompiledCacheRestoreMode::FileBacked,
            ..PitRuntimeConfig::default()
        })
        .expect("HTTP runtime should initialize");
        let artifact = WasmArtifact::from_bytes(HTTP_COMPONENT.to_vec());
        let first = runtime
            .prepare_http_cached(artifact.clone(), &cache)
            .expect("component should compile");
        assert_eq!(first.1.source, CompiledCacheSource::ColdCompiled);

        let digest = super::artifact_digest(HTTP_COMPONENT);
        let (cache_path, _) =
            cache_entry_paths(&cache, &digest, &runtime.http_component_fingerprint);
        let mut corrupted = std::fs::read(&cache_path).expect("compiled cache should exist");
        let corruption_index = 0;
        corrupted[corruption_index] ^= 0x5a;
        std::fs::write(&cache_path, corrupted).expect("test cache should be corruptible");

        let second = runtime
            .prepare_http_cached(artifact, &cache)
            .expect("corrupt derived cache should fall back to WASM");
        assert_eq!(second.1.source, CompiledCacheSource::ColdCompiled);
        assert!(second.1.compile_duration > Duration::ZERO);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn stdout_and_stderr_are_captured_separately() {
        let (prepared, request) = prepared_request(
            r#"
            (module
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (data (i32.const 64) "out\n")
              (data (i32.const 68) "err\n")
              (func (export "_start")
                (i32.store (i32.const 0) (i32.const 64))
                (i32.store (i32.const 4) (i32.const 4))
                (i32.store (i32.const 8) (i32.const 68))
                (i32.store (i32.const 12) (i32.const 4))
                (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 16))
                drop
                (call $fd_write (i32.const 2) (i32.const 8) (i32.const 1) (i32.const 16))
                drop))
            "#,
        );
        let result = prepared
            .execute(&request, CancellationToken::new())
            .expect("test module should execute");
        assert_eq!(result.status, ExecutionStatus::Completed);
        assert_eq!(result.stdout, b"out\n");
        assert_eq!(result.stderr, b"err\n");
    }

    #[test]
    fn wasi_arguments_preserve_order() {
        let (prepared, request) = prepared_request(
            r#"
            (module
              (import "wasi_snapshot_preview1" "args_sizes_get"
                (func $args_sizes_get (param i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "args_get"
                (func $args_get (param i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (func (export "_start")
                (call $args_sizes_get (i32.const 100) (i32.const 104)) drop
                (call $args_get (i32.const 0) (i32.const 64)) drop
                (i32.store (i32.const 300) (i32.load (i32.const 4)))
                (i32.store (i32.const 304) (i32.const 5))
                (call $fd_write (i32.const 1) (i32.const 300) (i32.const 1) (i32.const 316))
                drop
                (i32.store (i32.const 308) (i32.load (i32.const 8)))
                (i32.store (i32.const 312) (i32.const 5))
                (call $fd_write (i32.const 1) (i32.const 308) (i32.const 1) (i32.const 316))
                drop))
            "#,
        );
        let request = request.with_args(["hello".to_owned(), "world".to_owned()]);
        let result = prepared
            .execute(&request, CancellationToken::new())
            .expect("argument test should execute");
        assert_eq!(result.stdout, b"helloworld");
    }

    #[test]
    fn wasi_environment_is_explicit_only() {
        let (prepared, request) = prepared_request(
            r#"
            (module
              (import "wasi_snapshot_preview1" "environ_sizes_get"
                (func $environ_sizes_get (param i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "environ_get"
                (func $environ_get (param i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (data (i32.const 512) "empty")
              (data (i32.const 520) "nonempty")
              (func (export "_start")
                (call $environ_sizes_get (i32.const 100) (i32.const 104)) drop
                (call $environ_get (i32.const 0) (i32.const 128)) drop
                (if (i32.eqz (i32.load (i32.const 100)))
                  (then
                    (i32.store (i32.const 300) (i32.const 512))
                    (i32.store (i32.const 304) (i32.const 5)))
                  (else
                    (i32.store (i32.const 300) (i32.const 520))
                    (i32.store (i32.const 304) (i32.const 8)))
                )
                (call $fd_write (i32.const 1) (i32.const 300) (i32.const 1) (i32.const 308))
                drop))
            "#,
        );
        let result = prepared
            .execute(&request, CancellationToken::new())
            .expect("empty environment test should execute");
        assert_eq!(result.stdout, b"empty");

        let request = request.with_env([("MODE".to_owned(), "production".to_owned())]);
        let result = prepared
            .execute(&request, CancellationToken::new())
            .expect("explicit environment test should execute");
        assert_eq!(result.stdout, b"nonempty");
    }

    #[test]
    fn timeout_interrupts_cpu_loop() {
        let (prepared, request) =
            prepared_request(r#"(module (func (export "_start") (loop br 0)))"#);
        let request = request.with_limits(ExecutionLimits {
            timeout: Some(Duration::from_millis(20)),
            memory_bytes: None,
        });
        let result = prepared
            .execute(&request, CancellationToken::new())
            .expect("timeout should be a result");
        assert_eq!(result.status, ExecutionStatus::TimedOut);
    }

    #[test]
    fn memory_growth_is_limited_per_store() {
        let (prepared, request) = prepared_request(
            r#"
            (module
              (memory (export "memory") 1)
              (func (export "_start")
                (drop (memory.grow (i32.const 200)))))
            "#,
        );
        let request = request.with_limits(ExecutionLimits {
            timeout: None,
            memory_bytes: Some(8 * 1024 * 1024),
        });
        let result = prepared
            .execute(&request, CancellationToken::new())
            .expect("memory violation should be a result");
        assert_eq!(result.status, ExecutionStatus::MemoryLimitExceeded);
    }
}
