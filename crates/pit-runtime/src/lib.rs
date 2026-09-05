//! Reusable, constrained WASI Preview 1 execution for PitFast.
//!
//! The runtime owns one shared Wasmtime engine, prepares artifacts once, and
//! creates a fresh Store and WASI context for every invocation.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, anyhow, bail};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWrite;
use wasmtime::{Config, Engine, Linker, Module, ResourceLimiter, Store, UpdateDeadline};
use wasmtime_wasi::cli::{IsTerminal, StdoutStream};
use wasmtime_wasi::{I32Exit, WasiCtxBuilder, p1::WasiP1Ctx};

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

/// A WASI Preview 1 artifact independent of CLI path handling.
#[derive(Debug, Clone)]
pub struct WasmArtifact {
    id: ArtifactId,
    source: ArtifactSource,
}

impl WasmArtifact {
    pub fn from_path(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        Self {
            id: ArtifactId(format!("path:{}", path.display())),
            source: ArtifactSource::Path(path),
        }
    }

    pub fn from_bytes(bytes: impl Into<Arc<[u8]>>) -> Self {
        let bytes = bytes.into();
        let digest = Sha256::digest(&bytes);
        Self {
            id: ArtifactId(format!("sha256:{digest:x}")),
            source: ArtifactSource::Bytes(bytes),
        }
    }

    pub fn id(&self) -> &ArtifactId {
        &self.id
    }

    pub fn source(&self) -> &ArtifactSource {
        &self.source
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
    ticker: Arc<EpochTicker>,
}

impl PitRuntime {
    pub fn new() -> Result<Self> {
        let mut config = Config::new();
        config.epoch_interruption(true);
        let engine = Engine::new(&config).context("failed to create Wasmtime engine")?;
        let ticker = EpochTicker::start(engine.clone())?;
        Ok(Self { engine, ticker })
    }

    pub fn prepare(&self, artifact: WasmArtifact) -> Result<PreparedArtifact> {
        let module = match artifact.source() {
            ArtifactSource::Path(path) => {
                if !path.exists() {
                    bail!("WASM file does not exist: {}", path.display());
                }
                if !path.is_file() {
                    bail!("WASM path is not a file: {}", path.display());
                }
                Module::from_file(&self.engine, path).with_context(|| {
                    format!(
                        "WASM compilation failed for '{}'; expected a valid WASI Preview 1 module",
                        path.display()
                    )
                })?
            }
            ArtifactSource::Bytes(bytes) => Module::from_binary(&self.engine, bytes).with_context(|| {
                format!(
                    "WASM compilation failed for artifact '{}'; expected a valid WASI Preview 1 module",
                    artifact.id
                )
            })?,
        };
        Ok(PreparedArtifact {
            engine: self.engine.clone(),
            module,
            artifact,
            ticker: Arc::clone(&self.ticker),
        })
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<PreparedArtifact> {
        Self::new()?.prepare(WasmArtifact::from_path(path))
    }
}

/// Compiled artifact ready for isolated Store creation.
#[derive(Clone)]
pub struct PreparedArtifact {
    engine: Engine,
    module: Module,
    artifact: WasmArtifact,
    ticker: Arc<EpochTicker>,
}

impl PreparedArtifact {
    pub fn artifact(&self) -> &WasmArtifact {
        &self.artifact
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

        let mut linker = Linker::new(&self.engine);
        wasmtime_wasi::p1::add_to_linker_sync(&mut linker, |state: &mut HostState| &mut state.wasi)
            .context("failed to link WASI Preview 1 imports")?;

        let call_result = match linker.instantiate(&mut store, &self.module) {
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
        CancellationToken, ExecutionLimits, ExecutionRequest, ExecutionStatus, PitRuntime,
        PreparedArtifact, WasmArtifact,
    };
    use std::time::Duration;

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
