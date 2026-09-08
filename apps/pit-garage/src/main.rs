use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use clap::Parser;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use pit_circuit_core::{
    GarageCapabilities, GarageId, GarageSessionId, HeartbeatRequest, LogicalInvocationRequest,
    LogicalInvocationResponse, REMOTE_PROTOCOL_VERSION, RegisterGarageRequest,
    RegisterGarageResponse, RemoteExecutionError, RemoteExecutionRequest, RemoteExecutionResponse,
    RemoteExecutionTiming, RuntimeContract,
};
use pit_deployment::{ArtifactAcquirer, LocalArtifactStore};
use pit_lane_core::{ServiceInvocationRequest, ServiceInvocationResponse, ServiceInvoker};
use pit_node::{
    CancellationToken, CompiledCacheSource, ExecutionLimits, HttpRequest, PitHttpDispatcher,
};
use pit_paddock_core::{NamespaceId, PaddockBackend, PaddockGrant, PaddockObjectBackend};
use pit_paddock_factory::PaddockConfig;
use pit_paddock_fs::FilesystemPaddock;
use reqwest::Client;
use tokio::net::TcpListener;
use tokio::sync::{OnceCell, Semaphore};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    if !args.listen.ip().is_loopback() {
        bail!("Garage listener must be loopback-only in v0.9");
    }
    let agent = Arc::new(GarageAgent::new(&args)?);
    agent.register().await?;
    agent.spawn_heartbeat();
    let listener = TcpListener::bind(args.listen).await?;
    println!("PitFast Garage {} listening on {}", args.id, args.listen);
    agent.serve(listener).await
}

#[derive(Debug, Parser)]
#[command(name = "pit-garage", about = "PitFast Garage execution agent")]
struct Args {
    #[arg(long)]
    id: GarageId,
    #[arg(long, default_value = "127.0.0.1:7101")]
    listen: SocketAddr,
    #[arg(long, default_value = "http://127.0.0.1:7090")]
    circuit: String,
    #[arg(long, default_value = "http://127.0.0.1:7081")]
    pitlane: String,
    #[arg(long, default_value_t = 8)]
    lanes: usize,
    #[arg(long)]
    artifact_store: Option<PathBuf>,
    #[arg(long)]
    paddock_dir: Option<PathBuf>,
    #[arg(long, default_value = "local")]
    paddock: String,
    #[arg(long, default_value_t = 64)]
    max_pending: usize,
    #[arg(long, default_value_t = 16)]
    max_invocation_depth: u16,
    #[arg(long)]
    compiled_cache: Option<PathBuf>,
    #[arg(long, default_value_t = 2)]
    max_preparations: usize,
    /// Explicit namespace granted to the generic Paddock capability.
    #[arg(long)]
    paddock_namespace: Option<String>,
}

struct GarageAgent {
    id: GarageId,
    endpoint: String,
    circuit_endpoint: String,
    pitlane_endpoint: String,
    dispatcher: Arc<PitHttpDispatcher>,
    artifact_store: LocalArtifactStore,
    paddock_name: String,
    paddock: Option<Arc<dyn PaddockBackend>>,
    object_paddock: Option<Arc<dyn PaddockObjectBackend>>,
    paddock_grants: Vec<PaddockGrant>,
    warmups: WarmupMap,
    pending: Arc<Semaphore>,
    client: Client,
    blocking_client: reqwest::blocking::Client,
    session: RwLock<Option<GarageSessionId>>,
    max_invocation_depth: u16,
    remote_acquisitions: AtomicUsize,
    preparations: AtomicUsize,
    compilations: AtomicUsize,
    warm_restores: AtomicUsize,
    cache_publications: AtomicUsize,
    executions: AtomicUsize,
    preparation_budget: Arc<Semaphore>,
}

#[derive(Debug, Clone)]
struct WarmupReport {
    preparation: pit_node::PreparationReport,
    duration: Duration,
}

type WarmupMap = Mutex<HashMap<String, Arc<OnceCell<std::result::Result<WarmupReport, String>>>>>;

impl GarageAgent {
    fn new(args: &Args) -> Result<Self> {
        let artifact_store = LocalArtifactStore::new(match &args.artifact_store {
            Some(path) => path.clone(),
            None => LocalArtifactStore::default_root()?,
        });
        let paddock: Option<Arc<dyn PaddockBackend>> = if let Some(root) = &args.paddock_dir {
            Some(Arc::new(FilesystemPaddock::new(root.clone())))
        } else {
            let config = PaddockConfig::load(Path::new("."))?;
            Some(config.open(&args.paddock, None)?)
        };
        let object_paddock: Option<Arc<dyn PaddockObjectBackend>> =
            if let Some(root) = &args.paddock_dir {
                Some(Arc::new(FilesystemPaddock::new(root.clone())))
            } else {
                let config = PaddockConfig::load(Path::new("."))?;
                Some(config.open_objects(&args.paddock, None)?)
            };
        let paddock_grants = args
            .paddock_namespace
            .as_deref()
            .map(NamespaceId::new)
            .transpose()?
            .map(|namespace| vec![PaddockGrant::full(namespace)])
            .unwrap_or_default();
        let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
        let blocking_client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        let compiled_cache = match &args.compiled_cache {
            Some(path) => path.clone(),
            None => pit_node::CompiledCacheConfig::default_root()?,
        };
        Ok(Self {
            id: args.id.clone(),
            endpoint: args.listen.to_string(),
            circuit_endpoint: args.circuit.trim_end_matches('/').to_owned(),
            pitlane_endpoint: args.pitlane.trim_end_matches('/').to_owned(),
            dispatcher: Arc::new(PitHttpDispatcher::with_compiled_cache(
                args.lanes,
                &compiled_cache,
            )?),
            artifact_store,
            paddock_name: args.paddock.clone(),
            paddock,
            object_paddock,
            paddock_grants,
            warmups: Mutex::new(HashMap::new()),
            pending: Arc::new(Semaphore::new(args.max_pending)),
            client,
            blocking_client,
            session: RwLock::new(None),
            max_invocation_depth: args.max_invocation_depth,
            remote_acquisitions: AtomicUsize::new(0),
            preparations: AtomicUsize::new(0),
            compilations: AtomicUsize::new(0),
            warm_restores: AtomicUsize::new(0),
            cache_publications: AtomicUsize::new(0),
            executions: AtomicUsize::new(0),
            preparation_budget: Arc::new(Semaphore::new(args.max_preparations.max(1))),
        })
    }

    async fn register(&self) -> Result<()> {
        let mut contracts = BTreeSet::new();
        contracts.insert(RuntimeContract::WasiHttpProxy);
        let response = self
            .client
            .post(format!("{}/v1/garages/register", self.circuit_endpoint))
            .json(&RegisterGarageRequest {
                garage_id: self.id.clone(),
                endpoint: self.endpoint.clone(),
                capabilities: GarageCapabilities {
                    architecture: std::env::consts::ARCH.to_owned(),
                    runtime_contracts: contracts,
                    lane_capacity: self.dispatcher.execution_lanes(),
                },
            })
            .send()
            .await
            .context("Circuit registration request failed")?;
        if !response.status().is_success() {
            bail!("Circuit registration rejected: {}", response.text().await?);
        }
        let value: RegisterGarageResponse = response.json().await?;
        *self.session.write().unwrap() = Some(value.session_id);
        Ok(())
    }

    fn spawn_heartbeat(self: &Arc<Self>) {
        let agent = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if let Err(error) = agent.heartbeat().await {
                    tracing::warn!(%error, garage = %agent.id, "Garage heartbeat failed");
                    if let Err(register_error) = agent.register().await {
                        tracing::debug!(
                            error = %register_error,
                            garage = %agent.id,
                            "Garage re-registration is not yet accepted"
                        );
                    }
                }
            }
        });
    }

    async fn heartbeat(&self) -> Result<()> {
        let session = self
            .session
            .read()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow!("Garage is not registered"))?;
        let live = self.dispatcher.scheduler_snapshot();
        let response = self
            .client
            .post(format!(
                "{}/v1/garages/{}/heartbeat",
                self.circuit_endpoint, self.id
            ))
            .json(&HeartbeatRequest {
                session_id: session,
                total_lanes: live.lane_count,
                active_lanes: live.running,
                free_lanes: live.lane_count.saturating_sub(live.running),
                queue_depth: live.queued,
                accepting: self.pending.available_permits() > 0,
                local_artifacts: self.local_digests(),
                prepared_artifacts: self.prepared_digests(),
                warm_artifacts: self.warm_digests(),
                hot_artifacts: self.prepared_digests(),
                capabilities: self.capabilities(),
            })
            .send()
            .await?;
        if !response.status().is_success() {
            bail!("Circuit heartbeat rejected with {}", response.status())
        }
        Ok(())
    }

    fn capabilities(&self) -> GarageCapabilities {
        GarageCapabilities {
            architecture: std::env::consts::ARCH.to_owned(),
            runtime_contracts: BTreeSet::from([RuntimeContract::WasiHttpProxy]),
            lane_capacity: self.dispatcher.execution_lanes(),
        }
    }

    fn local_digests(&self) -> Vec<pit_paddock_core::ArtifactDigest> {
        let root = self.artifact_store.root().join("sha256");
        let mut values = Vec::new();
        let Ok(prefixes) = fs::read_dir(root) else {
            return values;
        };
        for prefix in prefixes.flatten() {
            let Ok(files) = fs::read_dir(prefix.path()) else {
                continue;
            };
            for file in files.flatten() {
                if file.path().extension().and_then(|value| value.to_str()) != Some("wasm") {
                    continue;
                }
                if let Some(stem) = file.path().file_stem().and_then(|value| value.to_str())
                    && let Ok(digest) = format!("sha256:{stem}").parse()
                {
                    values.push(digest);
                }
            }
        }
        values.sort();
        values.dedup();
        values
    }

    fn prepared_digests(&self) -> Vec<pit_paddock_core::ArtifactDigest> {
        self.dispatcher
            .prepared_keys()
            .into_iter()
            .filter_map(|key| key.parse().ok())
            .collect()
    }

    fn warm_digests(&self) -> Vec<pit_paddock_core::ArtifactDigest> {
        self.dispatcher
            .warm_keys()
            .into_iter()
            .filter_map(|key| key.parse().ok())
            .collect()
    }

    async fn warmup(
        &self,
        digest: &pit_paddock_core::ArtifactDigest,
        source_paddock: Option<&str>,
    ) -> Result<WarmupReport> {
        let key = digest.to_string();
        let cell = {
            let mut warmups = self.warmups.lock().unwrap();
            warmups
                .entry(key.clone())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        let result = cell
            .get_or_init(|| async {
                let result = self.warmup_once(digest, source_paddock).await;
                result.map_err(|error| error.to_string())
            })
            .await;
        match result {
            Ok(report) => Ok(report.clone()),
            Err(error) => {
                self.warmups.lock().unwrap().remove(&key);
                bail!("{error}")
            }
        }
    }

    async fn warmup_once(
        &self,
        digest: &pit_paddock_core::ArtifactDigest,
        source_paddock: Option<&str>,
    ) -> Result<WarmupReport> {
        let started = Instant::now();
        if self.dispatcher.has_prepared(&digest.to_string()) {
            return Ok(WarmupReport {
                preparation: pit_node::PreparationReport {
                    readiness: pit_node::ReadinessState::Hot,
                    source: CompiledCacheSource::WarmRestored,
                    ..pit_node::PreparationReport::default()
                },
                duration: started.elapsed(),
            });
        }
        let _preparation_permit = self
            .preparation_budget
            .clone()
            .acquire_owned()
            .await
            .context("preparation budget closed")?;
        let (_manifest, artifact_path) = match self.artifact_store.open(digest) {
            Ok(value) => value,
            Err(_) => {
                self.remote_acquisitions.fetch_add(1, Ordering::Relaxed);
                let paddock = self.paddock.as_ref().ok_or_else(|| {
                    anyhow!("artifact {digest} is not local and no Paddock is configured")
                })?;
                let acquisition = ArtifactAcquirer::default()
                    .ensure_local(
                        digest,
                        source_paddock.unwrap_or(&self.paddock_name),
                        paddock.as_ref(),
                        &self.artifact_store,
                    )
                    .await?;
                (acquisition.manifest, acquisition.artifact_path)
            }
        };
        self.preparations.fetch_add(1, Ordering::Relaxed);
        let dispatcher = Arc::clone(&self.dispatcher);
        let path = artifact_path.clone();
        let key = digest.to_string();
        let report = tokio::task::spawn_blocking(move || {
            dispatcher.register_with_report(
                key.clone(),
                pit_node::WasmArtifact::from_path_with_digest(&path, key),
            )
        })
        .await??;
        match report.source {
            CompiledCacheSource::ColdCompiled => {
                self.compilations.fetch_add(1, Ordering::Relaxed);
            }
            CompiledCacheSource::WarmRestored => {
                self.warm_restores.fetch_add(1, Ordering::Relaxed);
            }
        }
        if report.cache_publication_duration > Duration::ZERO {
            self.cache_publications.fetch_add(1, Ordering::Relaxed);
        }
        Ok(WarmupReport {
            preparation: report,
            duration: started.elapsed(),
        })
    }

    async fn execute(&self, request: RemoteExecutionRequest) -> RemoteExecutionResponse {
        let request_started = Instant::now();
        let base = RemoteExecutionResponse {
            protocol_version: REMOTE_PROTOCOL_VERSION,
            accepted: false,
            started: false,
            execution_id: request.execution_id.clone(),
            garage_id: self.id.clone(),
            lane_id: None,
            guest_status: None,
            headers: Vec::new(),
            body: Vec::new(),
            timing: None,
            error: None,
            error_message: None,
        };
        if request.protocol_version != REMOTE_PROTOCOL_VERSION {
            return failure(
                base,
                RemoteExecutionError::UnsupportedProtocol,
                "unsupported remote protocol",
            );
        }
        if request.runtime_contract != RuntimeContract::WasiHttpProxy {
            return failure(
                base,
                RemoteExecutionError::UnsupportedRuntime,
                "Garage currently executes wasi:http/proxy remotely",
            );
        }
        if request.visited_garages.iter().any(|id| id == &self.id) {
            return failure(
                base,
                RemoteExecutionError::InvocationCycle,
                "Garage already appears in invocation path",
            );
        }
        if request.invocation_depth > 16 {
            return failure(
                base,
                RemoteExecutionError::InvocationCycle,
                "maximum invocation depth exceeded",
            );
        }
        let Ok(_permit) = self.pending.clone().try_acquire_owned() else {
            return failure(
                base,
                RemoteExecutionError::Overloaded,
                "Garage admission limit exceeded",
            );
        };
        let warmup = match self
            .warmup(&request.artifact_digest, request.source_paddock.as_deref())
            .await
        {
            Ok(report) => report,
            Err(error) => {
                return failure(
                    base,
                    RemoteExecutionError::ArtifactUnavailable,
                    &error.to_string(),
                );
            }
        };
        let mut visited = request.visited_garages.clone();
        visited.push(self.id.clone());
        let http_request = HttpRequest {
            method: request.payload.method,
            path_and_query: request.payload.path_and_query,
            headers: request.payload.headers,
            body: request.payload.body,
            env: Vec::new(),
            allowed_tcp: Vec::new(),
            source_garage_id: Some(self.id.to_string()),
            visited_garages: visited.into_iter().map(|id| id.to_string()).collect(),
            application_id: request.application_id.clone(),
            release_id: request.release_id.clone(),
            paddock: self.object_paddock.clone(),
            paddock_grants: self.paddock_grants.clone(),
        };
        self.executions.fetch_add(1, Ordering::Relaxed);
        let dispatcher = Arc::clone(&self.dispatcher);
        let key = request.artifact_digest.to_string();
        let invoker: Arc<dyn ServiceInvoker> = Arc::new(GarageServiceInvoker {
            client: self.blocking_client.clone(),
            endpoint: self.pitlane_endpoint.clone(),
            garage_id: self.id.clone(),
            max_depth: self.max_invocation_depth,
        });
        let result = tokio::task::spawn_blocking(move || {
            dispatcher.execute_http_with_invoker(
                &key,
                http_request,
                ExecutionLimits::default(),
                CancellationToken::new(),
                Some(invoker),
                request.invocation_depth,
            )
        })
        .await;
        let result = match result {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                return failure_started(
                    base,
                    RemoteExecutionError::GuestFailure,
                    &error.to_string(),
                );
            }
            Err(error) => {
                return failure_started(base, RemoteExecutionError::Internal, &error.to_string());
            }
        };
        let mut response = base;
        response.accepted = true;
        response.started = true;
        response.execution_id = result.execution_id.to_string();
        response.lane_id = Some(result.lane_id.0);
        match result.response {
            Some(value) => {
                response.guest_status = Some(value.status);
                response.headers = value.headers;
                response.body = value.body;
            }
            None => {
                response.error = Some(
                    if result
                        .error
                        .as_deref()
                        .is_some_and(|e| e.contains("timeout"))
                    {
                        RemoteExecutionError::Timeout
                    } else {
                        RemoteExecutionError::GuestFailure
                    },
                );
                response.error_message = result.error;
            }
        }
        response.timing = Some(RemoteExecutionTiming {
            preparation_us: warmup.duration.as_micros(),
            compile_us: warmup.preparation.compile_duration.as_micros(),
            restore_us: warmup.preparation.restore_duration.as_micros(),
            prepared_lookup_us: result.prepared_lookup.as_micros(),
            scheduler_queue_us: result.queued_for.as_micros(),
            guest_started_us: result.guest_started_after_request.as_micros(),
            guest_execution_us: result.duration.as_micros(),
            total_us: request_started.elapsed().as_micros(),
        });
        response
    }

    async fn serve(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        loop {
            let (stream, _) = listener.accept().await?;
            let agent = Arc::clone(&self);
            tokio::spawn(async move {
                let service = service_fn(move |request| {
                    let agent = Arc::clone(&agent);
                    async move { Ok::<_, hyper::Error>(agent.handle(request).await) }
                });
                if let Err(error) = hyper::server::conn::http1::Builder::new()
                    .keep_alive(true)
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                {
                    tracing::debug!(%error, "Garage connection ended");
                }
            });
        }
    }

    async fn handle(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let path = request.uri().path().to_owned();
        let method = request.method().clone();
        let body = match request.into_body().collect().await {
            Ok(body) => body.to_bytes(),
            Err(error) => return error_response(StatusCode::BAD_REQUEST, &error.to_string()),
        };
        match (method, path.as_str()) {
            (Method::POST, "/v1/execute") => {
                match serde_json::from_slice::<RemoteExecutionRequest>(&body) {
                    Ok(request) => json_response(StatusCode::OK, &self.execute(request).await)
                        .unwrap_or_else(|error| {
                            error_response(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string())
                        }),
                    Err(error) => error_response(StatusCode::BAD_REQUEST, &error.to_string()),
                }
            }
            (Method::GET, "/v1/runtime") => {
                let live = self.dispatcher.scheduler_snapshot();
                json_response(StatusCode::OK, &serde_json::json!({"garage_id": self.id, "total_lanes": live.lane_count, "active_lanes": live.running, "free_lanes": live.lane_count.saturating_sub(live.running), "queue_depth": live.queued, "grid_utilization": self.dispatcher.grid_utilization(), "local_artifacts": self.local_digests(), "warm_artifacts": self.warm_digests(), "prepared_artifacts": self.prepared_digests(), "hot_artifacts": self.prepared_digests(), "remote_acquisitions": self.remote_acquisitions.load(Ordering::Relaxed), "preparations": self.preparations.load(Ordering::Relaxed), "compilations": self.compilations.load(Ordering::Relaxed), "warm_restores": self.warm_restores.load(Ordering::Relaxed), "cache_publications": self.cache_publications.load(Ordering::Relaxed), "executions": self.executions.load(Ordering::Relaxed)})).unwrap()
            }
            _ => error_response(StatusCode::NOT_FOUND, "unknown Garage operation"),
        }
    }
}

#[derive(Clone)]
struct GarageServiceInvoker {
    client: reqwest::blocking::Client,
    endpoint: String,
    garage_id: GarageId,
    max_depth: u16,
}

impl ServiceInvoker for GarageServiceInvoker {
    fn invoke(&self, request: ServiceInvocationRequest) -> Result<ServiceInvocationResponse> {
        if request.depth > self.max_depth {
            bail!("maximum invocation depth {} exceeded", self.max_depth)
        }
        let target = request.target;
        let mut headers = request
            .headers
            .iter()
            .map(|(name, value)| {
                (
                    name.to_string(),
                    value.to_str().unwrap_or_default().to_owned(),
                )
            })
            .collect::<Vec<_>>();
        if !headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("host"))
        {
            headers.push(("host".to_owned(), target.service.to_string()));
        }
        let response = self
            .client
            .post(format!("{}/v1/internal/invoke", self.endpoint))
            .json(&LogicalInvocationRequest {
                protocol_version: REMOTE_PROTOCOL_VERSION,
                request_id: format!("nested-{}", pit_lane_core::RequestId::next()),
                parent_execution_id: request.parent_execution_id,
                target_service: target.service.to_string(),
                method: request.method.to_string(),
                path_and_query: target.path_and_query,
                headers,
                body: request.body.to_vec(),
                invocation_depth: request.depth,
                source_garage_id: Some(self.garage_id.clone()),
                visited_garages: request
                    .visited_garages
                    .into_iter()
                    .filter_map(|value| value.parse().ok())
                    .collect(),
                application_id: request.application_id,
                release_id: request.release_id,
            })
            .send()
            .context("PitLane internal invocation request failed")?;
        if !response.status().is_success() {
            bail!(
                "PitLane internal invocation rejected with {}",
                response.status()
            )
        }
        let response: LogicalInvocationResponse = response.json()?;
        if !response.accepted || !response.started {
            bail!(
                "nested service invocation rejected: {}",
                response
                    .error_message
                    .unwrap_or_else(|| "unknown error".into())
            )
        }
        let status = response
            .guest_status
            .ok_or_else(|| anyhow!("nested invocation returned no guest status"))?;
        Ok(ServiceInvocationResponse {
            status: StatusCode::from_u16(status)?,
            headers: response
                .headers
                .into_iter()
                .filter_map(|(name, value)| Some((name.parse().ok()?, value.parse().ok()?)))
                .collect(),
            body: Bytes::from(response.body),
            child_execution_id: response.child_execution_id,
        })
    }
}

fn failure(
    mut response: RemoteExecutionResponse,
    error: RemoteExecutionError,
    message: &str,
) -> RemoteExecutionResponse {
    response.error = Some(error);
    response.error_message = Some(message.to_owned());
    response
}
fn failure_started(
    mut response: RemoteExecutionResponse,
    error: RemoteExecutionError,
    message: &str,
) -> RemoteExecutionResponse {
    response.accepted = true;
    response.started = true;
    failure(response, error, message)
}

fn json_response<T: serde::Serialize>(
    status: StatusCode,
    value: &T,
) -> Result<Response<Full<Bytes>>> {
    Ok(Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(serde_json::to_vec(value)?)))?)
}
fn error_response(status: StatusCode, message: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        .body(Full::new(Bytes::from(message.to_owned())))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
}
