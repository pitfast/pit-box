# PitFast pit-box

PitFast is a WebAssembly-native execution runtime.

PitBox is the local execution engine: it owns Wasmtime preparation, isolated
stores, WASI capabilities, limits, and the bounded scheduler. It supports WASI
Preview 1 core modules, WASI Preview 2 command components, and
`wasi:http/proxy` components. Network ingress and logical service routing live
in the independent PitLane repository.

No pods. No containers. Just isolated WASM execution scheduled across available
CPU lanes.

This repository is the local execution foundation. It embeds Wasmtime, prepares
one WASI Preview 1 module or WASI Preview 2 component once, and schedules independent invocations
across a bounded set of host threads. It does not create a process per
invocation.

## PitFast execution model

~~~text
.wasm
  ↓
compiled once
  ↓
many isolated executions
  ↓
scheduler
  ↓
execution lanes
  ↓
host CPU
~~~

PitFast concurrency means running independent WASM invocations concurrently. It
does not magically parallelize one sequential WASM invocation; multicore
utilization comes from having multiple isolated executions active at once.

## PitFast pit-box responsibilities

pit-box is the safe local WASM execution system. It validates an execution
request, prepares and reuses a compiled artifact, schedules isolated Stores,
enforces execution limits, and returns structured results.

pit-box is not the PitFast developer CLI, PitCrew builder, a cluster controller,
a load balancer, or a container runtime. PitCrew turns source code into
standardized artifacts; this repository executes already-built WASI Preview 1
modules and WASI Preview 2 command/HTTP components.

PitBox supports manifest runtime ABIs wasi-preview1 and wasi-preview2. P1 uses
the `_start` core-module entrypoint; P2 uses the `wasi:cli/command` component
world. Raw WasmArtifact values remain supported and do not require a .pit
directory. The scheduler is ABI-independent.

The runtime prepares a `Module` or `Component` once, reuses immutable Engine,
linker, and compiled-artifact state, then creates a new Store, WASI context,
resource table, output pipes, and limits for every invocation. P1 and command
P2 use the synchronous engine; the `wasi:http/proxy` path uses a dedicated
async-support engine because Wasmtime 39 does not permit synchronous instance
calls on an async engine:

~~~text
PreparedArtifact
   ├── PreparedModule     (WASI Preview 1)
   └── PreparedComponent  (WASI Preview 2)
             ↓
       common ExecutionResult
             ↓
       ABI-independent scheduler
~~~

P2 supports command-style and `wasi:http/proxy` components. HTTP execution uses
the Wasmtime 39 asynchronous component path (`ProxyPre::instantiate_async` and
`call_handle`) so bounded outgoing response bodies can be consumed fully.
PitBox also hosts the minimal `pitfast:service@0.1.0` invocation import when
configured by PitLane. Filesystem preopens and unrestricted network capability
are not enabled by default. The HTTP outgoing adapter intercepts registered
logical authorities; raw TCP, DNS, and external HTTP remain denied unless the
request carries an exact host-owned resource endpoint allowlist.

Each request can provide guest arguments, explicit environment variables,
captured stdout/stderr, a timeout, and a per-Store linear-memory limit. Host
environment inheritance, stdin inheritance, filesystem exposure, and network
exposure are disabled by default.

Nested logical service calls are handled inline by the PitLane-backed host
invoker rather than recursively submitted to the scheduler. This prevents all
execution lanes from waiting on child calls.

## Execution readiness and the PitStop fast path

For HTTP components, PitBox keeps the portable artifact authoritative and
treats machine-ready compilation as a disposable Garage-local cache:

~~~text
canonical .wasm  →  COLD
compiled .cwasm  →  WARM
PreparedArtifact  →  HOT
active lane       →  RUN
~~~

The compiled cache is keyed by the artifact digest and a Wasmtime 39 engine
compatibility fingerprint (host architecture, format/world, compiler
configuration hash, and cache schema). The normal Garage path uses Wasmtime's
file-backed component deserializer; the byte-buffer path remains an explicit
compatibility/A-B option. Cache metadata is checked before deserialization;
missing, corrupt, or incompatible entries fall back to the canonical `.wasm`.
Cache publication is temporary-file plus fsync plus atomic rename, so it
cannot become deployment authority or expose a partial entry.

The deployment plane passes its already-verified immutable digest to
`WasmArtifact::from_path_with_digest`. This avoids re-hashing large canonical
artifacts during warm cache lookup without changing the canonical-artifact
trust boundary. Callers that only have a path still get memoized digest
calculation.

Garage artifact acquisition, verification, compilation, compiled-cache restore,
and PreparedArtifact construction run under a bounded preparation budget before
PitBox scheduler admission. Execution Lanes are reserved only after the
artifact is execution-ready. A per-digest singleflight prevents concurrent
requests from repeating the same warmup. The scheduler reports queue wait as
Scheduling Gap and exposes logical Grid Utilization; neither metric represents
physical CPU utilization.

PitFast does not expose a separate `PRIMED` readiness state in this runtime:
the safe reusable HTTP representation is already the HOT `ProxyPre` plus
immutable compiled component. Pre-instantiating guest Stores would create
replica-like state and threaten per-invocation isolation, while the shared
Wasmtime allocator is infrastructure rather than service readiness. Pooling
allocator configurations were evaluated experimentally and remain opt-in.

## Quick start

Build the workspace:

~~~bash
cargo build --workspace
~~~

Build the example modules (once):

~~~bash
rustup target add wasm32-wasip1
rustc --target wasm32-wasip1 -O examples/hello/src/main.rs -o examples/hello.wasm
rustc --target wasm32-wasip1 -O examples/cpu-burn/src/main.rs -o examples/cpu-burn.wasm
rustc --target wasm32-wasip1 -O examples/env/src/main.rs -o examples/env.wasm
rustc --target wasm32-wasip1 -O examples/infinite-loop/src/main.rs -o examples/infinite-loop.wasm
rustc --target wasm32-wasip1 -O examples/memory-grow/src/main.rs -o examples/memory-grow.wasm
rustc --target wasm32-wasip1 -O examples/exit/src/main.rs -o examples/exit.wasm
~~~

Build the standalone developer CLI from the sibling pit-cli repository, then
inspect the local execution hardware:

~~~bash
cargo run --release --manifest-path ../pit-cli/Cargo.toml -- system
~~~

Run one or many independent invocations:

~~~bash
cargo run --release --manifest-path ../pit-cli/Cargo.toml -- run ./examples/hello.wasm
cargo run --release --manifest-path ../pit-cli/Cargo.toml -- run ./examples/hello.wasm --concurrency 100
cargo run --release --manifest-path ../pit-cli/Cargo.toml -- run ./examples/hello.wasm -- hello world
cargo run --release --manifest-path ../pit-cli/Cargo.toml -- run ./examples/env.wasm --env MODE=production --env REGION=jakarta
cargo run --release --manifest-path ../pit-cli/Cargo.toml -- run ./examples/infinite-loop.wasm --timeout 500ms
cargo run --release --manifest-path ../pit-cli/Cargo.toml -- run ./examples/memory-grow.wasm --memory 8MiB
~~~

Run the CPU-bound benchmark with release optimizations:

~~~bash
cargo run --release --manifest-path ../pit-cli/Cargo.toml -- run ./examples/cpu-burn.wasm --concurrency 100
cargo run --release --manifest-path ../pit-cli/Cargo.toml -- bench ./examples/cpu-burn.wasm
~~~

Use --verbose on pit run to print queued, started, completed, and failed
execution events.

## Current scope

The current milestone includes local execution plus the v0.9 Circuit/Garage
placement path, with scheduler telemetry, queue/backpressure visibility, and
the v0.10 PitStop readiness path. Garage startup restores WARM compiled entries
lazily; it does not load every cache entry into memory.

Future work includes multinode routing, adaptive execution, PitCrew warm
capabilities, and a telemetry cockpit. Networking, containers, Kubernetes,
artifact registries, authentication, and production cluster behavior are not
part of this milestone.

## Development checks

~~~bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo build --workspace
cargo build --workspace --release
~~~
