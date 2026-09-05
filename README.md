# PitFast pit-box

PitFast is a WebAssembly-native execution runtime.

No pods. No containers. Just isolated WASM execution scheduled across available
CPU lanes.

This repository is the local execution foundation. The MVP embeds Wasmtime,
compiles one WASI Preview 1 module once, and schedules independent invocations
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

pit-box is not the PitCrew builder, a cluster controller, a load balancer, or a
container runtime. PitCrew will eventually turn source code into standardized
artifacts; this repository executes already-built WASI Preview 1 modules.

Each request can provide guest arguments, explicit environment variables,
captured stdout/stderr, a timeout, and a per-Store linear-memory limit. Host
environment inheritance, stdin inheritance, filesystem exposure, and network
exposure are disabled by default.

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

Inspect the local execution hardware:

~~~bash
cargo run -p pit -- system
~~~

Run one or many independent invocations:

~~~bash
cargo run -p pit -- run ./examples/hello.wasm
cargo run -p pit -- run ./examples/hello.wasm --concurrency 100
cargo run -p pit -- run ./examples/hello.wasm -- hello world
cargo run -p pit -- run ./examples/env.wasm --env MODE=production --env REGION=jakarta
cargo run -p pit -- run ./examples/infinite-loop.wasm --timeout 500ms
cargo run -p pit -- run ./examples/memory-grow.wasm --memory 8MiB
~~~

Run the CPU-bound benchmark with release optimizations:

~~~bash
cargo run --release -p pit -- run ./examples/cpu-burn.wasm --concurrency 100
cargo run --release -p pit -- bench ./examples/cpu-burn.wasm
~~~

Use --verbose on pit run to print queued, started, completed, and failed
execution events.

## Current scope

The current milestone is single-node local execution with scheduler telemetry,
queue/backpressure visibility, and a CPU-bound concurrency benchmark.

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
