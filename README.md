# PitFast `pit-box`

PitFast is a WebAssembly-native execution runtime.

No pods. No containers. Just isolated WASM execution scheduled across available
CPU lanes.

This repository is the local execution foundation. The MVP embeds Wasmtime,
compiles one WASI Preview 1 module once, and schedules independent invocations
across a bounded set of host threads. It does not create a process per
invocation.

## Quick start

Build the workspace:

```bash
cargo build --workspace
```

Build the example module (once):

```bash
rustup target add wasm32-wasip1
rustc --target wasm32-wasip1 -O examples/hello/src/main.rs -o examples/hello.wasm
```

Inspect the local execution hardware:

```bash
cargo run -p pit -- system
```

Run one or many independent invocations:

```bash
cargo run -p pit -- run ./examples/hello.wasm
cargo run -p pit -- run ./examples/hello.wasm --concurrency 100
```

## Current scope

Current milestone: single-node local execution.

Future work includes multinode routing, adaptive execution, PitCrew warm
capabilities, and a telemetry cockpit. Networking, containers, Kubernetes,
artifact registries, authentication, and production cluster behavior are not
part of this milestone.

## Development checks

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo build --workspace
```
