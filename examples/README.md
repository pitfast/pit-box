# Hello WASM example

The local MVP uses WASI Preview 1 modules. Build the example with the Rust
WASI target:

```bash
rustup target add wasm32-wasip1
rustc --target wasm32-wasip1 -O examples/hello/src/main.rs -o examples/hello.wasm
```

Then run it from the repository root:

```bash
cargo run -p pit -- run ./examples/hello.wasm
cargo run -p pit -- run ./examples/hello.wasm --concurrency 100
```

Each invocation gets a fresh Wasmtime Store. The compiled module is reused by
the local node, and its stdout/stderr are connected to the host process.
