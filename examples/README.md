# WASI Preview 1 examples

The local MVP uses WASI Preview 1 core modules. Add the target once:

~~~bash
rustup target add wasm32-wasip1
~~~

Build the lightweight Hello World module:

~~~bash
rustc --target wasm32-wasip1 -O examples/hello/src/main.rs -o examples/hello.wasm
~~~

Build the deterministic CPU-bound benchmark workload:

~~~bash
rustc --target wasm32-wasip1 -O examples/cpu-burn/src/main.rs -o examples/cpu-burn.wasm
~~~

Run Hello World from the repository root:

~~~bash
cargo run -p pit -- run ./examples/hello.wasm
cargo run -p pit -- run ./examples/hello.wasm --concurrency 100
~~~

Run the CPU benchmark in release mode:

~~~bash
cargo run --release -p pit -- run ./examples/cpu-burn.wasm --concurrency 100
cargo run --release -p pit -- bench ./examples/cpu-burn.wasm
~~~

cpu-burn performs deterministic integer mixing for 50 million iterations by
default. The source accepts an optional first WASI argument when a host supplies
one; the current pit CLI uses the documented default. It uses black_box on the
checksum so the computation cannot be optimized away and does not sleep or wait
on I/O.

Every invocation gets a fresh Wasmtime Store. The compiled module is reused by
the local node, and the Hello World module's stdout/stderr are connected to the
host process.
