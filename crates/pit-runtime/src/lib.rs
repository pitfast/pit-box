//! The embedded Wasmtime runtime used by PitFast.
//!
//! The MVP intentionally supports WASI Preview 1 modules. The module is compiled
//! once when [`PitRuntime::from_file`] is called, then each invocation gets a new
//! Wasmtime [`wasmtime::Store`]. This keeps execution state isolated while allowing
//! the compiled artifact and engine to be shared across host threads.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use wasmtime::{Engine, Linker, Module, Store};
use wasmtime_wasi::{WasiCtxBuilder, p1::WasiP1Ctx};

struct HostState {
    wasi: WasiP1Ctx,
}

/// A compiled, reusable WASI Preview 1 WebAssembly artifact.
pub struct PitRuntime {
    engine: Engine,
    module: Module,
    artifact_path: PathBuf,
}

impl PitRuntime {
    /// Loads and compiles a WASI Preview 1 module exactly once.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            bail!("WASM file does not exist: {}", path.display());
        }
        if !path.is_file() {
            bail!("WASM path is not a file: {}", path.display());
        }

        let engine = Engine::default();
        let module = Module::from_file(&engine, path).with_context(|| {
            format!(
                "WASM compilation failed for '{}'; expected a valid WASI Preview 1 module",
                path.display()
            )
        })?;

        Ok(Self {
            engine,
            module,
            artifact_path: path.to_path_buf(),
        })
    }

    /// Returns the path used to load this runtime's artifact.
    pub fn artifact_path(&self) -> &Path {
        &self.artifact_path
    }

    /// Runs the module's WASI `_start` entry point in an isolated store.
    pub fn run_once(&self) -> Result<()> {
        let mut linker = Linker::new(&self.engine);
        wasmtime_wasi::p1::add_to_linker_sync(&mut linker, |state: &mut HostState| &mut state.wasi)
            .context("failed to link WASI Preview 1 imports")?;

        let mut wasi_builder = WasiCtxBuilder::new();
        wasi_builder.inherit_stdout().inherit_stderr();
        let wasi = wasi_builder.build_p1();
        let mut store = Store::new(&self.engine, HostState { wasi });
        let start = linker
            .instantiate(&mut store, &self.module)
            .context("WASM instantiation failed")?
            .get_typed_func::<(), ()>(&mut store, "_start")
            .context("WASM module does not expose a compatible WASI _start function")?;

        start
            .call(&mut store, ())
            .context("WASM execution failed")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::PitRuntime;

    #[test]
    fn missing_artifact_is_reported() {
        let result = PitRuntime::from_file("does-not-exist.wasm");
        assert!(matches!(
            result,
            Err(error) if error.to_string().contains("WASM file does not exist")
        ));
    }
}
