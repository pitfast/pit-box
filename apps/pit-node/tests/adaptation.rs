//! Runtime smoke test for Components produced by an external application
//! interface adapter. Set `PITFAST_ADAPTER_WASM` to a built v0.11 fixture.

use pit_node::{CancellationToken, ExecutionLimits, HttpRequest, PitHttpDispatcher, WasmArtifact};

#[test]
fn adapted_http_component_executes_through_generic_pitbox() {
    let Some(path) = std::env::var_os("PITFAST_ADAPTER_WASM") else {
        eprintln!("skipping adapter runtime smoke test: PITFAST_ADAPTER_WASM is unset");
        return;
    };
    let dispatcher = PitHttpDispatcher::with_lanes(2).expect("dispatcher should initialize");
    eprintln!("adapter smoke: dispatcher ready");
    dispatcher
        .register("adapted", WasmArtifact::from_path(path))
        .expect("adapter-produced Component should prepare");
    eprintln!("adapter smoke: component prepared");
    let result = dispatcher
        .execute_http(
            "adapted",
            HttpRequest {
                method: "GET".into(),
                path_and_query: "/language".into(),
                headers: vec![("host".into(), "pitfast".into())],
                body: Vec::new(),
                env: Vec::new(),
                allowed_tcp: Vec::new(),
                source_garage_id: None,
                visited_garages: Vec::new(),
            },
            ExecutionLimits::default(),
            CancellationToken::new(),
        )
        .expect("adapted Component should execute");
    eprintln!("adapter smoke: request completed");
    let response = result
        .response
        .unwrap_or_else(|| panic!("guest should return a response: {:?}", result.error));
    assert_eq!(response.status, 200);
    let expected =
        std::env::var("PITFAST_ADAPTER_EXPECTED").unwrap_or_else(|_| "mystery-asgi".into());
    assert_eq!(response.body, expected.as_bytes());
    if std::env::var_os("PITFAST_ADAPTER_TEST_ECHO").is_some() {
        let echo_result = dispatcher
            .execute_http(
                "adapted",
                HttpRequest {
                    method: "POST".into(),
                    path_and_query: "/echo?source=adapter".into(),
                    headers: vec![("host".into(), "pitfast".into())],
                    body: b"adapter-body".to_vec(),
                    env: Vec::new(),
                    allowed_tcp: Vec::new(),
                    source_garage_id: None,
                    visited_garages: Vec::new(),
                },
                ExecutionLimits::default(),
                CancellationToken::new(),
            )
            .expect("adapted POST should execute");
        let echo = echo_result.response.unwrap_or_else(|| {
            panic!(
                "adapted POST should return a response: {:?}",
                echo_result.error
            )
        });
        let expected_status = std::env::var("PITFAST_ADAPTER_ECHO_STATUS")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(200);
        assert_eq!(echo.status, expected_status);
        assert_eq!(echo.body, b"adapter-body");
    }
}
