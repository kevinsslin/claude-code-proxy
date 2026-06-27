// End-to-end tests for local server health, provider routing, Kimi, Codex HTTP,
// and Codex WebSocket through in-process mock upstreams with isolated auth.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::response::Response;
use claude_code_proxy::providers::codex::websocket::clear_codex_websocket_pool_for_tests;
use claude_code_proxy::{registry::Registry, server::app};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex, OnceLock};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tower::util::ServiceExt;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Serialize all env-var-mutating tests so they never run concurrently.
fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    // Recover from a poisoned mutex so a failing test doesn't cascade
    let m = ENV_LOCK.get_or_init(|| Mutex::new(()));
    match m.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Write a valid auth.json for `provider` under `config_dir`.
fn write_auth(config_dir: &std::path::Path, provider: &str) {
    let dir = config_dir.join(provider);
    std::fs::create_dir_all(&dir).unwrap();
    let expires: i64 = 4102444800000;
    let auth = if provider == "codex" {
        json!({"access":"test-access","refresh":"test-refresh","expires":expires,"account_id":"acct_test"})
    } else {
        json!({"access":"test-access","refresh":"test-refresh","expires":expires,"scope":"openid","userId":"user_test"})
    };
    std::fs::write(dir.join("auth.json"), serde_json::to_vec(&auth).unwrap()).unwrap();
}

struct EnvGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvGuard {
    fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let previous = std::env::var_os(key);
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        unsafe {
            match self.previous.take() {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

/// Send a minimal `POST /v1/messages` through the in-process app.
async fn call_messages(model: &str) -> Response {
    app(Arc::new(Registry::with_default_alias()))
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .header("x-claude-code-session-id", "smoke-session")
                .body(Body::from(
                    json!({
                        "model": model,
                        "max_tokens": 64,
                        "messages": [{"role":"user","content":"hello"}]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap()
}

/// Spawn a mock axum HTTP server that accepts requests at any path, calls
/// `handler(request_json)` and returns the handler's response body as a 200
/// with `content-type: text/event-stream`.
async fn spawn_http_upstream<F>(handler: F) -> String
where
    F: Fn(Value) -> Vec<u8> + Send + Sync + 'static,
{
    let handler = Arc::new(handler);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{addr}");

    let app = axum::Router::new().fallback({
        let handler = handler.clone();
        move |body: String| {
            let handler = handler.clone();
            async move {
                let json: Value = serde_json::from_str(&body).unwrap_or_default();
                let response_bytes = handler(json);
                http::Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(Body::from(response_bytes))
                    .unwrap()
            }
        }
    });

    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    addr_str
}

/// Spawn a mock WebSocket server that accepts one connection, captures the
/// first text message, and responds with Codex WebSocket events that
/// accumulate to `"codex websocket ok"`.
async fn spawn_websocket_upstream(captured: Arc<Mutex<Option<Value>>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{addr}");

    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await
            && let Ok(ws) = tokio_tungstenite::accept_async(stream).await
        {
            let (mut sender, mut receiver) = ws.split();

            // Read the incoming response.create message
            if let Some(Ok(Message::Text(text))) = receiver.next().await
                && let Ok(json) = serde_json::from_str::<Value>(&text)
            {
                let _ = captured.lock().map(|mut g| *g = Some(json));
            }

            // Send Codex Responses events as WebSocket text messages
            let events = [
                r#"{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_up"}}"#,
                r#"{"type":"response.output_text.delta","output_index":0,"delta":"codex websocket ok"}"#,
                r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"message"}}"#,
                r#"{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":5,"output_tokens":2}}}"#,
            ];

            for event in &events {
                let _ = sender.send(Message::Text(event.to_string())).await;
            }
        }
    });

    addr_str
}

// ---------------------------------------------------------------------------
// Health and routing smoke tests (no env var mutation needed)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn smoke_healthz_returns_ok() {
    let app = app(Arc::new(Registry::with_default_alias()));
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap();
    assert_eq!(body, json!({"ok": true}));
}

#[tokio::test]
async fn smoke_codex_model_routes_to_real_provider() {
    let response = call_messages("gpt-5.5").await;
    // Should attempt auth (not return 501 placeholder)
    assert!(
        response.status() != StatusCode::NOT_IMPLEMENTED,
        "codex models must resolve to the real provider, not a placeholder"
    );
}

#[test]
fn smoke_kimi_model_is_registered() {
    // Kimi uses reqwest::blocking::Client internally, which panics when
    // dropped from an async context (it joins a dedicated runtime thread).
    // Test routing at the Registry level instead of through the HTTP stack.
    let registry = Registry::with_default_alias();
    let provider = registry.provider_for_model("kimi-for-coding", None);
    assert!(
        provider.is_some(),
        "kimi-for-coding must resolve to a registered provider"
    );
    assert_eq!(
        provider.unwrap().name(),
        "kimi",
        "kimi-for-coding must route to the kimi provider"
    );
}

// ---------------------------------------------------------------------------
// Kimi smoke: mock upstream verifies request shape and returns a valid
// streaming response. Uses multi-thread runtime because KimiHttpClient uses
// reqwest::blocking::Client internally.
// ---------------------------------------------------------------------------

#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn smoke_kimi_messages_uses_mock_upstream() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "kimi");

    let captured = Arc::new(Mutex::new(None));
    let upstream = spawn_http_upstream({
        let captured = captured.clone();
        move |body: Value| {
            let _ = captured.lock().map(|mut g| *g = Some(body));
            concat!(
                "data: {\"choices\":[{\"delta\":{\"content\":\"kimi ok\"}}]}\n\n",
                "data: {\"choices\":[{\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
                "data: [DONE]\n\n"
            )
            .as_bytes()
            .to_vec()
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_KIMI_BASE_URL", &upstream);
    let response = call_messages("kimi-for-coding").await;

    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(value["content"][0]["text"], "kimi ok");

    let sent = captured.lock().unwrap().clone().unwrap();
    assert_eq!(sent["model"], "kimi-for-coding");
    assert_eq!(sent["stream"], true);
}

// ---------------------------------------------------------------------------
// Codex HTTP smoke: mock upstream verifies request shape and returns
// Responses SSE events.
// ---------------------------------------------------------------------------

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn smoke_codex_http_messages_uses_mock_upstream() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");

    let captured = Arc::new(Mutex::new(None));
    let upstream = spawn_http_upstream({
        let captured = captured.clone();
        move |body: Value| {
            let _ = captured.lock().map(|mut g| *g = Some(body));
            concat!(
                "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_up\"}}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"codex http ok\"}\n\n",
                "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"usage\":{\"input_tokens\":5,\"output_tokens\":2}}}\n\n"
            )
            .as_bytes()
            .to_vec()
        }
    })
    .await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "http");
    let response = call_messages("gpt-5.5").await;

    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(value["content"][0]["text"], "codex http ok");

    let sent = captured.lock().unwrap().clone().unwrap();
    assert_eq!(sent["model"], "gpt-5.5");
    assert_eq!(sent["stream"], true);
}

// ---------------------------------------------------------------------------
// Codex WebSocket smoke: mock upstream verifies request shape and returns
// Responses events over WebSocket.
// ---------------------------------------------------------------------------

// Multi-threaded runtime so the spawned accept task runs independently and
// the listener is registered with the I/O driver before connect_async starts.
// A single-threaded runtime risks the root task (connect_async) outpacing the
// spawned accept task, causing connection-refused races.
#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn smoke_codex_websocket_messages_uses_mock_upstream() {
    let _guard = env_lock();
    let config = TempDir::new().unwrap();
    write_auth(config.path(), "codex");
    clear_codex_websocket_pool_for_tests();

    let captured = Arc::new(Mutex::new(None));
    let upstream = spawn_websocket_upstream(captured.clone()).await;

    let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
    let _base_url_env = EnvGuard::set("CCP_CODEX_BASE_URL", &upstream);
    let _transport_env = EnvGuard::set("CCP_CODEX_TRANSPORT", "websocket");
    let response = call_messages("gpt-5.5").await;

    let ws_status = response.status();
    let ws_body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    if ws_status != StatusCode::OK {
        panic!(
            "WS: expected 200, got {}: {}",
            ws_status,
            String::from_utf8_lossy(&ws_body_bytes)
        );
    }
    let value: Value = serde_json::from_slice(&ws_body_bytes).unwrap();
    assert_eq!(value["content"][0]["text"], "codex websocket ok");

    let guard = captured.lock().unwrap();
    let sent = guard.clone().unwrap_or_else(|| {
        panic!(
            "WS mock did not capture a request. Response body: {}",
            String::from_utf8_lossy(&ws_body_bytes)
        );
    });
    // WebSocket transport sends the ResponsesRequest JSON directly; the
    // protocol context implies response.create so no "type" field is injected.
    assert_eq!(sent["model"], "gpt-5.5");
    assert!(sent.get("stream").is_none());
}
