#![cfg(all(
    unix,
    feature = "transport-streamable-http-client-unix-socket",
    not(feature = "local")
))]

use std::{collections::HashMap, sync::Arc};

use axum::{
    Router, body::Bytes, extract::State, http::StatusCode, response::IntoResponse, routing::post,
};
use http::{HeaderName, HeaderValue};
use hyper_util::rt::TokioIo;
use rmcp::{
    ClientLifecycleMode, ClientServiceExt, ServiceExt,
    model::{ClientInfo, ProtocolVersion},
    transport::{
        StreamableHttpClientTransport, UnixSocketHttpClient,
        streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::json;
use tokio::sync::Mutex;

#[derive(Clone)]
struct ServerState {
    received_headers: Arc<Mutex<HashMap<String, String>>>,
    initialize_called: Arc<tokio::sync::Notify>,
}

async fn mcp_handler(
    State(state): State<ServerState>,
    headers: http::HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let mut headers_map = HashMap::new();
    for (name, value) in headers.iter() {
        let name_str = name.as_str();
        if name_str.starts_with("x-") || name_str == "host" {
            if let Ok(v) = value.to_str() {
                headers_map.insert(name_str.to_string(), v.to_string());
            }
        }
    }

    let mut stored = state.received_headers.lock().await;
    stored.extend(headers_map);
    drop(stored);

    if let Ok(json_body) = serde_json::from_slice::<serde_json::Value>(&body) {
        if let Some(method) = json_body.get("method").and_then(|m| m.as_str()) {
            if method == "initialize" {
                state.initialize_called.notify_one();
                let response = json!({
                    "jsonrpc": "2.0",
                    "id": json_body.get("id"),
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {},
                        "serverInfo": {
                            "name": "test-unix-server",
                            "version": "1.0.0"
                        }
                    }
                });
                return (
                    StatusCode::OK,
                    [
                        (http::header::CONTENT_TYPE, "application/json"),
                        (
                            http::HeaderName::from_static("mcp-session-id"),
                            "unix-test-session",
                        ),
                    ],
                    response.to_string(),
                );
            } else if method == "notifications/initialized" {
                return (
                    StatusCode::ACCEPTED,
                    [
                        (http::header::CONTENT_TYPE, "application/json"),
                        (
                            http::HeaderName::from_static("mcp-session-id"),
                            "unix-test-session",
                        ),
                    ],
                    String::new(),
                );
            }
        }
    }

    let request_id = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|j| j.get("id").cloned())
        .unwrap_or(serde_json::Value::Null);
    let response = json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {}
    });
    (
        StatusCode::OK,
        [
            (http::header::CONTENT_TYPE, "application/json"),
            (
                http::HeaderName::from_static("mcp-session-id"),
                "unix-test-session",
            ),
        ],
        response.to_string(),
    )
}

/// Spawns an HTTP/1.1 server on a Unix socket using hyper directly.
/// Avoids `axum::serve(UnixListener, ...)` which uses `spawn_local` on Linux.
fn spawn_unix_server(
    listener: tokio::net::UnixListener,
    app: Router,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let tower_service = app.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let hyper_service = hyper::service::service_fn(
                    move |req: hyper::Request<hyper::body::Incoming>| {
                        let mut tower_service = tower_service.clone();
                        async move {
                            use tower_service::Service;
                            tower_service.call(req).await
                        }
                    },
                );
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, hyper_service)
                    .await
                    .ok();
            });
        }
    })
}

/// Integration test: MCP client connects and completes handshake over a Unix domain socket.
#[tokio::test]
async fn test_unix_socket_mcp_handshake() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("rmcp-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let socket_path = dir.join("mcp.sock");

    let _ = std::fs::remove_file(&socket_path);

    let state = ServerState {
        received_headers: Arc::new(Mutex::new(HashMap::new())),
        initialize_called: Arc::new(tokio::sync::Notify::new()),
    };

    let app = Router::new()
        .route("/mcp", post(mcp_handler))
        .with_state(state.clone());

    let listener = tokio::net::UnixListener::bind(&socket_path)?;
    let server_handle = spawn_unix_server(listener, app);

    let socket_str = socket_path.to_str().unwrap();
    let uri = "http://mcp-server.internal/mcp";
    let client = UnixSocketHttpClient::new(socket_str, uri);
    let config = StreamableHttpClientTransportConfig::with_uri(uri);
    let transport = StreamableHttpClientTransport::with_client(client, config);

    let mcp_client = ().serve(transport).await.expect("MCP handshake should succeed");

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        state.initialize_called.notified(),
    )
    .await
    .expect("Initialize request should be received");

    let headers = state.received_headers.lock().await;
    assert_eq!(
        headers.get("host"),
        Some(&"mcp-server.internal".to_string()),
        "Host header should be derived from URI"
    );

    drop(mcp_client);
    server_handle.abort();
    let _ = std::fs::remove_file(&socket_path);
    let _ = std::fs::remove_dir(&dir);

    Ok(())
}

/// Integration test: Custom headers are sent through the Unix socket transport.
#[tokio::test]
async fn test_unix_socket_custom_headers() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("rmcp-test-headers-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let socket_path = dir.join("mcp.sock");
    let _ = std::fs::remove_file(&socket_path);

    let state = ServerState {
        received_headers: Arc::new(Mutex::new(HashMap::new())),
        initialize_called: Arc::new(tokio::sync::Notify::new()),
    };

    let app = Router::new()
        .route("/mcp", post(mcp_handler))
        .with_state(state.clone());

    let listener = tokio::net::UnixListener::bind(&socket_path)?;
    let server_handle = spawn_unix_server(listener, app);

    let mut custom_headers = HashMap::new();
    custom_headers.insert(
        HeaderName::from_static("x-test-header"),
        HeaderValue::from_static("test-value-123"),
    );
    custom_headers.insert(
        HeaderName::from_static("x-client-id"),
        HeaderValue::from_static("unix-test-client"),
    );

    let socket_str = socket_path.to_str().unwrap();
    let uri = "http://mcp-server.internal/mcp";
    let client = UnixSocketHttpClient::new(socket_str, uri);
    let config = StreamableHttpClientTransportConfig::with_uri(uri).custom_headers(custom_headers);
    let transport = StreamableHttpClientTransport::with_client(client, config);

    let mcp_client = ().serve(transport).await.expect("MCP handshake should succeed");

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        state.initialize_called.notified(),
    )
    .await
    .expect("Initialize request should be received");

    let headers = state.received_headers.lock().await;
    assert_eq!(
        headers.get("x-test-header"),
        Some(&"test-value-123".to_string()),
        "Custom header x-test-header should be received"
    );
    assert_eq!(
        headers.get("x-client-id"),
        Some(&"unix-test-client".to_string()),
        "Custom header x-client-id should be received"
    );

    drop(mcp_client);
    server_handle.abort();
    let _ = std::fs::remove_file(&socket_path);
    let _ = std::fs::remove_dir(&dir);

    Ok(())
}

/// Integration test: Convenience constructor `from_unix_socket` works end-to-end.
#[tokio::test]
async fn test_unix_socket_convenience_constructor() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("rmcp-test-conv-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let socket_path = dir.join("mcp.sock");
    let _ = std::fs::remove_file(&socket_path);

    let state = ServerState {
        received_headers: Arc::new(Mutex::new(HashMap::new())),
        initialize_called: Arc::new(tokio::sync::Notify::new()),
    };

    let app = Router::new()
        .route("/mcp", post(mcp_handler))
        .with_state(state.clone());

    let listener = tokio::net::UnixListener::bind(&socket_path)?;
    let server_handle = spawn_unix_server(listener, app);

    let socket_str = socket_path.to_str().unwrap();
    let transport =
        StreamableHttpClientTransport::from_unix_socket(socket_str, "http://localhost/mcp");

    let mcp_client = ().serve(transport).await.expect("MCP handshake should succeed");

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        state.initialize_called.notified(),
    )
    .await
    .expect("Initialize request should be received");

    drop(mcp_client);
    server_handle.abort();
    let _ = std::fs::remove_file(&socket_path);
    let _ = std::fs::remove_dir(&dir);

    Ok(())
}

#[tokio::test]
async fn unix_socket_auto_lifecycle_handles_legacy_http_prevalidation() -> anyhow::Result<()> {
    let directory = std::env::temp_dir().join(format!(
        "rmcp-unix-discovery-fallback-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&directory)?;
    let socket_path = directory.join("mcp.sock");
    let _ = std::fs::remove_file(&socket_path);

    let observed_methods = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed = Arc::clone(&observed_methods);
    let app = Router::new().route(
        "/mcp",
        post(move |body: Bytes| {
            let observed = Arc::clone(&observed);
            async move {
                let request: serde_json::Value =
                    serde_json::from_slice(&body).expect("parse JSON-RPC request");
                let method = request["method"].as_str().expect("JSON-RPC method");
                observed.lock().await.push(method.to_string());

                match method {
                    "server/discover" => axum::http::Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(axum::body::Body::from(
                            json!({
                                "jsonrpc": "2.0",
                                "id": null,
                                "error": {
                                    "code": -32000,
                                    "message": "Bad Request: No valid session ID provided"
                                }
                            })
                            .to_string(),
                        ))
                        .expect("build legacy prevalidation response"),
                    "initialize" => axum::http::Response::builder()
                        .status(StatusCode::OK)
                        .header(http::header::CONTENT_TYPE, "application/json")
                        .body(axum::body::Body::from(
                            json!({
                                "jsonrpc": "2.0",
                                "id": request["id"],
                                "result": {
                                    "protocolVersion": "2025-06-18",
                                    "capabilities": {},
                                    "serverInfo": {"name":"legacy-unix", "version":"1.0"}
                                }
                            })
                            .to_string(),
                        ))
                        .expect("build initialize response"),
                    "notifications/initialized" => axum::http::Response::builder()
                        .status(StatusCode::ACCEPTED)
                        .body(axum::body::Body::empty())
                        .expect("build initialized response"),
                    other => panic!("unexpected JSON-RPC method {other}"),
                }
            }
        }),
    );
    let listener = tokio::net::UnixListener::bind(&socket_path)?;
    let server = spawn_unix_server(listener, app);
    let uri = "http://mcp-server.internal/mcp";
    let transport = StreamableHttpClientTransport::with_client(
        UnixSocketHttpClient::new(socket_path.to_str().expect("socket path"), uri),
        StreamableHttpClientTransportConfig::with_uri(uri),
    );

    let client = ClientInfo::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Auto {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
                legacy_version: Some(ProtocolVersion::V_2025_06_18),
            },
        )
        .await?;
    client.cancel().await?;
    assert_eq!(
        *observed_methods.lock().await,
        ["server/discover", "initialize", "notifications/initialized"]
    );

    server.abort();
    let _ = std::fs::remove_file(&socket_path);
    let _ = std::fs::remove_dir(&directory);
    Ok(())
}
