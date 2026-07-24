#![cfg(all(
    not(feature = "local"),
    feature = "client",
    feature = "reqwest",
    feature = "transport-streamable-http-server"
))]

use std::{
    borrow::Cow,
    sync::{Arc, Mutex},
};

use axum::{
    body::Bytes,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use rmcp::{
    ClientLifecycleMode, ClientServiceExt, ServerHandler,
    model::{
        ClientInfo, DiscoverResult, ErrorCode, ErrorData, InitializeResult, ProtocolVersion,
        ServerCapabilities,
    },
    service::{MaybeSendFuture, RequestContext, RoleServer},
    transport::{
        StreamableHttpClientTransport,
        streamable_http_client::StreamableHttpClientTransportConfig,
        streamable_http_server::{
            StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
        },
    },
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default)]
struct DiscoverHttpServer;

impl ServerHandler for DiscoverHttpServer {
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
    }
}

#[derive(Clone, Default)]
struct LegacyHttpServer;

impl ServerHandler for LegacyHttpServer {
    fn discover(
        &self,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<DiscoverResult, ErrorData>> + MaybeSendFuture + '_ {
        std::future::ready(Err(ErrorData::new(
            ErrorCode::METHOD_NOT_FOUND,
            "Method not found",
            None,
        )))
    }
}

#[tokio::test]
async fn discover_http_client_bootstraps_headers_without_initialize() {
    let ct = CancellationToken::new();
    let service: StreamableHttpService<DiscoverHttpServer, LocalSessionManager> =
        StreamableHttpService::new(
            || Ok(DiscoverHttpServer),
            Default::default(),
            StreamableHttpServerConfig::default()
                .with_legacy_session_mode(false)
                .with_json_response(true)
                .with_cancellation_token(ct.child_token()),
        );
    let router = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn({
        let ct = ct.clone();
        async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move { ct.cancelled_owned().await })
                .await;
        }
    });

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp")),
    );
    let client = ClientInfo::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .expect("discover HTTP client should start");
    client.list_tools(None).await.expect("list tools");
    client.cancel().await.expect("cancel client");

    ct.cancel();
    server.await.expect("server task");
}

#[tokio::test]
async fn auto_http_client_falls_back_to_stateful_legacy_startup() {
    let ct = CancellationToken::new();
    let service: StreamableHttpService<LegacyHttpServer, LocalSessionManager> =
        StreamableHttpService::new(
            || Ok(LegacyHttpServer),
            Default::default(),
            StreamableHttpServerConfig::default()
                .with_json_response(true)
                .with_cancellation_token(ct.child_token()),
        );
    let router = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn({
        let ct = ct.clone();
        async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move { ct.cancelled_owned().await })
                .await;
        }
    });

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp")),
    );
    let client = ClientInfo::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Auto {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
                legacy_version: Some(ProtocolVersion::V_2025_11_25),
            },
        )
        .await
        .expect("auto HTTP client should fall back");
    client.list_tools(None).await.expect("list tools");
    client.cancel().await.expect("cancel client");

    ct.cancel();
    server.await.expect("server task");
}

#[derive(Clone, Copy)]
enum LegacyDiscoveryRejection {
    UnsupportedProtocol,
    MissingSession,
    NotFound,
    MethodNotAllowed,
    Unauthorized,
    Forbidden,
}

#[derive(Clone)]
struct LegacyPrevalidationState {
    rejection: LegacyDiscoveryRejection,
    methods: Arc<Mutex<Vec<String>>>,
}

async fn legacy_prevalidation_handler(
    State(state): State<LegacyPrevalidationState>,
    body: Bytes,
) -> Response {
    let message: serde_json::Value = serde_json::from_slice(&body).expect("JSON-RPC request body");
    let method = message
        .get("method")
        .and_then(serde_json::Value::as_str)
        .expect("JSON-RPC request method");
    state
        .methods
        .lock()
        .expect("methods lock")
        .push(method.into());

    match method {
        "server/discover" => match state.rejection {
            LegacyDiscoveryRejection::UnsupportedProtocol => json_response(
                StatusCode::BAD_REQUEST,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": {
                        "code": -32000,
                        "message": "Bad Request: Unsupported protocol version: 2026-07-28 \
                            (supported versions: 2025-11-25, 2025-06-18, 2025-03-26, \
                            2024-11-05, 2024-10-07)",
                    },
                }),
            ),
            LegacyDiscoveryRejection::MissingSession => json_response(
                StatusCode::BAD_REQUEST,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": {
                        "code": -32000,
                        "message": "Bad Request: No valid session ID provided",
                    },
                }),
            ),
            LegacyDiscoveryRejection::NotFound => {
                (StatusCode::NOT_FOUND, "legacy endpoint not found").into_response()
            }
            LegacyDiscoveryRejection::MethodNotAllowed => {
                (StatusCode::METHOD_NOT_ALLOWED, "legacy method not allowed").into_response()
            }
            LegacyDiscoveryRejection::Unauthorized => {
                (StatusCode::UNAUTHORIZED, "authentication required").into_response()
            }
            LegacyDiscoveryRejection::Forbidden => {
                (StatusCode::FORBIDDEN, "access forbidden").into_response()
            }
        },
        "initialize" => {
            assert_eq!(
                message
                    .get("params")
                    .and_then(|params| params.get("protocolVersion")),
                Some(&serde_json::json!("2025-06-18"))
            );
            let mut result = InitializeResult::new(ServerCapabilities::default());
            result.protocol_version = ProtocolVersion::V_2025_06_18;
            json_response(
                StatusCode::OK,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "result": result,
                }),
            )
        }
        "notifications/initialized" => StatusCode::ACCEPTED.into_response(),
        "tools/list" => json_response(
            StatusCode::OK,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": message.get("id"),
                "result": { "tools": [] },
            }),
        ),
        _ => (StatusCode::BAD_REQUEST, "unexpected request").into_response(),
    }
}

fn json_response(status: StatusCode, value: serde_json::Value) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        value.to_string(),
    )
        .into_response()
}

async fn assert_http_legacy_fallback(rejection: LegacyDiscoveryRejection) {
    let methods = Arc::new(Mutex::new(Vec::new()));
    let router = axum::Router::new()
        .route("/mcp", post(legacy_prevalidation_handler))
        .with_state(LegacyPrevalidationState {
            rejection,
            methods: methods.clone(),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("listener address");
    let cancellation = CancellationToken::new();
    let server = tokio::spawn({
        let cancellation = cancellation.clone();
        async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(cancellation.cancelled_owned())
                .await
                .expect("serve legacy HTTP endpoint");
        }
    });

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp")),
    );
    let client = ClientInfo::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Auto {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
                legacy_version: Some(ProtocolVersion::V_2025_06_18),
            },
        )
        .await
        .expect("Auto mode should recognize the deployed legacy HTTP response");
    client.list_tools(None).await.expect("list legacy tools");
    client.cancel().await.expect("cancel client");

    assert_eq!(
        *methods.lock().expect("methods lock"),
        [
            "server/discover",
            "initialize",
            "notifications/initialized",
            "tools/list",
        ]
    );
    cancellation.cancel();
    server.await.expect("server task");
}

#[tokio::test]
async fn auto_http_client_falls_back_after_unsupported_protocol_prevalidation() {
    assert_http_legacy_fallback(LegacyDiscoveryRejection::UnsupportedProtocol).await;
}

#[tokio::test]
async fn auto_http_client_falls_back_after_missing_session_prevalidation() {
    assert_http_legacy_fallback(LegacyDiscoveryRejection::MissingSession).await;
}

#[tokio::test]
async fn auto_http_client_falls_back_after_initial_http_not_found() {
    assert_http_legacy_fallback(LegacyDiscoveryRejection::NotFound).await;
}

#[tokio::test]
async fn auto_http_client_falls_back_after_initial_http_method_not_allowed() {
    assert_http_legacy_fallback(LegacyDiscoveryRejection::MethodNotAllowed).await;
}

async fn assert_http_auth_rejection_does_not_downgrade(rejection: LegacyDiscoveryRejection) {
    let methods = Arc::new(Mutex::new(Vec::new()));
    let router = axum::Router::new()
        .route("/mcp", post(legacy_prevalidation_handler))
        .with_state(LegacyPrevalidationState {
            rejection,
            methods: methods.clone(),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("listener address");
    let cancellation = CancellationToken::new();
    let server = tokio::spawn({
        let cancellation = cancellation.clone();
        async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(cancellation.cancelled_owned())
                .await
                .expect("serve auth-rejecting HTTP endpoint");
        }
    });

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp")),
    );
    let result = ClientInfo::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Auto {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
                legacy_version: Some(ProtocolVersion::V_2025_06_18),
            },
        )
        .await;
    assert!(
        result.is_err(),
        "authentication failures must not downgrade"
    );
    assert_eq!(*methods.lock().expect("methods lock"), ["server/discover"]);
    cancellation.cancel();
    server.await.expect("server task");
}

#[tokio::test]
async fn auto_http_client_does_not_downgrade_after_http_401() {
    assert_http_auth_rejection_does_not_downgrade(LegacyDiscoveryRejection::Unauthorized).await;
}

#[tokio::test]
async fn auto_http_client_does_not_downgrade_after_http_403() {
    assert_http_auth_rejection_does_not_downgrade(LegacyDiscoveryRejection::Forbidden).await;
}
