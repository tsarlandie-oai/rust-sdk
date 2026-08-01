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

use rmcp::{
    ClientLifecycleMode, ClientServiceExt, ServerHandler,
    model::{ClientInfo, DiscoverResult, ErrorCode, ErrorData, ProtocolVersion},
    service::{MaybeSendFuture, RequestContext, RoleServer},
    transport::{
        StreamableHttpClientTransport,
        streamable_http_client::StreamableHttpClientTransportConfig,
        streamable_http_server::{
            StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
        },
    },
};
use serde_json::{Value, json};
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
enum DiscoveryResponseId {
    Matching,
    Null,
    Unrelated,
}

#[derive(Clone)]
struct DiscoveryHttpFixture {
    status: u16,
    code: Option<i32>,
    message: &'static str,
    response_id: DiscoveryResponseId,
    supported: Option<Vec<&'static str>>,
    content_type: Option<&'static str>,
    observed_methods: Arc<Mutex<Vec<String>>>,
}

async fn discovery_fixture_handler(
    axum::extract::State(fixture): axum::extract::State<DiscoveryHttpFixture>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let request: Value = serde_json::from_slice(&body).expect("valid JSON-RPC request");
    let method = request["method"].as_str().expect("JSON-RPC method");
    fixture
        .observed_methods
        .lock()
        .expect("observed methods lock")
        .push(method.to_string());

    match method {
        "server/discover" => {
            let body = match fixture.code {
                Some(code) => {
                    let id = match fixture.response_id {
                        DiscoveryResponseId::Matching => request["id"].clone(),
                        DiscoveryResponseId::Null => Value::Null,
                        DiscoveryResponseId::Unrelated => json!("unrelated-request"),
                    };
                    let mut error = json!({"code": code, "message": fixture.message});
                    if let Some(supported) = &fixture.supported {
                        error["data"] = json!({"supported": supported});
                    }
                    serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"error":error}))
                        .expect("serialize discovery error")
                }
                None => fixture.message.as_bytes().to_vec(),
            };
            let mut builder = axum::http::Response::builder().status(fixture.status);
            if let Some(content_type) = fixture.content_type {
                builder = builder.header(axum::http::header::CONTENT_TYPE, content_type);
            }
            builder
                .body(axum::body::Body::from(body))
                .expect("build discovery response")
        }
        "initialize" => axum::http::Response::builder()
            .status(200)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(
                serde_json::to_vec(&json!({
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": {
                        "protocolVersion": request["params"]["protocolVersion"],
                        "capabilities": {},
                        "serverInfo": {"name": "legacy-http", "version": "1.0"}
                    }
                }))
                .expect("serialize initialize response"),
            ))
            .expect("build initialize response"),
        "notifications/initialized" => axum::http::Response::builder()
            .status(202)
            .body(axum::body::Body::empty())
            .expect("build initialized response"),
        other => panic!("unexpected JSON-RPC method: {other}"),
    }
}

async fn start_discovery_fixture(
    fixture: DiscoveryHttpFixture,
) -> (String, tokio::task::JoinHandle<()>) {
    let router = axum::Router::new()
        .route("/mcp", axum::routing::post(discovery_fixture_handler))
        .with_state(fixture);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fixture listener should bind");
    let address = listener.local_addr().expect("fixture listener address");
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("fixture server should run");
    });
    (format!("http://{address}/mcp"), server)
}

#[tokio::test]
async fn auto_http_client_negotiates_production_legacy_rejections() {
    // These are production response shapes reported in rust-sdk #1040, not
    // fabricated JSON-RPC errors supplied by a downstream HTTP adapter.
    let cases = [
        (
            "Python legacy InvalidRequest",
            400,
            Some(-32600),
            "legacy request rejected",
            DiscoveryResponseId::Matching,
            None,
            Some("application/json"),
        ),
        (
            "legacy InvalidParams",
            400,
            Some(-32602),
            "legacy request rejected",
            DiscoveryResponseId::Matching,
            None,
            Some("application/json"),
        ),
        (
            "legacy-only structured version rejection",
            400,
            Some(-32022),
            "unsupported protocol version",
            DiscoveryResponseId::Matching,
            Some(vec!["2025-11-25", "2025-06-18"]),
            Some("application/json"),
        ),
        (
            "deployed TypeScript null-ID version rejection",
            400,
            Some(-32000),
            "Bad Request: Unsupported protocol version: 2026-07-28 (supported versions: 2025-11-25, 2025-06-18, 2025-03-26, 2024-11-05, 2024-10-07)",
            DiscoveryResponseId::Null,
            None,
            Some("application/json"),
        ),
        (
            "TinyMCP null-ID version rejection without JSON content type",
            400,
            Some(-32000),
            "Bad Request: Unsupported protocol version (supported versions: 2025-06-18, 2025-03-26, 2024-11-05, 2024-10-07)",
            DiscoveryResponseId::Null,
            None,
            Some("text/plain"),
        ),
        (
            "legacy missing-session prevalidation",
            400,
            Some(-32000),
            "Bad Request: No valid session ID provided",
            DiscoveryResponseId::Null,
            None,
            Some("application/json"),
        ),
        (
            "initial plain HTTP 404",
            404,
            None,
            "Not Found",
            DiscoveryResponseId::Null,
            None,
            Some("text/plain"),
        ),
        (
            "initial plain HTTP 405",
            405,
            None,
            "Method Not Allowed",
            DiscoveryResponseId::Null,
            None,
            Some("text/plain"),
        ),
    ];

    for (name, status, code, message, response_id, supported, content_type) in cases {
        let observed_methods = Arc::new(Mutex::new(Vec::new()));
        let (url, server) = start_discovery_fixture(DiscoveryHttpFixture {
            status,
            code,
            message,
            response_id,
            supported,
            content_type,
            observed_methods: Arc::clone(&observed_methods),
        })
        .await;
        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(url),
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
            .unwrap_or_else(|error| panic!("{name} should permit legacy initialization: {error}"));
        client.cancel().await.expect("cancel client");
        assert_eq!(
            *observed_methods.lock().expect("observed methods lock"),
            ["server/discover", "initialize", "notifications/initialized"],
            "unexpected lifecycle for {name}"
        );
        server.abort();
    }
}

#[tokio::test]
async fn auto_http_client_rejects_unsafe_discovery_downgrades() {
    let cases = [
        (
            "authentication failure",
            401,
            Some(-32600),
            "authentication required",
            DiscoveryResponseId::Matching,
            None,
        ),
        (
            "authorization failure",
            403,
            Some(-32600),
            "forbidden",
            DiscoveryResponseId::Matching,
            None,
        ),
        (
            "server failure",
            500,
            Some(-32600),
            "server error",
            DiscoveryResponseId::Matching,
            None,
        ),
        (
            "modern header mismatch",
            400,
            Some(-32020),
            "header mismatch",
            DiscoveryResponseId::Matching,
            None,
        ),
        (
            "modern missing capability",
            400,
            Some(-32021),
            "missing capability",
            DiscoveryResponseId::Matching,
            None,
        ),
        (
            "modern header mismatch wrapped in HTTP 404",
            404,
            Some(-32020),
            "header mismatch",
            DiscoveryResponseId::Matching,
            None,
        ),
        (
            "modern missing capability wrapped in HTTP 405",
            405,
            Some(-32021),
            "missing capability",
            DiscoveryResponseId::Matching,
            None,
        ),
        (
            "unrelated response ID",
            400,
            Some(-32600),
            "invalid request",
            DiscoveryResponseId::Unrelated,
            None,
        ),
        (
            "future-only advertised protocol",
            400,
            Some(-32022),
            "unsupported protocol version",
            DiscoveryResponseId::Matching,
            Some(vec!["2099-01-01"]),
        ),
        (
            "unsupported version without downgrade evidence",
            400,
            Some(-32022),
            "unsupported protocol version",
            DiscoveryResponseId::Matching,
            None,
        ),
        (
            "mixed legacy and future protocol evidence",
            400,
            Some(-32000),
            "Bad Request: Unsupported protocol version (supported versions: 2025-06-18, 2099-01-01)",
            DiscoveryResponseId::Null,
            None,
        ),
        (
            "arbitrary null-ID prevalidation rejection",
            400,
            Some(-32000),
            "Bad Request: malformed request",
            DiscoveryResponseId::Null,
            None,
        ),
    ];

    for (name, status, code, message, response_id, supported) in cases {
        let observed_methods = Arc::new(Mutex::new(Vec::new()));
        let (url, server) = start_discovery_fixture(DiscoveryHttpFixture {
            status,
            code,
            message,
            response_id,
            supported,
            content_type: Some("application/json"),
            observed_methods: Arc::clone(&observed_methods),
        })
        .await;
        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(url),
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
        match result {
            Ok(client) => {
                let _ = client.cancel().await;
                panic!("{name} must not downgrade to legacy")
            }
            Err(_) => {}
        }
        assert_eq!(
            *observed_methods.lock().expect("observed methods lock"),
            ["server/discover"],
            "unsafe fallback occurred for {name}"
        );
        server.abort();
    }
}

#[tokio::test]
async fn auto_http_client_accepts_typed_rejections_from_custom_http_backends() {
    use std::collections::HashMap;

    use futures::stream::BoxStream;
    use http::{HeaderName, HeaderValue};
    use rmcp::{
        model::{
            ClientJsonRpcMessage, ClientRequest, InitializeResult, ServerCapabilities,
            ServerJsonRpcMessage, ServerResult,
        },
        transport::streamable_http_client::{
            HttpStatusError, StreamableHttpClient, StreamableHttpError, StreamableHttpPostResponse,
        },
    };

    #[derive(Clone, Default)]
    struct CustomHttpBackend {
        observed: Arc<Mutex<Vec<String>>>,
    }

    impl StreamableHttpClient for CustomHttpBackend {
        type Error = std::io::Error;

        async fn post_message(
            &self,
            _uri: Arc<str>,
            message: ClientJsonRpcMessage,
            _session_id: Option<Arc<str>>,
            _auth_header: Option<String>,
            _custom_headers: HashMap<HeaderName, HeaderValue>,
        ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
            match message {
                ClientJsonRpcMessage::Request(request)
                    if matches!(&request.request, ClientRequest::DiscoverRequest(_)) =>
                {
                    self.observed
                        .lock()
                        .expect("record discover")
                        .push("server/discover".into());
                    Err(StreamableHttpError::UnexpectedHttpStatus(
                        HttpStatusError::new(
                            400,
                            r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32000,"message":"Bad Request: No valid session ID provided"}}"#,
                        ),
                    ))
                }
                ClientJsonRpcMessage::Request(request)
                    if matches!(&request.request, ClientRequest::InitializeRequest(_)) =>
                {
                    self.observed
                        .lock()
                        .expect("record initialize")
                        .push("initialize".into());
                    Ok(StreamableHttpPostResponse::Json(
                        ServerJsonRpcMessage::response(
                            ServerResult::InitializeResult(
                                InitializeResult::new(ServerCapabilities::default())
                                    .with_protocol_version(ProtocolVersion::V_2025_06_18),
                            ),
                            request.id,
                        ),
                        None,
                    ))
                }
                ClientJsonRpcMessage::Notification(_) => {
                    self.observed
                        .lock()
                        .expect("record initialized")
                        .push("notifications/initialized".into());
                    Ok(StreamableHttpPostResponse::Accepted)
                }
                other => panic!("unexpected request: {other:?}"),
            }
        }

        async fn delete_session(
            &self,
            _uri: Arc<str>,
            _session_id: Arc<str>,
            _auth_header: Option<String>,
            _custom_headers: HashMap<HeaderName, HeaderValue>,
        ) -> Result<(), StreamableHttpError<Self::Error>> {
            Ok(())
        }

        async fn get_stream(
            &self,
            _uri: Arc<str>,
            _session_id: Option<Arc<str>>,
            _last_event_id: Option<String>,
            _auth_header: Option<String>,
            _custom_headers: HashMap<HeaderName, HeaderValue>,
        ) -> Result<
            BoxStream<'static, Result<sse_stream::Sse, sse_stream::Error>>,
            StreamableHttpError<Self::Error>,
        > {
            Err(StreamableHttpError::ServerDoesNotSupportSse)
        }
    }

    let backend = CustomHttpBackend::default();
    let observed = Arc::clone(&backend.observed);
    let transport = StreamableHttpClientTransport::with_client(
        backend,
        StreamableHttpClientTransportConfig::with_uri("http://custom/mcp"),
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
        .expect("typed custom HTTP rejection should permit legacy fallback");
    client.cancel().await.expect("cancel client");

    assert_eq!(
        *observed.lock().expect("read custom HTTP requests"),
        ["server/discover", "initialize", "notifications/initialized"]
    );
}

#[tokio::test]
async fn auto_http_client_falls_back_after_a_correlated_sse_discovery_error() {
    let observed_methods = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&observed_methods);
    let router = axum::Router::new().route(
        "/mcp",
        axum::routing::post(move |body: axum::body::Bytes| {
            let observed = Arc::clone(&observed);
            async move {
                let request: Value = serde_json::from_slice(&body).expect("JSON-RPC request");
                let method = request["method"].as_str().expect("JSON-RPC method");
                observed
                    .lock()
                    .expect("record request")
                    .push(method.to_string());
                match method {
                    "server/discover" => axum::http::Response::builder()
                        .status(200)
                        .header(axum::http::header::CONTENT_TYPE, "text/event-stream")
                        .body(axum::body::Body::from(format!(
                            "data: {}\n\n",
                            json!({
                                "jsonrpc": "2.0",
                                "id": request["id"],
                                "error": {"code": -32601, "message": "method not found"}
                            })
                        )))
                        .expect("build discovery SSE response"),
                    "initialize" => axum::http::Response::builder()
                        .status(200)
                        .header(axum::http::header::CONTENT_TYPE, "application/json")
                        .body(axum::body::Body::from(
                            json!({
                                "jsonrpc":"2.0",
                                "id": request["id"],
                                "result": {
                                    "protocolVersion":"2025-06-18",
                                    "capabilities":{},
                                    "serverInfo":{"name":"legacy-sse", "version":"1.0"}
                                }
                            })
                            .to_string(),
                        ))
                        .expect("build initialize response"),
                    "notifications/initialized" => axum::http::Response::builder()
                        .status(202)
                        .body(axum::body::Body::empty())
                        .expect("build initialized response"),
                    other => panic!("unexpected JSON-RPC method: {other}"),
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("SSE fixture listener should bind");
    let address = listener.local_addr().expect("SSE fixture address");
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("SSE fixture server should run");
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
        .expect("correlated SSE discovery error should permit legacy fallback");
    client.cancel().await.expect("cancel client");
    assert_eq!(
        *observed_methods.lock().expect("read observed methods"),
        ["server/discover", "initialize", "notifications/initialized"]
    );
    server.abort();
}

#[cfg(feature = "auth")]
#[tokio::test]
async fn oauth_metadata_discovery_remains_get_only_without_starting_an_mcp_lifecycle() {
    let methods = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&methods);
    let router =
        axum::Router::new().fallback(move |request: axum::http::Request<axum::body::Body>| {
            let observed = Arc::clone(&observed);
            async move {
                observed
                    .lock()
                    .expect("record OAuth discovery request")
                    .push(request.method().clone());
                axum::http::StatusCode::NOT_FOUND
            }
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("OAuth fixture listener should bind");
    let address = listener.local_addr().expect("OAuth fixture address");
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("OAuth fixture should run");
    });

    let manager = rmcp::transport::AuthorizationManager::new(format!("http://{address}/mcp"))
        .await
        .expect("create authorization manager");
    let resolution = manager.resolve_metadata().await.expect("resolve metadata");

    assert_eq!(
        resolution.source,
        rmcp::transport::auth::AuthorizationMetadataSource::LegacyEndpointFallback
    );
    let requests = methods.lock().expect("read OAuth discovery requests");
    assert!(
        !requests.is_empty(),
        "OAuth metadata should have been probed"
    );
    assert!(
        requests
            .iter()
            .all(|method| method == axum::http::Method::GET),
        "OAuth status discovery must never POST initialize: {requests:?}"
    );
    drop(requests);
    server.abort();
}

#[cfg(feature = "auth")]
#[tokio::test]
async fn oauth_uses_the_real_discovery_post_challenge_without_a_second_protocol_probe() {
    use axum::http::{StatusCode, header::WWW_AUTHENTICATE};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("OAuth challenge fixture listener should bind");
    let address = listener
        .local_addr()
        .expect("OAuth challenge fixture address");
    let origin = format!("http://{address}");
    let metadata_url = format!("{origin}/.well-known/oauth-protected-resource/mcp");
    let challenge = format!(r#"Bearer resource_metadata="{metadata_url}", scope="mcp:read""#);
    let observed_methods = Arc::new(Mutex::new(Vec::<String>::new()));
    let discovered = Arc::clone(&observed_methods);
    let challenge_for_server = challenge.clone();
    let resource = format!("{origin}/mcp");
    let authorization_origin = origin.clone();

    let router = axum::Router::new()
        .route(
            "/mcp",
            axum::routing::get(|| async { StatusCode::NOT_FOUND }).post(
                move |body: axum::body::Bytes| {
                    let discovered = Arc::clone(&discovered);
                    let challenge = challenge_for_server.clone();
                    async move {
                        let request: Value =
                            serde_json::from_slice(&body).expect("valid JSON-RPC request");
                        discovered.lock().expect("record discovery method").push(
                            request["method"]
                                .as_str()
                                .expect("JSON-RPC method")
                                .to_string(),
                        );
                        (StatusCode::UNAUTHORIZED, [(WWW_AUTHENTICATE, challenge)])
                    }
                },
            ),
        )
        .route(
            "/.well-known/oauth-protected-resource/mcp",
            axum::routing::get(move || {
                let resource = resource.clone();
                let authorization_origin = authorization_origin.clone();
                async move {
                    (
                        [(axum::http::header::CONTENT_TYPE, "application/json")],
                        serde_json::to_string(&json!({
                            "resource": resource,
                            "authorization_servers": [authorization_origin]
                        }))
                        .expect("serialize resource metadata"),
                    )
                }
            }),
        )
        .route(
            "/.well-known/oauth-authorization-server",
            axum::routing::get({
                let origin = origin.clone();
                move || {
                    let origin = origin.clone();
                    async move {
                        (
                            [(axum::http::header::CONTENT_TYPE, "application/json")],
                            serde_json::to_string(&json!({
                                "issuer": origin,
                                "authorization_endpoint": format!("{origin}/authorize"),
                                "token_endpoint": format!("{origin}/token")
                            }))
                            .expect("serialize authorization metadata"),
                        )
                    }
                }
            }),
        );
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("OAuth challenge fixture should run");
    });
    let endpoint = format!("{origin}/mcp");
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(endpoint.clone()),
    );

    let error = match ClientInfo::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Auto {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
                legacy_version: Some(ProtocolVersion::V_2025_06_18),
            },
        )
        .await
    {
        Ok(client) => {
            let _ = client.cancel().await;
            panic!("unauthorized discovery must not start a legacy session")
        }
        Err(error) => error,
    };
    let observed_challenge = error
        .auth_challenge()
        .expect("initial server/discover challenge must reach OAuth");
    assert_eq!(observed_challenge, challenge);

    let manager = rmcp::transport::AuthorizationManager::new(endpoint)
        .await
        .expect("create authorization manager");
    let resolution = manager
        .resolve_metadata_from_challenge(Some(observed_challenge))
        .await
        .expect("resolve OAuth metadata from the real discovery challenge");

    assert_eq!(
        resolution.source,
        rmcp::transport::auth::AuthorizationMetadataSource::ProtectedResourceMetadata
    );
    assert_eq!(resolution.metadata.issuer.as_deref(), Some(origin.as_str()));
    assert_eq!(
        *observed_methods.lock().expect("read discovery methods"),
        ["server/discover"],
        "OAuth must reuse the real lifecycle challenge without initialize or another protocol probe"
    );
    server.abort();
}
