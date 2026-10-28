//! Link probe through the HTTP ingress: answered by the server and client, never by the local service.

use super::{start_echo_server, start_test_server, TestConfig};
use ferrotunnel::Client;
use std::time::Duration;

#[tokio::test]
async fn test_probe_reports_rtt_and_burst() {
    let config = TestConfig::default();
    let _echo_handle = start_echo_server(config.local_service_addr).await;
    let mut server = start_test_server(&config).await;

    let mut client = Client::builder()
        .server_addr(config.server_addr.to_string())
        .token(config.token)
        .local_addr(config.local_service_addr.to_string())
        .build()
        .expect("Failed to build client");
    let info = client.start().await.expect("Client failed to connect");
    let session_id = info.session_id().expect("Session ID").to_string();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let per_stream: u64 = 2 * 1024 * 1024;
    let url = format!(
        "http://{}/tunnel-probe?bytes={per_stream}&streams=3",
        config.http_addr
    );
    let response = super::make_client()
        .get(&url)
        .header("Host", session_id)
        .send()
        .await
        .expect("Failed to send probe request");

    assert_eq!(response.status(), 200);
    let body = response.text().await.expect("Failed to read probe body");
    assert!(
        !body.contains("Hello, World!"),
        "probe reached the local service: {body}"
    );
    assert!(body.contains("\"rtt_ms\":"), "missing rtt: {body}");
    assert!(
        body.contains(&format!("\"bytes\":{}", per_stream * 3)),
        "total bytes should cover all streams: {body}"
    );
    assert_eq!(
        body.matches("\"elapsed_ms\"").count(),
        4,
        "one total plus three streams: {body}"
    );

    let _ = client.shutdown().await;
    let _ = server.shutdown().await;
}

#[tokio::test]
async fn test_probe_unknown_tunnel_is_not_found() {
    let config = TestConfig::default();
    let mut server = start_test_server(&config).await;

    let url = format!("http://{}/tunnel-probe", config.http_addr);
    let response = super::make_client()
        .get(&url)
        .header("Host", "no-such-tunnel")
        .send()
        .await
        .expect("Failed to send probe request");

    assert_eq!(response.status(), 404);
    let _ = server.shutdown().await;
}
