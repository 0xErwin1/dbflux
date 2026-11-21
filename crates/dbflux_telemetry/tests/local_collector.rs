//! Opt-in collector contract test; no application state or user database access.

use dbflux_telemetry::{
    Decision,
    transport::{Endpoint, Transport, TransportError},
};
use std::net::IpAddr;
use uuid::Uuid;

#[test]
#[ignore = "requires a caller-started local collector admitting 1.2.3/stable and DBFLUX_TELEMETRY_TEST_ENDPOINT"]
fn local_collector_stored_duplicate_and_conflict() {
    let configured = std::env::var("DBFLUX_TELEMETRY_TEST_ENDPOINT")
        .expect("explicit local collector endpoint required");
    let url = reqwest::Url::parse(&configured).expect("endpoint URL");
    assert_eq!(url.scheme(), "http", "only the isolated local collector");
    let address: IpAddr = url
        .host_str()
        .expect("numeric host")
        .trim_matches(['[', ']'])
        .parse()
        .expect("numeric loopback required");
    assert!(address.is_loopback());
    let endpoint = Endpoint::parse(Some(&configured), true).expect("debug-only local endpoint");
    let transport = Transport::new(endpoint).expect("transport");
    let install_id = Uuid::new_v4();
    for event in ["ping", "active", "basic"] {
        let mut payload = serde_json::json!({
            "schema": "dbflux.telemetry-event/v1",
            "event": event,
            "install_id": install_id,
            "delivery_id": Uuid::new_v4(),
            "sent_at": "2026-01-01T00:00:00Z",
            "version": "1.2.3",
            "channel": "stable",
            "os": "linux",
            "arch": "amd64"
        });
        if event == "basic" {
            payload["system_language"] = serde_json::json!("en");
            payload["country"] = serde_json::json!("ZZ");
            payload["features"] = serde_json::json!({
                "query_run": 1,
                "visual_query_builder": 2,
                "dashboard_open": 3,
                "script_run": 4,
                "mcp_tool": 5,
                "schema_inspect": 6
            });
        }
        let frozen = serde_json::to_string(&payload).expect("fixture serialization");
        assert!(matches!(
            transport.attempt(&frozen).expect("first delivery").decision,
            Decision::Stored
        ));
        assert!(matches!(
            transport
                .attempt(&frozen)
                .expect("unchanged replay")
                .decision,
            Decision::Duplicate
        ));
        payload["arch"] = serde_json::json!("arm64");
        let conflicting = serde_json::to_string(&payload).expect("conflicting fixture");
        assert_eq!(
            transport
                .attempt(&conflicting)
                .expect_err("same ID with altered bytes"),
            TransportError::Conflict
        );
    }
}
