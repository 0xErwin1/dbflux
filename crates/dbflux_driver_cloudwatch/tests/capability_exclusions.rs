//! Pins capability exclusions that are permanent design decisions, not gaps.
//!
//! CloudWatch Logs is a read-only log-streaming surface: its own server-side
//! metrics already live in CloudWatch Metrics, and it has no SQL-shaped
//! mutation/read templates to preview. These tests fail if either capability
//! flag or accessor is added later without revisiting that decision.

use dbflux_core::DriverCapabilities;
use dbflux_driver_cloudwatch::CLOUDWATCH_METADATA;

#[test]
fn metadata_excludes_instance_catalog_capabilities() {
    let excluded = [
        DriverCapabilities::INSTANCE_METRICS,
        DriverCapabilities::INSTANCE_INSPECTOR,
    ];

    for capability in excluded {
        assert!(
            !CLOUDWATCH_METADATA.capabilities.contains(capability),
            "capability {capability:?} must be absent: CloudWatch Logs' own server-side metrics belong to CloudWatch Metrics, not a per-driver InstanceCatalog"
        );
    }
}
