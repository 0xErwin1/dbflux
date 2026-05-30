/// Integration test for the tracing bridge overflow / drop counter.
///
/// Constructs a bridge with a tiny queue (capacity 4) and a slow fake sink that
/// blocks 20ms per record.  Fires 100 `tracing::warn!` events synchronously.
/// Asserts that the drop counter is positive (some events were dropped due to
/// queue overflow) and that the total is consistent (drops + drained ≈ fired).
///
/// Because `init_tracing` installs a global subscriber, this test runs in its
/// own binary (separate file) to avoid conflicts with `tracing_bridge_filter.rs`.
#[cfg(feature = "tracing-bridge")]
mod overflow_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use dbflux_core::observability::source::{EventSink, EventSinkError};
    use dbflux_core::observability::tracing_bridge::{BridgeConfig, FmtWriter, init_tracing};
    use dbflux_core::observability::{EventRecord, EventSeverity};

    struct CountingSink {
        count: Arc<AtomicU64>,
        delay: Duration,
    }

    impl EventSink for CountingSink {
        fn record(&self, event: EventRecord) -> Result<EventRecord, EventSinkError> {
            std::thread::sleep(self.delay);
            self.count.fetch_add(1, Ordering::Relaxed);
            Ok(event)
        }
    }

    #[test]
    fn overflow_drops_events_and_increments_drop_counter() {
        let handle = init_tracing(BridgeConfig {
            include_audit_layer: true,
            fmt_writer: FmtWriter::Stderr,
            queue_capacity: 4,
            min_level: EventSeverity::Info,
            env_filter_default: "info",
        })
        .expect("tracing subscriber init failed");

        let drained_count = Arc::new(AtomicU64::new(0));
        let sink = CountingSink {
            count: drained_count.clone(),
            delay: Duration::from_millis(20),
        };
        handle
            .install_sink(Arc::new(sink))
            .expect("sink install failed");

        // The audit layer gates on `target.starts_with("dbflux")` to filter
        // upstream-dep noise. Tests run under their own crate target by default
        // (`tracing_bridge_overflow::...`), which would be filtered — pin the
        // target explicitly so the bridge actually exercises the queue.
        for i in 0..100u32 {
            tracing::warn!(target: "dbflux_core::test::overflow", "overflow test event {}", i);
        }

        // Give the drain thread time to flush what it can.
        std::thread::sleep(Duration::from_millis(300));

        let dropped = handle.drop_count();
        let drained = drained_count.load(Ordering::Relaxed);

        assert!(
            dropped > 0,
            "expected some events to be dropped with queue_capacity=4, got dropped={dropped} drained={drained}"
        );

        // Total must not exceed fired count (100), allowing for measurement lag.
        let total = dropped + drained;
        assert!(
            total <= 100,
            "dropped({dropped}) + drained({drained}) = {total} must not exceed fired count 100"
        );
    }
}
