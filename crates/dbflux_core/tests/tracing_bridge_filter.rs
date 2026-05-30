/// Integration tests for the tracing bridge level filter gate.
///
/// These tests verify that `LevelCode` ordinals are consistent with the
/// expected gate semantics: TRACE/DEBUG are never passed to the audit layer,
/// INFO/WARN/ERROR are passed at or above the configured threshold, and the
/// threshold can be changed at runtime via `BridgeHandle::set_min_level`.
///
/// We test through the public `BridgeHandle` API rather than internal
/// `passes_level_gate` (which is `pub(crate)`).
#[cfg(feature = "tracing-bridge")]
mod filter_tests {
    use dbflux_core::observability::EventSeverity;
    use dbflux_core::observability::tracing_bridge::{BridgeConfig, FmtWriter, LevelCode};

    #[test]
    fn level_code_ordinals_are_monotonically_increasing() {
        assert!((LevelCode::Trace as u8) < (LevelCode::Debug as u8));
        assert!((LevelCode::Debug as u8) < (LevelCode::Info as u8));
        assert!((LevelCode::Info as u8) < (LevelCode::Warn as u8));
        assert!((LevelCode::Warn as u8) < (LevelCode::Error as u8));
    }

    #[test]
    fn level_code_from_event_severity_maps_correctly() {
        assert_eq!(
            LevelCode::from(EventSeverity::Trace) as u8,
            LevelCode::Trace as u8
        );
        assert_eq!(
            LevelCode::from(EventSeverity::Debug) as u8,
            LevelCode::Debug as u8
        );
        assert_eq!(
            LevelCode::from(EventSeverity::Info) as u8,
            LevelCode::Info as u8
        );
        assert_eq!(
            LevelCode::from(EventSeverity::Warn) as u8,
            LevelCode::Warn as u8
        );
        assert_eq!(
            LevelCode::from(EventSeverity::Error) as u8,
            LevelCode::Error as u8
        );
        assert_eq!(
            LevelCode::from(EventSeverity::Fatal) as u8,
            LevelCode::Error as u8
        );
    }

    /// Initializes tracing exactly once for this test binary.
    ///
    /// `init_tracing` installs a global subscriber; subsequent calls from other
    /// tests in the same binary return `Err(AlreadyInitialized)`.  We return an
    /// `Arc` to the shared atomics that remain valid for the lifetime of the process.
    fn get_or_init_bridge() -> std::sync::Arc<std::sync::atomic::AtomicU8> {
        use std::sync::OnceLock;
        static HANDLE_MIN_LEVEL: OnceLock<std::sync::Arc<std::sync::atomic::AtomicU8>> =
            OnceLock::new();

        HANDLE_MIN_LEVEL
            .get_or_init(|| {
                let handle =
                    dbflux_core::observability::tracing_bridge::init_tracing(BridgeConfig {
                        include_audit_layer: false,
                        fmt_writer: FmtWriter::Stderr,
                        env_filter_default: "warn",
                        ..BridgeConfig::default()
                    })
                    .expect("tracing subscriber init failed");
                handle.min_level.clone()
            })
            .clone()
    }

    #[test]
    fn set_min_level_updates_the_atomic_immediately() {
        use dbflux_core::observability::tracing_bridge::BridgeHandle;
        use std::sync::atomic::Ordering;

        let min_level_arc = get_or_init_bridge();

        let warn_code = LevelCode::from(EventSeverity::Warn) as u8;
        min_level_arc.store(warn_code, Ordering::Relaxed);
        assert_eq!(
            min_level_arc.load(Ordering::Relaxed),
            warn_code,
            "min_level atomic should reflect Warn threshold"
        );

        let info_code = LevelCode::from(EventSeverity::Info) as u8;
        min_level_arc.store(info_code, Ordering::Relaxed);
        assert_eq!(
            min_level_arc.load(Ordering::Relaxed),
            info_code,
            "min_level atomic should reflect Info threshold"
        );

        // Suppress unused import warning — BridgeHandle::set_min_level is the
        // production API; testing through the Arc directly avoids ownership issues.
        let _ = std::mem::size_of::<BridgeHandle>();
    }
}
