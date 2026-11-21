use super::*;

/// Pings only: at Minimal the queue also holds a weekly `active`, so a
/// per-build assertion must not count deliveries.
fn pending_pings(state: &TelemetryState) -> usize {
    state
        .pending
        .iter()
        .filter(|delivery| !delivery.basic && !delivery.active)
        .count()
}

fn clock() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .expect("fixture clock")
        .with_timezone(&Utc)
}

#[test]
fn telemetry_language_normalization_is_bounded_and_script_first() {
    for (locale, expected) in [
        ("PT_br.UTF-8", "pt"),
        ("de-DE", "de"),
        ("C", "und"),
        ("english", "und"),
        ("", "und"),
        ("zh_TW", "zh-Hant"),
        ("zh_CN", "zh-Hans"),
        ("zh-TW-Hans", "zh-Hans"),
        ("zh", "zh"),
        ("ja_JP", "ja"),
    ] {
        assert_eq!(normalize_system_language(locale), expected, "{locale}");
    }
}

/// Minimal is the default level, so it must be able to report activity.
/// It sends `active` weekly — never `basic`, which carries the opted-in
/// dimensions — and Basic replaces it with the full heartbeat.
#[test]
fn minimal_sends_active_weekly_and_basic_replaces_it() {
    let now = clock();
    let build = Build {
        version: "1.5.0".into(),
        channel: Channel::Stable,
    };
    let mut state = TelemetryState::new(now);
    state.set_level(Level::Minimal);
    state.dismiss_disclosure();
    state
        .schedule(
            now,
            &build,
            Platform::Linux,
            Architecture::Amd64,
            "en",
            None,
        )
        .expect("schedule at Minimal");

    let kinds: Vec<&str> = state
        .pending
        .iter()
        .map(|delivery| {
            if delivery.basic {
                "basic"
            } else if delivery.active {
                "active"
            } else {
                "ping"
            }
        })
        .collect();
    assert_eq!(
        kinds,
        vec!["ping", "active"],
        "Minimal sends ping plus active"
    );

    let active = state
        .pending
        .iter()
        .find(|delivery| delivery.active)
        .expect("active queued");
    let wire: serde_json::Value = serde_json::from_str(active.json()).expect("wire");
    assert_eq!(wire["event"], "active");
    assert_eq!(wire.as_object().expect("object").len(), 9);

    let ack = Acknowledgement::parse(
        200,
        br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#,
    )
    .expect("ack");
    let active_id = active.id;
    state
        .acknowledge(active_id, &ack, now + Duration::hours(1))
        .expect("acknowledge");
    state
        .schedule(
            now + Duration::days(1),
            &build,
            Platform::Linux,
            Architecture::Amd64,
            "en",
            None,
        )
        .expect("second schedule");
    assert!(
        !state.pending.iter().any(|delivery| delivery.active),
        "active must not repeat within the week"
    );

    let mut basic = TelemetryState::new(now);
    basic.dismiss_disclosure();
    basic.set_level(Level::Basic);
    basic
        .schedule(
            now,
            &build,
            Platform::Linux,
            Architecture::Amd64,
            "en",
            None,
        )
        .expect("schedule at Basic");
    assert!(
        basic.pending.iter().any(|delivery| delivery.basic),
        "Basic queues its heartbeat"
    );
    assert!(
        !basic.pending.iter().any(|delivery| delivery.active),
        "Basic must not also queue an active event"
    );
}

#[test]
fn telemetry_minimal_ping_wire_and_rotation() {
    let now = clock();
    let mut state = TelemetryState::new(now);
    state.set_level(Level::Minimal);
    let build = Build {
        version: "1.2.3-rc.1+metadata".into(),
        channel: Channel::Rc,
    };
    assert_eq!(state.level(), Level::Minimal);
    state.increment(Feature::McpTool);
    state.dismiss_disclosure();
    state
        .schedule(
            now,
            &build,
            Platform::Darwin,
            Architecture::Arm64,
            "secret",
            Some("bad"),
        )
        .expect("queue");
    assert_eq!(pending_pings(&state), 1);
    let original = state.pending[0].clone();
    let json: serde_json::Value = serde_json::from_str(original.json()).expect("wire");
    let object = json.as_object().expect("object");
    assert_eq!(object.len(), 9);
    for key in [
        "schema",
        "event",
        "install_id",
        "delivery_id",
        "sent_at",
        "version",
        "channel",
        "os",
        "arch",
    ] {
        assert!(object.contains_key(key));
    }
    assert_eq!(json["schema"], "dbflux.telemetry-event/v1");
    assert_eq!(json["event"], "ping");
    assert_eq!(json["version"], build.version);
    assert_eq!(json["os"], "darwin");
    assert_eq!(json["arch"], "arm64");
    assert_eq!(json["sent_at"], "2026-01-01T00:00:00Z");
    assert_eq!(state.install_id.get_version(), Some(uuid::Version::Random));
    assert_eq!(original.id.get_version(), Some(uuid::Version::Random));
    state.retry_later(original.id, now);
    assert!(!state.pending[0].ready(now));
    assert!(state.pending[0].ready(now + Duration::days(1)));
    assert_eq!(state.pending[0].json(), original.json());
    // The installation id is stable for the life of the installation:
    // time passing never re-mints it, because a rotating id cannot count
    // unique installations across periods.
    let original_install = state.install_id();
    state
        .schedule(
            now + Duration::days(400),
            &build,
            Platform::Darwin,
            Architecture::Arm64,
            "en",
            None,
        )
        .expect("queue after more than a year");
    assert_eq!(
        state.install_id(),
        original_install,
        "the installation id must never rotate on its own"
    );
    assert_eq!(pending_pings(&state), 1);
    let ack = Acknowledgement::parse(
        200,
        br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#,
    )
    .expect("ack");
    state
        .acknowledge(state.pending[0].id, &ack, now + Duration::days(90))
        .expect("acknowledge valid state");
    state.reset_identity(now + Duration::days(91));
    state
        .schedule(
            now + Duration::days(91),
            &build,
            Platform::Darwin,
            Architecture::Arm64,
            "en",
            None,
        )
        .expect("acknowledged build");
    assert_eq!(
        pending_pings(&state),
        0,
        "an acknowledged build never pings again"
    );
    let other_build = Build {
        version: "1.2.3-rc.1+other".into(),
        channel: Channel::Rc,
    };
    state
        .schedule(
            now + Duration::days(91),
            &other_build,
            Platform::Linux,
            Architecture::Amd64,
            "en",
            None,
        )
        .expect("exact metadata build");
    assert_eq!(pending_pings(&state), 1, "a new build pings once");
}

fn fixture_build() -> Build {
    Build {
        version: "1.2.3".into(),
        channel: Channel::Stable,
    }
}

fn schedule_fixture(state: &mut TelemetryState, now: DateTime<Utc>) {
    state
        .schedule(
            now,
            &fixture_build(),
            Platform::Linux,
            Architecture::Amd64,
            "en_US",
            Some("US"),
        )
        .expect("schedule");
}

#[test]
fn telemetry_off_queues_nothing_after_the_disclosure_is_dismissed() {
    let now = clock();
    let mut state = TelemetryState::new(now);
    state.dismiss_disclosure();
    state.set_level(Level::Off);

    state.increment(Feature::QueryRun);
    schedule_fixture(&mut state, now);
    schedule_fixture(&mut state, now + Duration::days(8));

    assert!(state.pending.is_empty());
    assert!(state.counters.is_empty());
    state.validate().expect("valid Off state");
}

#[test]
fn telemetry_counts_nothing_before_the_disclosure_is_accepted() {
    let mut state = TelemetryState::new(clock());
    assert_eq!(state.level(), Level::Basic);

    state.increment(Feature::QueryRun);
    state.increment_batch([
        (Feature::QueryRun, 2),
        (Feature::VisualQueryBuilder, 2),
        (Feature::DashboardOpen, 2),
        (Feature::ScriptRun, 2),
        (Feature::McpTool, 2),
        (Feature::SchemaInspect, 2),
    ]);
    assert!(state.counters.is_empty());

    state.dismiss_disclosure();
    state.increment(Feature::QueryRun);
    assert_eq!(state.counters.get(&Feature::QueryRun), Some(&1));
}

#[test]
fn telemetry_new_state_preselects_basic_without_sending() {
    let mut state = TelemetryState::new(clock());

    assert_eq!(state.level(), Level::Basic);
    assert!(!state.disclosure_dismissed());
    state.increment(Feature::McpTool);
    state
        .schedule(
            clock(),
            &Build {
                version: "1.2.3".into(),
                channel: Channel::Stable,
            },
            Platform::Linux,
            Architecture::Amd64,
            "en",
            None,
        )
        .expect("schedule before the disclosure");
    assert!(state.pending().is_empty());
    assert!(!state.disclosure_dismissed());
}

#[test]
fn telemetry_minimal_queues_exactly_one_ping_per_build() {
    let now = clock();
    let mut state = TelemetryState::new(now);

    schedule_fixture(&mut state, now);
    assert!(state.pending.is_empty(), "nothing before the disclosure");

    state.dismiss_disclosure();
    state.set_level(Level::Minimal);
    state.increment(Feature::QueryRun);
    schedule_fixture(&mut state, now);
    schedule_fixture(&mut state, now + Duration::days(8));

    assert_eq!(pending_pings(&state), 1);
    assert!(
        state.pending.iter().any(|delivery| delivery.active),
        "Minimal also queues its weekly active event"
    );
    assert!(
        !state.pending.iter().any(|delivery| delivery.basic),
        "Minimal never queues a Basic report"
    );
    assert!(!state.pending[0].basic);
    assert!(state.counters.is_empty(), "usage counting is Basic-only");
    let json: serde_json::Value = serde_json::from_str(state.pending[0].json()).expect("wire");
    assert_eq!(json["event"], "ping");
    assert_eq!(json.as_object().expect("object").len(), 9);

    let ack = Acknowledgement::parse(
        200,
        br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#,
    )
    .expect("ack");
    let id = state.pending[0].id;
    state.acknowledge(id, &ack, now).expect("acknowledge");
    schedule_fixture(&mut state, now + Duration::days(30));
    assert_eq!(pending_pings(&state), 0, "one ping per build");
}

#[test]
fn telemetry_switching_basic_to_minimal_keeps_the_ping_and_drops_usage() {
    let now = clock();
    let mut state = TelemetryState::new(now);
    state.dismiss_disclosure();
    state.set_level(Level::Basic);
    state.increment(Feature::QueryRun);
    schedule_fixture(&mut state, now);
    state.increment(Feature::ScriptRun);

    state.set_level(Level::Minimal);

    assert_eq!(state.pending.len(), 1);
    assert!(!state.pending[0].basic);
    assert!(state.counters.is_empty());
    state.validate().expect("valid Minimal state");
}

#[test]
fn telemetry_persisted_levels_keep_their_stored_names() {
    for (level, stored) in [
        (Level::Off, "Off"),
        (Level::Minimal, "Minimal"),
        (Level::Basic, "Basic"),
    ] {
        let mut state = TelemetryState::new(clock());
        state.set_level(level);
        let mut json = serde_json::to_value(&state).expect("state");
        assert_eq!(json["level"], stored);

        json["level"] = stored.into();
        let loaded: TelemetryState = serde_json::from_value(json).expect("stored row");
        assert_eq!(loaded.level(), level);
        loaded.validate().expect("valid stored row");
    }
}

#[test]
fn telemetry_stored_off_row_with_a_queued_ping_loads_and_sends_nothing() {
    let now = clock();
    let mut state = TelemetryState::new(now);
    state.set_level(Level::Minimal);
    state.dismiss_disclosure();
    schedule_fixture(&mut state, now);
    assert_eq!(pending_pings(&state), 1);

    let mut json = serde_json::to_value(&state).expect("state");
    json["level"] = "Off".into();
    let mut loaded: TelemetryState = serde_json::from_value(json).expect("stored row");
    loaded
        .validate()
        .expect("row written before Off sent nothing");

    schedule_fixture(&mut loaded, now);
    assert!(loaded.pending.is_empty());
}

#[test]
fn telemetry_switching_to_off_clears_a_pending_ping() {
    let now = clock();
    let mut state = TelemetryState::new(now);
    state.dismiss_disclosure();
    state.set_level(Level::Basic);
    state.increment(Feature::QueryRun);
    schedule_fixture(&mut state, now);
    assert_eq!(state.pending.len(), 2);

    state.set_level(Level::Off);

    assert!(state.pending.is_empty());
    assert!(state.counters.is_empty());
    schedule_fixture(&mut state, now);
    assert!(state.pending.is_empty());
}

#[test]
fn telemetry_basic_snapshot_saturates_and_off_discards_prior_usage() {
    let now = clock();
    let mut state = TelemetryState::new(now);
    let build = Build {
        version: "1.2.3".into(),
        channel: Channel::Nightly,
    };
    state.dismiss_disclosure();
    state.set_level(Level::Basic);
    for feature in [
        Feature::QueryRun,
        Feature::VisualQueryBuilder,
        Feature::DashboardOpen,
        Feature::ScriptRun,
        Feature::McpTool,
        Feature::SchemaInspect,
    ] {
        for _ in 0..10_005 {
            state.increment(feature);
        }
    }
    state
        .schedule(
            now,
            &build,
            Platform::Windows,
            Architecture::Amd64,
            "zh_TW",
            None,
        )
        .expect("queue");
    let basic = state
        .pending
        .iter()
        .find(|delivery| delivery.basic)
        .expect("basic");
    let json: serde_json::Value = serde_json::from_str(basic.json()).expect("wire");
    assert_eq!(json.as_object().expect("object").len(), 12);
    assert_eq!(json["system_language"], "zh-Hant");
    assert_eq!(json["country"], "ZZ");
    let features = json["features"].as_object().expect("features");
    assert_eq!(features.len(), 6);
    assert!(features.values().all(|count| count == 10_000));
    let basic_id = basic.id;
    state.increment(Feature::QueryRun);
    let ack = Acknowledgement::parse(
        200,
        br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#,
    )
    .expect("ack");
    state
        .acknowledge(basic_id, &ack, now + Duration::days(2))
        .expect("acknowledge valid state");
    assert_eq!(state.counters.get(&Feature::QueryRun), Some(&1));
    state
        .schedule(
            now + Duration::days(8),
            &build,
            Platform::Windows,
            Architecture::Amd64,
            "en",
            None,
        )
        .expect("too early");
    assert!(!state.pending.iter().any(|delivery| delivery.basic));
    state
        .schedule(
            now + Duration::days(9),
            &build,
            Platform::Windows,
            Architecture::Amd64,
            "en",
            Some("US"),
        )
        .expect("weekly since success");
    state.set_level(Level::Minimal);
    assert_eq!(state.pending.len(), 1);
    assert!(!state.pending[0].basic);
    state.set_level(Level::Off);
    assert!(state.pending.is_empty());
    state.set_level(Level::Basic);
    state
        .schedule(
            now + Duration::days(10),
            &build,
            Platform::Windows,
            Architecture::Amd64,
            "en",
            None,
        )
        .expect("reenable");
    let json: serde_json::Value = serde_json::from_str(
        state
            .pending
            .iter()
            .find(|delivery| delivery.basic)
            .expect("basic")
            .json(),
    )
    .expect("wire");
    assert_eq!(json["features"], serde_json::json!({}));
    let previous = state.clone();
    let invalid = Build {
        version: "not a version".into(),
        channel: Channel::Stable,
    };
    assert!(
        state
            .schedule(
                now,
                &invalid,
                Platform::Linux,
                Architecture::Amd64,
                "en",
                None
            )
            .is_err()
    );
    assert_eq!(state, previous);
}

#[test]
fn telemetry_batch_caps_all_six_and_respects_off() {
    let features = [
        Feature::QueryRun,
        Feature::VisualQueryBuilder,
        Feature::DashboardOpen,
        Feature::ScriptRun,
        Feature::McpTool,
        Feature::SchemaInspect,
    ];
    let batch = features.map(|feature| (feature, u16::MAX));
    let mut state = TelemetryState::new(clock());
    state.dismiss_disclosure();
    state.set_level(Level::Minimal);
    state.increment_batch(batch);
    assert!(state.counters.is_empty());
    state.set_level(Level::Basic);
    state.increment_batch(batch);
    assert_eq!(state.counters.len(), 6);
    assert!(state.counters.values().all(|count| *count == 10_000));
}

fn pending_basic() -> TelemetryState {
    let mut state = TelemetryState::new(clock());
    state.dismiss_disclosure();
    state.set_level(Level::Basic);
    state.increment(Feature::QueryRun);
    state
        .schedule(
            clock(),
            &Build {
                version: "1.2.3".into(),
                channel: Channel::Stable,
            },
            Platform::Linux,
            Architecture::Amd64,
            "en",
            None,
        )
        .expect("fixture queue");
    state
}

#[test]
fn telemetry_persisted_payload_rejects_duplicate_feature_keys() {
    let mut state = pending_basic();
    let delivery = state
        .pending
        .iter_mut()
        .find(|delivery| delivery.basic)
        .expect("basic");
    delivery.json = delivery
        .json
        .replace("\"query_run\":1", "\"query_run\":1,\"query_run\":2");
    let decoded: WireEvent =
        serde_json::from_str(&delivery.json).expect("ordinary typed decode loses duplicates");
    assert_eq!(
        decoded.features.expect("features").get(&Feature::QueryRun),
        Some(&2)
    );
    let frozen = delivery.json.clone();
    assert!(
        state.validate().is_err(),
        "collector rejects duplicate nested keys"
    );
    assert_eq!(
        state
            .pending
            .iter()
            .find(|delivery| delivery.basic)
            .expect("basic")
            .json,
        frozen
    );
}

#[test]
fn telemetry_strict_json_rejects_duplicates_at_every_depth() {
    for raw in [
        r#"{"a":1,"a":2}"#,
        r#"{"a":{"b":1,"b":2}}"#,
        r#"{"a":[{"b":1,"\u0062":2}]}"#,
        r#"{"a":1} {"b":2}"#,
    ] {
        assert_eq!(
            strict_json(raw.as_bytes(), 4096).expect_err("strict JSON"),
            ProtocolError
        );
    }
    assert!(
        strict_json(
            br#"{"a":[{"b":1},{"b":2}],"escaped":"\"query_run\":1,\"query_run\":2"}"#,
            4096
        )
        .is_ok()
    );
    assert!(strict_json(&[b' '; 4097], 4096).is_err());
}

#[test]
fn telemetry_persisted_payload_contract_parity_and_immutability() {
    let state = pending_basic();
    let index = state
        .pending
        .iter()
        .position(|delivery| delivery.basic)
        .expect("basic");
    let original = &state.pending[index].json;
    let id = state.install_id().to_string();
    let malformed_id = id.replace('-', "");
    let variants = [
        original.replace(
            "\"schema\":",
            "\"schema\":\"dbflux.telemetry-event/v1\",\"schema\":",
        ),
        original.replace("\"query_run\":1", "\"query_run\":1,\"\\u0071uery_run\":2"),
        original.replace("\"query_run\":1", "\"unknown_feature\":1"),
        original.replace("\"os\":\"linux\"", "\"os\":\"linux\",\"extra\":null"),
        original.replace(&id, &malformed_id),
        original.replace(&id, &format!("urn:uuid:{id}")),
        original.replace(&id, &format!("{{{id}}}")),
        original.replace(&id, "00000000-0000-7000-8000-000000000000"),
        original.replace(&id, "00000000-0000-4000-0000-000000000000"),
        original.replace("2026-01-01T00:00:00Z", "2026-01-01 00:00:00Z"),
        original.replace("2026-01-01T00:00:00Z", "2026-01-01T00:00:60Z"),
        original.replace("2026-01-01T00:00:00Z", "2026-02-30T00:00:00Z"),
        original.replace("2026-01-01T00:00:00Z", "2026-01-01T00:00:00"),
        original.replace("2026-01-01T00:00:00Z", "0001-01-01T00:00:00Z"),
        original.replace("\"system_language\":\"en\"", "\"system_language\":null"),
        original.replace("\"features\":{\"query_run\":1}", "\"features\":null"),
    ];
    for raw in variants {
        let mut corrupt = state.clone();
        corrupt.pending[index].json = raw.clone();
        let error = corrupt
            .validate()
            .expect_err("out-of-contract frozen payload");
        assert_eq!(error.to_string(), "invalid telemetry protocol");
        assert_eq!(
            corrupt.pending[index].json, raw,
            "validation never repairs a frozen body"
        );
    }
    for time in [
        "2026-01-01T00:00:00.123Z",
        "2026-01-01T01:00:00+01:00",
        "2025-12-31T23:00:00-01:00",
    ] {
        let mut valid = state.clone();
        valid.pending[index].json = original
            .replace("2026-01-01T00:00:00Z", time)
            .replace(&id, &id.to_ascii_uppercase());
        let frozen = valid.pending[index].json.clone();
        valid
            .validate()
            .expect("collector UUID casing and RFC3339 offsets");
        assert_eq!(valid.pending[index].json, frozen);
    }
}

#[test]
fn telemetry_feature_numbers_use_exact_bounded_decimal_arithmetic() {
    let state = pending_basic();
    let index = state
        .pending
        .iter()
        .position(|delivery| delivery.basic)
        .expect("basic");
    for (token, expected) in [
        ("12", true),
        ("12.0", true),
        ("1.2e1", true),
        ("10000.0", true),
        ("-0", true),
        ("0e99999999999999999999", true),
        ("0e-99999999999999999999", true),
        ("100000e-1", true),
        ("0.001e3", true),
        ("10000.000000000000001", false),
        ("10000000000000001e-12", false),
        ("10001", false),
        ("1.5", false),
        ("-1", false),
        ("1e400", false),
        ("1e-400", false),
        ("null", false),
        ("\"12\"", false),
        ("01", false),
    ] {
        let mut candidate = state.clone();
        candidate.pending[index].json = candidate.pending[index]
            .json
            .replace("\"query_run\":1", &format!("\"query_run\":{token}"));
        let frozen = candidate.pending[index].json.clone();
        assert_eq!(candidate.validate().is_ok(), expected, "{token}");
        assert_eq!(candidate.pending[index].json, frozen);
    }
}

#[test]
fn telemetry_persisted_queue_rejects_conflicting_delivery_and_build_identities() {
    let state = pending_basic();
    let ping = state
        .pending
        .iter()
        .find(|delivery| !delivery.basic)
        .expect("ping")
        .clone();
    let mut duplicate_id = state.clone();
    duplicate_id.pending.push(ping.clone());
    let mut duplicate_build = state.clone();
    let mut other_ping = ping.clone();
    other_ping.id = Uuid::new_v4();
    other_ping.json = other_ping
        .json
        .replace(&ping.id.to_string(), &other_ping.id.to_string());
    duplicate_build.pending.push(other_ping);
    let mut shared_id = state.clone();
    let basic = shared_id
        .pending
        .iter_mut()
        .find(|delivery| delivery.basic)
        .expect("basic");
    basic.json = basic
        .json
        .replace(&basic.id.to_string(), &ping.id.to_string());
    basic.id = ping.id;
    let mut acknowledged_pending = state.clone();
    acknowledged_pending.sent_builds.push(ping.build.clone());
    let mut duplicate_history = state.clone();
    duplicate_history.sent_builds.extend(vec![
        Build {
            version: "1.0.0".into(),
            channel: Channel::Stable
        };
        2
    ]);
    let cases = [
        ("duplicate delivery", duplicate_id),
        ("duplicate ping build", duplicate_build),
        ("shared ping/basic identity", shared_id),
        ("acknowledged pending build", acknowledged_pending),
        ("duplicate sent history", duplicate_history),
    ];
    let observed: Vec<_> = cases
        .iter()
        .map(|(label, corrupt)| (*label, corrupt.validate().is_err()))
        .collect();
    let expected: Vec<_> = cases.iter().map(|(label, _)| (*label, true)).collect();
    assert_eq!(observed, expected);
    let ack = Acknowledgement::parse(
        200,
        br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#,
    )
    .expect("ack");
    for (_, mut corrupt) in cases {
        let before = corrupt.clone();
        assert!(corrupt.acknowledge(ping.id, &ack, clock()).is_err());
        assert_eq!(corrupt, before, "invalid ACK state must remain unchanged");
        assert!(
            corrupt
                .schedule(
                    clock(),
                    &ping.build,
                    Platform::Linux,
                    Architecture::Amd64,
                    "en",
                    None
                )
                .is_err()
        );
        assert_eq!(
            corrupt, before,
            "invalid schedule state must remain unchanged"
        );
    }
}

#[test]
fn telemetry_zero_time_cannot_be_queued() {
    let mut state = TelemetryState::new(clock());
    state.dismiss_disclosure();
    let before = state.clone();
    let zero = DateTime::parse_from_rfc3339("0001-01-01T00:00:00Z")
        .expect("Go zero time")
        .with_timezone(&Utc);
    assert!(
        state
            .schedule(
                zero,
                &Build {
                    version: "1.2.3".into(),
                    channel: Channel::Stable
                },
                Platform::Linux,
                Architecture::Amd64,
                "en",
                None
            )
            .is_err()
    );
    assert_eq!(state, before);
    state
        .schedule(
            zero + Duration::seconds(1),
            &Build {
                version: "1.2.3".into(),
                channel: Channel::Stable,
            },
            Platform::Linux,
            Architecture::Amd64,
            "en",
            None,
        )
        .expect("year one is valid outside the exact zero instant");
    state.validate().expect("valid year-one payload");
}

#[test]
fn telemetry_persisted_state_rejects_out_of_contract_values() {
    let state = TelemetryState::new(clock());
    let mut json = serde_json::to_value(&state).expect("state");
    json["counters"] = serde_json::json!({"query_run": 10_001});
    let corrupt: TelemetryState = serde_json::from_value(json).expect("structural decode");
    assert!(corrupt.validate().is_err());
}

#[test]
fn telemetry_ack_is_strict_and_bounded() {
    assert!(
        Acknowledgement::parse(
            200,
            br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#
        )
        .is_ok()
    );
    for bytes in [
        br#"{}"#.as_slice(),
        br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored","extra":1}"#,
        br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored","decision":"duplicate"}"#,
    ] {
        assert!(Acknowledgement::parse(200, bytes).is_err());
    }
    assert!(Acknowledgement::parse(200, &[b' '; 1025]).is_err());
    assert!(
        Acknowledgement::parse(
            503,
            br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#
        )
        .is_err()
    );
}

#[test]
fn telemetry_schedule_preserves_snapshot_and_history() {
    let now = clock();
    let mut state = TelemetryState::new(now);
    let build = Build {
        version: "1.2.3+fixture".into(),
        channel: Channel::Stable,
    };
    state
        .schedule(
            now,
            &build,
            Platform::Linux,
            Architecture::Amd64,
            "en_US",
            None,
        )
        .expect("schedule");
    assert!(state.pending.is_empty());
    state.dismiss_disclosure();
    state.set_level(Level::Basic);
    state.increment(Feature::QueryRun);
    state
        .schedule(
            now,
            &build,
            Platform::Linux,
            Architecture::Amd64,
            "en_US",
            None,
        )
        .expect("schedule");
    assert_eq!(state.pending.len(), 2);
    let pending = state.pending.clone();
    state.increment(Feature::QueryRun);
    state
        .schedule(
            now,
            &build,
            Platform::Linux,
            Architecture::Amd64,
            "de",
            None,
        )
        .expect("retry");
    assert_eq!(state.pending, pending);
    assert_eq!(state.counters.get(&Feature::QueryRun), Some(&1));
    let ack = Acknowledgement::parse(
        200,
        br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"duplicate"}"#,
    )
    .expect("ack");
    for delivery in pending {
        state
            .acknowledge(delivery.id, &ack, now)
            .expect("acknowledge valid state");
    }
    state.reset_identity(now);
    state
        .schedule(
            now,
            &build,
            Platform::Linux,
            Architecture::Amd64,
            "en",
            None,
        )
        .expect("schedule");
    assert!(state.pending.is_empty());
    state
        .schedule(
            now + Duration::days(7),
            &build,
            Platform::Linux,
            Architecture::Amd64,
            "en",
            None,
        )
        .expect("weekly");
    assert_eq!(state.pending.len(), 1);
    state.set_level(Level::Off);
    assert!(state.pending.is_empty());
    assert!(state.counters.is_empty());
}

#[test]
fn telemetry_ack_rejects_arrays_and_non_string_enums() {
    for bytes in [
        br#"["dbflux.telemetry-delivery/v1","stored"]"#.as_slice(),
        br#"{"schema":"dbflux.telemetry-delivery/v1","decision":{"stored":null}}"#,
    ] {
        assert!(Acknowledgement::parse(200, bytes).is_err());
    }
    for decision in ["stored", "duplicate"] {
        let body =
            format!(r#"{{"schema":"dbflux.telemetry-delivery/v1","decision":"{decision}"}}"#);
        assert!(Acknowledgement::parse(200, body.as_bytes()).is_ok());
    }
}

#[test]
fn telemetry_payload_rejects_non_string_wire_enums() {
    let now = clock();
    let mut state = TelemetryState::new(now);
    state.dismiss_disclosure();
    state.set_level(Level::Minimal);
    schedule_fixture(&mut state, now);
    let json = state.pending[0].json().to_owned();
    validate_payload(&json).expect("the queued ping is valid");

    for (string_form, object_form) in [
        (r#""os":"linux""#, r#""os":{"linux":null}"#),
        (r#""arch":"amd64""#, r#""arch":{"amd64":null}"#),
        (r#""channel":"stable""#, r#""channel":{"stable":null}"#),
        (r#""event":"ping""#, r#""event":{"ping":null}"#),
    ] {
        assert!(json.contains(string_form), "{string_form} in {json}");
        let tampered = json.replace(string_form, object_form);
        assert!(validate_payload(&tampered).is_err(), "{object_form}");

        let mut corrupt = state.clone();
        corrupt.pending[0].json = tampered;
        assert!(corrupt.validate().is_err(), "{object_form} in the queue");
    }
}

#[test]
fn telemetry_switching_to_basic_drops_a_pending_active_and_keeps_the_ping() {
    let now = clock();
    let mut state = TelemetryState::new(now);
    state.dismiss_disclosure();
    state.set_level(Level::Minimal);
    schedule_fixture(&mut state, now);
    assert!(state.pending.iter().any(|delivery| delivery.active));

    state.set_level(Level::Basic);
    assert!(!state.pending.iter().any(|delivery| delivery.active));
    assert_eq!(pending_pings(&state), 1, "the build ping stays queued");

    schedule_fixture(&mut state, now);
    assert_eq!(
        state
            .pending
            .iter()
            .filter(|delivery| delivery.basic)
            .count(),
        1
    );
    assert!(!state.pending.iter().any(|delivery| delivery.active));
    state.validate().expect("valid Basic state");
}
