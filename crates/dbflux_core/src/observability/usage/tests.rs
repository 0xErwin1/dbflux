use super::*;
fn counts(owner: &mut UsageController) -> [u16; 6] {
    let mut result = [0; 6];
    owner
        .commit(|counts| {
            result = counts;
            Ok::<(), ()>(())
        })
        .expect("gate")
        .expect("commit");
    result
}
#[test]
fn usage_stale_ticket_cannot_credit_new_generation() {
    let mut owner = UsageController::new();
    owner.set_enabled(true).expect("enable");
    let ticket = owner.source().ticket().expect("gate").expect("ticket");
    owner.invalidate().expect("invalidate");
    owner.set_enabled(true).expect("enable new era");
    assert!(!ticket.record(UsageFeature::QueryRun).expect("record"));
    assert_eq!(counts(&mut owner), [0; 6]);
}
#[test]
fn usage_clones_deduplicate_each_feature_but_fresh_operations_count() {
    let mut owner = UsageController::new();
    assert!(owner.source().ticket().expect("gate").is_none());
    owner.set_enabled(true).expect("enable");
    let ticket = owner.source().ticket().expect("gate").expect("ticket");
    for feature in UsageFeature::ALL {
        assert!(ticket.record(feature).expect("record"));
        assert!(!ticket.clone().record(feature).expect("duplicate"));
        assert!(
            owner
                .source()
                .ticket()
                .expect("gate")
                .expect("fresh")
                .record(feature)
                .expect("record")
        );
    }
    assert_eq!(counts(&mut owner), [2; 6]);
}
#[test]
fn usage_failed_commit_retains_counts_and_rejection_does_not_consume_bit() {
    let mut owner = UsageController::new();
    owner.set_enabled(true).expect("enable");
    let ticket = owner.source().ticket().expect("gate").expect("ticket");
    assert!(ticket.record(UsageFeature::QueryRun).expect("record"));
    assert_eq!(owner.commit(|_| Err::<(), _>(())).expect("gate"), Err(()));
    assert!(!ticket.record(UsageFeature::ScriptRun).expect("disabled"));
    assert!(owner.source().ticket().expect("gate").is_none());
    assert_eq!(counts(&mut owner), [1, 0, 0, 0, 0, 0]);
    owner.set_enabled(true).expect("retry committed");
    assert!(ticket.record(UsageFeature::ScriptRun).expect("retry"));
    assert_eq!(counts(&mut owner), [0, 0, 0, 1, 0, 0]);
    assert_eq!(counts(&mut owner), [0; 6]);
}
#[test]
fn usage_owner_drop_disables_send_tickets_and_sources() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<UsageSource>();
    assert_send_sync::<UsageTicket>();
    let mut owner = UsageController::new();
    owner.set_enabled(true).expect("enable");
    let source = owner.source();
    let ticket = source.ticket().expect("gate").expect("ticket");
    drop(owner);
    assert!(source.ticket().expect("gate").is_none());
    assert!(!ticket.record(UsageFeature::QueryRun).expect("record"));
}
#[test]
fn usage_permission_is_checked_by_retained_sources_and_tickets() {
    let permission = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut owner = UsageController::with_permission({
        let permission = permission.clone();
        Arc::new(move || permission.load(Ordering::SeqCst))
    });
    owner.set_enabled(true).expect("enable");
    let source = owner.source();
    let ticket = source.ticket().expect("gate").expect("ticket");
    permission.store(false, Ordering::SeqCst);
    assert!(source.ticket().expect("gate").is_none());
    assert!(!ticket.record(UsageFeature::QueryRun).expect("denied"));
    assert_eq!(counts(&mut owner), [0; 6]);
}
#[test]
fn usage_commit_serializes_concurrent_credit_without_losing_delta() {
    let mut owner = UsageController::new();
    owner.set_enabled(true).expect("enable");
    let first = owner.source().ticket().expect("gate").expect("first");
    assert!(first.record(UsageFeature::QueryRun).expect("first credit"));
    let next = owner.source().ticket().expect("gate").expect("next");
    let (send, receive) = std::sync::mpsc::sync_channel(0);
    let mut snapshot = [0; 6];
    let mut worker = None;
    owner
        .commit(|counts| {
            snapshot = counts;
            worker = Some(std::thread::spawn(move || {
                send.send(()).expect("attempt credit");
                next.record(UsageFeature::QueryRun)
                    .expect("concurrent credit")
            }));
            receive.recv().expect("producer started while gate held");
            Ok::<(), ()>(())
        })
        .expect("gate")
        .expect("persist");
    assert!(worker.expect("worker").join().expect("join"));
    assert_eq!(snapshot, [1, 0, 0, 0, 0, 0]);
    assert_eq!(counts(&mut owner), [1, 0, 0, 0, 0, 0]);
    assert_eq!(counts(&mut owner), [0; 6]);
}
#[test]
fn usage_validity_is_side_effect_free_and_preserves_feature_credits() {
    let mut owner = UsageController::new();
    owner.set_enabled(true).expect("enable");
    let ticket = owner.source().ticket().expect("gate").expect("ticket");
    for _ in 0..3 {
        assert!(ticket.is_current().expect("current"));
        assert!(ticket.clone().is_current().expect("clone current"));
    }
    assert_eq!(counts(&mut owner), [0; 6]);
    for feature in UsageFeature::ALL {
        assert!(ticket.record(feature).expect("first credit"));
        assert!(ticket.is_current().expect("current after credit"));
        assert!(!ticket.record(feature).expect("duplicate credit"));
    }
    assert_eq!(counts(&mut owner), [1; 6]);
}

#[test]
fn usage_validity_tracks_permission_enabled_generation_and_owner_drop() {
    let permission = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut owner = UsageController::with_permission({
        let permission = permission.clone();
        Arc::new(move || permission.load(Ordering::SeqCst))
    });
    owner.set_enabled(true).expect("enable");
    let ticket = owner.source().ticket().expect("gate").expect("ticket");
    assert!(ticket.is_current().expect("current"));
    owner.set_enabled(false).expect("disable");
    assert!(!ticket.is_current().expect("disabled"));
    owner.set_enabled(true).expect("enable");
    permission.store(false, Ordering::SeqCst);
    assert!(!ticket.is_current().expect("permission denied"));
    permission.store(true, Ordering::SeqCst);
    assert!(ticket.is_current().expect("permission restored"));
    owner.invalidate().expect("new generation");
    owner.set_enabled(true).expect("enable new generation");
    assert!(!ticket.is_current().expect("stale"));
    let current = owner.source().ticket().expect("gate").expect("ticket");
    drop(owner);
    assert!(!current.is_current().expect("owner dropped"));
}

#[test]
fn usage_validity_reports_poisoned_gate_without_crediting() {
    let mut owner = UsageController::new();
    owner.set_enabled(true).expect("enable");
    let ticket = owner.source().ticket().expect("gate").expect("ticket");
    let gate = ticket.gate.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = gate.lock().expect("gate");
            panic!("poison test gate");
        })
        .join()
        .is_err()
    );
    assert_eq!(ticket.is_current(), Err(UsageError::Poisoned));
    assert_eq!(
        ticket.record(UsageFeature::McpTool),
        Err(UsageError::Poisoned)
    );
}

#[test]
fn usage_counts_saturate_at_canonical_bound() {
    let mut owner = UsageController::new();
    owner.set_enabled(true).expect("enable");
    for _ in 0..=MAX_COUNT {
        let ticket = owner.source().ticket().expect("gate").expect("ticket");
        assert!(ticket.record(UsageFeature::QueryRun).expect("record"));
    }
    assert_eq!(counts(&mut owner), [MAX_COUNT, 0, 0, 0, 0, 0]);
}
