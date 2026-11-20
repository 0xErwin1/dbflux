//! Bounded successful-use credits. Producers receive no persistence or consent controls.

use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

pub const MAX_COUNT: u16 = 10_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum UsageFeature {
    QueryRun,
    VisualQueryBuilder,
    DashboardOpen,
    ScriptRun,
    McpTool,
    SchemaInspect,
}
impl UsageFeature {
    pub const ALL: [Self; 6] = [
        Self::QueryRun,
        Self::VisualQueryBuilder,
        Self::DashboardOpen,
        Self::ScriptRun,
        Self::McpTool,
        Self::SchemaInspect,
    ];
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UsageError {
    Poisoned,
    GenerationExhausted,
}

struct Gate {
    generation: u64,
    enabled: bool,
    counts: [u16; 6],
    permission: Arc<dyn Fn() -> bool + Send + Sync>,
}

#[derive(Clone)]
pub struct UsageSource {
    gate: Arc<Mutex<Gate>>,
}

#[derive(Clone)]
pub struct UsageTicket {
    gate: Arc<Mutex<Gate>>,
    generation: u64,
    used: Arc<AtomicU8>,
}

/// Not Send: lifecycle and persistence belong to the foreground owner.
pub struct UsageController {
    gate: Arc<Mutex<Gate>>,
    foreground: PhantomData<Rc<()>>,
}

impl UsageController {
    pub fn new() -> Self {
        Self::with_permission(Arc::new(|| true))
    }
    pub fn with_permission(permission: Arc<dyn Fn() -> bool + Send + Sync>) -> Self {
        Self {
            gate: Arc::new(Mutex::new(Gate {
                generation: 0,
                enabled: false,
                counts: [0; 6],
                permission,
            })),
            foreground: PhantomData,
        }
    }
    pub fn source(&self) -> UsageSource {
        UsageSource {
            gate: self.gate.clone(),
        }
    }
    pub fn set_enabled(&mut self, enabled: bool) -> Result<(), UsageError> {
        self.gate.lock().map_err(|_| UsageError::Poisoned)?.enabled = enabled;
        Ok(())
    }
    pub fn invalidate(&mut self) -> Result<(), UsageError> {
        let mut gate = self.gate.lock().map_err(|_| UsageError::Poisoned)?;
        gate.enabled = false;
        gate.counts = [0; 6];
        gate.generation = gate
            .generation
            .checked_add(1)
            .ok_or(UsageError::GenerationExhausted)?;
        Ok(())
    }
    /// The finite six-slot snapshot is cleared only after a successful foreground commit.
    /// Holding the gate excludes concurrent credits until the transaction has a disposition.
    pub fn commit<E>(
        &mut self,
        persist: impl FnOnce([u16; 6]) -> Result<(), E>,
    ) -> Result<Result<(), E>, UsageError> {
        let mut gate = self.gate.lock().map_err(|_| UsageError::Poisoned)?;
        match persist(gate.counts) {
            Ok(()) => {
                gate.counts = [0; 6];
                Ok(Ok(()))
            }
            Err(error) => {
                gate.enabled = false;
                Ok(Err(error))
            }
        }
    }
}
impl Default for UsageController {
    fn default() -> Self {
        Self::new()
    }
}
impl Drop for UsageController {
    fn drop(&mut self) {
        let mut gate = match self.gate.lock() {
            Ok(gate) => gate,
            Err(poisoned) => poisoned.into_inner(),
        };
        gate.enabled = false;
        gate.counts = [0; 6];
    }
}
impl UsageSource {
    /// Capture at operation start, never at asynchronous completion.
    pub fn ticket(&self) -> Result<Option<UsageTicket>, UsageError> {
        let gate = self.gate.lock().map_err(|_| UsageError::Poisoned)?;
        Ok((gate.enabled && (gate.permission)()).then(|| UsageTicket {
            gate: self.gate.clone(),
            generation: gate.generation,
            used: Arc::new(AtomicU8::new(0)),
        }))
    }
}
impl UsageTicket {
    /// Check lifecycle permission without consuming any feature credit.
    pub fn is_current(&self) -> Result<bool, UsageError> {
        let gate = self.gate.lock().map_err(|_| UsageError::Poisoned)?;
        Ok(gate.enabled && (gate.permission)() && gate.generation == self.generation)
    }

    /// Call only on success. Rejected or duplicate credits return false.
    pub fn record(&self, feature: UsageFeature) -> Result<bool, UsageError> {
        let mut gate = self.gate.lock().map_err(|_| UsageError::Poisoned)?;
        if !gate.enabled || !(gate.permission)() || gate.generation != self.generation {
            return Ok(false);
        }
        let index = feature as usize;
        let bit = 1 << index;
        if self.used.fetch_or(bit, Ordering::Relaxed) & bit != 0 {
            return Ok(false);
        }
        let Some(count) = gate.counts.get_mut(index) else {
            return Ok(false);
        };
        *count = count.saturating_add(1).min(MAX_COUNT);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
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
}
