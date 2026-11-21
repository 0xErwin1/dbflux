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
mod tests;
