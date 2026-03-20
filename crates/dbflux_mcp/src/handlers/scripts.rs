use std::collections::HashMap;

use dbflux_policy::{
    ExecutionClassification, PolicyDecision, PolicyEngine, PolicyEvaluationRequest,
};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptLifecycleState {
    Draft,
    Runnable,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptRecord {
    pub id: Uuid,
    pub name: String,
    pub body: String,
    pub lifecycle_state: ScriptLifecycleState,
}

#[derive(Debug, Error)]
pub enum ScriptHandlerError {
    #[error("script not found: {0}")]
    ScriptNotFound(Uuid),
    #[error("script is not runnable in current lifecycle state")]
    NotRunnable,
    #[error("policy denied request")]
    PolicyDenied,
    #[error("policy evaluation failed: {0}")]
    Policy(#[from] dbflux_policy::PolicyEngineError),
}

#[derive(Debug, Default)]
pub struct ScriptHandler {
    scripts: HashMap<Uuid, ScriptRecord>,
}

impl ScriptHandler {
    pub fn list_scripts(&self) -> Vec<ScriptRecord> {
        self.scripts.values().cloned().collect()
    }

    pub fn get_script(&self, script_id: Uuid) -> Result<ScriptRecord, ScriptHandlerError> {
        self.scripts
            .get(&script_id)
            .cloned()
            .ok_or(ScriptHandlerError::ScriptNotFound(script_id))
    }

    pub fn create_script(
        &mut self,
        name: String,
        body: String,
        lifecycle_state: ScriptLifecycleState,
    ) -> ScriptRecord {
        let script = ScriptRecord {
            id: Uuid::new_v4(),
            name,
            body,
            lifecycle_state,
        };

        self.scripts.insert(script.id, script.clone());
        script
    }

    pub fn update_script(
        &mut self,
        script_id: Uuid,
        body: String,
    ) -> Result<ScriptRecord, ScriptHandlerError> {
        let Some(script) = self.scripts.get_mut(&script_id) else {
            return Err(ScriptHandlerError::ScriptNotFound(script_id));
        };

        script.body = body;
        Ok(script.clone())
    }

    pub fn delete_script(&mut self, script_id: Uuid) -> Result<(), ScriptHandlerError> {
        if self.scripts.remove(&script_id).is_some() {
            return Ok(());
        }

        Err(ScriptHandlerError::ScriptNotFound(script_id))
    }

    pub fn run_script(
        &self,
        policy_engine: &PolicyEngine,
        actor_id: &str,
        connection_id: &str,
        script_id: Uuid,
    ) -> Result<ScriptRecord, ScriptHandlerError> {
        let script = self.get_script(script_id)?;

        if script.lifecycle_state != ScriptLifecycleState::Runnable {
            return Err(ScriptHandlerError::NotRunnable);
        }

        let decision = policy_engine.evaluate(&PolicyEvaluationRequest {
            actor_id: actor_id.to_string(),
            connection_id: connection_id.to_string(),
            tool_id: "run_script".to_string(),
            classification: ExecutionClassification::Admin,
        })?;

        if !matches!(decision, PolicyDecision::Allow) {
            return Err(ScriptHandlerError::PolicyDenied);
        }

        Ok(script)
    }
}
