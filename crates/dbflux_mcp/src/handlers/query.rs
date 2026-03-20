use dbflux_core::{QueryLanguage, classify_query_for_governance};
use dbflux_policy::{
    ExecutionClassification, PolicyDecision, PolicyEngine, PolicyEvaluationRequest,
};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryExecutionRequest {
    pub actor_id: String,
    pub connection_id: String,
    pub tool_id: String,
    pub query_language: QueryLanguage,
    pub query: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryExecutionResponse {
    pub classification: ExecutionClassification,
    pub execute: bool,
    pub preview_only: bool,
}

#[derive(Debug, Error)]
pub enum QueryHandlerError {
    #[error("policy denied request")]
    PolicyDenied,
    #[error("policy evaluation failed: {0}")]
    Policy(#[from] dbflux_policy::PolicyEngineError),
}

pub fn handle_query_tool(
    request: &QueryExecutionRequest,
    policy_engine: &PolicyEngine,
) -> Result<QueryExecutionResponse, QueryHandlerError> {
    let classification = classify_query_for_governance(&request.query_language, &request.query);

    let decision = policy_engine.evaluate(&PolicyEvaluationRequest {
        actor_id: request.actor_id.clone(),
        connection_id: request.connection_id.clone(),
        tool_id: request.tool_id.clone(),
        classification,
    })?;

    if !matches!(decision, PolicyDecision::Allow) {
        return Err(QueryHandlerError::PolicyDenied);
    }

    let preview_only = request.tool_id == "preview_mutation";

    Ok(QueryExecutionResponse {
        classification,
        execute: !preview_only,
        preview_only,
    })
}
