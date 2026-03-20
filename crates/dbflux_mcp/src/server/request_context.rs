use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestIdentity {
    pub client_id: String,
    pub issuer: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestContext {
    pub identity: RequestIdentity,
    pub tool_id: String,
    pub connection_id: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RequestContextError {
    #[error("missing required request identity field: client_id")]
    MissingClientId,
}

pub fn resolve_request_identity(
    payload: &serde_json::Value,
) -> Result<RequestIdentity, RequestContextError> {
    let client_id = payload
        .get("client_id")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or(RequestContextError::MissingClientId)?;

    let issuer = payload
        .get("issuer")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    Ok(RequestIdentity { client_id, issuer })
}
