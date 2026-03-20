use serde::{Deserialize, Serialize};

/// Canonical governance classification used by policy and approvals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionClassification {
    Metadata,
    Read,
    Write,
    Destructive,
    Admin,
}
