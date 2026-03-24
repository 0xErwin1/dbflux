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
    AdminSafe,
    AdminDestructive,
}

impl ExecutionClassification {
    /// Returns the highest (most restrictive) classification.
    pub fn max(self, other: Self) -> Self {
        use ExecutionClassification::*;

        let rank = |c: Self| -> u8 {
            match c {
                Metadata => 0,
                Read => 1,
                Write => 2,
                Destructive => 3,
                AdminSafe => 4,
                Admin => 5,
                AdminDestructive => 6,
            }
        };

        if rank(self) >= rank(other) {
            self
        } else {
            other
        }
    }
}
