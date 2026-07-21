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

#[cfg(test)]
mod tests {
    use super::ExecutionClassification;

    /// The escalation ladder from least to most restrictive. `max` must always
    /// return whichever of its two operands sits higher on this ladder, since it
    /// is the gate that decides how dangerous a composed operation is.
    const ORDER: [ExecutionClassification; 7] = [
        ExecutionClassification::Metadata,
        ExecutionClassification::Read,
        ExecutionClassification::Write,
        ExecutionClassification::Destructive,
        ExecutionClassification::AdminSafe,
        ExecutionClassification::Admin,
        ExecutionClassification::AdminDestructive,
    ];

    #[test]
    fn max_returns_higher_rank_for_every_pair() {
        for (lower_index, lower) in ORDER.iter().enumerate() {
            for (higher_index, higher) in ORDER.iter().enumerate() {
                let expected = if higher_index >= lower_index {
                    *higher
                } else {
                    *lower
                };

                assert_eq!(
                    lower.max(*higher),
                    expected,
                    "max({lower:?}, {higher:?}) must be the higher-ranked variant"
                );
                assert_eq!(
                    higher.max(*lower),
                    expected,
                    "max is symmetric: max({higher:?}, {lower:?}) must match"
                );
            }
        }
    }

    #[test]
    fn max_is_idempotent_for_each_variant() {
        for variant in ORDER {
            assert_eq!(variant.max(variant), variant);
        }
    }

    #[test]
    fn serde_round_trip_preserves_every_variant() {
        for variant in ORDER {
            let json = serde_json::to_string(&variant).expect("serialize");
            let decoded: ExecutionClassification =
                serde_json::from_str(&json).expect("deserialize");

            assert_eq!(decoded, variant, "round-trip must preserve {variant:?}");
        }
    }

    #[test]
    fn serde_uses_snake_case_wire_names() {
        let cases = [
            (ExecutionClassification::Metadata, "\"metadata\""),
            (ExecutionClassification::Read, "\"read\""),
            (ExecutionClassification::Write, "\"write\""),
            (ExecutionClassification::Destructive, "\"destructive\""),
            (ExecutionClassification::AdminSafe, "\"admin_safe\""),
            (ExecutionClassification::Admin, "\"admin\""),
            (
                ExecutionClassification::AdminDestructive,
                "\"admin_destructive\"",
            ),
        ];

        for (variant, wire) in cases {
            assert_eq!(serde_json::to_string(&variant).expect("serialize"), wire);
        }
    }
}
