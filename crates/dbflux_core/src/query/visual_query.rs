use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Top-level spec; what the builder owns and what the generator renders.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VisualQuerySpec {
    pub source: SourceTable,
    pub projection: Projection,
    pub joins: Vec<JoinStep>,
    pub filter: Option<FilterNode>,
    pub sort: Vec<SortEntry>,
    /// `None` = no LIMIT; `Some(0)` is collapsed to None at build time.
    pub limit: Option<u64>,
    pub offset: u64,
}

impl VisualQuerySpec {
    /// Returns `Ok(())` if the spec can produce a runnable query, or `Err` with
    /// the first validation failure found.
    pub fn is_runnable(&self) -> Result<(), SpecError> {
        if self.source.table.trim().is_empty() {
            return Err(SpecError::MissingSourceTable);
        }
        Ok(())
    }

    /// Walks the filter tree and assigns fresh monotonically increasing `node_id`
    /// values to every `Predicate` node, starting from `next_id`.
    ///
    /// Call this after deserialising a spec so that predicate node IDs are valid
    /// before the UI constructs `InputState` entities.
    pub fn reassign_node_ids(&mut self, next_id: &mut u64) {
        if let Some(root) = &mut self.filter {
            Self::assign_ids_in_node(root, next_id);
        }

        for join in &mut self.joins {
            if let JoinOn::Conditions { predicates, .. } = &mut join.on {
                for pred in predicates.iter_mut() {
                    *next_id += 1;
                    pred.node_id = *next_id;
                }
            }
        }
    }

    fn assign_ids_in_node(node: &mut FilterNode, next_id: &mut u64) {
        match node {
            FilterNode::Predicate(pred) => {
                *next_id += 1;
                pred.node_id = *next_id;
            }
            FilterNode::Group { children, .. } => {
                for child in children.iter_mut() {
                    Self::assign_ids_in_node(child, next_id);
                }
            }
        }
    }

    /// Returns a map from alias string to its origin in this spec.
    pub fn referenced_aliases(&self) -> BTreeMap<String, AliasOrigin> {
        let mut map = BTreeMap::new();

        map.insert(self.source.alias.clone(), AliasOrigin::Source);

        for join in &self.joins {
            map.insert(join.to_alias.clone(), AliasOrigin::Join);
        }

        map
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceTable {
    pub schema: Option<String>,
    pub table: String,
    /// Defaults to table name; aliases used in joins/projection.
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Projection {
    All,
    Explicit(Vec<ProjectedColumn>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectedColumn {
    /// Alias of the source or join target.
    pub source_alias: String,
    pub column: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinStep {
    pub kind: JoinKind,
    pub from_alias: String,
    pub to_schema: Option<String>,
    pub to_table: String,
    pub to_alias: String,
    pub on: JoinOn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinOn {
    /// FK-resolved path: equality on a column pair.
    FkPath {
        from_column: String,
        to_column: String,
    },
    /// Free-text expression (FK metadata unavailable or user typed raw).
    RawExpression(String),
    /// Structured list of conditions joined with `op` (AND or OR).
    Conditions {
        #[serde(default = "default_join_op")]
        op: BoolOp,
        predicates: Vec<JoinPredicate>,
    },
}

fn default_join_op() -> BoolOp {
    BoolOp::And
}

/// Structured ON-clause predicate: `left <op> right`. Both sides are dotted
/// column references (e.g. `users.id`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinPredicate {
    /// Stable per-predicate identifier; regenerated on load.
    #[serde(skip)]
    pub node_id: u64,
    pub left: String,
    pub op: Comparator,
    pub right: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FilterNode {
    Group {
        op: BoolOp,
        children: Vec<FilterNode>,
    },
    Predicate(Predicate),
}

impl FilterNode {
    /// Returns the nesting depth of the deepest `Group` in this subtree.
    ///
    /// A single `Group` with no nested groups has depth 1. A `Predicate` has
    /// depth 0 (it does not contribute to `Group` nesting depth).
    pub fn depth(&self) -> usize {
        match self {
            FilterNode::Predicate(_) => 0,
            FilterNode::Group { children, .. } => {
                let max_child = children.iter().map(|c| c.depth()).max().unwrap_or(0);
                1 + max_child
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BoolOp {
    And,
    Or,
}

/// A single filter condition in the filter tree.
///
/// `node_id` is a stable identifier used by the UI to keep `Entity<InputState>`
/// lifecycle aligned with predicate nodes. It is not persisted — the field is
/// skipped during serialisation and regenerated from a monotonic counter when a
/// spec is loaded or when a new predicate is added. The SQL generator ignores it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Predicate {
    pub source_alias: String,
    pub column: String,
    pub comparator: Comparator,
    pub value: PredicateValue,

    /// Stable per-node identifier for UI `InputState` lifecycle management.
    /// Not persisted; regenerated on load. Zero is the sentinel "unassigned" value.
    #[serde(skip, default)]
    pub node_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Comparator {
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    Like,
    ILike,
    In,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PredicateValue {
    None,
    Single(LiteralValue),
    List(Vec<LiteralValue>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LiteralValue {
    Text(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    /// ISO-8601 string; the driver parses the format it expects.
    Timestamp(String),
    Null,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortEntry {
    pub source_alias: String,
    pub column: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// The origin of an alias used in a `VisualQuerySpec`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AliasOrigin {
    Source,
    Join,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum SpecError {
    #[error("source table name must not be empty")]
    MissingSourceTable,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_predicate(alias: &str, col: &str) -> FilterNode {
        FilterNode::Predicate(Predicate {
            source_alias: alias.to_string(),
            column: col.to_string(),
            comparator: Comparator::Eq,
            value: PredicateValue::Single(LiteralValue::Integer(1)),
            node_id: 0,
        })
    }

    fn make_group(op: BoolOp, children: Vec<FilterNode>) -> FilterNode {
        FilterNode::Group { op, children }
    }

    // --- FilterNode::depth ---

    #[test]
    fn depth_of_predicate_is_zero() {
        let node = make_predicate("t", "id");
        assert_eq!(node.depth(), 0);
    }

    #[test]
    fn depth_of_flat_group_is_one() {
        let node = make_group(BoolOp::And, vec![make_predicate("t", "id")]);
        assert_eq!(node.depth(), 1);
    }

    #[test]
    fn depth_counts_nested_groups() {
        let inner = make_group(BoolOp::Or, vec![make_predicate("t", "name")]);
        let outer = make_group(BoolOp::And, vec![make_predicate("t", "id"), inner]);
        assert_eq!(outer.depth(), 2);
    }

    #[test]
    fn depth_returns_max_over_sibling_branches() {
        let deep_branch = make_group(
            BoolOp::And,
            vec![make_group(
                BoolOp::Or,
                vec![make_group(BoolOp::And, vec![make_predicate("t", "x")])],
            )],
        );
        let shallow_branch = make_group(BoolOp::Or, vec![make_predicate("t", "y")]);
        let root = make_group(BoolOp::And, vec![deep_branch, shallow_branch]);
        // root(1) + deep_branch(1) + inner(1) + innermost(1) = depth 4 from root
        assert_eq!(root.depth(), 4);
    }

    #[test]
    fn depth_of_empty_group_is_one() {
        let node = make_group(BoolOp::And, vec![]);
        assert_eq!(node.depth(), 1);
    }

    // --- VisualQuerySpec::is_runnable ---

    #[test]
    fn is_runnable_rejects_empty_table_name() {
        let spec = VisualQuerySpec {
            source: SourceTable {
                schema: None,
                table: String::new(),
                alias: "t".to_string(),
            },
            projection: Projection::All,
            joins: vec![],
            filter: None,
            sort: vec![],
            limit: Some(100),
            offset: 0,
        };
        assert_eq!(spec.is_runnable(), Err(SpecError::MissingSourceTable));
    }

    #[test]
    fn is_runnable_rejects_whitespace_only_table_name() {
        let spec = VisualQuerySpec {
            source: SourceTable {
                schema: None,
                table: "   ".to_string(),
                alias: "t".to_string(),
            },
            projection: Projection::All,
            joins: vec![],
            filter: None,
            sort: vec![],
            limit: Some(100),
            offset: 0,
        };
        assert_eq!(spec.is_runnable(), Err(SpecError::MissingSourceTable));
    }

    #[test]
    fn is_runnable_accepts_valid_table_name() {
        let spec = VisualQuerySpec {
            source: SourceTable {
                schema: Some("public".to_string()),
                table: "users".to_string(),
                alias: "users".to_string(),
            },
            projection: Projection::All,
            joins: vec![],
            filter: None,
            sort: vec![],
            limit: Some(100),
            offset: 0,
        };
        assert_eq!(spec.is_runnable(), Ok(()));
    }

    // --- serde round-trip ---

    #[test]
    fn serde_round_trip_fully_populated_spec() {
        let spec = VisualQuerySpec {
            source: SourceTable {
                schema: Some("public".to_string()),
                table: "orders".to_string(),
                alias: "o".to_string(),
            },
            projection: Projection::Explicit(vec![
                ProjectedColumn {
                    source_alias: "o".to_string(),
                    column: "id".to_string(),
                    alias: None,
                },
                ProjectedColumn {
                    source_alias: "u".to_string(),
                    column: "name".to_string(),
                    alias: Some("customer_name".to_string()),
                },
            ]),
            joins: vec![JoinStep {
                kind: JoinKind::Left,
                from_alias: "o".to_string(),
                to_schema: Some("public".to_string()),
                to_table: "users".to_string(),
                to_alias: "u".to_string(),
                on: JoinOn::FkPath {
                    from_column: "user_id".to_string(),
                    to_column: "id".to_string(),
                },
            }],
            filter: Some(FilterNode::Group {
                op: BoolOp::And,
                children: vec![
                    FilterNode::Predicate(Predicate {
                        source_alias: "o".to_string(),
                        column: "status".to_string(),
                        comparator: Comparator::Eq,
                        value: PredicateValue::Single(LiteralValue::Text("active".to_string())),
                        node_id: 0,
                    }),
                    FilterNode::Group {
                        op: BoolOp::Or,
                        children: vec![
                            FilterNode::Predicate(Predicate {
                                source_alias: "o".to_string(),
                                column: "total".to_string(),
                                comparator: Comparator::Gt,
                                value: PredicateValue::Single(LiteralValue::Float(100.0)),
                                node_id: 0,
                            }),
                            FilterNode::Predicate(Predicate {
                                source_alias: "u".to_string(),
                                column: "vip".to_string(),
                                comparator: Comparator::Eq,
                                value: PredicateValue::Single(LiteralValue::Bool(true)),
                                node_id: 0,
                            }),
                        ],
                    },
                ],
            }),
            sort: vec![
                SortEntry {
                    source_alias: "o".to_string(),
                    column: "created_at".to_string(),
                    direction: SortDirection::Desc,
                },
                SortEntry {
                    source_alias: "u".to_string(),
                    column: "name".to_string(),
                    direction: SortDirection::Asc,
                },
            ],
            limit: Some(50),
            offset: 10,
        };

        let json = serde_json::to_string(&spec).expect("serialization must succeed");
        let roundtripped: VisualQuerySpec =
            serde_json::from_str(&json).expect("deserialization must succeed");

        assert_eq!(spec, roundtripped);
    }

    // --- referenced_aliases ---

    #[test]
    fn referenced_aliases_includes_source_and_joins() {
        let spec = VisualQuerySpec {
            source: SourceTable {
                schema: None,
                table: "orders".to_string(),
                alias: "o".to_string(),
            },
            projection: Projection::All,
            joins: vec![JoinStep {
                kind: JoinKind::Inner,
                from_alias: "o".to_string(),
                to_schema: None,
                to_table: "users".to_string(),
                to_alias: "u".to_string(),
                on: JoinOn::RawExpression("o.user_id = u.id".to_string()),
            }],
            filter: None,
            sort: vec![],
            limit: None,
            offset: 0,
        };

        let aliases = spec.referenced_aliases();
        assert_eq!(aliases.get("o"), Some(&AliasOrigin::Source));
        assert_eq!(aliases.get("u"), Some(&AliasOrigin::Join));
        assert_eq!(aliases.len(), 2);
    }

    // --- reassign_node_ids ---

    fn base_spec() -> VisualQuerySpec {
        VisualQuerySpec {
            source: SourceTable {
                schema: None,
                table: "users".to_string(),
                alias: "users".to_string(),
            },
            projection: Projection::All,
            joins: vec![],
            filter: None,
            sort: vec![],
            limit: Some(100),
            offset: 0,
        }
    }

    #[test]
    fn reassign_node_ids_assigns_nonzero_ids_to_all_predicates() {
        let mut spec = base_spec();
        spec.filter = Some(FilterNode::Group {
            op: BoolOp::And,
            children: vec![
                make_predicate("users", "email"),
                make_predicate("users", "name"),
            ],
        });

        let mut counter = 0u64;
        spec.reassign_node_ids(&mut counter);

        if let Some(FilterNode::Group { children, .. }) = &spec.filter {
            for child in children {
                if let FilterNode::Predicate(pred) = child {
                    assert_ne!(pred.node_id, 0, "node_id must be non-zero after assignment");
                }
            }
        } else {
            panic!("filter must be a group");
        }
    }

    #[test]
    fn reassign_node_ids_assigns_distinct_ids() {
        let mut spec = base_spec();
        spec.filter = Some(FilterNode::Group {
            op: BoolOp::And,
            children: vec![
                make_predicate("users", "email"),
                FilterNode::Group {
                    op: BoolOp::Or,
                    children: vec![
                        make_predicate("users", "name"),
                        make_predicate("users", "age"),
                    ],
                },
            ],
        });

        let mut counter = 0u64;
        spec.reassign_node_ids(&mut counter);

        let mut ids = Vec::new();
        fn collect_ids(node: &FilterNode, ids: &mut Vec<u64>) {
            match node {
                FilterNode::Predicate(pred) => ids.push(pred.node_id),
                FilterNode::Group { children, .. } => {
                    for child in children {
                        collect_ids(child, ids);
                    }
                }
            }
        }
        collect_ids(spec.filter.as_ref().unwrap(), &mut ids);

        assert_eq!(ids.len(), 3);
        let mut sorted = ids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 3, "all node_ids must be distinct");
    }

    #[test]
    fn node_id_is_skipped_in_serde() {
        let mut pred = Predicate {
            source_alias: "t".to_string(),
            column: "id".to_string(),
            comparator: Comparator::Eq,
            value: PredicateValue::None,
            node_id: 42,
        };

        let json = serde_json::to_string(&pred).expect("serialise");
        assert!(!json.contains("node_id"), "node_id must not appear in JSON");

        let roundtripped: Predicate = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(
            roundtripped.node_id, 0,
            "node_id must reset to 0 after deserialise"
        );
        pred.node_id = 0;
        assert_eq!(pred, roundtripped);
    }
}
