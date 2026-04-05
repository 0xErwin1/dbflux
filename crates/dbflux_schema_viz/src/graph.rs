use petgraph::prelude::{DiGraph, EdgeRef, NodeIndex};
use std::collections::{HashMap, HashSet, VecDeque};

use dbflux_core::TableInfo;

/// Identifier for a table node in the schema graph.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TableNodeId {
    pub schema: Option<String>,
    pub name: String,
}

/// Summary of a column for schema visualization purposes.
#[derive(Clone, Debug)]
pub struct ColumnSummary {
    pub name: String,
    pub type_name: String,
    pub is_pk: bool,
    pub is_fk: bool,
}

/// Node weight: a table with its column summaries.
#[derive(Clone, Debug)]
pub struct TableNode {
    pub id: TableNodeId,
    pub columns: Vec<ColumnSummary>,
}

/// Edge weight: a foreign key relationship between two tables.
#[derive(Clone, Debug)]
pub struct FkEdge {
    pub name: String,
    pub from_columns: Vec<String>,
    pub to_columns: Vec<String>,
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
}

/// Directed graph of tables and their FK relationships.
pub struct SchemaGraph {
    pub(crate) graph: DiGraph<TableNode, FkEdge>,
    pub(crate) node_index_by_id: HashMap<TableNodeId, NodeIndex>,
}

impl SchemaGraph {
    /// Build a `SchemaGraph` from a slice of `TableInfo`.
    ///
    /// Each table becomes a node. Each `ForeignKeyInfo` in a table's
    /// `foreign_keys` field produces a directed edge from the child table
    /// to the referenced (parent) table. Tables with `foreign_keys: None`
    /// or an empty vector are isolated nodes.
    ///
    /// If a FK references a table not present in `tables`, that edge is
    /// skipped silently.
    pub fn build(tables: &[TableInfo]) -> Self {
        let mut graph = DiGraph::with_capacity(tables.len(), tables.len());
        let mut node_index_by_id = HashMap::new();

        // First pass: create all nodes.
        for table in tables {
            let id = TableNodeId {
                schema: table.schema.clone(),
                name: table.name.clone(),
            };

            let columns = if let Some(ref cols) = table.columns {
                cols.iter()
                    .map(|col| {
                        let is_fk = table
                            .foreign_keys
                            .as_ref()
                            .map(|fks| fks.iter().any(|fk| fk.columns.contains(&col.name)))
                            .unwrap_or(false);

                        ColumnSummary {
                            name: col.name.clone(),
                            type_name: col.type_name.clone(),
                            is_pk: col.is_primary_key,
                            is_fk,
                        }
                    })
                    .collect()
            } else {
                Vec::new()
            };

            let node = TableNode {
                id: id.clone(),
                columns,
            };
            let node_idx = graph.add_node(node);
            node_index_by_id.insert(id, node_idx);
        }

        // Second pass: create edges for FKs.
        for table in tables {
            let Some(ref fks) = table.foreign_keys else {
                continue;
            };

            let Some(&from_idx) = node_index_by_id.get(&TableNodeId {
                schema: table.schema.clone(),
                name: table.name.clone(),
            }) else {
                continue;
            };

            for fk in fks {
                let to_id = TableNodeId {
                    schema: fk.referenced_schema.clone(),
                    name: fk.referenced_table.clone(),
                };

                let Some(&to_idx) = node_index_by_id.get(&to_id) else {
                    continue;
                };

                let edge = FkEdge {
                    name: fk.name.clone(),
                    from_columns: fk.columns.clone(),
                    to_columns: fk.referenced_columns.clone(),
                    on_delete: fk.on_delete.clone(),
                    on_update: fk.on_update.clone(),
                };

                graph.add_edge(from_idx, to_idx, edge);
            }
        }

        SchemaGraph {
            graph,
            node_index_by_id,
        }
    }

    /// Return a subgraph containing the focal table and all tables reachable
    /// within `depth` hops via FK edges in either direction.
    ///
    /// The focal table is identified by `name` and optional `schema`.
    /// Returns an empty `SchemaGraph` if the focal table is not found.
    pub fn neighborhood(&self, table: &str, schema: Option<&str>, depth: usize) -> SchemaGraph {
        let focal_id = TableNodeId {
            schema: schema.map(str::to_owned),
            name: table.to_owned(),
        };

        let Some(&focal_idx) = self.node_index_by_id.get(&focal_id) else {
            return SchemaGraph {
                graph: DiGraph::new(),
                node_index_by_id: HashMap::new(),
            };
        };

        // BFS in both outgoing and incoming directions, bounded by depth.
        let mut visited: HashSet<NodeIndex> = HashSet::new();
        let mut queue: VecDeque<(NodeIndex, usize)> = VecDeque::from([(focal_idx, 0)]);
        visited.insert(focal_idx);

        while let Some((current, current_depth)) = queue.pop_front() {
            if current_depth >= depth {
                continue;
            }

            // Outgoing edges (this table references others).
            for edge in self
                .graph
                .edges_directed(current, petgraph::Direction::Outgoing)
            {
                let neighbor = edge.target();
                if visited.insert(neighbor) {
                    queue.push_back((neighbor, current_depth + 1));
                }
            }

            // Incoming edges (others reference this table).
            for edge in self
                .graph
                .edges_directed(current, petgraph::Direction::Incoming)
            {
                let neighbor = edge.source();
                if visited.insert(neighbor) {
                    queue.push_back((neighbor, current_depth + 1));
                }
            }
        }

        // Collect subgraph: nodes in `visited` + edges where both ends are in `visited`.
        // Sort visited by table name for deterministic subgraph node insertion order.
        let visited_set = visited.clone();
        let mut sorted_visited: Vec<NodeIndex> = visited.into_iter().collect();
        sorted_visited.sort_by_key(|&idx| {
            self.graph
                .node_weight(idx)
                .map(|n| n.id.name.as_str())
                .unwrap_or("")
        });

        let mut subgraph = DiGraph::with_capacity(sorted_visited.len(), sorted_visited.len());
        let mut new_index_by_old: HashMap<NodeIndex, NodeIndex> = HashMap::new();

        for &old_idx in &sorted_visited {
            let node = self
                .graph
                .node_weight(old_idx)
                .expect("invariant: old_idx is from the same source graph")
                .clone();
            let new_idx = subgraph.add_node(node);
            new_index_by_old.insert(old_idx, new_idx);
        }

        for edge in self.graph.edge_indices() {
            let (source, target) = self
                .graph
                .edge_endpoints(edge)
                .expect("invariant: edge is from the same source graph");
            if visited_set.contains(&source) && visited_set.contains(&target) {
                let weight = self
                    .graph
                    .edge_weight(edge)
                    .expect("invariant: edge is from the same source graph")
                    .clone();
                let new_source = *new_index_by_old
                    .get(&source)
                    .expect("invariant: source was visited and added to new_index_by_old");
                let new_target = *new_index_by_old
                    .get(&target)
                    .expect("invariant: target was visited and added to new_index_by_old");
                subgraph.add_edge(new_source, new_target, weight);
            }
        }

        let node_index_by_id = new_index_by_old
            .iter()
            .map(|(_old_idx, &new_idx)| {
                let node = subgraph
                    .node_weight(new_idx)
                    .expect("invariant: new_idx was added to subgraph via add_node");
                (node.id.clone(), new_idx)
            })
            .collect();

        SchemaGraph {
            graph: subgraph,
            node_index_by_id,
        }
    }

    /// Iterate over all nodes in the graph.
    pub fn nodes(&self) -> impl Iterator<Item = (NodeIndex, &TableNode)> {
        self.graph.node_indices().map(|idx| {
            (
                idx,
                self.graph
                    .node_weight(idx)
                    .expect("invariant: idx is from node_indices() on the same graph"),
            )
        })
    }

    /// Iterate over all edges in the graph.
    pub fn edges(&self) -> impl Iterator<Item = &FkEdge> {
        self.graph
            .edge_indices()
            .filter_map(move |idx| self.graph.edge_weight(idx))
    }

    /// Returns the total number of nodes in the graph.
    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    /// Returns the total number of edges in the graph.
    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{ColumnInfo, ForeignKeyInfo, TableInfo};

    fn make_table(
        name: &str,
        schema: Option<&str>,
        columns: Vec<(&str, &str, bool)>,
        fks: Vec<ForeignKeyInfo>,
    ) -> TableInfo {
        TableInfo {
            name: name.to_owned(),
            schema: schema.map(str::to_owned),
            columns: Some(
                columns
                    .into_iter()
                    .map(|(n, t, pk)| ColumnInfo {
                        name: n.to_owned(),
                        type_name: t.to_owned(),
                        nullable: !pk,
                        is_primary_key: pk,
                        default_value: None,
                        enum_values: None,
                    })
                    .collect(),
            ),
            indexes: None,
            foreign_keys: if fks.is_empty() { None } else { Some(fks) },
            constraints: None,
            sample_fields: None,
        }
    }

    // ── 9.1: isolated node ─────────────────────────────────────────────────────

    #[test]
    fn test_build_isolated_node() {
        let tables = vec![make_table(
            "audit_log",
            None,
            vec![("id", "integer", true)],
            vec![],
        )];
        let graph = SchemaGraph::build(&tables);

        assert_eq!(graph.node_count(), 1);
        assert_eq!(graph.edge_count(), 0);

        let (idx, node) = graph.nodes().next().unwrap();
        assert_eq!(node.id.name, "audit_log");
        assert_eq!(node.id.schema, None);
        assert_eq!(idx.index(), 0);
    }

    // ── 9.2: composite FK ─────────────────────────────────────────────────────

    #[test]
    fn test_build_composite_fk() {
        let line_items = make_table(
            "line_items",
            None,
            vec![
                ("order_id", "integer", false),
                ("line_id", "integer", false),
                ("qty", "integer", false),
            ],
            vec![ForeignKeyInfo {
                name: "fk_line_orders".into(),
                columns: vec!["order_id".into(), "line_id".into()],
                referenced_table: "orders".into(),
                referenced_schema: None,
                referenced_columns: vec!["id".into(), "line_seq".into()],
                on_delete: None,
                on_update: None,
            }],
        );

        let orders = make_table(
            "orders",
            None,
            vec![
                ("id", "integer", true),
                ("line_seq", "integer", true),
                ("total", "numeric", false),
            ],
            vec![],
        );

        let graph = SchemaGraph::build(&[line_items, orders]);

        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1);

        let edge = graph.edges().next().unwrap();
        assert_eq!(edge.from_columns, vec!["order_id", "line_id"]);
        assert_eq!(edge.to_columns, vec!["id", "line_seq"]);
    }

    // ── 9.3: cyclic graph ──────────────────────────────────────────────────────

    #[test]
    fn test_build_cyclic_graph() {
        // A → B → C → A
        let a = make_table(
            "a",
            None,
            vec![("id", "integer", true)],
            vec![ForeignKeyInfo {
                name: "fk_a_b".into(),
                columns: vec!["id".into()],
                referenced_table: "b".into(),
                referenced_schema: None,
                referenced_columns: vec!["id".into()],
                on_delete: None,
                on_update: None,
            }],
        );
        let b = make_table(
            "b",
            None,
            vec![("id", "integer", true)],
            vec![ForeignKeyInfo {
                name: "fk_b_c".into(),
                columns: vec!["id".into()],
                referenced_table: "c".into(),
                referenced_schema: None,
                referenced_columns: vec!["id".into()],
                on_delete: None,
                on_update: None,
            }],
        );
        let c = make_table(
            "c",
            None,
            vec![("id", "integer", true)],
            vec![ForeignKeyInfo {
                name: "fk_c_a".into(),
                columns: vec!["id".into()],
                referenced_table: "a".into(),
                referenced_schema: None,
                referenced_columns: vec!["id".into()],
                on_delete: None,
                on_update: None,
            }],
        );

        let graph = SchemaGraph::build(&[a, b, c]);

        assert_eq!(graph.node_count(), 3);
        assert_eq!(graph.edge_count(), 3);
    }

    // ── 9.4: neighborhood — inbound only ─────────────────────────────────────

    #[test]
    fn test_neighborhood_inbound_only() {
        // C → B → A
        let c = make_table(
            "c",
            None,
            vec![("id", "integer", true)],
            vec![ForeignKeyInfo {
                name: "fk_c_b".into(),
                columns: vec!["id".into()],
                referenced_table: "b".into(),
                referenced_schema: None,
                referenced_columns: vec!["id".into()],
                on_delete: None,
                on_update: None,
            }],
        );
        let b = make_table(
            "b",
            None,
            vec![("id", "integer", true)],
            vec![ForeignKeyInfo {
                name: "fk_b_a".into(),
                columns: vec!["id".into()],
                referenced_table: "a".into(),
                referenced_schema: None,
                referenced_columns: vec!["id".into()],
                on_delete: None,
                on_update: None,
            }],
        );
        let a = make_table("a", None, vec![("id", "integer", true)], vec![]);

        let graph = SchemaGraph::build(&[c, b, a]);
        let sub = graph.neighborhood("a", None, 1);

        let node_names: HashSet<_> = sub.nodes().map(|(_, n)| n.id.name.clone()).collect();

        assert!(node_names.contains(&"a".to_owned()));
        assert!(node_names.contains(&"b".to_owned()));
        assert!(!node_names.contains(&"c".to_owned()));
        assert_eq!(sub.edge_count(), 1); // B → A
    }

    // ── 9.5: neighborhood — outbound only ───────────────────────────────────

    #[test]
    fn test_neighborhood_outbound_only() {
        // A → B, A → C
        let a = make_table(
            "a",
            None,
            vec![("id", "integer", true)],
            vec![
                ForeignKeyInfo {
                    name: "fk_a_b".into(),
                    columns: vec!["id".into()],
                    referenced_table: "b".into(),
                    referenced_schema: None,
                    referenced_columns: vec!["id".into()],
                    on_delete: None,
                    on_update: None,
                },
                ForeignKeyInfo {
                    name: "fk_a_c".into(),
                    columns: vec!["id".into()],
                    referenced_table: "c".into(),
                    referenced_schema: None,
                    referenced_columns: vec!["id".into()],
                    on_delete: None,
                    on_update: None,
                },
            ],
        );
        let b = make_table("b", None, vec![("id", "integer", true)], vec![]);
        let c = make_table("c", None, vec![("id", "integer", true)], vec![]);

        let graph = SchemaGraph::build(&[a, b, c]);
        let sub = graph.neighborhood("a", None, 1);

        let node_names: HashSet<_> = sub.nodes().map(|(_, n)| n.id.name.clone()).collect();

        assert!(node_names.contains(&"a".to_owned()));
        assert!(node_names.contains(&"b".to_owned()));
        assert!(node_names.contains(&"c".to_owned()));
        assert_eq!(sub.edge_count(), 2); // A→B, A→C
    }

    // ── 9.6: neighborhood — bidirectional ────────────────────────────────────

    #[test]
    fn test_build_empty() {
        let graph = SchemaGraph::build(&[]);
        assert_eq!(graph.node_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_fk_fields_preserved() {
        let a = make_table("a", None, vec![("id", "integer", true)], vec![]);
        let b = make_table(
            "b",
            None,
            vec![("id", "integer", true), ("a_id", "integer", false)],
            vec![ForeignKeyInfo {
                name: "fk_b_a".into(),
                columns: vec!["a_id".into()],
                referenced_table: "a".into(),
                referenced_schema: None,
                referenced_columns: vec!["id".into()],
                on_delete: Some("CASCADE".into()),
                on_update: Some("RESTRICT".into()),
            }],
        );

        let graph = SchemaGraph::build(&[a, b]);
        let sub = graph.neighborhood("b", None, 1);

        let edge = sub.edges().next().expect("expected one edge in subgraph");
        assert_eq!(edge.on_delete.as_deref(), Some("CASCADE"));
        assert_eq!(edge.on_update.as_deref(), Some("RESTRICT"));
    }

    #[test]
    fn test_neighborhood_bidirectional() {
        // B → A, C → A
        let b = make_table(
            "b",
            None,
            vec![("id", "integer", true)],
            vec![ForeignKeyInfo {
                name: "fk_b_a".into(),
                columns: vec!["id".into()],
                referenced_table: "a".into(),
                referenced_schema: None,
                referenced_columns: vec!["id".into()],
                on_delete: None,
                on_update: None,
            }],
        );
        let c = make_table(
            "c",
            None,
            vec![("id", "integer", true)],
            vec![ForeignKeyInfo {
                name: "fk_c_a".into(),
                columns: vec!["id".into()],
                referenced_table: "a".into(),
                referenced_schema: None,
                referenced_columns: vec!["id".into()],
                on_delete: None,
                on_update: None,
            }],
        );
        let a = make_table("a", None, vec![("id", "integer", true)], vec![]);

        let graph = SchemaGraph::build(&[b, c, a]);
        let sub = graph.neighborhood("a", None, 1);

        let node_names: HashSet<_> = sub.nodes().map(|(_, n)| n.id.name.clone()).collect();

        assert!(node_names.contains(&"a".to_owned()));
        assert!(node_names.contains(&"b".to_owned()));
        assert!(node_names.contains(&"c".to_owned()));
        assert_eq!(sub.edge_count(), 2); // B→A, C→A
    }
}
