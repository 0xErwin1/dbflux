//! Topological load order for the data-transfer engine's migration flow.
//!
//! Same-engine migrations must load parent tables (referenced by a foreign
//! key) before the child tables that reference them. This module computes
//! that order over the foreign keys already exposed by
//! [`crate::core::traits::Connection::schema_foreign_keys`], and isolates the
//! cyclic subset (via Tarjan's strongly-connected-components algorithm) when
//! no such order exists so the caller can offer a manual reorder step.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::query::table_browser::TableRef;
use crate::schema::types::SchemaForeignKeyInfo;

/// Result of [`topological_order`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderResult {
    /// A full parents-before-children order exists.
    Ordered(Vec<TableRef>),

    /// No full order exists. `ordered_prefix` holds the tables that could be
    /// ordered before the cycle was reached; `cycle` holds every table that
    /// remained unresolved (the strongly-connected cyclic subset and any
    /// table transitively depending on it), in undefined but deterministic order.
    Cyclic {
        ordered_prefix: Vec<TableRef>,
        cycle: Vec<TableRef>,
    },
}

/// Computes a parents-before-children load order for `tables` given `fks`.
///
/// Matching between a foreign key and a [`TableRef`] is by table name only:
/// `SchemaForeignKeyInfo` does not carry the owning table's schema (its
/// schema is implicit in the `Connection::schema_foreign_keys` call that
/// produced it), so this function assumes table names are unique across the
/// supplied `tables` slice. Self-referencing foreign keys (a table
/// referencing itself) are ignored as ordering edges.
pub fn topological_order(tables: &[TableRef], fks: &[SchemaForeignKeyInfo]) -> OrderResult {
    let node_count = tables.len();

    let mut name_to_indices: HashMap<&str, Vec<usize>> = HashMap::new();
    for (index, table) in tables.iter().enumerate() {
        name_to_indices
            .entry(table.name.as_str())
            .or_default()
            .push(index);
    }

    // Edges point parent -> child: the parent must be ordered first.
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); node_count];
    let mut in_degree: Vec<usize> = vec![0; node_count];
    let mut seen_edges: HashSet<(usize, usize)> = HashSet::new();

    for fk in fks {
        let Some(child_indices) = name_to_indices.get(fk.table_name.as_str()) else {
            continue;
        };
        let Some(parent_indices) = name_to_indices.get(fk.referenced_table.as_str()) else {
            continue;
        };

        for &child in child_indices {
            for &parent in parent_indices {
                if child == parent {
                    // Self-referencing FK: not an ordering constraint.
                    continue;
                }
                if seen_edges.insert((parent, child)) {
                    children[parent].push(child);
                    in_degree[child] += 1;
                }
            }
        }
    }

    let sort_key = |index: usize| -> (&str, &str) {
        let table = &tables[index];
        (table.schema.as_deref().unwrap_or(""), table.name.as_str())
    };

    let mut ready: Vec<usize> = (0..node_count).filter(|&i| in_degree[i] == 0).collect();
    ready.sort_by_key(|&i| sort_key(i));
    let mut queue: VecDeque<usize> = ready.into_iter().collect();

    let mut ordered_indices: Vec<usize> = Vec::with_capacity(node_count);

    while let Some(node) = queue.pop_front() {
        ordered_indices.push(node);

        let mut newly_ready: Vec<usize> = Vec::new();
        for &child in &children[node] {
            in_degree[child] -= 1;
            if in_degree[child] == 0 {
                newly_ready.push(child);
            }
        }
        newly_ready.sort_by_key(|&i| sort_key(i));
        for child in newly_ready {
            queue.push_back(child);
        }
    }

    if ordered_indices.len() == node_count {
        return OrderResult::Ordered(
            ordered_indices
                .into_iter()
                .map(|i| tables[i].clone())
                .collect(),
        );
    }

    let ordered_set: HashSet<usize> = ordered_indices.iter().copied().collect();
    let remaining: Vec<usize> = (0..node_count)
        .filter(|i| !ordered_set.contains(i))
        .collect();

    let mut cycle_indices: Vec<usize> = tarjan_scc(&remaining, &children)
        .into_iter()
        .flatten()
        .collect();
    cycle_indices.sort_by_key(|&i| sort_key(i));

    OrderResult::Cyclic {
        ordered_prefix: ordered_indices
            .into_iter()
            .map(|i| tables[i].clone())
            .collect(),
        cycle: cycle_indices
            .into_iter()
            .map(|i| tables[i].clone())
            .collect(),
    }
}

/// Tarjan's strongly-connected-components algorithm, restricted to `nodes`
/// and to edges in `children` whose endpoints are both in `nodes`.
///
/// Every node in `nodes` appears in exactly one returned component (Tarjan
/// partitions its whole input), including trivial single-node components
/// with no self-loop.
fn tarjan_scc(nodes: &[usize], children: &[Vec<usize>]) -> Vec<Vec<usize>> {
    struct State<'a> {
        children: &'a [Vec<usize>],
        allowed: HashSet<usize>,
        index_counter: usize,
        stack: Vec<usize>,
        on_stack: HashSet<usize>,
        indices: HashMap<usize, usize>,
        lowlink: HashMap<usize, usize>,
        sccs: Vec<Vec<usize>>,
    }

    impl State<'_> {
        fn strongconnect(&mut self, v: usize) {
            self.indices.insert(v, self.index_counter);
            self.lowlink.insert(v, self.index_counter);
            self.index_counter += 1;
            self.stack.push(v);
            self.on_stack.insert(v);

            for &w in &self.children[v] {
                if !self.allowed.contains(&w) {
                    continue;
                }
                if !self.indices.contains_key(&w) {
                    self.strongconnect(w);
                    let low_w = self.lowlink[&w];
                    let low_v = self.lowlink[&v];
                    self.lowlink.insert(v, low_v.min(low_w));
                } else if self.on_stack.contains(&w) {
                    let idx_w = self.indices[&w];
                    let low_v = self.lowlink[&v];
                    self.lowlink.insert(v, low_v.min(idx_w));
                }
            }

            if self.lowlink[&v] == self.indices[&v] {
                let mut component = Vec::new();
                while let Some(w) = self.stack.pop() {
                    self.on_stack.remove(&w);
                    component.push(w);
                    if w == v {
                        break;
                    }
                }
                self.sccs.push(component);
            }
        }
    }

    let mut state = State {
        children,
        allowed: nodes.iter().copied().collect(),
        index_counter: 0,
        stack: Vec::new(),
        on_stack: HashSet::new(),
        indices: HashMap::new(),
        lowlink: HashMap::new(),
        sccs: Vec::new(),
    };

    for &v in nodes {
        if !state.indices.contains_key(&v) {
            state.strongconnect(v);
        }
    }

    state.sccs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str) -> TableRef {
        TableRef::new(name)
    }

    fn fk(table_name: &str, referenced_table: &str) -> SchemaForeignKeyInfo {
        SchemaForeignKeyInfo {
            name: format!("fk_{table_name}_{referenced_table}"),
            table_name: table_name.to_string(),
            columns: vec!["id".to_string()],
            referenced_schema: None,
            referenced_table: referenced_table.to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: None,
            on_update: None,
        }
    }

    fn names(tables: &[TableRef]) -> Vec<&str> {
        tables.iter().map(|t| t.name.as_str()).collect()
    }

    #[test]
    fn linear_chain_orders_parent_before_child() {
        // A references B: B must load before A.
        let tables = vec![table("a"), table("b")];
        let fks = vec![fk("a", "b")];

        let result = topological_order(&tables, &fks);
        match result {
            OrderResult::Ordered(ordered) => {
                let names = names(&ordered);
                let b_pos = names.iter().position(|&n| n == "b").unwrap();
                let a_pos = names.iter().position(|&n| n == "a").unwrap();
                assert!(b_pos < a_pos, "b (parent) must load before a (child)");
            }
            other => panic!("expected Ordered, got {other:?}"),
        }
    }

    #[test]
    fn diamond_orders_all_dependencies_before_dependents() {
        // a -> b, a -> c, b -> d, c -> d  (d is the common ancestor).
        let tables = vec![table("a"), table("b"), table("c"), table("d")];
        let fks = vec![fk("a", "b"), fk("a", "c"), fk("b", "d"), fk("c", "d")];

        let result = topological_order(&tables, &fks);
        match result {
            OrderResult::Ordered(ordered) => {
                let names = names(&ordered);
                let pos = |n: &str| names.iter().position(|&x| x == n).unwrap();
                assert!(pos("d") < pos("b"));
                assert!(pos("d") < pos("c"));
                assert!(pos("b") < pos("a"));
                assert!(pos("c") < pos("a"));
            }
            other => panic!("expected Ordered, got {other:?}"),
        }
    }

    #[test]
    fn cycle_reports_cyclic_subset() {
        let tables = vec![table("a"), table("b")];
        let fks = vec![fk("a", "b"), fk("b", "a")];

        let result = topological_order(&tables, &fks);
        match result {
            OrderResult::Cyclic {
                ordered_prefix,
                cycle,
            } => {
                assert!(ordered_prefix.is_empty());
                let mut cycle_names = names(&cycle);
                cycle_names.sort_unstable();
                assert_eq!(cycle_names, vec!["a", "b"]);
            }
            other => panic!("expected Cyclic, got {other:?}"),
        }
    }

    #[test]
    fn self_referencing_fk_is_ignored_and_table_still_orders() {
        let tables = vec![table("a"), table("employee")];
        let fks = vec![fk("a", "employee"), fk("employee", "employee")];

        let result = topological_order(&tables, &fks);
        match result {
            OrderResult::Ordered(ordered) => {
                let names = names(&ordered);
                let pos = |n: &str| names.iter().position(|&x| x == n).unwrap();
                assert!(pos("employee") < pos("a"));
            }
            other => panic!("expected Ordered (self-ref ignored), got {other:?}"),
        }
    }

    #[test]
    fn tables_with_no_foreign_keys_order_deterministically() {
        let tables = vec![table("z"), table("a"), table("m")];
        let result = topological_order(&tables, &[]);

        match result {
            OrderResult::Ordered(ordered) => {
                assert_eq!(names(&ordered), vec!["a", "m", "z"]);
            }
            other => panic!("expected Ordered, got {other:?}"),
        }
    }
}
