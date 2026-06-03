// Types are consumed by the public entry point (T19).
#![allow(dead_code)]

use std::collections::HashMap;

use crate::query::relational_filter::parser::{
    FilterExpr, Lhs, ParsedPredicate, RelationalFilterAst, Span,
};
use crate::query::visual_query::{
    FilterNode, JoinKind, JoinOn, JoinStep, Predicate, Projection, SourceTable, VisualQuerySpec,
};
use crate::schema::types::SchemaForeignKeyInfo;
use crate::sql::dialect::SqlDialect;

// =============================================================================
// Output types
// =============================================================================

/// Output of a successful FK resolution pass.
#[derive(Debug)]
pub struct RelationalLowering {
    pub spec: VisualQuerySpec,
    pub diagnostics: LoweringDiagnostics,
}

/// Counters attached to a successful lowering for chip label rendering.
#[derive(Debug)]
pub struct LoweringDiagnostics {
    pub relational_predicate_count: usize,
    pub join_count: usize,
}

/// Concise summary of an FK used in ambiguity error messages.
#[derive(Debug, Clone, PartialEq)]
pub struct FkSummary {
    pub fk_name: Option<String>,
    pub local_columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

impl FkSummary {
    fn from_fk(fk: &SchemaForeignKeyInfo) -> Self {
        Self {
            fk_name: if fk.name.is_empty() {
                None
            } else {
                Some(fk.name.clone())
            },
            local_columns: fk.columns.clone(),
            referenced_table: fk.referenced_table.clone(),
            referenced_columns: fk.referenced_columns.clone(),
        }
    }
}

// =============================================================================
// Errors
// =============================================================================

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ResolveError {
    #[error(
        "ambiguous path segment `{segment}` from `{from_table}`: {}",
        format_candidates(candidates)
    )]
    Ambiguous {
        segment: String,
        from_table: String,
        candidates: Vec<FkSummary>,
        span: Span,
        partial_spec: VisualQuerySpec,
    },

    #[error("unknown relation `{segment}` from `{from_table}`")]
    Unknown {
        segment: String,
        from_table: String,
        span: Span,
        partial_spec: VisualQuerySpec,
    },
}

fn format_candidates(candidates: &[FkSummary]) -> String {
    candidates
        .iter()
        .map(|c| c.local_columns.join(", "))
        .collect::<Vec<_>>()
        .join(", ")
}

// =============================================================================
// Alias generator
// =============================================================================

/// Generates unique, deterministic aliases for join targets.
///
/// The map is seeded with `(source_table, 1)` so any first JOIN onto a table
/// that shares the source table's name (self-join) gets the `_2` suffix.
pub(crate) struct AliasGen {
    used: HashMap<String, usize>,
}

impl AliasGen {
    pub fn new(source_table: &str) -> Self {
        let mut used = HashMap::new();
        used.insert(source_table.to_lowercase(), 1);
        Self { used }
    }

    /// Returns the next unique alias for `target_table`.
    pub fn next_alias(&mut self, target_table: &str) -> String {
        let key = target_table.to_lowercase();
        let count = self.used.entry(key).or_insert(0);
        *count += 1;
        if *count == 1 {
            target_table.to_string()
        } else {
            format!("{}_{}", target_table, count)
        }
    }
}

// =============================================================================
// Resolver
// =============================================================================

/// Resolve a parsed AST against the schema FK list into a `VisualQuerySpec`.
///
/// `source` provides the base table name, alias, and optional schema.
/// `fks` is the slice of FK metadata for the current `(database, schema)`.
/// `dialect` is used for identifier case-normalisation only.
pub fn resolve(
    ast: RelationalFilterAst,
    source: SourceTable,
    fks: &[SchemaForeignKeyInfo],
    dialect: &dyn SqlDialect,
) -> Result<RelationalLowering, ResolveError> {
    let mut ctx = ResolveCtx {
        source_alias: source.alias.clone(),
        source_schema: source.schema.clone(),
        fks,
        dialect,
        joins: Vec::new(),
        aliases: AliasGen::new(&source.table),
        relational_predicate_count: 0,
    };

    let filter_node = ctx.lower_expr(&ast.root)?;

    let join_count = ctx.joins.len();
    let relational_predicate_count = ctx.relational_predicate_count;

    let spec = VisualQuerySpec {
        source,
        projection: Projection::All,
        joins: ctx.joins,
        filter: Some(filter_node),
        group_by: Vec::new(),
        aggregates: Vec::new(),
        having: None,
        sort: Vec::new(),
        limit: None,
        offset: 0,
    };

    Ok(RelationalLowering {
        spec,
        diagnostics: LoweringDiagnostics {
            relational_predicate_count,
            join_count,
        },
    })
}

struct ResolveCtx<'a> {
    source_alias: String,
    source_schema: Option<String>,
    fks: &'a [SchemaForeignKeyInfo],
    dialect: &'a dyn SqlDialect,
    joins: Vec<JoinStep>,
    aliases: AliasGen,
    relational_predicate_count: usize,
}

impl<'a> ResolveCtx<'a> {
    fn lower_expr(&mut self, expr: &FilterExpr) -> Result<FilterNode, ResolveError> {
        match expr {
            FilterExpr::Predicate(pred) => self.lower_predicate(pred),
            FilterExpr::Bool { op, children } => {
                let mut nodes = Vec::with_capacity(children.len());
                for child in children {
                    nodes.push(self.lower_expr(child)?);
                }
                Ok(FilterNode::Group {
                    op: *op,
                    children: nodes,
                })
            }
        }
    }

    fn lower_predicate(&mut self, pred: &ParsedPredicate) -> Result<FilterNode, ResolveError> {
        match &pred.lhs {
            Lhs::BareColumn(col) => Ok(FilterNode::Predicate(Predicate {
                source_alias: self.source_alias.clone(),
                column: col.clone(),
                comparator: pred.comparator,
                value: pred.rhs.clone(),
                node_id: 0,
            })),

            Lhs::DottedPath { segments } => {
                let hops = &segments[..segments.len() - 1];
                let terminal_column = &segments[segments.len() - 1];

                let mut current_table = self.source_alias.clone();
                let mut current_alias = self.source_alias.clone();

                for segment in hops {
                    let (fk, next_alias) =
                        self.resolve_hop(segment, &current_table, &current_alias, pred.span)?;

                    self.joins.push(JoinStep {
                        kind: JoinKind::Inner,
                        from_alias: current_alias.clone(),
                        to_schema: self.source_schema.clone(),
                        to_table: fk.referenced_table.clone(),
                        to_alias: next_alias.clone(),
                        on: JoinOn::FkPath {
                            from_column: fk.columns[0].clone(),
                            to_column: fk.referenced_columns[0].clone(),
                        },
                    });

                    current_table = fk.referenced_table.clone();
                    current_alias = next_alias;
                }

                self.relational_predicate_count += 1;

                Ok(FilterNode::Predicate(Predicate {
                    source_alias: current_alias,
                    column: terminal_column.clone(),
                    comparator: pred.comparator,
                    value: pred.rhs.clone(),
                    node_id: 0,
                }))
            }
        }
    }

    /// Resolve one hop: find the FK from `current_table` that matches `segment`.
    ///
    /// Returns the matching FK and the newly-generated alias for the join target.
    fn resolve_hop(
        &mut self,
        segment: &str,
        current_table: &str,
        current_alias: &str,
        span: Span,
    ) -> Result<(SchemaForeignKeyInfo, String), ResolveError> {
        let norm_seg = self.dialect.normalize_identifier(segment);
        let norm_table = self.dialect.normalize_identifier(current_table);

        let candidates_for_table: Vec<&SchemaForeignKeyInfo> = self
            .fks
            .iter()
            .filter(|fk| self.dialect.normalize_identifier(&fk.table_name) == norm_table)
            .collect();

        let mut matches: Vec<&SchemaForeignKeyInfo> = Vec::new();

        // Pass 1: column-prefix heuristics (S, S_id, SId) — all comparisons case-folded.
        //
        // `norm_seg` is already lowercased. The three candidate column names are:
        //   - `S`           (exact segment)
        //   - `S_id`        (snake_case FK convention)
        //   - `SId`         (camelCase FK convention — lowercased: `sid`)
        let s_id = format!("{}_id", norm_seg);
        let s_id_camel = format!("{}id", norm_seg);

        for fk in &candidates_for_table {
            if fk.columns.is_empty() {
                continue;
            }
            let norm_col = self.dialect.normalize_identifier(&fk.columns[0]);
            if norm_col == norm_seg || norm_col == s_id || norm_col == s_id_camel {
                matches.push(fk);
            }
        }

        // Pass 2: referenced-table-name fallback (only if pass 1 found nothing).
        //
        // The segment may be the singular or truncated form of the table name
        // (e.g. `user` for `users`, `organization` for `organizations`).
        // We match when `referenced_table` starts with the segment.
        if matches.is_empty() {
            for fk in &candidates_for_table {
                let norm_ref_table = self.dialect.normalize_identifier(&fk.referenced_table);
                if norm_ref_table == norm_seg || norm_ref_table.starts_with(norm_seg.as_ref()) {
                    matches.push(fk);
                }
            }
        }

        // Deduplicate by FK name (or by columns if name is empty)
        matches.dedup_by_key(|fk| fk.name.clone());

        match matches.len() {
            0 => {
                let partial_spec = self.build_partial_spec_so_far(current_alias);
                Err(ResolveError::Unknown {
                    segment: segment.to_string(),
                    from_table: current_table.to_string(),
                    span,
                    partial_spec,
                })
            }
            1 => {
                let fk = (*matches[0]).clone();
                let next_alias = self.aliases.next_alias(&fk.referenced_table);
                Ok((fk, next_alias))
            }
            _ => {
                let candidates = matches.iter().map(|fk| FkSummary::from_fk(fk)).collect();
                let partial_spec = self.build_partial_spec_so_far(current_alias);
                Err(ResolveError::Ambiguous {
                    segment: segment.to_string(),
                    from_table: current_table.to_string(),
                    candidates,
                    span,
                    partial_spec,
                })
            }
        }
    }

    /// Build a `VisualQuerySpec` with the joins resolved so far (no filter).
    ///
    /// Used as the `partial_spec` payload in error variants so the user can
    /// open the builder pre-seeded with the hops that did resolve.
    fn build_partial_spec_so_far(&self, _current_alias: &str) -> VisualQuerySpec {
        VisualQuerySpec {
            source: SourceTable {
                schema: self.source_schema.clone(),
                table: self.source_alias.clone(),
                alias: self.source_alias.clone(),
            },
            projection: Projection::All,
            joins: self.joins.clone(),
            filter: None,
            group_by: Vec::new(),
            aggregates: Vec::new(),
            having: None,
            sort: Vec::new(),
            limit: None,
            offset: 0,
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::relational_filter::parser::parse;
    use crate::query::visual_query::BoolOp;
    use crate::sql::dialect::{DefaultSqlDialect, PlaceholderStyle, SqlDialect};

    struct NopDialect;

    impl SqlDialect for NopDialect {
        fn quote_identifier(&self, name: &str) -> String {
            format!("\"{}\"", name)
        }
        fn qualified_table(&self, _schema: Option<&str>, table: &str) -> String {
            table.to_string()
        }
        fn value_to_literal(&self, _v: &crate::Value) -> String {
            "?".to_string()
        }
        fn escape_string(&self, s: &str) -> String {
            s.to_string()
        }
        fn placeholder_style(&self) -> PlaceholderStyle {
            PlaceholderStyle::QuestionMark
        }
    }

    fn make_source(table: &str) -> SourceTable {
        SourceTable {
            schema: None,
            table: table.to_string(),
            alias: table.to_string(),
        }
    }

    fn make_fk(
        from_table: &str,
        from_col: &str,
        to_table: &str,
        to_col: &str,
    ) -> SchemaForeignKeyInfo {
        SchemaForeignKeyInfo {
            name: format!("fk_{}_{}", from_table, from_col),
            table_name: from_table.to_string(),
            columns: vec![from_col.to_string()],
            referenced_schema: None,
            referenced_table: to_table.to_string(),
            referenced_columns: vec![to_col.to_string()],
            on_delete: None,
            on_update: None,
        }
    }

    // T12: BareColumn predicates mapped to source alias
    #[test]
    fn resolve_bare_column_to_source_alias() {
        let ast = parse("status = 'active'").unwrap();
        let result = resolve(ast, make_source("posts"), &[], &NopDialect).unwrap();
        assert_eq!(result.spec.joins.len(), 0);
        if let Some(FilterNode::Predicate(pred)) = result.spec.filter {
            assert_eq!(pred.source_alias, "posts");
            assert_eq!(pred.column, "status");
        } else {
            panic!("expected single predicate");
        }
    }

    // T13: alias generator
    #[test]
    fn alias_generator_increments_on_collision() {
        let mut ag = AliasGen::new("source_table");
        let a1 = ag.next_alias("categories");
        let a2 = ag.next_alias("categories");
        assert_eq!(a1, "categories");
        assert_eq!(a2, "categories_2");
    }

    #[test]
    fn alias_generator_self_join_gets_suffix() {
        // Source is "categories"; first join onto "categories" must get "_2"
        let mut ag = AliasGen::new("categories");
        let a = ag.next_alias("categories");
        assert_eq!(a, "categories_2");
    }

    #[test]
    fn alias_generator_three_hops_to_same_table() {
        let mut ag = AliasGen::new("source");
        assert_eq!(ag.next_alias("t"), "t");
        assert_eq!(ag.next_alias("t"), "t_2");
        assert_eq!(ag.next_alias("t"), "t_3");
    }

    // T14: single-hop FK resolution via column-prefix rule
    #[test]
    fn resolve_single_hop_by_column_prefix() {
        // FK: posts.created_by_id → users.id
        let fks = [make_fk("posts", "created_by_id", "users", "id")];
        let ast = parse("created_by.email = 'alice'").unwrap();
        let result = resolve(ast, make_source("posts"), &fks, &DefaultSqlDialect).unwrap();

        assert_eq!(result.spec.joins.len(), 1);
        let join = &result.spec.joins[0];
        assert_eq!(join.from_alias, "posts");
        assert_eq!(join.to_table, "users");
        assert_eq!(join.to_alias, "users");
        assert_eq!(
            join.on,
            JoinOn::FkPath {
                from_column: "created_by_id".to_string(),
                to_column: "id".to_string(),
            }
        );

        if let Some(FilterNode::Predicate(pred)) = result.spec.filter {
            assert_eq!(pred.source_alias, "users");
            assert_eq!(pred.column, "email");
        } else {
            panic!("expected predicate on users");
        }
    }

    #[test]
    fn resolve_single_hop_by_sid_camel() {
        // FK: posts.createdById → users.id; segment = "createdBy"
        let fks = [make_fk("posts", "createdById", "users", "id")];
        let ast = parse("createdBy.email = 'x'").unwrap();
        let result = resolve(ast, make_source("posts"), &fks, &DefaultSqlDialect).unwrap();

        assert_eq!(result.spec.joins.len(), 1);
        assert_eq!(result.spec.joins[0].to_table, "users");
    }

    // T15: referenced-table-name fallback
    #[test]
    fn resolve_single_hop_by_referenced_table_name() {
        // FK column is "author" (no "_id" suffix) → users; segment matches by table name
        let fks = [make_fk("posts", "author", "users", "id")];
        let ast = parse("users.email = 'x'").unwrap();
        let result = resolve(ast, make_source("posts"), &fks, &DefaultSqlDialect).unwrap();
        assert_eq!(result.spec.joins.len(), 1);
        assert_eq!(result.spec.joins[0].to_table, "users");
    }

    // T16: AmbiguousPath and UnknownRelation errors with partial_spec
    #[test]
    fn resolver_emits_ambiguous_with_partial_spec() {
        // Two FKs from posts to users
        let fks = [
            make_fk("posts", "created_by_id", "users", "id"),
            make_fk("posts", "updated_by_id", "users", "id"),
        ];
        // "user" matches both via referenced_table = "users"
        let ast = parse("user.email = 'x'").unwrap();
        let err = resolve(ast, make_source("posts"), &fks, &DefaultSqlDialect).unwrap_err();

        if let ResolveError::Ambiguous {
            segment,
            candidates,
            partial_spec,
            ..
        } = err
        {
            assert_eq!(segment, "user");
            assert_eq!(candidates.len(), 2);
            // No hops were completed before the failure.
            assert_eq!(partial_spec.joins.len(), 0);
        } else {
            panic!("expected Ambiguous, got {:?}", err);
        }
    }

    #[test]
    fn resolver_emits_unknown() {
        let fks = [make_fk("posts", "created_by_id", "users", "id")];
        let ast = parse("nonexistent.col = 1").unwrap();
        let err = resolve(ast, make_source("posts"), &fks, &DefaultSqlDialect).unwrap_err();

        assert!(
            matches!(err, ResolveError::Unknown { ref segment, .. } if segment == "nonexistent"),
            "got: {:?}",
            err
        );
    }

    // T17: multi-hop traversal
    #[test]
    fn resolve_multi_hop() {
        let fks = [
            make_fk("posts", "created_by_id", "users", "id"),
            make_fk("users", "org_id", "organizations", "id"),
        ];
        let ast = parse("created_by.organization.name = 'Acme'").unwrap();
        let result = resolve(ast, make_source("posts"), &fks, &DefaultSqlDialect).unwrap();

        assert_eq!(result.spec.joins.len(), 2);
        assert_eq!(result.spec.joins[0].to_table, "users");
        assert_eq!(result.spec.joins[1].to_table, "organizations");
        assert_eq!(result.spec.joins[1].from_alias, "users");

        if let Some(FilterNode::Predicate(pred)) = result.spec.filter {
            assert_eq!(pred.source_alias, "organizations");
            assert_eq!(pred.column, "name");
        } else {
            panic!("expected predicate on organizations");
        }
    }

    #[test]
    fn resolve_self_join_aliases() {
        // categories.parent_id → categories.id (S-12)
        let fks = [make_fk("categories", "parent_id", "categories", "id")];
        let ast = parse("parent.parent.name = 'Root'").unwrap();
        let result = resolve(ast, make_source("categories"), &fks, &DefaultSqlDialect).unwrap();

        assert_eq!(result.spec.joins.len(), 2);
        assert_eq!(result.spec.joins[0].to_alias, "categories_2");
        assert_eq!(result.spec.joins[1].to_alias, "categories_3");
    }

    // T18: mixed bare + dotted composition
    #[test]
    fn resolve_mixed_bare_and_dotted() {
        let fks = [make_fk("posts", "created_by_id", "users", "id")];
        let ast = parse("status = 'active' AND created_by.email LIKE '%@acme.com'").unwrap();
        let result = resolve(ast, make_source("posts"), &fks, &DefaultSqlDialect).unwrap();

        assert_eq!(result.spec.joins.len(), 1);

        if let Some(FilterNode::Group { op, children }) = result.spec.filter {
            assert_eq!(op, BoolOp::And);
            assert_eq!(children.len(), 2);

            if let FilterNode::Predicate(bare) = &children[0] {
                assert_eq!(bare.source_alias, "posts");
                assert_eq!(bare.column, "status");
            } else {
                panic!("expected bare predicate");
            }

            if let FilterNode::Predicate(dotted) = &children[1] {
                assert_eq!(dotted.source_alias, "users");
                assert_eq!(dotted.column, "email");
            } else {
                panic!("expected dotted predicate");
            }
        } else {
            panic!("expected And group");
        }
    }

    #[test]
    fn resolve_preserves_sort_as_empty() {
        let ast = parse("a = 1").unwrap();
        let result = resolve(ast, make_source("t"), &[], &NopDialect).unwrap();
        assert_eq!(result.spec.sort.len(), 0, "FR-FK-5: sort must be empty");
    }
}
