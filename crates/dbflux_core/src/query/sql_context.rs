//! Tree-sitter based cursor-context detection for SQL completion: what kind
//! of item belongs at the cursor ([`SqlCompletionContext`]) and which
//! relations are visible there ([`StatementScope`]). Uses the same permissive
//! grammar as the editor diagnostics (`sql_editor_diagnostics`).
//!
//! Best-effort by contract: [`SqlContextEngine::analyze`] reports
//! `context: None` whenever the position cannot be classified with confidence
//! and callers keep their heuristic fallback. The engine must never make
//! completion worse than that fallback, only more precise.
//!
//! Mid-keystroke SQL routinely produces `ERROR` nodes; two recovery
//! strategies keep classification working there:
//!
//! - A trailing clause keyword lands in its own error node (`SELECT * FROM `
//!   parses as `(statement …) (ERROR (keyword_from))`): the anchor token's
//!   kind decides the context.
//! - Inside larger broken constructs (`UPDATE t SET `) the leaves before the
//!   cursor are scanned backwards for the nearest clause keyword.

use std::cell::RefCell;

use tree_sitter::{Node, Parser, Tree};

/// The clause a column reference sits in. Used for ranking and
/// clause-specific suggestions; detection is not exhaustive: unknown clauses
/// classify as [`SqlClause::Other`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlClause {
    SelectList,
    Where,
    On,
    GroupBy,
    OrderBy,
    Having,
    Set,
    InsertColumns,
    Other,
}

/// What kind of completion item belongs at the cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlCompletionContext {
    /// A table, view, or CTE name is expected (after FROM / JOIN / UPDATE /
    /// INSERT INTO).
    TableRef,
    /// A column expression is expected.
    ColumnRef { clause: SqlClause },
}

/// A relation visible at the cursor: `FROM billing.invoices inv` yields
/// `{ schema: Some("billing"), table: "invoices", alias: Some("inv") }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeRelation {
    pub schema: Option<String>,
    pub table: String,
    pub alias: Option<String>,
}

/// The relations and CTE names visible at the cursor position, innermost
/// query level first (correlated subqueries see their outer relations).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatementScope {
    pub relations: Vec<ScopeRelation>,
    pub cte_names: Vec<String>,
    /// Output-column aliases of the innermost query's SELECT list
    /// (`SELECT total * 2 AS doubled`). Valid completion targets in
    /// GROUP BY / ORDER BY / HAVING, not in WHERE.
    pub select_aliases: Vec<String>,
}

/// Result of one cursor analysis. `context` is `None` when the position could
/// not be classified; `scope` is always best-effort (possibly empty) and can
/// be used independently, e.g. for alias resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCursorAnalysis {
    pub context: Option<SqlCompletionContext>,
    pub scope: StatementScope,
}

/// Foreground-only engine (interior mutability, `!Sync`): one instance per
/// completion provider.
pub struct SqlContextEngine {
    parser: RefCell<Parser>,
    /// Last parsed `(source, tree)`; completion often looks at the same
    /// buffer several times per keystroke.
    cache: RefCell<Option<(String, Tree)>>,
}

impl SqlContextEngine {
    /// Returns `None` when the grammar cannot be loaded (version mismatch
    /// between `tree-sitter` and `tree-sitter-sequel`); callers treat that as
    /// "no context engine" and keep the fallback path.
    pub fn new() -> Option<Self> {
        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter::Language::new(tree_sitter_sequel::LANGUAGE))
            .ok()?;

        Some(Self {
            parser: RefCell::new(parser),
            cache: RefCell::new(None),
        })
    }

    /// Classifies the cursor position and collects the visible scope.
    ///
    /// Returns `None` only when parsing itself fails; a parse with `ERROR`
    /// nodes still yields an analysis (with `context: None` where the
    /// position is unclassifiable).
    pub fn analyze(&self, source: &str, offset: usize) -> Option<SqlCursorAnalysis> {
        let offset = source.floor_char_boundary(offset);

        // Scope to the statement under the cursor: the parse stays
        // O(statement) not O(buffer) per keystroke, and the cursor never
        // anchors to a neighboring statement's tokens.
        let range = crate::QueryLanguage::Sql.statement_bounds_at(source, offset);
        let statement = &source[range.clone()];
        let local_offset = offset - range.start;

        let mut cache = self.cache.borrow_mut();
        if cache.as_ref().is_none_or(|(cached, _)| cached != statement) {
            let tree = self.parser.borrow_mut().parse(statement, None)?;
            *cache = Some((statement.to_string(), tree));
        }
        let (_, tree) = cache.as_ref()?;

        let anchor_start = statement[..local_offset].rfind(|ch: char| !ch.is_whitespace())?;
        let anchor_width = statement[anchor_start..]
            .chars()
            .next()
            .map_or(1, |ch| ch.len_utf8());

        let anchor = tree
            .root_node()
            .descendant_for_byte_range(anchor_start, anchor_start + anchor_width)?;

        // Whitespace between the anchor token and the cursor means the token
        // is finished and the cursor sits in the NEXT slot.
        let detached = local_offset > anchor.end_byte();

        let context = classify(anchor, detached);
        let scope = collect_scope(anchor, statement);

        Some(SqlCursorAnalysis { context, scope })
    }
}

fn classify(anchor: Node, detached: bool) -> Option<SqlCompletionContext> {
    // A keyword anchor names the slot after it, whether the cursor touches
    // the keyword or hangs behind it (while it is still being typed, the
    // prefix filter neutralizes the items).
    if let Some(context) = context_after_keyword(anchor) {
        return Some(context);
    }

    // A finished identifier (`FROM invoices |`) puts the cursor in keyword
    // territory: no table/column context, let the fallback offer keywords.
    if detached && is_identifier_like(anchor) {
        return None;
    }

    classify_by_ancestors(anchor).or_else(|| classify_by_backscan(anchor))
}

/// The column clause a keyword token directly introduces, or `None`.
fn column_clause_for_keyword(kind: &str) -> Option<SqlClause> {
    Some(match kind {
        "keyword_where" => SqlClause::Where,
        "keyword_on" => SqlClause::On,
        "keyword_set" => SqlClause::Set,
        "keyword_select" | "keyword_distinct" => SqlClause::SelectList,
        "keyword_having" => SqlClause::Having,
        _ => return None,
    })
}

fn column_ref(clause: SqlClause) -> Option<SqlCompletionContext> {
    Some(SqlCompletionContext::ColumnRef { clause })
}

/// The context of the slot following a clause keyword, or `None` when the
/// anchor is not a clause keyword.
fn context_after_keyword(anchor: Node) -> Option<SqlCompletionContext> {
    if let Some(clause) = column_clause_for_keyword(anchor.kind()) {
        return column_ref(clause);
    }

    match anchor.kind() {
        "keyword_from" | "keyword_join" | "keyword_update" | "keyword_into" => {
            Some(SqlCompletionContext::TableRef)
        }
        "keyword_by" => match anchor.prev_named_sibling().map(|node| node.kind()) {
            Some("keyword_group") => column_ref(SqlClause::GroupBy),
            Some("keyword_order") => column_ref(SqlClause::OrderBy),
            _ => None,
        },
        "keyword_and" | "keyword_or" | "keyword_not" => {
            // AND/OR continue whatever predicate clause encloses them.
            classify_by_ancestors(anchor)
                .or_else(|| classify_by_backscan(anchor))
                .filter(|context| matches!(context, SqlCompletionContext::ColumnRef { .. }))
        }
        _ => None,
    }
}

fn is_identifier_like(anchor: Node) -> bool {
    matches!(anchor.kind(), "identifier" | "all_fields" | "literal")
}

/// Classifies via the enclosing structure. Works whenever the grammar
/// recovered a real clause node around the cursor.
fn classify_by_ancestors(anchor: Node) -> Option<SqlCompletionContext> {
    let mut node = Some(anchor);

    while let Some(current) = node {
        match current.kind() {
            "relation" | "from" | "insert" => return Some(SqlCompletionContext::TableRef),
            "where" => return column_ref(SqlClause::Where),
            "select_expression" => return column_ref(SqlClause::SelectList),
            "group_by" => return column_ref(SqlClause::GroupBy),
            "order_by" => return column_ref(SqlClause::OrderBy),
            "ordered_columns" | "column" => return column_ref(SqlClause::InsertColumns),
            kind if kind.ends_with("join") => {
                // Inside a join: table position before ON, predicate after.
                return if past_child_keyword(current, anchor, "keyword_on") {
                    column_ref(SqlClause::On)
                } else {
                    Some(SqlCompletionContext::TableRef)
                };
            }
            "update" => {
                return if past_child_keyword(current, anchor, "keyword_set") {
                    column_ref(SqlClause::Set)
                } else {
                    Some(SqlCompletionContext::TableRef)
                };
            }
            "ERROR" => return None,
            "statement" | "subquery" | "program" => return None,
            _ => {}
        }

        node = current.parent();
    }

    None
}

/// Whether `container` has a direct `keyword` child that ends at or before
/// the anchor, i.e. the cursor already passed that keyword.
fn past_child_keyword(container: Node, anchor: Node, keyword: &str) -> bool {
    let mut cursor = container.walk();
    container
        .children(&mut cursor)
        .any(|child| child.kind() == keyword && child.end_byte() <= anchor.start_byte())
}

/// Last-resort classification inside broken constructs: scan the leaves
/// before the anchor backwards for the nearest clause keyword.
fn classify_by_backscan(anchor: Node) -> Option<SqlCompletionContext> {
    let root = root_of(anchor);
    let mut leaves = Vec::new();
    collect_leaves(root, &mut leaves);

    let mut crossed_identifier = false;
    // The anchor itself may be the opening paren (`INSERT INTO t (`); the
    // scan below only looks at leaves BEFORE it.
    let mut crossed_open_paren = anchor.kind() == "(";
    let mut pending_by = false;

    for leaf in leaves
        .into_iter()
        .rev()
        .skip_while(|leaf| leaf.start_byte() >= anchor.start_byte())
    {
        if leaf.is_missing() {
            continue;
        }

        if let Some(clause) = column_clause_for_keyword(leaf.kind()) {
            return column_ref(clause);
        }

        match leaf.kind() {
            "identifier" | "all_fields" | "literal" => crossed_identifier = true,
            "(" => crossed_open_paren = true,
            ")" | ";" => return None,
            "keyword_from" | "keyword_join" | "keyword_update" => {
                return (!crossed_identifier).then_some(SqlCompletionContext::TableRef);
            }
            "keyword_into" => {
                return if crossed_identifier && crossed_open_paren {
                    column_ref(SqlClause::InsertColumns)
                } else if crossed_identifier {
                    None
                } else {
                    Some(SqlCompletionContext::TableRef)
                };
            }
            "keyword_by" => pending_by = true,
            "keyword_group" if pending_by => return column_ref(SqlClause::GroupBy),
            "keyword_order" if pending_by => return column_ref(SqlClause::OrderBy),
            _ => {}
        }
    }

    None
}

fn root_of(node: Node) -> Node {
    let mut current = node;
    while let Some(parent) = current.parent() {
        current = parent;
    }
    current
}

fn collect_leaves<'tree>(node: Node<'tree>, leaves: &mut Vec<Node<'tree>>) {
    if node.child_count() == 0 {
        leaves.push(node);
        return;
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_leaves(child, leaves);
    }
}

/// Collects the relations and CTE names visible from the anchor, innermost
/// query level first.
fn collect_scope(anchor: Node, source: &str) -> StatementScope {
    let mut scope = StatementScope::default();
    let mut crossed_level = false;
    let mut select_level_seen = false;
    let mut program_child = anchor;

    let mut node = Some(anchor);
    while let Some(current) = node {
        match current.kind() {
            "statement" | "subquery" => {
                crossed_level = true;
                collect_level(current, source, &mut scope, &mut select_level_seen);
            }
            // Broken constructs (`UPDATE t SET `) keep their relation nodes
            // inside the error node.
            "ERROR" => collect_error_relations(current, source, &mut scope),
            _ => {}
        }

        if current
            .parent()
            .is_some_and(|parent| parent.kind() == "program")
        {
            program_child = current;
        }

        node = current.parent();
    }

    // Trailing clause keywords land in a top-level error node NEXT TO the
    // statement they belong to (`… WHERE ` parses as
    // `(statement …) (ERROR (keyword_where))`), so recover the scope from the
    // statement immediately before the error.
    if !crossed_level {
        let mut sibling = program_child.prev_named_sibling();
        while let Some(current) = sibling {
            if current.kind() == "statement" {
                collect_level(current, source, &mut scope, &mut select_level_seen);
                break;
            }
            sibling = current.prev_named_sibling();
        }
    }

    scope.relations.dedup();
    scope.cte_names.dedup();
    scope.select_aliases.dedup();
    scope
}

fn collect_level(
    level: Node,
    source: &str,
    scope: &mut StatementScope,
    select_level_seen: &mut bool,
) {
    let mut cursor = level.walk();

    for child in level.named_children(&mut cursor) {
        match child.kind() {
            "from" => collect_from(child, source, scope),
            // Output aliases are only usable at their own query level, so
            // only the innermost SELECT (visited first) contributes, even
            // when its expression list carries no aliases.
            "select" if !*select_level_seen => {
                *select_level_seen = true;
                collect_select_aliases(child, source, scope);
            }
            "cte" => {
                let mut cte_cursor = child.walk();
                if let Some(name) = child
                    .named_children(&mut cte_cursor)
                    .find(|node| node.kind() == "identifier")
                    && let Some(text) = node_text(name, source)
                {
                    scope.cte_names.push(text);
                }
            }
            "update" | "insert" => {
                let mut inner_cursor = child.walk();
                for inner in child.named_children(&mut inner_cursor) {
                    match inner.kind() {
                        "relation" => push_relation(inner, source, scope),
                        "object_reference" => push_object_reference(inner, None, source, scope),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
}

/// Collects the `AS`-style output aliases from a `select` node's expression
/// list (`(select_expression (term … alias: (identifier)))`; implicit aliases
/// without AS carry the same field).
fn collect_select_aliases(select: Node, source: &str, scope: &mut StatementScope) {
    let mut cursor = select.walk();
    let Some(select_expression) = select
        .named_children(&mut cursor)
        .find(|node| node.kind() == "select_expression")
    else {
        return;
    };

    let mut term_cursor = select_expression.walk();
    for term in select_expression.named_children(&mut term_cursor) {
        if term.kind() != "term" {
            continue;
        }

        if let Some(alias) = term.child_by_field_name("alias")
            && let Some(text) = node_text(alias, source)
        {
            scope.select_aliases.push(text);
        }
    }
}

fn collect_from(from_node: Node, source: &str, scope: &mut StatementScope) {
    let mut cursor = from_node.walk();

    for child in from_node.named_children(&mut cursor) {
        match child.kind() {
            "relation" => push_relation(child, source, scope),
            kind if kind.ends_with("join") => {
                let mut join_cursor = child.walk();
                for join_child in child.named_children(&mut join_cursor) {
                    if join_child.kind() == "relation" {
                        push_relation(join_child, source, scope);
                    }
                }
            }
            _ => {}
        }
    }
}

fn collect_error_relations(error: Node, source: &str, scope: &mut StatementScope) {
    let mut cursor = error.walk();

    for child in error.named_children(&mut cursor) {
        match child.kind() {
            "relation" => push_relation(child, source, scope),
            "object_reference" => push_object_reference(child, None, source, scope),
            _ => {}
        }
    }
}

fn push_relation(relation: Node, source: &str, scope: &mut StatementScope) {
    let mut cursor = relation.walk();
    let Some(object_reference) = relation
        .named_children(&mut cursor)
        .find(|node| node.kind() == "object_reference")
    else {
        // Derived table (`FROM (SELECT …) x`): no base relation to resolve.
        return;
    };

    let alias = relation
        .child_by_field_name("alias")
        .and_then(|node| node_text(node, source));

    push_object_reference(object_reference, alias, source, scope);
}

fn push_object_reference(
    object_reference: Node,
    alias: Option<String>,
    source: &str,
    scope: &mut StatementScope,
) {
    let Some(table) = object_reference
        .child_by_field_name("name")
        .and_then(|node| node_text(node, source))
    else {
        return;
    };

    let schema = object_reference
        .child_by_field_name("schema")
        .and_then(|node| node_text(node, source));

    scope.relations.push(ScopeRelation {
        schema,
        table,
        alias,
    });
}

fn node_text(node: Node, source: &str) -> Option<String> {
    node.utf8_text(source.as_bytes())
        .ok()
        .map(|text| text.trim_matches(['"', '`', '[', ']']).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> SqlContextEngine {
        SqlContextEngine::new().expect("grammar loads")
    }

    fn analyze_at_end(source: &str) -> SqlCursorAnalysis {
        engine()
            .analyze(source, source.len())
            .expect("analysis succeeds")
    }

    fn relation(schema: Option<&str>, table: &str, alias: Option<&str>) -> ScopeRelation {
        ScopeRelation {
            schema: schema.map(String::from),
            table: table.to_string(),
            alias: alias.map(String::from),
        }
    }

    #[test]
    fn trailing_from_expects_table() {
        let analysis = analyze_at_end("SELECT * FROM ");
        assert_eq!(analysis.context, Some(SqlCompletionContext::TableRef));
    }

    #[test]
    fn trailing_join_expects_table_and_sees_existing_relation() {
        let analysis = analyze_at_end("SELECT * FROM invoices inv JOIN ");
        assert_eq!(analysis.context, Some(SqlCompletionContext::TableRef));
        assert_eq!(
            analysis.scope.relations,
            vec![relation(None, "invoices", Some("inv"))]
        );
    }

    #[test]
    fn trailing_where_expects_column_with_scope() {
        let analysis = analyze_at_end("SELECT * FROM invoices inv WHERE ");
        assert_eq!(
            analysis.context,
            Some(SqlCompletionContext::ColumnRef {
                clause: SqlClause::Where
            })
        );
        assert_eq!(
            analysis.scope.relations,
            vec![relation(None, "invoices", Some("inv"))]
        );
    }

    #[test]
    fn trailing_on_expects_column_with_both_relations() {
        let analysis = analyze_at_end("SELECT * FROM invoices inv JOIN payments p ON ");
        assert_eq!(
            analysis.context,
            Some(SqlCompletionContext::ColumnRef {
                clause: SqlClause::On
            })
        );
        assert_eq!(
            analysis.scope.relations,
            vec![
                relation(None, "invoices", Some("inv")),
                relation(None, "payments", Some("p")),
            ]
        );
    }

    #[test]
    fn subquery_where_sees_inner_scope_first() {
        let analysis = analyze_at_end(
            "SELECT * FROM invoices WHERE id IN (SELECT invoice_id FROM payments WHERE ",
        );
        assert_eq!(
            analysis.context,
            Some(SqlCompletionContext::ColumnRef {
                clause: SqlClause::Where
            })
        );
        assert_eq!(
            analysis.scope.relations,
            vec![
                relation(None, "payments", None),
                relation(None, "invoices", None),
            ]
        );
    }

    #[test]
    fn cte_names_are_visible_as_scope() {
        let analysis =
            analyze_at_end("WITH recent AS (SELECT * FROM invoices) SELECT * FROM recent WHERE ");
        assert_eq!(
            analysis.context,
            Some(SqlCompletionContext::ColumnRef {
                clause: SqlClause::Where
            })
        );
        assert_eq!(analysis.scope.cte_names, vec!["recent".to_string()]);
        assert_eq!(
            analysis.scope.relations,
            vec![relation(None, "recent", None)]
        );
    }

    #[test]
    fn update_set_expects_column_of_updated_table() {
        let analysis = analyze_at_end("UPDATE invoices SET ");
        assert_eq!(
            analysis.context,
            Some(SqlCompletionContext::ColumnRef {
                clause: SqlClause::Set
            })
        );
        assert_eq!(
            analysis.scope.relations,
            vec![relation(None, "invoices", None)]
        );
    }

    #[test]
    fn insert_column_list_expects_columns() {
        let analysis = analyze_at_end("INSERT INTO invoices (");
        assert_eq!(
            analysis.context,
            Some(SqlCompletionContext::ColumnRef {
                clause: SqlClause::InsertColumns
            })
        );
        assert_eq!(
            analysis.scope.relations,
            vec![relation(None, "invoices", None)]
        );
    }

    #[test]
    fn trailing_clause_keywords_classify_as_column_contexts() {
        let select = analyze_at_end("SELECT ");
        assert_eq!(
            select.context,
            Some(SqlCompletionContext::ColumnRef {
                clause: SqlClause::SelectList
            })
        );

        let group_by = analyze_at_end("SELECT * FROM invoices GROUP BY ");
        assert_eq!(
            group_by.context,
            Some(SqlCompletionContext::ColumnRef {
                clause: SqlClause::GroupBy
            })
        );
    }

    #[test]
    fn select_aliases_visible_in_group_by() {
        let analysis = analyze_at_end("SELECT total_amount * 2 AS doubled FROM invoices GROUP BY ");
        assert_eq!(
            analysis.context,
            Some(SqlCompletionContext::ColumnRef {
                clause: SqlClause::GroupBy
            })
        );
        assert_eq!(analysis.scope.select_aliases, vec!["doubled".to_string()]);
    }

    #[test]
    fn subquery_does_not_inherit_outer_select_aliases() {
        let analysis = analyze_at_end(
            "SELECT total AS outer_alias FROM invoices WHERE id IN (SELECT invoice_id FROM payments WHERE ",
        );
        assert!(
            analysis.scope.select_aliases.is_empty(),
            "the innermost SELECT has no aliases; outer aliases must not leak in"
        );
    }

    #[test]
    fn schema_qualified_relation_keeps_schema() {
        let analysis = analyze_at_end("SELECT * FROM billing.invoices b WHERE ");
        assert_eq!(
            analysis.scope.relations,
            vec![relation(Some("billing"), "invoices", Some("b"))]
        );
    }

    #[test]
    fn finished_identifier_yields_no_context() {
        // Cursor detached from a complete table name: keyword territory,
        // fallback decides.
        let analysis = analyze_at_end("SELECT * FROM invoices ");
        assert_eq!(analysis.context, None);
    }

    #[test]
    fn multi_statement_scopes_to_cursor_statement() {
        let analysis = analyze_at_end("SELECT * FROM archive; SELECT * FROM invoices inv WHERE ");
        assert_eq!(
            analysis.scope.relations,
            vec![relation(None, "invoices", Some("inv"))]
        );
    }

    #[test]
    fn cursor_in_earlier_statement_scopes_to_that_statement() {
        let source = "SELECT * FROM orders o WHERE ; SELECT * FROM invoices inv";
        let cursor = source.find(" WHERE ").expect("where") + " WHERE ".len();
        let analysis = engine().analyze(source, cursor).expect("analysis");
        assert_eq!(
            analysis.context,
            Some(SqlCompletionContext::ColumnRef {
                clause: SqlClause::Where
            })
        );
        assert_eq!(
            analysis.scope.relations,
            vec![relation(None, "orders", Some("o"))]
        );
    }

    #[test]
    fn typing_table_name_after_from_expects_table() {
        let source = "SELECT * FROM inv";
        let analysis = engine().analyze(source, source.len()).expect("analysis");
        assert_eq!(analysis.context, Some(SqlCompletionContext::TableRef));
    }

    #[test]
    fn typing_column_in_where_expects_column() {
        let source = "SELECT * FROM invoices WHERE tot";
        let analysis = engine().analyze(source, source.len()).expect("analysis");
        assert_eq!(
            analysis.context,
            Some(SqlCompletionContext::ColumnRef {
                clause: SqlClause::Where
            })
        );
        assert_eq!(
            analysis.scope.relations,
            vec![relation(None, "invoices", None)]
        );
    }

    /// Dialect-tolerance contract: foreign dialects must never panic and may
    /// classify only where the permissive grammar genuinely understands the
    /// construct.
    #[test]
    fn foreign_dialects_never_panic() {
        let engine = engine();
        let fixtures = [
            // PartiQL (DynamoDB)
            "SELECT * FROM \"Orders\" WHERE \"pk\" = 'x' AND ",
            // CQL (Cassandra)
            "SELECT * FROM ks.events WHERE token(id) > 0 AND ",
            // InfluxQL
            "SELECT mean(\"value\") FROM \"cpu\" WHERE time > now() - 1h GROUP BY ",
            // T-SQL bracket identifiers
            "SELECT * FROM [dbo].[Invoices] WHERE ",
            // Nonsense
            "!!! not sql at all ;;; ",
        ];

        for fixture in fixtures {
            for offset in [0, fixture.len() / 2, fixture.len()] {
                let _analysis = engine.analyze(fixture, offset);
            }
        }
    }

    /// Analyze must never panic on any byte offset, including one that lands
    /// inside a multi-byte char or past the end. Multi-byte chars appear in
    /// real SQL inside string literals.
    #[test]
    fn analyze_never_panics_on_arbitrary_byte_offsets() {
        let engine = engine();
        let fixtures = [
            "SELECT c1 FROM t1 WHERE c1 = 'héllo wörld' AND ",
            "SELECT c1 FROM t1 WHERE c1 = '名前' GROUP BY ",
            "not sql at all 🎉 ;;; ",
            "",
        ];

        for fixture in fixtures {
            for offset in 0..=fixture.len() + 2 {
                let _analysis = engine.analyze(fixture, offset);
            }
        }
    }

    #[test]
    fn influxql_group_by_classifies_via_backscan() {
        let analysis =
            analyze_at_end("SELECT mean(\"value\") FROM \"cpu\" WHERE time > now() GROUP BY ");
        assert_eq!(
            analysis.context,
            Some(SqlCompletionContext::ColumnRef {
                clause: SqlClause::GroupBy
            })
        );
    }
}
