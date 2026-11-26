use super::*;
use crate::QueryLanguage;
use crate::{QueryResult, Value};

#[test]
fn single_statement_is_returned_trimmed_without_the_terminator() {
    assert_eq!(single_plan_statement("  SELECT 1;  ").unwrap(), "SELECT 1");
    assert_eq!(
        single_plan_statement("DELETE FROM t WHERE id = 1").unwrap(),
        "DELETE FROM t WHERE id = 1"
    );
}

#[test]
fn semicolons_inside_literals_do_not_split() {
    assert_eq!(
        single_plan_statement("SELECT 'a;b'").unwrap(),
        "SELECT 'a;b'"
    );
}

#[test]
fn multiple_statements_are_refused() {
    let error = single_plan_statement("SELECT 1; DELETE FROM t").unwrap_err();

    assert_eq!(
        error.to_string(),
        "A query plan covers exactly one statement; the query has 2. Select a single statement."
    );
}

#[test]
fn a_trailing_comment_is_not_a_second_statement() {
    assert_eq!(
        single_plan_statement("SELECT 1; -- explanation").unwrap(),
        "SELECT 1"
    );
    assert_eq!(
        single_plan_statement("SELECT 1; /* note */").unwrap(),
        "SELECT 1"
    );
}

#[test]
fn two_executable_statements_around_a_comment_are_refused() {
    let error = single_plan_statement("SELECT 1; -- keep\nDELETE FROM t").unwrap_err();

    assert_eq!(
        error.to_string(),
        "A query plan covers exactly one statement; the query has 2. Select a single statement."
    );
}

#[test]
fn a_comment_only_query_is_empty() {
    let error = single_plan_statement("-- nothing").unwrap_err();

    assert_eq!(
        error.to_string(),
        "A query plan needs a statement; the query is empty."
    );
}

#[test]
fn empty_queries_are_refused() {
    for query in ["", "   \n\t", " ; "] {
        let error = single_plan_statement(query).unwrap_err();
        assert_eq!(
            error.to_string(),
            "A query plan needs a statement; the query is empty."
        );
    }
}

fn eqp_result(rows: &[(i64, i64, &str)]) -> QueryResult {
    let mut result = QueryResult::empty();
    result.rows = rows
        .iter()
        .map(|(id, parent, detail)| {
            vec![
                Value::Int(*id),
                Value::Int(*parent),
                Value::Int(0),
                Value::Text((*detail).to_string()),
            ]
        })
        .collect();
    result
}

fn detail<'a>(node: &'a PlanNode, key: &str) -> Option<&'a str> {
    node.details
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}

#[test]
fn search_using_an_index_maps_relation_index_and_condition() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[(3, 0, "SEARCH t USING INDEX idx_a (a=?)")]))
        .unwrap();

    assert_eq!(plan.raw_format, PlanFormat::Text);
    assert_eq!(plan.roots.len(), 1);
    let node = &plan.roots[0];
    assert_eq!(node.operation, "SEARCH");
    assert_eq!(node.relation.as_deref(), Some("t"));
    assert_eq!(node.index.as_deref(), Some("idx_a"));
    assert_eq!(detail(node, "Condition"), Some("(a=?)"));
    assert_eq!(node.estimated_total_cost, None);
    assert_eq!(node.estimated_rows, None);
}

#[test]
fn covering_index_and_alias_and_legacy_table_keyword() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[
        (2, 0, "SCAN TABLE users AS u USING COVERING INDEX idx_name"),
        (3, 0, "SCAN orders"),
    ]))
    .unwrap();

    let first = &plan.roots[0];
    assert_eq!(first.operation, "SCAN");
    assert_eq!(first.relation.as_deref(), Some("users"));
    assert_eq!(first.index.as_deref(), Some("idx_name"));
    assert_eq!(detail(first, "Alias"), Some("u"));
    assert_eq!(detail(first, "Covering"), Some("true"));

    let second = &plan.roots[1];
    assert_eq!(second.relation.as_deref(), Some("orders"));
    assert_eq!(second.index, None);
}

#[test]
fn a_virtual_table_scan_keeps_only_the_source_as_relation() {
    let plan =
        parse_sqlite_query_plan(&eqp_result(&[(2, 0, "SCAN docs VIRTUAL TABLE INDEX 0:M1")]))
            .unwrap();

    let node = &plan.roots[0];
    assert_eq!(node.operation, "SCAN");
    assert_eq!(node.relation.as_deref(), Some("docs"));
    assert_eq!(node.index, None);
    assert_eq!(detail(node, "Virtual table index"), Some("0:M1"));
    assert!(plan.raw.contains("SCAN docs VIRTUAL TABLE INDEX 0:M1"));
}

#[test]
fn a_left_join_annotation_is_not_part_of_the_condition_or_index() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[
        (
            2,
            0,
            "SEARCH child USING INDEX child_parent (parent_id=?) LEFT-JOIN",
        ),
        (3, 0, "SCAN other USING INDEX other_idx LEFT-JOIN"),
        (4, 0, "SCAN third LEFT-JOIN"),
    ]))
    .unwrap();

    let with_condition = &plan.roots[0];
    assert_eq!(with_condition.index.as_deref(), Some("child_parent"));
    assert_eq!(detail(with_condition, "Condition"), Some("(parent_id=?)"));
    assert_eq!(
        with_condition
            .key_condition
            .as_ref()
            .map(|c| c.value.as_str()),
        Some("(parent_id=?)")
    );
    assert_eq!(detail(with_condition, "Join"), Some("LEFT-JOIN"));

    let without_condition = &plan.roots[1];
    assert_eq!(without_condition.index.as_deref(), Some("other_idx"));
    assert_eq!(detail(without_condition, "Join"), Some("LEFT-JOIN"));

    let plain = &plan.roots[2];
    assert_eq!(plain.relation.as_deref(), Some("third"));
    assert_eq!(detail(plain, "Join"), Some("LEFT-JOIN"));
    assert!(plan.raw.contains("(parent_id=?) LEFT-JOIN"));
}

#[test]
fn primary_key_and_automatic_index_have_no_index_name() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[
        (2, 0, "SEARCH t USING INTEGER PRIMARY KEY (rowid=?)"),
        (3, 0, "SEARCH u USING AUTOMATIC COVERING INDEX (a=?)"),
    ]))
    .unwrap();

    assert_eq!(plan.roots[0].index, None);
    assert_eq!(detail(&plan.roots[0], "Key"), Some("INTEGER PRIMARY KEY"));
    assert_eq!(detail(&plan.roots[0], "Condition"), Some("(rowid=?)"));
    assert_eq!(plan.roots[1].index, None);
    assert_eq!(detail(&plan.roots[1], "Automatic"), Some("true"));
}

#[test]
fn non_scan_details_become_the_operation() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[
        (2, 0, "SCAN CONSTANT ROW"),
        (3, 0, "SCAN SUBQUERY 1"),
        (4, 0, "USE TEMP B-TREE FOR ORDER BY"),
    ]))
    .unwrap();

    assert_eq!(plan.roots[0].operation, "SCAN CONSTANT ROW");
    assert_eq!(plan.roots[0].relation, None);
    assert_eq!(plan.roots[1].operation, "SCAN SUBQUERY 1");
    assert_eq!(plan.roots[1].relation, None);
    assert_eq!(plan.roots[2].operation, "USE TEMP B-TREE FOR ORDER BY");
}

#[test]
fn tree_is_built_from_id_and_parent() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[
        (2, 0, "MATERIALIZE x"),
        (3, 2, "SCAN t"),
        (9, 0, "SCAN x"),
        (10, 9, "SEARCH y USING INDEX i (k=?)"),
        (11, 3, "USE TEMP B-TREE FOR DISTINCT"),
    ]))
    .unwrap();

    assert_eq!(plan.roots.len(), 2);
    assert_eq!(plan.roots[0].operation, "MATERIALIZE x");
    assert_eq!(plan.roots[0].children.len(), 1);
    assert_eq!(plan.roots[0].children[0].relation.as_deref(), Some("t"));
    assert_eq!(plan.roots[0].children[0].children.len(), 1);
    assert_eq!(plan.roots[1].children[0].index.as_deref(), Some("i"));
}

#[test]
fn an_orphan_parent_becomes_a_root() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[(5, 77, "SCAN t")])).unwrap();

    assert_eq!(plan.roots.len(), 1);
}

#[test]
fn raw_text_keeps_every_row() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[(2, 0, "SCAN t"), (3, 2, "SCAN u")])).unwrap();

    assert_eq!(plan.raw, "2\t0\tSCAN t\n3\t2\tSCAN u");
}

#[test]
fn a_result_without_rows_is_an_empty_plan_with_a_warning() {
    let plan = parse_sqlite_query_plan(&QueryResult::empty()).unwrap();

    assert!(plan.roots.is_empty());
    assert_eq!(
        plan.warnings,
        ["The engine reported no plan steps for this statement."]
    );
}

#[test]
fn a_malformed_row_is_an_error() {
    let mut malformed = QueryResult::empty();
    malformed.rows = vec![vec![Value::Text("x".into())]];
    assert!(parse_sqlite_query_plan(&malformed).is_err());
}

#[test]
fn plan_text_is_read_from_the_first_cell_or_the_text_body() {
    let mut result = QueryResult::empty();
    result.rows = vec![vec![Value::Json("{\"a\":1}".to_string())]];
    assert_eq!(plan_text_from_result(&result).unwrap(), "{\"a\":1}");

    let mut text = QueryResult::empty();
    text.text_body = Some("plan".to_string());
    assert_eq!(plan_text_from_result(&text).unwrap(), "plan");

    assert!(plan_text_from_result(&QueryResult::empty()).is_err());
}

fn costed(operation: &str, total: f64, children: Vec<PlanNode>) -> PlanNode {
    PlanNode {
        estimated_total_cost: Some(total),
        children,
        ..PlanNode::new(operation)
    }
}

#[test]
fn self_cost_subtracts_the_direct_children() {
    let node = costed(
        "Nested Loop",
        100.0,
        vec![
            costed("Seq Scan", 60.0, vec![]),
            costed("Index Scan", 15.0, vec![]),
        ],
    );

    assert_eq!(node.estimated_self_cost(), Some(25.0));
}

#[test]
fn self_cost_never_goes_below_zero() {
    let node = costed("Limit", 10.0, vec![costed("Sort", 12.0, vec![])]);

    assert_eq!(node.estimated_self_cost(), Some(0.0));
}

#[test]
fn self_cost_is_unknown_when_a_child_has_no_cost() {
    let node = costed("Nested Loop", 100.0, vec![PlanNode::new("SCAN t")]);

    assert_eq!(node.estimated_self_cost(), None);
    assert_eq!(PlanNode::new("SCAN t").estimated_self_cost(), None);
}

#[test]
fn search_gets_a_description_and_its_condition_as_the_key_condition() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[(3, 0, "SEARCH t USING INDEX idx_a (a=?)")]))
        .unwrap();
    let node = &plan.roots[0];

    assert_eq!(
        node.description.as_deref(),
        Some("Looks rows up through an index instead of scanning the whole table.")
    );
    let condition = node.key_condition.as_ref().expect("a key condition");
    assert_eq!(condition.label, "Condition");
    assert_eq!(condition.value, "(a=?)");
}

#[test]
fn a_scan_has_a_description_and_no_key_condition() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[(3, 0, "SCAN t")])).unwrap();

    assert!(plan.roots[0].description.is_some());
    assert_eq!(plan.roots[0].key_condition, None);
}

#[test]
fn engine_steps_get_their_glossary_text() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[
        (1, 0, "USE TEMP B-TREE FOR ORDER BY"),
        (2, 0, "MATERIALIZE x"),
        (3, 0, "CO-ROUTINE y"),
        (4, 0, "COMPOUND QUERY"),
        (5, 0, "SCAN CONSTANT ROW"),
        (6, 0, "SCAN SUBQUERY 1"),
    ]))
    .unwrap();

    assert_eq!(
        plan.roots[0].description.as_deref(),
        Some("Builds a temporary index to group or order the rows.")
    );
    for node in &plan.roots {
        assert!(
            node.description.is_some(),
            "{} has no description",
            node.operation
        );
    }
}

#[test]
fn an_unknown_step_has_no_description() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[(2, 0, "TELEPORT t")])).unwrap();

    assert_eq!(plan.roots[0].description, None);
    assert_eq!(plan.roots[0].key_condition, None);
}

#[test]
fn sqlite_plans_only_queries_and_writes() {
    for query in [
        "SELECT 1",
        "  select 1;",
        "WITH x AS (SELECT 1) SELECT * FROM x",
        "VALUES (1)",
        "INSERT INTO t VALUES (1)",
        "REPLACE INTO t VALUES (1)",
        "UPDATE t SET a = 1",
        "DELETE FROM t",
        "-- note\n/* more */ delete from t",
    ] {
        assert!(sqlite_plan_statement(query).is_ok(), "{query}");
    }

    for query in [
        "PRAGMA foreign_keys = OFF",
        "-- note\npragma foreign_keys = OFF",
        "ATTACH 'x.db' AS other",
        "CREATE TABLE t (a)",
        "VACUUM",
    ] {
        let error = sqlite_plan_statement(query).unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("A query plan covers SELECT, INSERT, UPDATE"),
            "{query}: {error}"
        );
    }
}

#[test]
fn sqlite_plan_keeps_the_single_statement_guard() {
    assert_eq!(
        sqlite_plan_statement("SELECT 1; DELETE FROM t")
            .unwrap_err()
            .to_string(),
        "A query plan covers exactly one statement; the query has 2. Select a single statement."
    );
    assert_eq!(
        sqlite_plan_statement(" ").unwrap_err().to_string(),
        "A query plan needs a statement; the query is empty."
    );
}

fn timed(operation: &str, total: f64, loops: u64, children: Vec<PlanNode>) -> PlanNode {
    PlanNode {
        actual_total_time: Some(total),
        actual_loops: Some(loops),
        children,
        ..PlanNode::new(operation)
    }
}

fn counted(operation: &str, estimated: f64, actual: f64, children: Vec<PlanNode>) -> PlanNode {
    PlanNode {
        estimated_rows: Some(estimated),
        actual_rows: Some(actual),
        actual_loops: Some(1),
        children,
        ..PlanNode::new(operation)
    }
}

fn plan_of(root: PlanNode) -> QueryPlan {
    QueryPlan::new(vec![root], "", PlanFormat::Text)
}

#[test]
fn requests_estimate_by_default_and_analyze_on_demand() {
    assert_eq!(
        QueryPlanRequest::new("SELECT 1").mode,
        QueryPlanMode::Estimate
    );
    assert_eq!(
        QueryPlanRequest::analyzed("SELECT 1").mode,
        QueryPlanMode::Analyze
    );
    assert_eq!(QueryPlanMode::default(), QueryPlanMode::Estimate);
}

#[test]
fn actual_self_time_subtracts_children_and_scales_by_loops() {
    let node = timed(
        "Nested Loop",
        10.0,
        1,
        vec![
            timed("Seq Scan", 2.0, 3, vec![]),
            timed("Index", 1.0, 1, vec![]),
        ],
    );

    // 10 - (2 * 3 + 1 * 1) = 3
    assert_eq!(node.actual_self_time(), Some(3.0));
}

#[test]
fn actual_self_time_never_goes_below_zero() {
    let node = timed("Limit", 1.0, 1, vec![timed("Scan", 5.0, 1, vec![])]);

    assert_eq!(node.actual_self_time(), Some(0.0));
}

#[test]
fn actual_self_time_is_unknown_when_a_time_is_missing() {
    let child = PlanNode::new("Scan");
    let node = timed("Limit", 4.0, 1, vec![child]);

    assert_eq!(node.actual_self_time(), None);
    assert_eq!(PlanNode::new("Scan").actual_self_time(), None);
}

#[test]
fn actual_self_time_of_a_leaf_is_its_total_across_loops() {
    assert_eq!(timed("Scan", 2.5, 4, vec![]).actual_self_time(), Some(10.0));
    // Loops not reported count as one.
    let node = PlanNode {
        actual_total_time: Some(2.5),
        ..PlanNode::new("Scan")
    };
    assert_eq!(node.actual_self_time(), Some(2.5));
}

#[test]
fn a_node_that_never_ran_costs_nothing() {
    let node = timed("Join", 3.0, 1, vec![timed("Skipped", 0.0, 0, vec![])]);

    assert_eq!(node.actual_self_time(), Some(3.0));
}

#[test]
fn the_gap_marker_needs_a_tenfold_gap() {
    let nine = plan_of(counted("Scan", 100.0, 900.0, vec![]));
    assert!(nine.largest_estimate_gap().is_none());

    let ten = plan_of(counted("Scan", 100.0, 1000.0, vec![]));
    let gap = ten.largest_estimate_gap().expect("10x is marked");
    assert_eq!(gap.ratio, 10.0);
    assert_eq!(gap.node.operation, "Scan");
}

#[test]
fn the_gap_marker_works_in_both_directions() {
    let over = plan_of(counted("Scan", 5000.0, 50.0, vec![]));
    assert_eq!(over.largest_estimate_gap().unwrap().ratio, 100.0);

    let under = plan_of(counted("Scan", 50.0, 5000.0, vec![]));
    assert_eq!(under.largest_estimate_gap().unwrap().ratio, 100.0);
}

#[test]
fn the_gap_marker_picks_the_largest_gap_and_reports_its_path() {
    let root = counted(
        "Join",
        10.0,
        10.0,
        vec![
            counted("A", 1.0, 20.0, vec![]),
            counted("B", 100.0, 1.0, vec![counted("C", 1.0, 500.0, vec![])]),
        ],
    );

    let plan = plan_of(root);
    let gap = plan.largest_estimate_gap().unwrap();

    assert_eq!(gap.node.operation, "C");
    assert_eq!(gap.path, vec![0, 1, 0]);
    assert_eq!(gap.ratio, 500.0);
}

#[test]
fn the_gap_marker_prefers_the_deepest_node_on_ties() {
    let root = counted(
        "Parent",
        10.0,
        100.0,
        vec![counted("Child", 10.0, 100.0, vec![])],
    );

    let plan = plan_of(root);
    assert_eq!(plan.largest_estimate_gap().unwrap().node.operation, "Child");

    let siblings = plan_of(counted(
        "Parent",
        1.0,
        1.0,
        vec![
            counted("First", 10.0, 100.0, vec![]),
            counted("Second", 10.0, 100.0, vec![]),
        ],
    ));
    assert_eq!(
        siblings.largest_estimate_gap().unwrap().node.operation,
        "First"
    );
}

#[test]
fn the_gap_marker_ignores_nodes_without_both_numbers_or_that_never_ran() {
    let estimate_only = PlanNode {
        estimated_rows: Some(1.0),
        ..PlanNode::new("Scan")
    };
    assert!(plan_of(estimate_only).largest_estimate_gap().is_none());

    let mut never = counted("Scan", 1.0, 0.0, vec![]);
    never.actual_loops = Some(0);
    never.estimated_rows = Some(1000.0);
    assert!(plan_of(never).largest_estimate_gap().is_none());
}

#[test]
fn zero_rows_are_compared_as_one() {
    let plan = plan_of(counted("Scan", 1.0, 0.0, vec![]));
    assert!(plan.largest_estimate_gap().is_none());

    let plan = plan_of(counted("Scan", 500.0, 0.0, vec![]));
    assert_eq!(plan.largest_estimate_gap().unwrap().ratio, 500.0);
}

const REFUSAL: &str = "Analyze is only available for read statements; this statement is not a plain read, so only the estimate can run.";

#[test]
fn analyze_is_allowed_for_reads_and_metadata() {
    for statement in [
        "SELECT 1",
        "select * from t where id = 1",
        "WITH x AS (SELECT 1) SELECT * FROM x",
        "SHOW TABLES",
    ] {
        analyze_allowed(&QueryLanguage::Sql, statement)
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
}

#[test]
fn analyze_is_refused_for_anything_that_changes_data() {
    for statement in [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a = 1",
        "DELETE FROM t",
        "DROP TABLE t",
        "CREATE TABLE t (a int)",
        "TRUNCATE t",
        "WITH gone AS (DELETE FROM t RETURNING *) SELECT * FROM gone",
        "SELECT 1 INTO OUTFILE '/tmp/x'",
    ] {
        let error = analyze_allowed(&QueryLanguage::Sql, statement)
            .expect_err(&format!("{statement} must be refused"));
        assert_eq!(error.to_string(), REFUSAL, "{statement}");
    }
}

#[test]
fn analyze_is_refused_for_languages_without_a_read_verdict() {
    let error = analyze_allowed(&QueryLanguage::Custom("x".into()), "anything").unwrap_err();
    assert_eq!(error.to_string(), REFUSAL);
}

#[test]
fn analyze_statement_applies_the_single_statement_guard_then_the_read_guard() {
    assert_eq!(
        analyze_statement(&QueryLanguage::Sql, " SELECT 1; ").unwrap(),
        "SELECT 1"
    );

    let many = analyze_statement(&QueryLanguage::Sql, "SELECT 1; SELECT 2").unwrap_err();
    assert!(many.to_string().contains("exactly one statement"));

    let write = analyze_statement(&QueryLanguage::Sql, "DELETE FROM t").unwrap_err();
    assert_eq!(write.to_string(), REFUSAL);
}

#[test]
fn drivers_without_analyze_refuse_with_one_message() {
    match analyze_unsupported() {
        DbError::NotSupported(message) => {
            assert_eq!(message, "Analyzed plans are not supported by this driver");
        }
        other => panic!("expected NotSupported, got {other:?}"),
    }
}

#[test]
fn analyze_refuses_writes_hidden_by_comments_quotes_or_a_nested_explain() {
    for statement in [
        "SELECT 1 /*! ; DELETE FROM t */",
        "SELECT 1 --; DELETE FROM t",
        "SELECT $$ ; DELETE FROM t $$",
        "SELECT E'a\\'; DELETE FROM t; --'",
        "SELECT 1 /* /* */ ; DELETE FROM t */",
        "EXPLAIN ANALYZE DELETE FROM t",
        "EXPLAIN (ANALYZE) DELETE FROM t",
        "SELECT * FROM t FOR NO KEY UPDATE",
        "SELECT * FROM t LOCK IN SHARE MODE",
        "COPY (SELECT 1) TO '/tmp/x'",
        "EXECUTE stmt",
        "ANALYZE t",
        "SELECT 1 # ; DELETE FROM t",
    ] {
        let error = analyze_statement(&QueryLanguage::Sql, statement)
            .expect_err(&format!("{statement} must be refused"));
        // A `#` comment is MySQL-only, so the splitter may refuse the text
        // as two statements before the classifier sees it; either refusal
        // sends nothing.
        let message = error.to_string();
        assert!(
            message == REFUSAL || message.contains("exactly one statement"),
            "{statement}: {message}"
        );
    }

    assert_eq!(
        analyze_statement(&QueryLanguage::Sql, "SELECT 1 /* ; DELETE */").unwrap(),
        "SELECT 1 /* ; DELETE */"
    );
}
