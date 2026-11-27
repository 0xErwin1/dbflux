use super::*;
use dbflux_core::PlanFormat;

const JOIN_PLAN: &str = r#"<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.6" Build="16.0.4100.1"><BatchSequence><Batch><Statements><StmtSimple StatementText="SELECT * FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.status = 'open'" StatementSubTreeCost="0.0099" StatementEstRows="5"><QueryPlan CachedPlanSize="24"><MissingIndexes><MissingIndexGroup Impact="87.5"><MissingIndex Database="[shop]" Schema="[dbo]" Table="[orders]"><ColumnGroup Usage="EQUALITY"><Column Name="[status]" ColumnId="3"/></ColumnGroup><ColumnGroup Usage="INCLUDE"><Column Name="[customer_id]" ColumnId="2"/></ColumnGroup></MissingIndex></MissingIndexGroup></MissingIndexes><RelOp NodeId="0" PhysicalOp="Nested Loops" LogicalOp="Inner Join" EstimateRows="5" EstimateIO="0" EstimateCPU="4.18E-06" AvgRowSize="72" EstimatedTotalSubtreeCost="0.0099" Parallel="0" EstimatedExecutionMode="Row"><OutputList/><NestedLoops Optimized="0"><OuterReferences><ColumnReference Column="customer_id"/></OuterReferences><RelOp NodeId="1" PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" EstimateRows="5" EstimateIO="0.0031" EstimateCPU="0.0001" EstimatedTotalSubtreeCost="0.0032"><IndexScan Ordered="0"><Object Database="[shop]" Schema="[dbo]" Table="[orders]" Index="[PK_orders]" Alias="[o]" IndexKind="Clustered"/></IndexScan></RelOp><RelOp NodeId="2" PhysicalOp="Clustered Index Seek" LogicalOp="Clustered Index Seek" EstimateRows="1" EstimateIO="0.0031" EstimateCPU="0.0001" EstimatedTotalSubtreeCost="0.0032"><Warnings SpillOccurred="true" NoJoinPredicate="false"/><IndexScan Ordered="1"><Object Database="[shop]" Schema="[dbo]" Table="[customers]" Index="[PK_customers]" IndexKind="Clustered"/></IndexScan></RelOp></NestedLoops></RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#;

fn detail<'a>(node: &'a PlanNode, key: &str) -> Option<&'a str> {
    node.details
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}

#[test]
fn parses_a_seek_and_scan_join_tree() {
    let plan = parse_showplan_xml(JOIN_PLAN).unwrap();

    assert_eq!(plan.raw_format, PlanFormat::Xml);
    assert_eq!(plan.raw, JOIN_PLAN);
    assert_eq!(plan.roots.len(), 1);

    let join = &plan.roots[0];
    assert_eq!(join.operation, "Nested Loops");
    assert_eq!(join.estimated_total_cost, Some(0.0099));
    assert_eq!(join.estimated_rows, Some(5.0));
    assert_eq!(join.estimated_startup_cost, None);
    assert_eq!(detail(join, "Logical Operation"), Some("Inner Join"));
    assert_eq!(detail(join, "Estimate IO"), Some("0"));
    assert_eq!(detail(join, "Estimate CPU"), Some("4.18E-06"));
    assert_eq!(detail(join, "Execution Mode"), Some("Row"));
    assert_eq!(join.relation, None);
    assert_eq!(join.children.len(), 2);

    let scan = &join.children[0];
    assert_eq!(scan.operation, "Clustered Index Scan");
    assert_eq!(scan.relation.as_deref(), Some("dbo.orders"));
    assert_eq!(scan.index.as_deref(), Some("PK_orders"));
    assert_eq!(detail(scan, "Alias"), Some("o"));
    assert_eq!(detail(scan, "Index Kind"), Some("Clustered"));

    let seek = &join.children[1];
    assert_eq!(seek.operation, "Clustered Index Seek");
    assert_eq!(seek.relation.as_deref(), Some("dbo.customers"));
    assert_eq!(seek.index.as_deref(), Some("PK_customers"));
    assert!(seek.children.is_empty());
}

#[test]
fn collects_node_and_missing_index_warnings_as_text() {
    let plan = parse_showplan_xml(JOIN_PLAN).unwrap();

    assert_eq!(plan.roots[0].children[1].warnings, vec!["SpillOccurred"]);
    assert!(plan.roots[0].children[0].warnings.is_empty());
    assert_eq!(
        plan.warnings,
        vec![
            "Missing index on [shop].[dbo].[orders] (impact 87.5%): equality ([status]); include ([customer_id])"
        ]
    );
}

#[test]
fn collects_plan_level_warnings_with_their_attributes() {
    let xml = r#"<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple><QueryPlan><Warnings><PlanAffectingConvert ConvertIssue="Seek Plan" Expression="CONVERT_IMPLICIT(int,[t].[a],0)"/><ColumnsWithNoStatistics><ColumnReference Column="[a]"/></ColumnsWithNoStatistics></Warnings><RelOp PhysicalOp="Table Scan" LogicalOp="Table Scan"/></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#;
    let plan = parse_showplan_xml(xml).unwrap();

    assert_eq!(
        plan.warnings,
        vec![
            "PlanAffectingConvert (ConvertIssue=Seek Plan, Expression=CONVERT_IMPLICIT(int,[t].[a],0))",
            "ColumnsWithNoStatistics (Column=[a])",
        ]
    );
    assert!(plan.roots[0].warnings.is_empty());
}

#[test]
fn tolerates_missing_optional_attributes() {
    let xml = r#"<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple><QueryPlan><RelOp PhysicalOp="Constant Scan"><ConstantScan/></RelOp><RelOp LogicalOp="Compute Scalar" EstimateRows="not-a-number"/></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#;
    let plan = parse_showplan_xml(xml).unwrap();

    assert_eq!(plan.roots.len(), 2);
    let first = &plan.roots[0];
    assert_eq!(first.operation, "Constant Scan");
    assert_eq!(first.estimated_total_cost, None);
    assert_eq!(first.estimated_rows, None);
    assert!(first.details.is_empty());
    assert_eq!(first.relation, None);
    assert_eq!(first.index, None);

    let second = &plan.roots[1];
    assert_eq!(second.operation, "Compute Scalar");
    assert_eq!(second.estimated_rows, None);
    assert!(plan.warnings.is_empty());
}

#[test]
fn a_plan_without_operators_has_no_roots() {
    let plan = parse_showplan_xml(
        r#"<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple/></Statements></Batch></BatchSequence></ShowPlanXML>"#,
    )
    .unwrap();

    assert!(plan.roots.is_empty());
}

fn message(error: DbError) -> String {
    error.to_string()
}

#[test]
fn malformed_xml_is_a_clear_error() {
    for xml in [
        "",
        "not xml at all",
        "<ShowPlanXML><RelOp PhysicalOp=\"x\"></ShowPlanXML>",
        "<ShowPlanXML><RelOp PhysicalOp=\"x\">",
        "<Other/>",
        "<ShowPlanXML><RelOp LogicalOp=\"\"/><RelOp/></ShowPlanXML>",
    ] {
        let error = parse_showplan_xml(xml).expect_err(xml);
        assert!(
            message(error).contains("SHOWPLAN"),
            "message must name SHOWPLAN for {xml:?}"
        );
    }
}

#[test]
fn guard_accepts_one_statement_and_returns_it_trimmed() {
    assert_eq!(plan_statement("  SELECT 1;  ").unwrap(), "SELECT 1;");
    assert_eq!(
        plan_statement("-- note\nSELECT ';' AS [a;b]\nGO\n").unwrap(),
        "-- note\nSELECT ';' AS [a;b]"
    );
    assert_eq!(
        plan_statement("SELECT 1 /* ; GO */ -- ; ").unwrap(),
        "SELECT 1 /* ; GO */ -- ;"
    );
}

#[test]
fn guard_keeps_the_terminator_a_merge_needs() {
    let merge =
        "MERGE dbo.target AS t USING dbo.source AS s ON t.id = s.id WHEN MATCHED THEN DELETE;";

    assert_eq!(plan_statement(merge).unwrap(), merge);
    assert_eq!(plan_statement(&format!(" {merge} \n")).unwrap(), merge);
    assert_eq!(plan_statement("SELECT 'a;b';").unwrap(), "SELECT 'a;b';");
}

#[test]
fn guard_refuses_empty_queries_with_the_shared_message() {
    for query in ["", "   \n", ";", "-- only a comment", "GO", "/* x */ ;\nGO"] {
        assert_eq!(
            message(plan_statement(query).unwrap_err()),
            DbError::query_failed("A query plan needs a statement; the query is empty.")
                .to_string(),
            "{query:?}"
        );
    }
}

#[test]
fn guard_refuses_several_statements_with_the_shared_message() {
    for (query, count) in [
        ("SELECT 1; SELECT 2", 2),
        ("SELECT 1\nGO\nSELECT 2", 2),
        ("select 1\n  go  \nselect 2\ngo 3\nselect 3", 3),
        ("SELECT 1; SELECT 2;", 2),
    ] {
        assert_eq!(
            message(plan_statement(query).unwrap_err()),
            DbError::query_failed(format!(
                "A query plan covers exactly one statement; the query has {count}. Select a single statement."
            ))
            .to_string(),
            "{query:?}"
        );
    }
}

struct FakeSession {
    batches: Vec<String>,
    fail_on: Option<&'static str>,
    xml: Option<String>,
}

impl FakeSession {
    fn new(fail_on: Option<&'static str>) -> Self {
        Self {
            batches: Vec::new(),
            fail_on,
            xml: Some("<ShowPlanXML/>".to_string()),
        }
    }
}

impl ShowplanSession for FakeSession {
    async fn batch(&mut self, sql: &str) -> Result<Option<String>, DbError> {
        self.batches.push(sql.to_string());

        if self.fail_on == Some(sql) {
            return Err(DbError::query_failed(format!("failed: {sql}")));
        }

        Ok(if sql == "SELECT 1" {
            self.xml.clone()
        } else {
            None
        })
    }
}

fn run(session: &mut FakeSession) -> ShowplanRun {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(capture_showplan(session, "SELECT 1"))
}

const ON: &str = "SET SHOWPLAN_XML ON";
const OFF: &str = "SET SHOWPLAN_XML OFF";

#[test]
fn capture_runs_on_statement_off_in_order() {
    let mut session = FakeSession::new(None);
    let run = run(&mut session);

    assert_eq!(session.batches, [ON, "SELECT 1", OFF]);
    assert_eq!(run.plan.unwrap(), "<ShowPlanXML/>");
    assert!(run.restore.is_ok());
}

#[test]
fn capture_turns_showplan_off_after_a_statement_error() {
    let mut session = FakeSession::new(Some("SELECT 1"));
    let run = run(&mut session);

    assert_eq!(session.batches, [ON, "SELECT 1", OFF]);
    assert!(message(run.plan.unwrap_err()).contains("failed: SELECT 1"));
    assert!(run.restore.is_ok());
}

#[test]
fn capture_turns_showplan_off_after_a_failed_on_without_running_the_statement() {
    let mut session = FakeSession::new(Some(ON));
    let run = run(&mut session);

    assert_eq!(session.batches, [ON, OFF]);
    assert!(run.plan.is_err());
    assert!(run.restore.is_ok());
}

#[test]
fn capture_turns_showplan_off_when_the_statement_returns_no_plan() {
    let mut session = FakeSession::new(None);
    session.xml = None;
    let run = run(&mut session);

    assert_eq!(session.batches, [ON, "SELECT 1", OFF]);
    assert!(message(run.plan.unwrap_err()).contains("no plan"));
    assert!(run.restore.is_ok());
}

#[test]
fn capture_reports_a_failed_off_separately_from_the_plan() {
    let mut session = FakeSession::new(Some(OFF));
    let run = run(&mut session);

    assert_eq!(run.plan.unwrap(), "<ShowPlanXML/>");
    assert!(message(run.restore.unwrap_err()).contains(OFF));
}

const TWO_STATEMENTS: &str = r#"<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple StatementText="SELECT 1 AS a"><QueryPlan><RelOp PhysicalOp="Constant Scan"/></QueryPlan></StmtSimple><StmtSimple StatementText="SELECT 2 AS b"><QueryPlan><RelOp PhysicalOp="Constant Scan"/></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#;

#[test]
fn a_plan_with_several_statements_is_refused() {
    let error = parse_showplan_xml(TWO_STATEMENTS).expect_err("two statements");

    assert_eq!(
        message(error),
        DbError::query_failed(
            "A query plan covers exactly one statement; the query has 2. Select a single statement."
        )
        .to_string()
    );
}

#[test]
fn an_if_block_with_nested_statements_counts_as_one() {
    let xml = r#"<ShowPlanXML><BatchSequence><Batch><Statements><StmtCond StatementText="IF 1=1 BEGIN SELECT 1 SELECT 2 END"><Condition/><Then><Statements><StmtSimple><QueryPlan><RelOp PhysicalOp="Constant Scan"/></QueryPlan></StmtSimple><StmtSimple><QueryPlan><RelOp PhysicalOp="Constant Scan"/></QueryPlan></StmtSimple></Statements></Then></StmtCond></Statements></Batch></BatchSequence></ShowPlanXML>"#;
    let plan = parse_showplan_xml(xml).unwrap();

    assert_eq!(plan.roots.len(), 2);
}

struct PanickingSession;

impl ShowplanSession for PanickingSession {
    async fn batch(&mut self, sql: &str) -> Result<Option<String>, DbError> {
        if sql == "SELECT 1" {
            panic!("boom");
        }
        Ok(None)
    }
}

#[test]
fn the_restore_guard_fires_when_the_capture_unwinds() {
    let fired = std::cell::Cell::new(false);

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = RestoreGuard::new(|| fired.set(true));
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(capture_showplan(&mut PanickingSession, "SELECT 1"))
    }));

    assert!(outcome.is_err());
    assert!(fired.get());
}

#[test]
fn a_disarmed_restore_guard_does_nothing() {
    let fired = std::cell::Cell::new(false);
    let mut guard = RestoreGuard::new(|| fired.set(true));
    guard.disarm();
    drop(guard);

    assert!(!fired.get());
}

const SEEK_PLAN: &str = r#"<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.6"><BatchSequence><Batch><Statements><StmtSimple><QueryPlan><RelOp NodeId="0" PhysicalOp="Filter" LogicalOp="Filter" EstimateRows="1" EstimatedTotalSubtreeCost="0.01"><Filter><Predicate><ScalarOperator ScalarString="[shop].[dbo].[orders].[total]&gt;(10)"><Compare CompareOp="GT"><ScalarOperator><Identifier/></ScalarOperator></Compare></ScalarOperator></Predicate><RelOp NodeId="1" PhysicalOp="Clustered Index Seek" LogicalOp="Clustered Index Seek" EstimateRows="1" EstimatedTotalSubtreeCost="0.003"><IndexScan Ordered="1"><Object Database="[shop]" Schema="[dbo]" Table="[orders]" Index="[PK_orders]" Alias="[o]" IndexKind="Clustered"/><SeekPredicates><SeekPredicateNew><SeekKeys><Prefix ScanType="EQ"><RangeColumns><ColumnReference Database="[shop]" Schema="[dbo]" Table="[orders]" Alias="[o]" Column="id"/></RangeColumns><RangeExpressions><ScalarOperator ScalarString="(1)"><Const ConstValue="(1)"/></ScalarOperator></RangeExpressions></Prefix></SeekKeys></SeekPredicateNew></SeekPredicates><Predicate><ScalarOperator ScalarString="[o].[status]='open'"/></Predicate></IndexScan></RelOp></Filter></RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#;

const UNKNOWN_PLAN: &str = r#"<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><BatchSequence><Batch><Statements><StmtSimple><QueryPlan><RelOp NodeId="0" PhysicalOp="Teleport" LogicalOp="Teleport" EstimateRows="1" EstimatedTotalSubtreeCost="0.01"/></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#;

fn condition(node: &PlanNode) -> Option<(&str, &str)> {
    node.key_condition
        .as_ref()
        .map(|condition| (condition.label.as_str(), condition.value.as_str()))
}

#[test]
fn a_seek_node_gets_a_description_and_its_seek_predicate() {
    let plan = parse_showplan_xml(SEEK_PLAN).unwrap();
    let seek = &plan.roots[0].children[0];

    assert_eq!(seek.operation, "Clustered Index Seek");
    assert_eq!(
        seek.description.as_deref(),
        Some(
            "Finds the matching rows through the clustered index instead of reading the whole table."
        )
    );
    assert_eq!(condition(seek), Some(("Seek Predicates", "[o].[id] = (1)")));
}

#[test]
fn a_predicate_is_the_key_condition_when_there_is_no_seek() {
    let plan = parse_showplan_xml(SEEK_PLAN).unwrap();
    let filter = &plan.roots[0];

    assert!(filter.description.is_some());
    assert_eq!(
        condition(filter),
        Some(("Predicate", "[shop].[dbo].[orders].[total]>(10)"))
    );
}

#[test]
fn a_node_without_predicates_has_no_key_condition() {
    let plan = parse_showplan_xml(JOIN_PLAN).unwrap();

    assert_eq!(plan.roots[0].key_condition, None);
    assert_eq!(plan.roots[0].children[0].key_condition, None);
    assert_eq!(
        plan.roots[0].description.as_deref(),
        Some("For each row from the first input, looks up the matching rows in the second.")
    );
}

#[test]
fn an_unknown_operation_has_no_description() {
    let plan = parse_showplan_xml(UNKNOWN_PLAN).unwrap();

    assert_eq!(plan.roots[0].description, None);
    assert_eq!(plan.roots[0].key_condition, None);
}

#[test]
fn every_listed_operation_has_a_description() {
    for operation in [
        "Clustered Index Seek",
        "Clustered Index Scan",
        "Index Seek",
        "Index Scan",
        "Table Scan",
        "Nested Loops",
        "Hash Match",
        "Merge Join",
        "Sort",
        "Stream Aggregate",
        "Top",
        "Compute Scalar",
        "Key Lookup",
        "RID Lookup",
        "Filter",
        "Parallelism",
    ] {
        let xml = format!(
            r#"<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple><QueryPlan><RelOp NodeId="0" PhysicalOp="{operation}" LogicalOp="{operation}"/></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#
        );
        let plan = parse_showplan_xml(&xml).unwrap();

        assert!(
            plan.roots[0].description.is_some(),
            "{operation} has no description"
        );
    }
}
