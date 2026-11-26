use super::*;
use dbflux_core::PlanFormat;

const MYSQL_8_JOIN: &str = r#"{
  "query_block": {
    "select_id": 1,
    "cost_info": {"query_cost": "2.35"},
    "nested_loop": [
      {
        "table": {
          "table_name": "o",
          "access_type": "ALL",
          "possible_keys": ["customer_id"],
          "rows_examined_per_scan": 5,
          "rows_produced_per_join": 5,
          "filtered": "100.00",
          "cost_info": {
            "read_cost": "0.75",
            "eval_cost": "0.50",
            "prefix_cost": "1.25",
            "data_read_per_join": "160"
          },
          "used_columns": ["id", "customer_id"]
        }
      },
      {
        "table": {
          "table_name": "c",
          "access_type": "eq_ref",
          "possible_keys": ["PRIMARY"],
          "key": "PRIMARY",
          "used_key_parts": ["id"],
          "key_length": "4",
          "ref": ["shop.o.customer_id"],
          "rows_examined_per_scan": 1,
          "rows_produced_per_join": 5,
          "filtered": "100.00",
          "cost_info": {
            "read_cost": "0.60",
            "eval_cost": "0.50",
            "prefix_cost": "2.35",
            "data_read_per_join": "1K"
          },
          "used_columns": ["id", "name"]
        }
      }
    ]
  }
}"#;

const MYSQL_8_SORT: &str = r#"{
  "query_block": {
    "select_id": 1,
    "cost_info": {"query_cost": "0.35"},
    "ordering_operation": {
      "using_filesort": true,
      "table": {
        "table_name": "t",
        "access_type": "ref",
        "key": "idx_a",
        "rows_examined_per_scan": 3,
        "cost_info": {"read_cost": "0.25", "eval_cost": "0.10", "prefix_cost": "0.35"}
      }
    }
  }
}"#;

const MYSQL_8_DELETE: &str = r#"{
  "query_block": {
    "select_id": 1,
    "table": {
      "delete": true,
      "table_name": "t",
      "access_type": "range",
      "key": "PRIMARY",
      "rows_examined_per_scan": 1
    }
  }
}"#;

const MARIADB_JOIN: &str = r#"{
  "query_block": {
    "select_id": 1,
    "cost": 0.0125,
    "nested_loop": [
      {
        "table": {
          "table_name": "o",
          "access_type": "ALL",
          "loops": 1,
          "rows": 5,
          "cost": 0.0043,
          "filtered": 100,
          "attached_condition": "o.status = 'open'"
        }
      },
      {
        "table": {
          "table_name": "c",
          "access_type": "eq_ref",
          "possible_keys": ["PRIMARY"],
          "key": "PRIMARY",
          "key_length": "4",
          "used_key_parts": ["id"],
          "ref": ["shop.o.customer_id"],
          "loops": 5,
          "rows": 1,
          "cost": 0.0082,
          "filtered": 100
        }
      }
    ]
  }
}"#;

const MARIADB_SORT: &str = r#"{
  "query_block": {
    "select_id": 1,
    "read_sorted_file": {
      "filesort": {
        "sort_key": "t.b",
        "table": {
          "table_name": "t",
          "access_type": "ALL",
          "rows": 10,
          "filtered": 100
        }
      }
    }
  }
}"#;

fn detail<'a>(node: &'a dbflux_core::PlanNode, key: &str) -> Option<&'a str> {
    node.details
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}

#[test]
fn mysql_8_nested_loop_maps_tables_access_keys_rows_and_costs() {
    let plan = parse_explain_json(MYSQL_8_JOIN).unwrap();

    assert_eq!(plan.raw_format, PlanFormat::Json);
    assert_eq!(plan.raw, MYSQL_8_JOIN);
    assert_eq!(plan.roots.len(), 1);

    let block = &plan.roots[0];
    assert_eq!(block.operation, "Query block");
    assert_eq!(block.estimated_total_cost, Some(2.35));
    assert_eq!(detail(block, "select_id"), Some("1"));

    let loop_node = &block.children[0];
    assert_eq!(loop_node.operation, "Nested loop");
    assert_eq!(loop_node.children.len(), 2);

    let first = &loop_node.children[0];
    assert_eq!(first.operation, "ALL");
    assert_eq!(first.relation.as_deref(), Some("o"));
    assert_eq!(first.index, None);
    assert_eq!(first.estimated_rows, Some(5.0));
    assert_eq!(first.estimated_total_cost, Some(1.25));
    assert_eq!(detail(first, "read_cost"), Some("0.75"));
    assert_eq!(detail(first, "possible_keys"), Some("customer_id"));
    assert_eq!(detail(first, "access_type"), None);
    assert_eq!(detail(first, "table_name"), None);

    let second = &loop_node.children[1];
    assert_eq!(second.operation, "eq_ref");
    assert_eq!(second.relation.as_deref(), Some("c"));
    assert_eq!(second.index.as_deref(), Some("PRIMARY"));
    assert_eq!(second.estimated_total_cost, Some(2.35));
    assert_eq!(detail(second, "used_key_parts"), Some("id"));
}

#[test]
fn mysql_8_operations_wrap_their_table() {
    let plan = parse_explain_json(MYSQL_8_SORT).unwrap();

    let ordering = &plan.roots[0].children[0];
    assert_eq!(ordering.operation, "Ordering operation");
    assert_eq!(detail(ordering, "using_filesort"), Some("true"));
    assert_eq!(ordering.children[0].index.as_deref(), Some("idx_a"));
    assert_eq!(ordering.children[0].estimated_rows, Some(3.0));
}

#[test]
fn mysql_8_delete_plans_a_table_without_a_nested_loop() {
    let plan = parse_explain_json(MYSQL_8_DELETE).unwrap();

    let table = &plan.roots[0].children[0];
    assert_eq!(table.operation, "range");
    assert_eq!(table.relation.as_deref(), Some("t"));
    assert_eq!(table.index.as_deref(), Some("PRIMARY"));
    assert_eq!(detail(table, "delete"), Some("true"));
}

#[test]
fn a_single_table_nested_loop_has_no_wrapper() {
    let plan = parse_explain_json(
        r#"{"query_block":{"select_id":1,"nested_loop":[{"table":{"table_name":"t","access_type":"ALL"}}]}}"#,
    )
    .unwrap();

    assert_eq!(plan.roots[0].children.len(), 1);
    assert_eq!(plan.roots[0].children[0].relation.as_deref(), Some("t"));
}

#[test]
fn mariadb_maps_rows_and_numeric_costs() {
    let plan = parse_explain_json(MARIADB_JOIN).unwrap();

    let block = &plan.roots[0];
    assert_eq!(block.estimated_total_cost, Some(0.0125));

    let loop_node = &block.children[0];
    let first = &loop_node.children[0];
    assert_eq!(first.operation, "ALL");
    assert_eq!(first.relation.as_deref(), Some("o"));
    assert_eq!(first.estimated_rows, Some(5.0));
    assert_eq!(first.estimated_total_cost, Some(0.0043));
    assert_eq!(
        detail(first, "attached_condition"),
        Some("o.status = 'open'")
    );
    assert_eq!(detail(first, "loops"), Some("1"));

    let second = &loop_node.children[1];
    assert_eq!(second.index.as_deref(), Some("PRIMARY"));
    assert_eq!(second.estimated_rows, Some(1.0));
}

#[test]
fn mariadb_filesort_wraps_its_table() {
    let plan = parse_explain_json(MARIADB_SORT).unwrap();

    let sorted = &plan.roots[0].children[0];
    assert_eq!(sorted.operation, "Read sorted file");
    let filesort = &sorted.children[0];
    assert_eq!(filesort.operation, "Filesort");
    assert_eq!(detail(filesort, "sort_key"), Some("t.b"));
    assert_eq!(filesort.children[0].relation.as_deref(), Some("t"));
}

#[test]
fn a_query_without_tables_keeps_its_message() {
    let plan = parse_explain_json(r#"{"query_block":{"select_id":1,"message":"No tables used"}}"#)
        .unwrap();

    assert!(plan.roots[0].children.is_empty());
    assert_eq!(detail(&plan.roots[0], "message"), Some("No tables used"));
}

#[test]
fn unions_keep_every_query_specification() {
    let plan = parse_explain_json(
        r#"{"query_block":{"union_result":{"using_temporary_table":true,"query_specifications":[
            {"dependent":false,"query_block":{"select_id":1,"table":{"table_name":"a","access_type":"ALL"}}},
            {"dependent":false,"query_block":{"select_id":2,"table":{"table_name":"b","access_type":"ALL"}}}
        ]}}}"#,
    )
    .unwrap();

    let union = &plan.roots[0].children[0];
    assert_eq!(union.operation, "Union result");
    assert_eq!(union.children.len(), 2);
    assert_eq!(
        union.children[1].children[0].children[0]
            .relation
            .as_deref(),
        Some("b")
    );
}

#[test]
fn invalid_or_unexpected_json_is_an_error() {
    assert!(parse_explain_json("not json").is_err());
    assert!(parse_explain_json("[]").is_err());
    assert!(parse_explain_json(r#"{"other":{}}"#).is_err());
}

#[test]
fn explain_statement_wraps_one_statement_without_analyze() {
    assert_eq!(
        explain_statement(" DELETE FROM t WHERE id = 1; ").unwrap(),
        "EXPLAIN FORMAT=JSON DELETE FROM t WHERE id = 1"
    );
}

#[test]
fn several_statements_and_empty_queries_are_refused() {
    assert_eq!(
        explain_statement("SELECT 1; SELECT 2")
            .unwrap_err()
            .to_string(),
        "A query plan covers exactly one statement; the query has 2. Select a single statement."
    );
    assert_eq!(
        explain_statement(" ").unwrap_err().to_string(),
        "A query plan needs a statement; the query is empty."
    );
}
