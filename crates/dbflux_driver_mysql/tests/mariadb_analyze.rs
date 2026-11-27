//! Analyzed plans against a real MariaDB server.
//!
//! The shared container helpers start MySQL only, so this suite runs against a
//! server the caller provides: set `DBFLUX_TEST_MARIADB_URL` (for example
//! `mysql://root:root@127.0.0.1:3306/testdb`) and run with `--run-ignored all`.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbKind, PlanFormat, QueryPlanRequest, QueryRequest,
    ReadOnlyEnforcement, Value,
};
use dbflux_driver_mysql::MysqlDriver;

#[test]
#[ignore = "requires a MariaDB server in DBFLUX_TEST_MARIADB_URL"]
fn mariadb_analyzed_plan_has_actuals_and_never_writes() {
    let Ok(uri) = std::env::var("DBFLUX_TEST_MARIADB_URL") else {
        eprintln!("skipped: DBFLUX_TEST_MARIADB_URL is not set");
        return;
    };

    let profile = ConnectionProfile::new(
        "live-mariadb",
        DbConfig::MySQL {
            use_uri: true,
            uri: Some(uri),
            host: String::new(),
            port: 3306,
            user: String::new(),
            database: None,
            ssl_mode: None,
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    );
    let connection = MysqlDriver::new(DbKind::MariaDB).connect(&profile).unwrap();

    connection
        .execute(&QueryRequest::new("DROP TABLE IF EXISTS analyze_live"))
        .unwrap();
    connection
        .execute(&QueryRequest::new(
            "CREATE TABLE analyze_live (id INT PRIMARY KEY, k INT, KEY (k))",
        ))
        .unwrap();
    connection
        .execute(&QueryRequest::new(
            "INSERT INTO analyze_live SELECT seq, seq % 5 FROM seq_1_to_200",
        ))
        .unwrap();

    let plan = connection
        .query_plan(&QueryPlanRequest::analyzed(
            "SELECT * FROM analyze_live WHERE k = 3",
        ))
        .unwrap();
    assert_eq!(plan.raw_format, PlanFormat::Json);
    assert!(plan.planning_time.is_some());
    assert!(plan.execution_time.is_some());

    let table = &plan.roots[0].children[0];
    assert_eq!(table.relation.as_deref(), Some("analyze_live"));
    assert_eq!(table.actual_rows, Some(40.0));
    assert_eq!(table.actual_loops, Some(1));
    assert!(table.actual_total_time.is_some());

    let refused = connection
        .query_plan(&QueryPlanRequest::analyzed("DELETE FROM analyze_live"))
        .unwrap_err();
    assert!(refused.to_string().starts_with("Analyze is only available"));

    // Even a write that got past the classifier is refused by the server.
    let mut sneaky = QueryRequest::new("ANALYZE FORMAT=JSON DELETE FROM analyze_live WHERE id = 1");
    sneaky.read_only = ReadOnlyEnforcement::Required;
    assert!(connection.execute(&sneaky).is_err());

    let count = connection
        .execute(&QueryRequest::new("SELECT count(*) FROM analyze_live"))
        .unwrap();
    assert_eq!(count.rows[0][0], Value::Int(200));

    connection
        .execute(&QueryRequest::new("DROP TABLE analyze_live"))
        .unwrap();
}
