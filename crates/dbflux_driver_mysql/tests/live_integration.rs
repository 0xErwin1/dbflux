use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbKind, QueryRequest, SchemaLoadingStrategy, SslMode,
};
use dbflux_driver_mysql::MysqlDriver;
use dbflux_test_support::containers;

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_live_connect_ping_query_and_schema() -> Result<(), dbflux_core::DbError> {
    containers::with_mysql_url(|uri| {
        let driver = MysqlDriver::new(DbKind::MySQL);
        let profile = ConnectionProfile::new(
            "live-mysql",
            DbConfig::MySQL {
                use_uri: true,
                uri: Some(uri),
                host: String::new(),
                port: 3306,
                user: String::new(),
                database: None,
                ssl_mode: SslMode::Disable,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
        );

        let connection =
            containers::retry_db_operation(std::time::Duration::from_secs(30), || {
                let connection = driver.connect(&profile)?;
                connection.ping()?;
                Ok(connection)
            })?;

        let result = connection.execute(&QueryRequest::new("SELECT 1 AS one"))?;
        assert_eq!(result.rows.len(), 1);

        assert_eq!(
            connection.schema_loading_strategy(),
            SchemaLoadingStrategy::LazyPerDatabase
        );

        let databases = connection.list_databases()?;
        assert!(!databases.is_empty());

        let schema = connection.schema()?;
        let _ = schema.databases();

        Ok(())
    })
}
