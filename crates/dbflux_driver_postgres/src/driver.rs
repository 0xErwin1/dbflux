use std::sync::Mutex;
use std::time::Instant;

use dbflux_core::{
    ColumnInfo, ColumnMeta, Connection, ConnectionProfile, DatabaseInfo, DbConfig, DbDriver,
    DbError, DbKind, DbSchemaInfo, IndexInfo, QueryHandle, QueryRequest, QueryResult, Row,
    SchemaSnapshot, SslMode, TableInfo, Value, ViewInfo,
};
use native_tls::TlsConnector;
use postgres::{Client, NoTls};
use postgres_native_tls::MakeTlsConnector;

pub struct PostgresDriver;

impl PostgresDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PostgresDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl DbDriver for PostgresDriver {
    fn kind(&self) -> DbKind {
        DbKind::Postgres
    }

    fn description(&self) -> &'static str {
        "Advanced open source relational database"
    }

    fn connect_with_password(
        &self,
        profile: &ConnectionProfile,
        password: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let (host, port, user, database, ssl_mode) = match &profile.config {
            DbConfig::Postgres {
                host,
                port,
                user,
                database,
                ssl_mode,
                ..
            } => (host, *port, user, database, *ssl_mode),
            _ => {
                return Err(DbError::InvalidProfile(
                    "Expected PostgreSQL configuration".to_string(),
                ));
            }
        };

        let password = password.unwrap_or("");
        let conn_string = format!(
            "host={} port={} user={} password={} dbname={}",
            host, port, user, password, database
        );

        let client = match ssl_mode {
            SslMode::Disable => Client::connect(&conn_string, NoTls)
                .map_err(|e| DbError::ConnectionFailed(format!("{:?}", e)))?,
            SslMode::Prefer | SslMode::Require => {
                let connector = TlsConnector::builder()
                    .danger_accept_invalid_certs(ssl_mode == SslMode::Prefer)
                    .build()
                    .map_err(|e| DbError::ConnectionFailed(format!("TLS error: {:?}", e)))?;
                let tls = MakeTlsConnector::new(connector);

                match Client::connect(&conn_string, tls) {
                    Ok(c) => c,
                    Err(e) if ssl_mode == SslMode::Prefer => {
                        Client::connect(&conn_string, NoTls)
                            .map_err(|e| DbError::ConnectionFailed(format!("{:?}", e)))?
                    }
                    Err(e) => return Err(DbError::ConnectionFailed(format!("{:?}", e))),
                }
            }
        };

        Ok(Box::new(PostgresConnection {
            client: Mutex::new(client),
        }))
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let conn = self.connect_with_password(profile, None)?;
        conn.ping()
    }
}

pub struct PostgresConnection {
    client: Mutex<Client>,
}

impl Connection for PostgresConnection {
    fn ping(&self) -> Result<(), DbError> {
        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;
        client
            .simple_query("SELECT 1")
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        let start = Instant::now();

        // Execute query with lock, release immediately after
        let rows = {
            let mut client = self
                .client
                .lock()
                .map_err(|e| DbError::QueryFailed(e.to_string()))?;

            client
                .query(&req.sql, &[])
                .map_err(|e| DbError::QueryFailed(e.to_string()))?
        };

        // Process results without holding lock
        if rows.is_empty() {
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                affected_rows: None,
                execution_time: start.elapsed(),
            });
        }

        let columns: Vec<ColumnMeta> = rows[0]
            .columns()
            .iter()
            .map(|col| ColumnMeta {
                name: col.name().to_string(),
                type_name: col.type_().name().to_string(),
                nullable: true,
            })
            .collect();

        let result_rows: Vec<Row> = rows
            .iter()
            .take(req.limit.unwrap_or(u32::MAX) as usize)
            .map(|row| {
                (0..columns.len())
                    .map(|i| postgres_value_to_value(row, i))
                    .collect()
            })
            .collect();

        Ok(QueryResult {
            columns,
            rows: result_rows,
            affected_rows: None,
            execution_time: start.elapsed(),
        })
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "Query cancellation not yet implemented".to_string(),
        ))
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        let databases = get_databases(&mut client)?;
        let current_database = get_current_database(&mut client)?;
        let schemas = get_schemas(&mut client)?;

        Ok(SchemaSnapshot {
            databases,
            current_database,
            schemas,
            tables: Vec::new(),
            views: Vec::new(),
        })
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        let mut client = self
            .client
            .lock()
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        get_databases(&mut client)
    }

    fn kind(&self) -> DbKind {
        DbKind::Postgres
    }
}

fn get_databases(client: &mut Client) -> Result<Vec<DatabaseInfo>, DbError> {
    let current = get_current_database(client)?;

    let rows = client
        .query(
            r#"
            SELECT datname
            FROM pg_database
            WHERE datistemplate = false
            ORDER BY datname
            "#,
            &[],
        )
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

    Ok(rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let is_current = current.as_ref() == Some(&name);
            DatabaseInfo { name, is_current }
        })
        .collect())
}

fn get_current_database(client: &mut Client) -> Result<Option<String>, DbError> {
    let rows = client
        .query("SELECT current_database()", &[])
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

    Ok(rows.first().map(|row| row.get(0)))
}

fn get_schemas(client: &mut Client) -> Result<Vec<DbSchemaInfo>, DbError> {
    let schema_rows = client
        .query(
            r#"
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY schema_name
            "#,
            &[],
        )
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

    let mut schemas = Vec::new();

    for row in schema_rows {
        let schema_name: String = row.get(0);
        let tables = get_tables_for_schema(client, &schema_name)?;
        let views = get_views_for_schema(client, &schema_name)?;

        schemas.push(DbSchemaInfo {
            name: schema_name,
            tables,
            views,
        });
    }

    Ok(schemas)
}

fn get_tables_for_schema(client: &mut Client, schema: &str) -> Result<Vec<TableInfo>, DbError> {
    let rows = client
        .query(
            r#"
            SELECT table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema = $1
            ORDER BY table_name
            "#,
            &[&schema],
        )
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

    let mut tables = Vec::new();
    for row in rows {
        let name: String = row.get(0);
        let columns = get_columns(client, schema, &name)?;
        let indexes = get_indexes(client, schema, &name)?;

        tables.push(TableInfo {
            name,
            schema: Some(schema.to_string()),
            columns,
            indexes,
        });
    }

    Ok(tables)
}

fn get_views_for_schema(client: &mut Client, schema: &str) -> Result<Vec<ViewInfo>, DbError> {
    let rows = client
        .query(
            r#"
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = $1
            ORDER BY table_name
            "#,
            &[&schema],
        )
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

    Ok(rows
        .iter()
        .map(|row| ViewInfo {
            name: row.get(0),
            schema: Some(schema.to_string()),
        })
        .collect())
}

fn get_columns(client: &mut Client, schema: &str, table: &str) -> Result<Vec<ColumnInfo>, DbError> {
    let rows = client
        .query(
            r#"
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable = 'YES' as nullable,
                c.column_default,
                COALESCE(
                    (SELECT true FROM information_schema.table_constraints tc
                     JOIN information_schema.key_column_usage kcu
                       ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                     WHERE tc.constraint_type = 'PRIMARY KEY'
                       AND tc.table_schema = c.table_schema
                       AND tc.table_name = c.table_name
                       AND kcu.column_name = c.column_name),
                    false
                ) as is_pk
            FROM information_schema.columns c
            WHERE c.table_schema = $1 AND c.table_name = $2
            ORDER BY c.ordinal_position
            "#,
            &[&schema, &table],
        )
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

    Ok(rows
        .iter()
        .map(|row| ColumnInfo {
            name: row.get(0),
            type_name: row.get(1),
            nullable: row.get(2),
            default_value: row.get(3),
            is_primary_key: row.get(4),
        })
        .collect())
}

fn get_indexes(client: &mut Client, schema: &str, table: &str) -> Result<Vec<IndexInfo>, DbError> {
    let rows = client
        .query(
            r#"
            SELECT
                i.relname as index_name,
                array_agg(a.attname ORDER BY k.n) as columns,
                ix.indisunique as is_unique,
                ix.indisprimary as is_primary
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, n) ON true
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
            WHERE n.nspname = $1 AND t.relname = $2
            GROUP BY i.relname, ix.indisunique, ix.indisprimary
            ORDER BY i.relname
            "#,
            &[&schema, &table],
        )
        .map_err(|e| DbError::QueryFailed(e.to_string()))?;

    Ok(rows
        .iter()
        .map(|row| {
            let columns: Vec<String> = row.get(1);
            IndexInfo {
                name: row.get(0),
                columns,
                is_unique: row.get(2),
                is_primary: row.get(3),
            }
        })
        .collect())
}

fn postgres_value_to_value(row: &postgres::Row, idx: usize) -> Value {
    let col_type = row.columns()[idx].type_();

    match col_type.name() {
        "bool" => row
            .try_get::<_, bool>(idx)
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        "int2" => row
            .try_get::<_, i16>(idx)
            .map(|v| Value::Int(v as i64))
            .unwrap_or(Value::Null),
        "int4" => row
            .try_get::<_, i32>(idx)
            .map(|v| Value::Int(v as i64))
            .unwrap_or(Value::Null),
        "int8" => row
            .try_get::<_, i64>(idx)
            .map(Value::Int)
            .unwrap_or(Value::Null),
        "float4" => row
            .try_get::<_, f32>(idx)
            .map(|v| Value::Float(v as f64))
            .unwrap_or(Value::Null),
        "float8" | "numeric" => row
            .try_get::<_, f64>(idx)
            .map(Value::Float)
            .unwrap_or(Value::Null),
        "bytea" => row
            .try_get::<_, Vec<u8>>(idx)
            .map(Value::Bytes)
            .unwrap_or(Value::Null),
        _ => row
            .try_get::<_, String>(idx)
            .map(Value::Text)
            .unwrap_or(Value::Null),
    }
}
