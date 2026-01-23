use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Instant;

use dbflux_core::{
    ColumnInfo, ColumnMeta, Connection, ConnectionProfile, DbConfig, DbDriver, DbError, DbKind,
    DbSchemaInfo, IndexInfo, QueryHandle, QueryRequest, QueryResult, Row, SchemaSnapshot,
    TableInfo, Value, ViewInfo,
};
use rusqlite::Connection as RusqliteConnection;

pub struct SqliteDriver;

impl SqliteDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SqliteDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl DbDriver for SqliteDriver {
    fn kind(&self) -> DbKind {
        DbKind::SQLite
    }

    fn description(&self) -> &'static str {
        "File-based embedded database"
    }

    fn requires_password(&self) -> bool {
        false
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        _password: Option<&str>,
        _ssh_secret: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let path = match &profile.config {
            DbConfig::SQLite { path } => path.clone(),
            _ => {
                return Err(DbError::InvalidProfile(
                    "Expected SQLite configuration".to_string(),
                ));
            }
        };

        let conn = RusqliteConnection::open(&path)
            .map_err(|e| DbError::ConnectionFailed(e.to_string()))?;

        Ok(Box::new(SqliteConnection {
            conn: Mutex::new(conn),
            path,
        }))
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let path = match &profile.config {
            DbConfig::SQLite { path } => path.clone(),
            _ => {
                return Err(DbError::InvalidProfile(
                    "Expected SQLite configuration".to_string(),
                ));
            }
        };

        let conn = RusqliteConnection::open(&path)
            .map_err(|e| DbError::ConnectionFailed(e.to_string()))?;

        conn.execute_batch("SELECT 1")
            .map_err(|e| DbError::ConnectionFailed(e.to_string()))?;

        Ok(())
    }
}

pub struct SqliteConnection {
    conn: Mutex<RusqliteConnection>,
    #[allow(dead_code)]
    path: PathBuf,
}

impl Connection for SqliteConnection {
    fn ping(&self) -> Result<(), DbError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;
        conn.execute_batch("SELECT 1")
            .map_err(|e| DbError::QueryFailed(e.to_string()))
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        let start = Instant::now();
        let conn = self
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        let mut stmt = conn
            .prepare(&req.sql)
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        let column_count = stmt.column_count();
        let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let columns: Vec<ColumnMeta> = column_names
            .into_iter()
            .map(|name| ColumnMeta {
                name,
                type_name: "TEXT".to_string(),
                nullable: true,
            })
            .collect();

        let mut rows: Vec<Row> = Vec::new();
        let mut result_rows = stmt
            .query([])
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        while let Some(row) = result_rows
            .next()
            .map_err(|e| DbError::QueryFailed(e.to_string()))?
        {
            let mut values: Vec<Value> = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let value = sqlite_value_to_value(row, i);
                values.push(value);
            }
            rows.push(values);

            if let Some(limit) = req.limit
                && rows.len() >= limit as usize
            {
                break;
            }
        }

        Ok(QueryResult {
            columns,
            rows,
            affected_rows: None,
            execution_time: start.elapsed(),
        })
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "SQLite does not support query cancellation".to_string(),
        ))
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        let tables = self.get_tables(&conn)?;
        let views = self.get_views(&conn)?;

        let main_schema = DbSchemaInfo {
            name: "main".to_string(),
            tables,
            views,
        };

        Ok(SchemaSnapshot {
            databases: Vec::new(),
            current_database: None,
            schemas: vec![main_schema],
            tables: Vec::new(),
            views: Vec::new(),
        })
    }

    fn kind(&self) -> DbKind {
        DbKind::SQLite
    }
}

impl SqliteConnection {
    fn get_tables(&self, conn: &RusqliteConnection) -> Result<Vec<TableInfo>, DbError> {
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        let table_names: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| DbError::QueryFailed(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();

        let mut tables = Vec::new();
        for name in table_names {
            let columns = self.get_columns(conn, &name)?;
            let indexes = self.get_indexes(conn, &name)?;
            tables.push(TableInfo {
                name,
                schema: None,
                columns,
                indexes,
            });
        }

        Ok(tables)
    }

    fn get_columns(
        &self,
        conn: &RusqliteConnection,
        table: &str,
    ) -> Result<Vec<ColumnInfo>, DbError> {
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info('{}')", table))
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        let columns = stmt
            .query_map([], |row| {
                Ok(ColumnInfo {
                    name: row.get(1)?,
                    type_name: row.get::<_, String>(2).unwrap_or_default(),
                    nullable: row.get::<_, i32>(3).unwrap_or(1) == 0,
                    is_primary_key: row.get::<_, i32>(5).unwrap_or(0) == 1,
                    default_value: row.get::<_, Option<String>>(4).unwrap_or(None),
                })
            })
            .map_err(|e| DbError::QueryFailed(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(columns)
    }

    fn get_indexes(
        &self,
        conn: &RusqliteConnection,
        table: &str,
    ) -> Result<Vec<IndexInfo>, DbError> {
        let mut stmt = conn
            .prepare(&format!("PRAGMA index_list('{}')", table))
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        let index_list: Vec<(String, bool)> = stmt
            .query_map([], |row| Ok((row.get(1)?, row.get::<_, i32>(2)? == 1)))
            .map_err(|e| DbError::QueryFailed(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();

        let mut indexes = Vec::new();
        for (index_name, is_unique) in index_list {
            let mut col_stmt = conn
                .prepare(&format!("PRAGMA index_info('{}')", index_name))
                .map_err(|e| DbError::QueryFailed(e.to_string()))?;

            let columns: Vec<String> = col_stmt
                .query_map([], |row| row.get(2))
                .map_err(|e| DbError::QueryFailed(e.to_string()))?
                .filter_map(|r| r.ok())
                .collect();

            indexes.push(IndexInfo {
                name: index_name,
                columns,
                is_unique,
                is_primary: false,
            });
        }

        Ok(indexes)
    }

    fn get_views(&self, conn: &RusqliteConnection) -> Result<Vec<ViewInfo>, DbError> {
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name")
            .map_err(|e| DbError::QueryFailed(e.to_string()))?;

        let views = stmt
            .query_map([], |row| {
                Ok(ViewInfo {
                    name: row.get(0)?,
                    schema: None,
                })
            })
            .map_err(|e| DbError::QueryFailed(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(views)
    }
}

fn sqlite_value_to_value(row: &rusqlite::Row, idx: usize) -> Value {
    use rusqlite::types::ValueRef;

    match row.get_ref(idx) {
        Ok(ValueRef::Null) => Value::Null,
        Ok(ValueRef::Integer(i)) => Value::Int(i),
        Ok(ValueRef::Real(f)) => Value::Float(f),
        Ok(ValueRef::Text(t)) => Value::Text(String::from_utf8_lossy(t).to_string()),
        Ok(ValueRef::Blob(b)) => Value::Bytes(b.to_vec()),
        Err(_) => Value::Null,
    }
}
