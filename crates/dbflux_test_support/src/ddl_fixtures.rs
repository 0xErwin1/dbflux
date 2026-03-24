use dbflux_core::{DbError, QueryRequest};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct DdlTestTable {
    pub name: String,
    pub create_sql: String,
}

#[derive(Debug, Clone)]
pub struct DdlTestIndex {
    pub name: String,
    pub table: String,
    pub create_sql: String,
}

#[derive(Debug, Clone)]
pub struct DdlTestView {
    pub name: String,
    pub create_sql: String,
}

#[derive(Debug, Clone)]
pub struct DdlTestScenario {
    pub name: String,
    pub setup_sql: Vec<String>,
    pub test_sql: String,
    pub cleanup_sql: Vec<String>,
}

pub struct PostgresFixtures;

impl PostgresFixtures {
    pub fn table_serial_pk() -> DdlTestTable {
        DdlTestTable {
            name: "users".to_string(),
            create_sql: "CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) NOT NULL,
                email VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
            .to_string(),
        }
    }

    pub fn table_composite_pk() -> DdlTestTable {
        DdlTestTable {
            name: "order_items".to_string(),
            create_sql: "CREATE TABLE order_items (
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 1,
                price NUMERIC(10, 2) NOT NULL,
                PRIMARY KEY (order_id, product_id)
            )"
            .to_string(),
        }
    }

    pub fn table_with_fk() -> DdlTestTable {
        DdlTestTable {
            name: "orders".to_string(),
            create_sql: "CREATE TABLE orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                total NUMERIC(10, 2) NOT NULL,
                status VARCHAR(20) DEFAULT 'pending'
            )"
            .to_string(),
        }
    }

    pub fn table_with_check() -> DdlTestTable {
        DdlTestTable {
            name: "products".to_string(),
            create_sql: "CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price NUMERIC(10, 2) NOT NULL,
                stock INTEGER NOT NULL DEFAULT 0,
                CONSTRAINT positive_price CHECK (price > 0),
                CONSTRAINT non_negative_stock CHECK (stock >= 0)
            )"
            .to_string(),
        }
    }

    pub fn table_with_unique() -> DdlTestTable {
        DdlTestTable {
            name: "accounts".to_string(),
            create_sql: "CREATE TABLE accounts (
                id SERIAL PRIMARY KEY,
                email VARCHAR(100) NOT NULL UNIQUE,
                username VARCHAR(50) NOT NULL,
                CONSTRAINT unique_username UNIQUE (username)
            )"
            .to_string(),
        }
    }

    pub fn index_single_column() -> DdlTestIndex {
        DdlTestIndex {
            name: "idx_users_email".to_string(),
            table: "users".to_string(),
            create_sql: "CREATE INDEX idx_users_email ON users(email)".to_string(),
        }
    }

    pub fn index_unique() -> DdlTestIndex {
        DdlTestIndex {
            name: "idx_users_username_unique".to_string(),
            table: "users".to_string(),
            create_sql: "CREATE UNIQUE INDEX idx_users_username_unique ON users(username)"
                .to_string(),
        }
    }

    pub fn index_composite() -> DdlTestIndex {
        DdlTestIndex {
            name: "idx_orders_user_status".to_string(),
            table: "orders".to_string(),
            create_sql: "CREATE INDEX idx_orders_user_status ON orders(user_id, status)"
                .to_string(),
        }
    }

    pub fn view_simple() -> DdlTestView {
        DdlTestView {
            name: "active_users".to_string(),
            create_sql: "CREATE VIEW active_users AS 
                SELECT id, username, email 
                FROM users 
                WHERE created_at > NOW() - INTERVAL '30 days'"
                .to_string(),
        }
    }

    pub fn alter_add_column() -> DdlTestScenario {
        DdlTestScenario {
            name: "add_column".to_string(),
            setup_sql: vec![
                "CREATE TABLE alter_test (id SERIAL PRIMARY KEY, name VARCHAR(50))".to_string(),
            ],
            test_sql: "ALTER TABLE alter_test ADD COLUMN age INTEGER".to_string(),
            cleanup_sql: vec!["DROP TABLE IF EXISTS alter_test".to_string()],
        }
    }

    pub fn alter_drop_column() -> DdlTestScenario {
        DdlTestScenario {
            name: "drop_column".to_string(),
            setup_sql: vec![
                "CREATE TABLE alter_test (id SERIAL PRIMARY KEY, name VARCHAR(50), age INTEGER)"
                    .to_string(),
            ],
            test_sql: "ALTER TABLE alter_test DROP COLUMN age".to_string(),
            cleanup_sql: vec!["DROP TABLE IF EXISTS alter_test".to_string()],
        }
    }

    pub fn alter_rename_column() -> DdlTestScenario {
        DdlTestScenario {
            name: "rename_column".to_string(),
            setup_sql: vec![
                "CREATE TABLE alter_test (id SERIAL PRIMARY KEY, old_name VARCHAR(50))".to_string(),
            ],
            test_sql: "ALTER TABLE alter_test RENAME COLUMN old_name TO new_name".to_string(),
            cleanup_sql: vec!["DROP TABLE IF EXISTS alter_test".to_string()],
        }
    }
}

pub struct MySqlFixtures;

impl MySqlFixtures {
    pub fn table_auto_increment_pk() -> DdlTestTable {
        DdlTestTable {
            name: "users".to_string(),
            create_sql: "CREATE TABLE users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50) NOT NULL,
                email VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
            .to_string(),
        }
    }

    pub fn table_composite_pk() -> DdlTestTable {
        DdlTestTable {
            name: "order_items".to_string(),
            create_sql: "CREATE TABLE order_items (
                order_id INT NOT NULL,
                product_id INT NOT NULL,
                quantity INT NOT NULL DEFAULT 1,
                price DECIMAL(10, 2) NOT NULL,
                PRIMARY KEY (order_id, product_id)
            )"
            .to_string(),
        }
    }

    pub fn table_with_fk() -> DdlTestTable {
        DdlTestTable {
            name: "orders".to_string(),
            create_sql: "CREATE TABLE orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                total DECIMAL(10, 2) NOT NULL,
                status VARCHAR(20) DEFAULT 'pending',
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )"
            .to_string(),
        }
    }

    pub fn table_with_check() -> DdlTestTable {
        DdlTestTable {
            name: "products".to_string(),
            create_sql: "CREATE TABLE products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10, 2) NOT NULL,
                stock INT NOT NULL DEFAULT 0,
                CONSTRAINT positive_price CHECK (price > 0),
                CONSTRAINT non_negative_stock CHECK (stock >= 0)
            )"
            .to_string(),
        }
    }

    pub fn table_with_unique() -> DdlTestTable {
        DdlTestTable {
            name: "accounts".to_string(),
            create_sql: "CREATE TABLE accounts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                email VARCHAR(100) NOT NULL UNIQUE,
                username VARCHAR(50) NOT NULL,
                CONSTRAINT unique_username UNIQUE (username)
            )"
            .to_string(),
        }
    }

    pub fn index_single_column() -> DdlTestIndex {
        DdlTestIndex {
            name: "idx_users_email".to_string(),
            table: "users".to_string(),
            create_sql: "CREATE INDEX idx_users_email ON users(email)".to_string(),
        }
    }

    pub fn index_unique() -> DdlTestIndex {
        DdlTestIndex {
            name: "idx_users_username_unique".to_string(),
            table: "users".to_string(),
            create_sql: "CREATE UNIQUE INDEX idx_users_username_unique ON users(username)"
                .to_string(),
        }
    }

    pub fn index_composite() -> DdlTestIndex {
        DdlTestIndex {
            name: "idx_orders_user_status".to_string(),
            table: "orders".to_string(),
            create_sql: "CREATE INDEX idx_orders_user_status ON orders(user_id, status)"
                .to_string(),
        }
    }

    pub fn view_simple() -> DdlTestView {
        DdlTestView {
            name: "active_users".to_string(),
            create_sql: "CREATE VIEW active_users AS 
                SELECT id, username, email 
                FROM users 
                WHERE created_at > DATE_SUB(NOW(), INTERVAL 30 DAY)"
                .to_string(),
        }
    }

    pub fn alter_add_column() -> DdlTestScenario {
        DdlTestScenario {
            name: "add_column".to_string(),
            setup_sql: vec![
                "CREATE TABLE alter_test (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50))"
                    .to_string(),
            ],
            test_sql: "ALTER TABLE alter_test ADD COLUMN age INT".to_string(),
            cleanup_sql: vec!["DROP TABLE IF EXISTS alter_test".to_string()],
        }
    }

    pub fn alter_drop_column() -> DdlTestScenario {
        DdlTestScenario {
            name: "drop_column".to_string(),
            setup_sql: vec![
                "CREATE TABLE alter_test (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50), age INT)"
                    .to_string(),
            ],
            test_sql: "ALTER TABLE alter_test DROP COLUMN age".to_string(),
            cleanup_sql: vec!["DROP TABLE IF EXISTS alter_test".to_string()],
        }
    }

    pub fn alter_rename_column() -> DdlTestScenario {
        DdlTestScenario {
            name: "rename_column".to_string(),
            setup_sql: vec![
                "CREATE TABLE alter_test (id INT AUTO_INCREMENT PRIMARY KEY, old_name VARCHAR(50))"
                    .to_string(),
            ],
            test_sql: "ALTER TABLE alter_test RENAME COLUMN old_name TO new_name".to_string(),
            cleanup_sql: vec!["DROP TABLE IF EXISTS alter_test".to_string()],
        }
    }
}

pub struct SqliteFixtures;

impl SqliteFixtures {
    pub fn table_integer_pk() -> DdlTestTable {
        DdlTestTable {
            name: "users".to_string(),
            create_sql: "CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                email TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )"
            .to_string(),
        }
    }

    pub fn table_composite_pk() -> DdlTestTable {
        DdlTestTable {
            name: "order_items".to_string(),
            create_sql: "CREATE TABLE order_items (
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 1,
                price REAL NOT NULL,
                PRIMARY KEY (order_id, product_id)
            )"
            .to_string(),
        }
    }

    pub fn table_with_fk() -> DdlTestTable {
        DdlTestTable {
            name: "orders".to_string(),
            create_sql: "CREATE TABLE orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                total REAL NOT NULL,
                status TEXT DEFAULT 'pending'
            )"
            .to_string(),
        }
    }

    pub fn table_with_check() -> DdlTestTable {
        DdlTestTable {
            name: "products".to_string(),
            create_sql: "CREATE TABLE products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                stock INTEGER NOT NULL DEFAULT 0,
                CHECK (price > 0),
                CHECK (stock >= 0)
            )"
            .to_string(),
        }
    }

    pub fn table_with_unique() -> DdlTestTable {
        DdlTestTable {
            name: "accounts".to_string(),
            create_sql: "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                username TEXT NOT NULL,
                CONSTRAINT unique_username UNIQUE (username)
            )"
            .to_string(),
        }
    }

    pub fn index_single_column() -> DdlTestIndex {
        DdlTestIndex {
            name: "idx_users_email".to_string(),
            table: "users".to_string(),
            create_sql: "CREATE INDEX idx_users_email ON users(email)".to_string(),
        }
    }

    pub fn index_unique() -> DdlTestIndex {
        DdlTestIndex {
            name: "idx_users_username_unique".to_string(),
            table: "users".to_string(),
            create_sql: "CREATE UNIQUE INDEX idx_users_username_unique ON users(username)"
                .to_string(),
        }
    }

    pub fn index_composite() -> DdlTestIndex {
        DdlTestIndex {
            name: "idx_orders_user_status".to_string(),
            table: "orders".to_string(),
            create_sql: "CREATE INDEX idx_orders_user_status ON orders(user_id, status)"
                .to_string(),
        }
    }

    pub fn view_simple() -> DdlTestView {
        DdlTestView {
            name: "active_users".to_string(),
            create_sql: "CREATE VIEW active_users AS 
                SELECT id, username, email 
                FROM users 
                WHERE created_at > datetime('now', '-30 days')"
                .to_string(),
        }
    }

    pub fn alter_add_column() -> DdlTestScenario {
        DdlTestScenario {
            name: "add_column".to_string(),
            setup_sql: vec![
                "CREATE TABLE alter_test (id INTEGER PRIMARY KEY, name TEXT)".to_string(),
            ],
            test_sql: "ALTER TABLE alter_test ADD COLUMN age INTEGER".to_string(),
            cleanup_sql: vec!["DROP TABLE IF EXISTS alter_test".to_string()],
        }
    }

    pub fn alter_rename_column() -> DdlTestScenario {
        DdlTestScenario {
            name: "rename_column".to_string(),
            setup_sql: vec![
                "CREATE TABLE alter_test (id INTEGER PRIMARY KEY, old_name TEXT)".to_string(),
            ],
            test_sql: "ALTER TABLE alter_test RENAME COLUMN old_name TO new_name".to_string(),
            cleanup_sql: vec!["DROP TABLE IF EXISTS alter_test".to_string()],
        }
    }
}

pub fn seed_table<C: dbflux_core::Connection + ?Sized>(
    conn: &C,
    table_name: &str,
    rows: Vec<HashMap<String, dbflux_core::Value>>,
) -> Result<(), DbError> {
    for row in rows {
        let columns: Vec<String> = row.keys().cloned().collect();
        let values: Vec<dbflux_core::Value> = columns.iter().map(|k| row[k].clone()).collect();

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name,
            columns.join(", "),
            (0..columns.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ")
        );

        let mut query = QueryRequest::new(insert_sql);
        query.params = values;
        conn.execute(&query)?;
    }

    Ok(())
}

pub fn cleanup_table<C: dbflux_core::Connection + ?Sized>(
    conn: &C,
    table_name: &str,
) -> Result<(), DbError> {
    let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
    conn.execute(&QueryRequest::new(drop_sql))?;
    Ok(())
}

pub fn cleanup_view<C: dbflux_core::Connection + ?Sized>(
    conn: &C,
    view_name: &str,
) -> Result<(), DbError> {
    let drop_sql = format!("DROP VIEW IF EXISTS {}", view_name);
    conn.execute(&QueryRequest::new(drop_sql))?;
    Ok(())
}

pub fn cleanup_index<C: dbflux_core::Connection + ?Sized>(
    conn: &C,
    index_name: &str,
    dialect: &str,
) -> Result<(), DbError> {
    let drop_sql = match dialect {
        "mysql" => format!("DROP INDEX {} ON {}", index_name, "users"),
        _ => format!("DROP INDEX IF EXISTS {}", index_name),
    };
    conn.execute(&QueryRequest::new(drop_sql))?;
    Ok(())
}
