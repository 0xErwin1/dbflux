use dbflux_core::{DbError, QueryRequest, Value};
use std::collections::HashMap;

pub struct SeedData {
    pub table_name: String,
    pub rows: Vec<HashMap<String, Value>>,
}

impl SeedData {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            rows: Vec::new(),
        }
    }

    pub fn add_row(mut self, row: HashMap<String, Value>) -> Self {
        self.rows.push(row);
        self
    }

    pub fn insert<C: dbflux_core::Connection + ?Sized>(&self, conn: &C) -> Result<(), DbError> {
        for row in &self.rows {
            let columns: Vec<String> = row.keys().cloned().collect();
            let values: Vec<Value> = columns.iter().map(|k| row[k].clone()).collect();

            let placeholders = (0..columns.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ");

            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                self.table_name,
                columns.join(", "),
                placeholders
            );

            let mut query = QueryRequest::new(insert_sql);
            query.params = values;
            conn.execute(&query)?;
        }

        Ok(())
    }
}

pub fn row(fields: Vec<(&str, Value)>) -> HashMap<String, Value> {
    fields
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
}

pub fn users_seed_data() -> SeedData {
    SeedData::new("users")
        .add_row(row(vec![
            ("username", Value::Text("alice".to_string())),
            ("email", Value::Text("alice@example.com".to_string())),
        ]))
        .add_row(row(vec![
            ("username", Value::Text("bob".to_string())),
            ("email", Value::Text("bob@example.com".to_string())),
        ]))
        .add_row(row(vec![
            ("username", Value::Text("charlie".to_string())),
            ("email", Value::Text("charlie@example.com".to_string())),
        ]))
}

pub fn products_seed_data() -> SeedData {
    SeedData::new("products")
        .add_row(row(vec![
            ("name", Value::Text("Widget A".to_string())),
            ("price", Value::Float(19.99)),
            ("stock", Value::Int(100)),
        ]))
        .add_row(row(vec![
            ("name", Value::Text("Widget B".to_string())),
            ("price", Value::Float(29.99)),
            ("stock", Value::Int(50)),
        ]))
        .add_row(row(vec![
            ("name", Value::Text("Widget C".to_string())),
            ("price", Value::Float(39.99)),
            ("stock", Value::Int(25)),
        ]))
}
