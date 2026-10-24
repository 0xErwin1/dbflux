//! How the rows of each page are laid out in the file.

use std::collections::{BTreeSet, HashMap};

use dbflux_core::{ColumnMeta, QueryResult, TransferColumn, Value};

/// Lays the pages of a read out under one set of columns.
///
/// A file starts with its header, so the columns are fixed by the first
/// page. A later page whose rows carry a column the first page did not have
/// (documents of one collection need not share their fields) is written
/// under the first page's columns, and the column it added is reported.
pub trait PageShape: Send {
    /// The file's columns, from the first page.
    fn columns(&mut self, first_page: &QueryResult) -> Vec<TransferColumn>;

    /// The page's rows, one value per file column.
    fn rows(&mut self, page: QueryResult) -> Vec<Vec<Value>>;

    /// Columns later pages had that the file leaves out, in name order.
    fn dropped_columns(&self) -> Vec<String>;

    /// Whether each row is one `Value::Document` to be written whole, with
    /// its own fields, instead of values under the file's columns.
    fn writes_whole_documents(&self) -> bool {
        false
    }
}

/// Writes each page's values under the first page's columns, matched by
/// name; a column a page lacks reads as null.
#[derive(Debug, Default)]
pub struct AlignByName {
    names: Vec<String>,
    dropped: BTreeSet<String>,
}

impl PageShape for AlignByName {
    fn columns(&mut self, first_page: &QueryResult) -> Vec<TransferColumn> {
        self.names = first_page
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect();
        first_page.columns.iter().map(to_transfer_column).collect()
    }

    fn rows(&mut self, page: QueryResult) -> Vec<Vec<Value>> {
        let same_columns = page.columns.len() == self.names.len()
            && page
                .columns
                .iter()
                .zip(&self.names)
                .all(|(column, name)| &column.name == name);
        if same_columns {
            return page.rows;
        }

        let positions: HashMap<&str, usize> = page
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| (column.name.as_str(), index))
            .collect();
        for column in &page.columns {
            if !self.names.contains(&column.name) {
                self.dropped.insert(column.name.clone());
            }
        }

        page.rows
            .iter()
            .map(|row| {
                self.names
                    .iter()
                    .map(|name| {
                        positions
                            .get(name.as_str())
                            .and_then(|index| row.get(*index))
                            .cloned()
                            .unwrap_or(Value::Null)
                    })
                    .collect()
            })
            .collect()
    }

    fn dropped_columns(&self) -> Vec<String> {
        self.dropped.iter().cloned().collect()
    }
}

/// The transfer column for a column a driver reported.
pub fn to_transfer_column(column: &ColumnMeta) -> TransferColumn {
    TransferColumn {
        name: column.name.clone(),
        type_name: Some(column.type_name.clone()),
        nullable: column.nullable,
        is_primary_key: column.is_primary_key,
    }
}
