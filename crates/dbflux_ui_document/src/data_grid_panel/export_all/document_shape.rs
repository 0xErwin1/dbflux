//! Lays document pages out the way the grid flattens them, so "Export all
//! rows" writes the columns the loaded-rows export writes for the same
//! documents.

use std::collections::BTreeSet;

use dbflux_core::{QueryResult, TransferColumn, Value, value_at_path};
use dbflux_transfer::{PageShape, to_transfer_column};

use crate::data_grid_panel::documents::columns::{self, FlatColumn};

/// Flattens each page with the grid's expanded object columns.
///
/// The columns come from the first page, as the grid's own first page has
/// them. A later page is projected onto those column paths, so a document
/// without a field writes null; a field that appears only on later pages has
/// no column in a file whose header is already written, and is reported
/// through `dropped_columns` instead.
///
/// With `whole_documents`, for a format without a header, each row is the
/// document itself and keeps every field it has.
pub(super) struct DocumentPageShape {
    whole_documents: bool,
    expanded: BTreeSet<String>,
    identity_columns: Vec<String>,
    columns: Vec<FlatColumn>,
    dropped: BTreeSet<String>,
}

impl DocumentPageShape {
    pub(super) fn new(expanded: BTreeSet<String>, identity_columns: Vec<String>) -> Self {
        Self {
            whole_documents: false,
            expanded,
            identity_columns,
            columns: Vec::new(),
            dropped: BTreeSet::new(),
        }
    }
}

impl DocumentPageShape {
    pub(super) fn whole_documents(mut self) -> Self {
        self.whole_documents = true;
        self
    }
}

fn top_order(page: &QueryResult) -> Vec<String> {
    page.columns
        .iter()
        .map(|column| column.name.clone())
        .collect()
}

impl PageShape for DocumentPageShape {
    fn columns(&mut self, first_page: &QueryResult) -> Vec<TransferColumn> {
        if self.whole_documents {
            // The sink writes each document under its own field names; this
            // single column only gives the pipeline a shape to map.
            return vec![TransferColumn {
                name: "_document".to_string(),
                type_name: None,
                nullable: false,
                is_primary_key: false,
            }];
        }

        let flat = columns::flatten(
            &columns::documents_from_result(first_page),
            &top_order(first_page),
            &self.expanded,
            None,
        );
        let flattened = flat.to_query_result(first_page, &self.identity_columns);
        self.columns = flat.columns;

        flattened.columns.iter().map(to_transfer_column).collect()
    }

    fn rows(&mut self, page: QueryResult) -> Vec<Vec<Value>> {
        let documents = columns::documents_from_result(&page);
        if self.whole_documents {
            return documents
                .into_iter()
                .map(|document| vec![document])
                .collect();
        }

        let page_columns =
            columns::flatten(&documents, &top_order(&page), &self.expanded, None).columns;
        for column in page_columns {
            if !self.columns.iter().any(|known| known.path == column.path) {
                self.dropped.insert(column.dotted());
            }
        }

        documents
            .iter()
            .map(|document| {
                self.columns
                    .iter()
                    .map(|column| {
                        value_at_path(document, &column.path)
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

    fn writes_whole_documents(&self) -> bool {
        self.whole_documents
    }
}
