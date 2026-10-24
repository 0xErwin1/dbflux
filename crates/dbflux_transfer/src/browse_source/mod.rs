//! Browse -> Row `RowSource`: pages through one table with
//! `Connection::browse_table`, keeping the request's filter, sort and column
//! projection, so an export writes exactly the rows a data grid shows across
//! all of its pages.
//!
//! Unlike [`crate::TableSource`], it never builds SQL: the driver renders the
//! request, so it works for every driver that browses tables.

use std::collections::VecDeque;
use std::sync::Arc;

use dbflux_core::{
    CancelToken, ColumnMeta, Connection, Pagination, TableBrowseRequest, TransferColumn, Value,
};

use crate::pipeline::{RowChunk, RowSource, TransferError};

/// Reads every page of a [`TableBrowseRequest`], `page_size` rows at a time.
///
/// The first page is read when the source opens, because the column shape
/// comes from the driver's answer and the sink needs it before any row.
pub struct BrowseSource {
    connection: Arc<dyn Connection>,
    request: TableBrowseRequest,
    page_size: u32,
    columns: Vec<TransferColumn>,
    buffered_page: VecDeque<Vec<Vec<Value>>>,
    next_offset: u64,
    exhausted: bool,
    estimated_total: Option<u64>,
}

impl BrowseSource {
    /// Opens the source at the first row, whatever page `request` points at.
    pub fn open(
        connection: Arc<dyn Connection>,
        request: TableBrowseRequest,
        page_size: u32,
        estimated_total: Option<u64>,
    ) -> Result<Self, TransferError> {
        let mut source = Self {
            connection,
            request,
            page_size: page_size.max(1),
            columns: Vec::new(),
            buffered_page: VecDeque::new(),
            next_offset: 0,
            exhausted: false,
            estimated_total,
        };

        let (columns, rows) = source.fetch_page()?;
        source.columns = columns.iter().map(to_transfer_column).collect();
        if !rows.is_empty() {
            source.buffered_page.push_back(rows);
        }

        Ok(source)
    }

    fn fetch_page(&mut self) -> Result<(Vec<ColumnMeta>, Vec<Vec<Value>>), TransferError> {
        let mut request = self.request.clone();
        request.pagination = Pagination::Offset {
            limit: self.page_size,
            offset: self.next_offset,
        };

        let result = self
            .connection
            .browse_table(&request)
            .map_err(|error| TransferError::Source(error.to_string()))?;

        let row_count = result.rows.len();
        self.next_offset += row_count as u64;
        // A short page is the last one. Waiting for an empty page instead
        // would loop forever on a driver that ignores the offset.
        if row_count < self.page_size as usize {
            self.exhausted = true;
        }

        Ok((result.columns, result.rows))
    }
}

fn to_transfer_column(column: &ColumnMeta) -> TransferColumn {
    TransferColumn {
        name: column.name.clone(),
        type_name: Some(column.type_name.clone()),
        nullable: column.nullable,
        is_primary_key: column.is_primary_key,
    }
}

impl RowSource for BrowseSource {
    fn columns(&self) -> &[TransferColumn] {
        &self.columns
    }

    fn next_chunk(&mut self, cancel: &CancelToken) -> Result<Option<RowChunk>, TransferError> {
        if let Some(rows) = self.buffered_page.pop_front() {
            return Ok(Some(RowChunk(rows)));
        }

        if self.exhausted || cancel.is_cancelled() {
            return Ok(None);
        }

        let (_, rows) = self.fetch_page()?;
        if rows.is_empty() {
            return Ok(None);
        }

        Ok(Some(RowChunk(rows)))
    }

    fn estimated_total(&self) -> Option<u64> {
        self.estimated_total
    }
}

#[cfg(test)]
mod tests;
