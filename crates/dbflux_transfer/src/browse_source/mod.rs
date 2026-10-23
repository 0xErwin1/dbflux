//! Browse -> Row `RowSource`: pages through a read a data grid ran (a table
//! browse, a collection browse or a query builder SELECT), keeping its
//! filter, sort and column projection, so an export writes exactly the rows
//! the grid shows across all of its pages.
//!
//! Unlike [`crate::TableSource`], it never builds SQL: the driver renders
//! every request, so it works for every driver that serves the read.

mod read;
mod shape;

use std::collections::VecDeque;
use std::sync::Arc;

use dbflux_core::{CancelToken, Connection, TransferColumn, Value};

use crate::pipeline::{RowChunk, RowSource, TransferError};

pub use read::{ReplayableRead, select_for_visual_spec};
pub use shape::{AlignByName, PageShape, to_transfer_column};

/// Reads every page of a [`ReplayableRead`], `page_size` rows at a time.
///
/// The first page is read when the source opens, because the column shape
/// comes from the driver's answer and the sink needs it before any row.
pub struct BrowseSource {
    connection: Arc<dyn Connection>,
    read: ReplayableRead,
    page_size: u32,
    shape: Box<dyn PageShape>,
    columns: Vec<TransferColumn>,
    buffered_page: VecDeque<Vec<Vec<Value>>>,
    read_so_far: u64,
    exhausted: bool,
    estimated_total: Option<u64>,
}

impl BrowseSource {
    /// Opens the source at the read's first row, laying pages out by column
    /// name.
    pub fn open(
        connection: Arc<dyn Connection>,
        read: ReplayableRead,
        page_size: u32,
        estimated_total: Option<u64>,
    ) -> Result<Self, TransferError> {
        Self::open_with_shape(
            connection,
            read,
            page_size,
            estimated_total,
            Box::new(AlignByName::default()),
        )
    }

    /// Opens the source at the read's first row, laying pages out with
    /// `shape`.
    pub fn open_with_shape(
        connection: Arc<dyn Connection>,
        read: ReplayableRead,
        page_size: u32,
        estimated_total: Option<u64>,
        shape: Box<dyn PageShape>,
    ) -> Result<Self, TransferError> {
        let mut source = Self {
            connection,
            read,
            page_size: page_size.max(1),
            shape,
            columns: Vec::new(),
            buffered_page: VecDeque::new(),
            read_so_far: 0,
            exhausted: false,
            estimated_total,
        };

        if let Some(first_page) = source.fetch_page()? {
            source.columns = source.shape.columns(&first_page);
            let rows = source.shape.rows(first_page);
            if !rows.is_empty() {
                source.buffered_page.push_back(rows);
            }
        }

        Ok(source)
    }

    /// Columns later pages had that the file leaves out.
    pub fn dropped_columns(&self) -> Vec<String> {
        self.shape.dropped_columns()
    }

    fn fetch_page(&mut self) -> Result<Option<dbflux_core::QueryResult>, TransferError> {
        let Some(page) =
            self.read
                .fetch_page(self.connection.as_ref(), self.page_size, self.read_so_far)?
        else {
            self.exhausted = true;
            return Ok(None);
        };

        let row_count = page.result.rows.len() as u64;
        self.read_so_far += row_count;
        // A short page is the last one. Waiting for an empty page instead
        // would loop forever on a driver that ignores the offset.
        if row_count < page.requested {
            self.exhausted = true;
        }

        Ok(Some(page.result))
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

        let Some(page) = self.fetch_page()? else {
            return Ok(None);
        };
        let rows = self.shape.rows(page);
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
