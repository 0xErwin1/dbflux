//! Data-transfer engine: a unified Source -> Map -> Sink pipeline used by
//! Export (table -> file), Import (file -> table), and Migration
//! (table -> table).
//!
//! The engine drives pagination itself rather than relying on a driver
//! cursor, because `Connection::execute` is a synchronous pull API with
//! `limit`/`offset`, not a streaming cursor. Drivers only need to implement
//! thin seams (`QueryGenerator::generate_bulk_insert` /
//! `generate_create_table`, `Connection::set_referential_integrity`); the
//! pipeline, chunking, cancellation, and progress reporting live here.

mod column_map;
mod pipeline;

pub use column_map::AutoColumnMap;
pub use dbflux_core::TransferColumn;
pub use pipeline::{
    ColumnMap, RowChunk, RowSink, RowSource, TableMappingMode, TransferError, TransferOutcome,
    TransferReport, run_transfer,
};
