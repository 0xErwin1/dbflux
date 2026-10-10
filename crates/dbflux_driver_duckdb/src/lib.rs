//! Embedded DuckDB driver, with optional DuckLake catalogs.
//!
//! DuckDB locks its database file per process, so every connection to the
//! same file (or to the same profile's in-memory database) is cloned from one
//! shared database instance instead of opening the file again.

#![allow(clippy::result_large_err)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

mod connection;
mod dialect;
mod driver;

pub use connection::DuckDbConnection;
pub use dialect::DuckDbDialect;
pub use driver::{DUCKDB_FORM, DuckDbDriver, METADATA};
