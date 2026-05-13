//! InfluxDB driver for DBFlux.

#![allow(clippy::result_large_err)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
    )
)]

pub mod connection;
pub mod driver;
pub mod error_formatter;
pub mod http;
pub mod injection;
pub mod metadata;
pub mod parser;
pub mod query_generator;

pub use driver::{INFLUXDB_METADATA, InfluxDriver};
