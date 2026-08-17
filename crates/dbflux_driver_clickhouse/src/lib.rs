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
pub mod dialect;
pub mod driver;
pub mod error_formatter;
mod http;
mod introspection;
pub mod types;

pub use connection::ClickHouseConnection;
pub use dialect::ClickHouseDialect;
pub use driver::{CLICKHOUSE_FORM, ClickHouseDriver, METADATA};
pub use error_formatter::ClickHouseErrorFormatter;
pub use types::{
    ClickHouseType, clickhouse_type_is_nullable, clickhouse_type_to_column_kind,
    parse_clickhouse_type,
};
