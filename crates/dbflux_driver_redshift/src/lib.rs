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
pub mod dialect;
pub mod driver;
pub mod error_formatter;
pub(crate) mod introspection;
pub mod types;

pub use dialect::RedshiftDialect;
pub use driver::{METADATA, REDSHIFT_FORM, RedshiftDriver};
pub use error_formatter::RedshiftErrorFormatter;
pub use types::redshift_oid_to_kind;
