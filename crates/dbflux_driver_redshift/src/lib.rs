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

pub mod driver;
pub mod types;

pub use driver::{METADATA, REDSHIFT_FORM, RedshiftDriver};
pub use types::redshift_oid_to_kind;
