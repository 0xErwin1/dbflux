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
pub mod query_generator;
pub mod query_parser;

pub use driver::{DYNAMODB_METADATA, DynamoDriver};
