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
pub mod language_service;
pub mod query_generator;
pub mod query_parser;

pub use driver::{MONGODB_METADATA, MongoDriver};
pub use language_service::MongoLanguageService;
pub use query_generator::MongoShellGenerator;
pub use query_parser::{MongoParseError, validate_query, validate_query_positional};
