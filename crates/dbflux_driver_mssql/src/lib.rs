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

pub use driver::{METADATA, MssqlDriver, SQLSERVER_FORM};
pub use language_service::TSqlLanguageService;
