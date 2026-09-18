//! Remote TursoDB / libSQL driver.
//!
//! Speaks the Hrana-over-HTTP protocol through the `turso_serverless` SDK. The
//! SDK is async-only while DBFlux drivers are synchronous, so every connection
//! owns a small Tokio runtime and bridges calls with `block_on` from the
//! background threads that invoke driver methods.

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
mod session;

pub use connection::{TursoConnection, TursoErrorFormatter};
pub use dialect::{TursoCodeGenerator, TursoDialect};
pub use driver::{METADATA, TURSO_FORM, TursoDriver};
