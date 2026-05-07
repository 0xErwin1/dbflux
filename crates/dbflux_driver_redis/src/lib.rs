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

pub mod command_generator;
pub mod driver;
pub mod language_service;

pub use command_generator::RedisCommandGenerator;
pub use driver::{REDIS_METADATA, RedisDriver};
pub use language_service::RedisLanguageService;
