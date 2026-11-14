//! AWS Secrets Manager and Systems Manager Parameter Store driver for DBFlux.
//!
//! One connection exposes both services as secret stores through the
//! `SecretStoreSource` seam defined in `dbflux_core`, using either AWS
//! profile/SSO auth or static access-key credentials.
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

mod driver;
mod error_formatter;
mod parameter_store;
mod secrets_manager;

pub use driver::{AWS_SECRETS_FORM, AWS_SECRETS_METADATA, AwsSecretsDriver};
pub use parameter_store::PARAMETER_STORE_ID;
pub use secrets_manager::SECRETS_MANAGER_ID;
