#![allow(clippy::result_large_err)]

pub mod containers;
pub mod ddl_fixtures;
pub mod fake_auth_provider_rpc;
pub mod fake_driver;
pub mod fixtures;
pub mod seed;

pub use fake_auth_provider_rpc::{
    FakeAuthProviderRpcConfig, FakeAuthProviderRpcServer, FakeAuthRpcResult,
};
pub use fake_driver::{FakeDriver, FakeDriverStats, FakeQueryOutcome};
