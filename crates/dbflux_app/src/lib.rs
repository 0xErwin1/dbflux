//! `dbflux_app` — Pure domain modules for DBFlux.
//!
//! This crate contains modules with no GPUI dependency, making them usable
//! from both the main application and external tools/servers.

pub mod access_manager;
pub mod auth_provider_registry;
pub mod config_loader;
pub mod history_manager_sqlite;
pub mod hook_executor;
pub mod keymap;
pub mod mcp_command;
pub mod proxy;

pub use access_manager::AppAccessManager;
pub use auth_provider_registry::{AuthProviderRegistry, RegistryAuthProviderWrapper};
pub use hook_executor::CompositeExecutor;
