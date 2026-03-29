//! Repository modules for DBFlux internal storage.
//!
//! Each repository provides CRUD operations for a specific config domain.
//! All repositories operate on the `config.db` database.

pub mod auth_profiles;
pub mod connection_profiles;
pub mod driver_settings;
pub mod hook_definitions;
pub mod proxy_profiles;
pub mod services;
pub mod settings;
pub mod ssh_tunnel_profiles;
