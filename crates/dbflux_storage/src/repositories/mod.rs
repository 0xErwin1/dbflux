//! Repository modules for DBFlux internal storage.
//!
//! Each repository provides CRUD operations for a specific config domain.
//! Config repositories operate on `config.db`; state repositories on `state.db`.

pub mod auth_profile_fields;
pub mod auth_profiles;
pub mod connection_driver_configs;
pub mod connection_folders;
pub mod connection_profile_access_params;
pub mod connection_profile_configs;
pub mod connection_profile_governance;
pub mod connection_profile_governance_binding_policies;
pub mod connection_profile_governance_binding_roles;
pub mod connection_profile_governance_bindings;
pub mod connection_profile_hook_args;
pub mod connection_profile_hook_bindings;
pub mod connection_profile_hook_envs;
pub mod connection_profile_hooks;
pub mod connection_profile_settings;
pub mod connection_profile_value_refs;
pub mod connection_profiles;
pub mod driver_overrides;
pub mod driver_settings;
pub mod driver_setting_values;
pub mod general_settings;
pub mod governance_settings;
pub mod hook_commands;
pub mod hook_definitions;
pub mod hook_environment;
pub mod legacy_imports;
pub mod proxy_auth;
pub mod proxy_profiles;
pub mod service_args;
pub mod service_env;
pub mod services;
pub mod settings;
pub mod ssh_tunnel_auth;
pub mod ssh_tunnel_profiles;

pub mod state;

pub use legacy_imports::{ImportStatus, LegacyImport, LegacyImportsRepository};
