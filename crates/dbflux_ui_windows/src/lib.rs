//! Connection manager, settings, and shared SSH UI for DBFlux.
//!
//! This crate holds the windows subsystem extracted from `dbflux_ui`:
//! the Connection Manager window, the Settings window, and shared SSH
//! authentication UI helpers.

pub mod connection_manager;
pub mod settings;
pub mod ssh_shared;

mod style_guardrails;
