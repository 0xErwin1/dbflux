use std::collections::HashMap;
use std::sync::Arc;

use dbflux_core::{
    ConnectionHook, DbDriver, DriverKey, FormValues, GeneralSettings, GlobalOverrides,
    ServiceConfig,
};

use crate::rpc_services::ExternalDriverDiagnostic;

pub(super) struct BuiltDrivers {
    pub(super) drivers: HashMap<String, Arc<dyn DbDriver>>,
    pub(super) external_driver_diagnostics: HashMap<String, ExternalDriverDiagnostic>,
    pub(super) general_settings: GeneralSettings,
    pub(super) driver_overrides: HashMap<DriverKey, GlobalOverrides>,
    pub(super) driver_settings: HashMap<DriverKey, FormValues>,
    pub(super) hook_definitions: HashMap<String, ConnectionHook>,
    pub(super) services: Vec<ServiceConfig>,
}
