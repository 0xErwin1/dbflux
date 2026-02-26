use std::sync::Arc;

use dbflux_core::{
    ConnectionProfile, DbConfig, DbError, DbKind, DriverCapabilities, DriverFormDef,
    DriverMetadata, FormValues,
};
use dbflux_ipc::driver_protocol::{DriverMetadataDto, DriverResponseBody};
use dbflux_ipc::driver_socket_name;

use crate::connection::IpcConnection;
use crate::transport::RpcClient;

/// An IPC-based driver that proxies all operations to a remote driver-host process.
///
/// The driver connects to a driver-host over a local socket identified by a
/// string name (not a filesystem path). The underlying transport is cross-platform:
/// abstract namespace UDS on Linux, UDS in /tmp on macOS, named pipes on Windows.
///
/// `kind` and `metadata` are provided at construction time (from the service
/// configuration), so the driver can satisfy `DbDriver::kind()` and
/// `DbDriver::metadata()` without needing an active connection.
pub struct IpcDriver {
    socket_id: String,
    kind: DbKind,
    metadata: &'static DriverMetadata,
}

impl IpcDriver {
    pub fn new(socket_id: String, kind: DbKind, metadata: &'static DriverMetadata) -> Self {
        Self {
            socket_id,
            kind,
            metadata,
        }
    }

    pub fn socket_id(&self) -> &str {
        &self.socket_id
    }
}

/// Converts a `DriverMetadataDto` into a `&'static DriverMetadata`.
///
/// This leaks memory intentionally — drivers live for the entire process
/// lifetime, so the metadata is never deallocated.
pub fn leak_metadata(dto: &DriverMetadataDto) -> &'static DriverMetadata {
    let query_language: dbflux_core::QueryLanguage = dto.query_language.clone().into();

    let metadata = DriverMetadata {
        id: Box::leak(dto.id.clone().into_boxed_str()),
        display_name: Box::leak(dto.display_name.clone().into_boxed_str()),
        description: Box::leak(dto.description.clone().into_boxed_str()),
        category: dto.category,
        query_language,
        capabilities: DriverCapabilities::from_bits(dto.capabilities)
            .unwrap_or_else(DriverCapabilities::empty),
        default_port: dto.default_port,
        uri_scheme: Box::leak(dto.uri_scheme.clone().into_boxed_str()),
        icon: dto.icon,
    };

    Box::leak(Box::new(metadata))
}

/// A minimal form definition for IPC drivers.
///
/// IPC drivers don't use the connection dialog — the host handles all
/// configuration. This stub satisfies the trait requirement.
static IPC_FORM: DriverFormDef = DriverFormDef { tabs: &[] };

impl dbflux_core::DbDriver for IpcDriver {
    fn kind(&self) -> DbKind {
        self.kind
    }

    fn metadata(&self) -> &'static DriverMetadata {
        self.metadata
    }

    fn form_definition(&self) -> &'static DriverFormDef {
        &IPC_FORM
    }

    fn build_config(&self, _values: &FormValues) -> Result<DbConfig, DbError> {
        Err(DbError::NotSupported(
            "IPC drivers do not support local config building".into(),
        ))
    }

    fn extract_values(&self, _config: &DbConfig) -> FormValues {
        FormValues::new()
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&str>,
        ssh_secret: Option<&str>,
    ) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
        let name = driver_socket_name(&self.socket_id)
            .map_err(|e| DbError::ConnectionFailed(e.to_string().into()))?;

        let client = RpcClient::connect(name).map_err(DbError::from)?;

        let profile_json = serde_json::to_string(profile)
            .map_err(|e| DbError::InvalidProfile(format!("JSON serialization failed: {e}")))?;

        let response = client
            .open_session(&profile_json, password, ssh_secret)
            .map_err(DbError::from)?;

        let DriverResponseBody::SessionOpened {
            session_id,
            kind,
            metadata: metadata_dto,
            schema_loading_strategy,
            schema_features,
            code_gen_capabilities,
        } = response
        else {
            return Err(DbError::ConnectionFailed(
                "Unexpected response from driver host".into(),
            ));
        };

        let connection_metadata = leak_metadata(&metadata_dto);
        let capabilities = connection_metadata.capabilities;

        Ok(Box::new(IpcConnection::new(
            Arc::new(client),
            session_id,
            kind,
            connection_metadata,
            capabilities,
            schema_loading_strategy,
            schema_features,
            code_gen_capabilities,
        )))
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let name = driver_socket_name(&self.socket_id)
            .map_err(|e| DbError::ConnectionFailed(e.to_string().into()))?;

        let client = RpcClient::connect(name).map_err(DbError::from)?;

        let profile_json = serde_json::to_string(profile)
            .map_err(|e| DbError::InvalidProfile(format!("JSON serialization failed: {e}")))?;

        let response = client
            .open_session(&profile_json, None, None)
            .map_err(DbError::from)?;

        let DriverResponseBody::SessionOpened { session_id, .. } = response else {
            return Err(DbError::ConnectionFailed(
                "Unexpected response from driver host".into(),
            ));
        };

        let result = client.ping(session_id).map_err(DbError::from);

        let _ = client.close_session(session_id);

        result
    }
}
