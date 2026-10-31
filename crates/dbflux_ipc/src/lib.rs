pub mod audit;
pub mod auth;
pub mod auth_provider_client;
pub mod auth_provider_protocol;
pub mod driver_protocol;
pub mod envelope;
pub mod framing;
pub mod protocol;
pub mod secret_provider;
pub mod socket;

pub use audit::{
    AuditEventEmitDto, EventCategoryDto, EventOutcomeDto, EventSeverityDto, ExternalAuditEmitter,
    ExternalAuditSource,
};
pub use auth::{
    APP_CONTROL_AUTH_TOKEN_ENV, AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV, DRIVER_RPC_AUTH_TOKEN_ENV,
    SECRET_PROVIDER_RPC_AUTH_TOKEN_ENV, app_control_token_path, init_process_auth_tokens,
    process_auth_token, read_app_control_token, write_app_control_token,
};
pub use auth_provider_client::{
    IpcServiceLaunchConfig, RpcAuthProvider, build_fetch_dependencies, hash_dependencies,
    negotiate_auth_provider_version, shutdown_managed_auth_provider_hosts,
};
pub use auth_provider_protocol::{
    AuthProviderHelloRequest, AuthProviderHelloResponse, AuthProviderHelloResponseV1_1,
    AuthProviderHelloResponseV1_2, AuthProviderHelloResponseV1_3, AuthProviderRequestBody,
    AuthProviderRequestEnvelope, AuthProviderResponseBody, AuthProviderResponseEnvelope,
    AuthProviderRpcError, AuthProviderRpcErrorCode, AuthSessionDto, AuthSessionStateDto,
    FetchFieldOptionsError, FetchFieldOptionsRequest, FetchFieldOptionsResponse, LoginRequest,
    LoginUrlProgress, ResolveCredentialsRequest, ResolvedCredentialsDto, ValidateSessionRequest,
    parse_auth_profile,
};
pub use driver_protocol::{
    DriverCapability, DriverHelloRequest, DriverHelloResponse, DriverRequestBody,
    DriverRequestEnvelope, DriverResponseBody, DriverResponseEnvelope, DriverRpcError,
    DriverRpcErrorCode, QueryRequestDto, QueryResultChunk, QueryResultDto, QueryResultSetsDto,
    QueryResultShapeDto, ResultSetDto, protocol_carries_request_row_limit,
    protocol_carries_result_sets, protocol_carries_root_sections,
};
pub use envelope::{
    APP_CONTROL_VERSION, AUTH_PROVIDER_RPC_API_CONTRACT, AUTH_PROVIDER_RPC_SUPPORTED_VERSIONS,
    AUTH_PROVIDER_RPC_V1_0, AUTH_PROVIDER_RPC_V1_1, AUTH_PROVIDER_RPC_V1_2, AUTH_PROVIDER_RPC_V1_3,
    AUTH_PROVIDER_RPC_VERSION, DRIVER_RPC_API_CONTRACT, DRIVER_RPC_SUPPORTED_VERSIONS,
    DRIVER_RPC_V1_0, DRIVER_RPC_V1_1, DRIVER_RPC_V1_2, DRIVER_RPC_V1_3, DRIVER_RPC_V1_4,
    DRIVER_RPC_V1_5, DRIVER_RPC_V1_6, DRIVER_RPC_V1_7, DRIVER_RPC_V1_8, DRIVER_RPC_V1_9,
    DRIVER_RPC_VERSION, ProtocolVersion, RpcApiContract, RpcApiFamily,
    SECRET_PROVIDER_RPC_API_CONTRACT, SECRET_PROVIDER_RPC_SUPPORTED_VERSIONS,
    SECRET_PROVIDER_RPC_V1_0, SECRET_PROVIDER_RPC_VERSION, auth_provider_rpc_supported_versions,
    driver_rpc_supported_versions, negotiate_highest_mutual_version,
    secret_provider_rpc_supported_versions,
};
pub use framing::{recv_msg, send_msg};
pub use protocol::{AppControlRequest, AppControlResponse, IpcMessage, IpcResponse};
pub use secret_provider::{
    AuthProfileDependency, BUILT_IN_SECRET_PROVIDER_IDS, GetSecretRequest, ManifestError,
    RpcSecretProvider, SecretProviderCapabilities, SecretProviderHelloRequest,
    SecretProviderHelloResponse, SecretProviderManifest, SecretProviderRequestBody,
    SecretProviderRequestEnvelope, SecretProviderResponseBody, SecretProviderResponseEnvelope,
    SecretProviderRpcError, SecretProviderRpcErrorCode, ValidatedManifest, is_source_lost_error,
    negotiate_secret_provider_version, shutdown_managed_secret_provider_hosts, validate_manifest,
};
pub use socket::{
    auth_provider_socket_name, driver_socket_name, secret_provider_socket_name, socket_name,
};
