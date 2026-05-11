pub mod auth;
pub mod auth_provider_client;
pub mod auth_provider_protocol;
pub mod driver_protocol;
pub mod envelope;
pub mod framing;
pub mod protocol;
pub mod socket;

pub use auth::{
    APP_CONTROL_AUTH_TOKEN_ENV, AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV, DRIVER_RPC_AUTH_TOKEN_ENV,
    app_control_token_path, init_process_auth_tokens, read_app_control_token,
    write_app_control_token,
};
pub use auth_provider_client::{
    IpcServiceLaunchConfig, RpcAuthProvider, build_fetch_dependencies, hash_dependencies,
    negotiate_auth_provider_version,
};
pub use auth_provider_protocol::{
    AuthProviderHelloRequest, AuthProviderHelloResponse, AuthProviderHelloResponseV1_1,
    AuthProviderHelloResponseV1_2, AuthProviderRequestBody, AuthProviderRequestEnvelope,
    AuthProviderResponseBody, AuthProviderResponseEnvelope, AuthProviderRpcError,
    AuthProviderRpcErrorCode, AuthSessionDto, AuthSessionStateDto, FetchFieldOptionsError,
    FetchFieldOptionsRequest, FetchFieldOptionsResponse, LoginRequest, LoginUrlProgress,
    ResolveCredentialsRequest, ResolvedCredentialsDto, ValidateSessionRequest, parse_auth_profile,
};
pub use driver_protocol::{
    DriverCapability, DriverHelloRequest, DriverHelloResponse, DriverRequestBody,
    DriverRequestEnvelope, DriverResponseBody, DriverResponseEnvelope, DriverRpcError,
    DriverRpcErrorCode, QueryRequestDto, QueryResultChunk, QueryResultDto, QueryResultShapeDto,
};
pub use envelope::{
    APP_CONTROL_VERSION, AUTH_PROVIDER_RPC_API_CONTRACT, AUTH_PROVIDER_RPC_SUPPORTED_VERSIONS,
    AUTH_PROVIDER_RPC_V1_0, AUTH_PROVIDER_RPC_VERSION, DRIVER_RPC_API_CONTRACT,
    DRIVER_RPC_SUPPORTED_VERSIONS, DRIVER_RPC_V1_0, DRIVER_RPC_VERSION, ProtocolVersion,
    RpcApiContract, RpcApiFamily, auth_provider_rpc_supported_versions,
    driver_rpc_supported_versions, negotiate_highest_mutual_version,
};
pub use framing::{recv_msg, send_msg};
pub use protocol::{AppControlRequest, AppControlResponse, IpcMessage, IpcResponse};
pub use socket::{auth_provider_socket_name, driver_socket_name, socket_name};
