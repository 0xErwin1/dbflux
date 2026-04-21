pub mod auth;
pub mod driver_protocol;
pub mod envelope;
pub mod framing;
pub mod protocol;
pub mod socket;

pub use auth::{
    APP_CONTROL_AUTH_TOKEN_ENV, DRIVER_RPC_AUTH_TOKEN_ENV, app_control_token_path,
    init_process_auth_tokens, read_app_control_token, write_app_control_token,
};
pub use driver_protocol::{
    DriverCapability, DriverHelloRequest, DriverHelloResponse, DriverRequestBody,
    DriverRequestEnvelope, DriverResponseBody, DriverResponseEnvelope, DriverRpcError,
    DriverRpcErrorCode, QueryRequestDto, QueryResultChunk, QueryResultDto, QueryResultShapeDto,
};
pub use envelope::{
    APP_CONTROL_VERSION, AUTH_PROVIDER_RPC_API_CONTRACT, DRIVER_RPC_API_CONTRACT,
    DRIVER_RPC_SUPPORTED_VERSIONS, DRIVER_RPC_V1_0, DRIVER_RPC_VERSION, ProtocolVersion,
    RpcApiContract, RpcApiFamily, driver_rpc_supported_versions, negotiate_highest_mutual_version,
};
pub use framing::{recv_msg, send_msg};
pub use protocol::{AppControlRequest, AppControlResponse, IpcMessage, IpcResponse};
pub use socket::{driver_socket_name, socket_name};
