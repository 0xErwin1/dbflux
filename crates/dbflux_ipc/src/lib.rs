pub mod driver_protocol;
pub mod envelope;
pub mod framing;
pub mod protocol;
pub mod socket;

pub use driver_protocol::{
    DriverCapability, DriverHelloRequest, DriverHelloResponse, DriverRequestBody,
    DriverRequestEnvelope, DriverResponseBody, DriverResponseEnvelope, DriverRpcError,
    DriverRpcErrorCode, QueryRequestDto, QueryResultChunk, QueryResultDto, QueryResultShapeDto,
};
pub use envelope::{ProtocolVersion, APP_CONTROL_VERSION, DRIVER_RPC_VERSION};
pub use framing::{recv_msg, send_msg};
pub use protocol::{AppControlRequest, AppControlResponse, IpcMessage, IpcResponse};
pub use socket::{ensure_socket_dir, socket_path};
