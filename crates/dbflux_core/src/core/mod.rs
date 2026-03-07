pub(crate) mod error;
pub(crate) mod error_formatter;
pub(crate) mod shutdown;
pub(crate) mod task;
pub(crate) mod traits;
pub(crate) mod value;

pub use error::DbError;
pub use error_formatter::{
    ConnectionErrorFormatter, DefaultErrorFormatter, ErrorLocation, FormattedError,
    QueryErrorFormatter, sanitize_uri,
};
pub use shutdown::{ShutdownCoordinator, ShutdownPhase};
pub use task::{
    CancelToken, TaskId, TaskKind, TaskManager, TaskSlot, TaskSnapshot, TaskStatus, TaskTarget,
};
pub use traits::{
    CodeGenScope, CodeGeneratorInfo, Connection, DbDriver, KeyValueApi, NoopCancelHandle,
    QueryCancelHandle, SchemaFeatures, SchemaLoadingStrategy,
};
pub use value::Value;
