pub(crate) mod error;
pub(crate) mod error_formatter;
pub(crate) mod log_err;
pub(crate) mod shutdown;
pub(crate) mod task;
pub(crate) mod traits;
pub(crate) mod value;

pub use error::DbError;
pub use error_formatter::{
    ConnectionErrorFormatter, DefaultErrorFormatter, ErrorLocation, FormattedError,
    QueryErrorFormatter, sanitize_uri,
};
pub use log_err::LogErr;
pub use shutdown::{ShutdownCoordinator, ShutdownPhase};
pub use task::{
    CancelToken, TaskId, TaskKind, TaskManager, TaskSlot, TaskSnapshot, TaskStatus, TaskTarget,
};
pub use traits::{
    BucketCreateOptions, BucketCreateOutcome, BucketDetails, BucketEncryption, BucketInfo,
    BucketSizeEstimate, CodeGenScope, CodeGeneratorInfo, Connection, ConnectionExt,
    ConnectionOverrides, DbDriver, DeletePrefixOutcome, DocumentConnection, EventStreamTarget,
    KeyValueApi, KeyValueConnection, NoopCancelHandle, ObjectListingPage, ObjectMetadata,
    ObjectStoreConnection, ObjectSummary, ObjectVersionSummary, PresignMethod, QueryCancelHandle,
    RelationalConnection, SchemaDropTarget, SchemaFeatures, SchemaLoadingStrategy,
    SchemaObjectKind, SourceContextSpec, SourceQueryMode, VersioningStatus,
};
pub use value::Value;
