pub mod outcome;
pub mod service;
pub mod store;

pub use outcome::{ApprovalOutcome, classify_execution};
pub use service::{
    ApprovalDecision, ApprovalError, ApprovalService, ApprovedExecution,
    MAX_REJECTION_REASON_CHARS, RejectedExecution, normalize_rejection_reason,
};
pub use store::{
    ExecutionPlan, InMemoryPendingExecutionStore, PendingExecution, PendingExecutionStore,
    PendingStatus, PendingStoreError, approval_matches_plan,
};
