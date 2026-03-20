#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditQueryFilter {
    pub actor_id: Option<String>,
    pub tool_id: Option<String>,
    pub decision: Option<String>,
    pub start_epoch_ms: Option<i64>,
    pub end_epoch_ms: Option<i64>,
    pub limit: Option<usize>,
}
