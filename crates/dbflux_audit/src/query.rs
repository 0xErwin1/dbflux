#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditQueryFilter {
    pub actor_id: Option<String>,
    pub tool_id: Option<String>,
    pub decision: Option<String>,
    pub start_epoch_ms: Option<i64>,
    pub end_epoch_ms: Option<i64>,
    pub limit: Option<usize>,
    // Extended filter fields
    pub level: Option<String>,
    pub category: Option<String>,
    pub action: Option<String>,
    pub source_id: Option<String>,
    pub outcome: Option<String>,
    pub object_type: Option<String>,
    pub free_text: Option<String>,
    pub correlation_id: Option<String>,
}
