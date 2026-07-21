use crate::observability::types::EventCategory;

/// Target prefix used for the bridge's own internal debug events.
///
/// Events emitted from this target are excluded from the audit path to prevent
/// recursive feedback loops where bridge debug logs feed back into themselves.
pub(crate) const BRIDGE_INTERNAL_TARGET: &str = "dbflux_core::observability::tracing_bridge";

/// Static prefix-to-category mapping table.
///
/// Entries are matched using longest-prefix semantics: the most specific (longest)
/// matching prefix wins. This table is retained for documentation purposes and
/// `action` derivation, but `coerce_to_bridge_allowed` always returns `System`
/// (see V1 resolution in tasks artifact: `validate_event` requires structured
/// fields like `connection_id` for Connection and `object_type`+`object_id`
/// for Config that free-form log events cannot supply).
pub(crate) const PREFIX_CATEGORY_MAP: &[(&str, EventCategory)] = &[
    // Connection plane (would be Connection but coerces to System — see above)
    ("dbflux_core::connection", EventCategory::Connection),
    ("dbflux_core::pipeline", EventCategory::Connection),
    ("dbflux_app::access_manager", EventCategory::Connection),
    ("dbflux_ssh", EventCategory::Connection),
    ("dbflux_proxy", EventCategory::Connection),
    ("dbflux_aws", EventCategory::Connection),
    ("dbflux_ssm", EventCategory::Connection),
    ("dbflux_driver_ipc", EventCategory::Connection),
    // Config plane (would be Config but coerces to System — see above)
    ("dbflux_app::config", EventCategory::Config),
    ("dbflux_app::aws_config", EventCategory::Config),
    ("dbflux_storage", EventCategory::Config),
    ("dbflux_core::storage", EventCategory::Config),
    // Driver crates: Query would require connection_id+driver_id, unavailable here
    ("dbflux_driver_", EventCategory::System),
    ("dbflux_core::facade", EventCategory::System),
    ("dbflux_mcp", EventCategory::System),
    ("dbflux_mcp_server", EventCategory::System),
    ("dbflux_app::mcp_command", EventCategory::System),
    // System plane
    ("dbflux_app::app_state", EventCategory::System),
    ("dbflux_ipc", EventCategory::System),
    ("dbflux_driver_host", EventCategory::System),
    ("dbflux_ui", EventCategory::System),
];

/// Resolves the `EventCategory` for a bridge event.
///
/// Resolution priority:
/// 1. If `explicit` is provided and parses to a known `EventCategory`, use it.
/// 2. Otherwise, longest-prefix match against `PREFIX_CATEGORY_MAP`.
/// 3. If no prefix matches, return `System`.
///
/// All resolved categories are passed through `coerce_to_bridge_allowed`, which
/// unconditionally returns `System`. This guarantees `validate_event` never
/// rejects bridge events for missing structured fields.
pub(crate) fn resolve_category(target: &str, explicit: Option<&str>) -> EventCategory {
    if let Some(name) = explicit
        && let Some(cat) = EventCategory::from_str_repr(name)
    {
        return coerce_to_bridge_allowed(cat);
    }

    let resolved = PREFIX_CATEGORY_MAP
        .iter()
        .filter(|(prefix, _)| target.starts_with(prefix))
        .max_by_key(|(prefix, _)| prefix.len())
        .map(|(_, cat)| *cat)
        .unwrap_or(EventCategory::System);

    coerce_to_bridge_allowed(resolved)
}

/// Coerces any category to the bridge-allowed set.
///
/// The bridge-allowed set is `{System}` only (V1 resolution). All other categories
/// require structured fields (`connection_id`, `object_type`, etc.) that free-form
/// log events cannot supply, so they would fail `validate_event`.
fn coerce_to_bridge_allowed(cat: EventCategory) -> EventCategory {
    match cat {
        EventCategory::System => EventCategory::System,
        _ => EventCategory::System,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_system_is_honored() {
        assert_eq!(
            resolve_category("some::module", Some("system")),
            EventCategory::System
        );
    }

    #[test]
    fn explicit_connection_coerces_to_system() {
        assert_eq!(
            resolve_category("some::module", Some("connection")),
            EventCategory::System
        );
    }

    #[test]
    fn explicit_config_coerces_to_system() {
        assert_eq!(
            resolve_category("some::module", Some("config")),
            EventCategory::System
        );
    }

    #[test]
    fn explicit_query_coerces_to_system() {
        assert_eq!(
            resolve_category("some::module", Some("query")),
            EventCategory::System
        );
    }

    #[test]
    fn explicit_mcp_coerces_to_system() {
        assert_eq!(
            resolve_category("some::module", Some("mcp")),
            EventCategory::System
        );
    }

    #[test]
    fn connection_prefix_coerces_to_system() {
        assert_eq!(
            resolve_category("dbflux_core::connection::manager", None),
            EventCategory::System
        );
    }

    #[test]
    fn storage_prefix_coerces_to_system() {
        assert_eq!(
            resolve_category("dbflux_storage::repositories::audit", None),
            EventCategory::System
        );
    }

    #[test]
    fn unknown_prefix_returns_system() {
        assert_eq!(
            resolve_category("totally_unknown::module", None),
            EventCategory::System
        );
    }

    #[test]
    fn longest_prefix_wins() {
        // "dbflux_core::connection" is longer than any shorter match that might exist
        assert_eq!(
            resolve_category("dbflux_core::connection::very_specific_submodule", None),
            EventCategory::System
        );
    }

    #[test]
    fn all_prefix_entries_resolve_deterministically() {
        for (prefix, _) in PREFIX_CATEGORY_MAP {
            let result = resolve_category(prefix, None);
            assert_eq!(
                result,
                EventCategory::System,
                "prefix '{}' should resolve to System (V1 coercion)",
                prefix
            );
        }
    }
}
