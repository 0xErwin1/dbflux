//! Key context identifiers that containers set on themselves.
//!
//! They name the panel keyboard focus is in, so a keymap predicate can say
//! where a user binding applies (`CodeEditor > Input`, `SidebarPanel`). No
//! default binding uses them: the defaults match the context a window root
//! reports, or an element context such as `DataTable` or `Modal`.

/// The code editor pane of a query or script document.
pub const CODE_EDITOR: &str = "CodeEditor";
/// The connections and scripts sidebar.
pub const SIDEBAR_PANEL: &str = "SidebarPanel";
/// The activity rail on the left edge of the workspace.
pub const ACTIVITY_RAIL: &str = "ActivityRail";
/// The command search field in the title bar.
pub const COMMAND_SEARCH: &str = "CommandSearch";
/// A result panel: the grid, record, chart and text views and their bars.
pub const RESULT_PANEL: &str = "ResultPanel";
/// The row inspector beside a grid.
pub const ROW_INSPECTOR: &str = "RowInspector";
/// The command console of a key-value document. Kept beside
/// [`NATIVE_CONSOLE`] so predicates written against it still match.
pub const KEY_VALUE_CONSOLE: &str = "KeyValueConsole";
/// The native command console docked under a document.
pub const NATIVE_CONSOLE: &str = "NativeConsole";
/// The filter, sort and projection bar of a document collection.
pub const DOCUMENT_QUERY_BAR: &str = "DocumentQueryBar";
/// The sampled schema view of a document collection.
pub const DOCUMENT_SCHEMA: &str = "DocumentSchema";
/// The aggregation pipeline view of a document collection.
pub const DOCUMENT_AGGREGATE: &str = "DocumentAggregate";
/// The dashboards panel of the sidebar.
pub const DASHBOARDS_PANEL: &str = "DashboardsPanel";
/// The active section of the settings window.
pub const SETTINGS_SECTION: &str = "SettingsSection";

/// Every container identifier, for predicate validation.
pub const ALL: &[&str] = &[
    CODE_EDITOR,
    SIDEBAR_PANEL,
    ACTIVITY_RAIL,
    COMMAND_SEARCH,
    RESULT_PANEL,
    ROW_INSPECTOR,
    KEY_VALUE_CONSOLE,
    NATIVE_CONSOLE,
    DOCUMENT_QUERY_BAR,
    DOCUMENT_SCHEMA,
    DOCUMENT_AGGREGATE,
    DASHBOARDS_PANEL,
    SETTINGS_SECTION,
];
