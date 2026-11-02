mod breadcrumb;
// Moved to `bolt`; re-exported as the module at its old path so every
// `composites::control_shell` / `composites::header` / `composites::list_row`
// and `activity_rail` path keeps compiling.
pub use bolt::composites::{activity_rail, control_shell, header, list_row};
mod master_detail_list;
mod notification_center;
mod rail_nav;
mod refresh_split_button;
mod wizard_rail;

#[cfg(test)]
mod tests;

/// Installs the icons bolt's title-bar controls draw: the app's own search
/// and bell glyphs. `theme::init` calls it.
pub fn install_shell_icons(cx: &mut gpui::App) {
    cx.set_global(ShellIcons {
        search: crate::icons::AppIcon::Search.into(),
        bell: crate::icons::AppIcon::Bell.into(),
    });
}

// The generic composites moved to `bolt`; re-exported at these paths so call
// sites keep compiling.
pub use bolt::composites::{
    ACTIVITY_RAIL_KEY_CONTEXT, ActivityRail, BellUrgency, COMMAND_SEARCH_KEY_CONTEXT,
    CommandSearch, NotificationBell, RailButtonColors, RailEntry, RailPlacement, ShellIcons,
    StatusChip, rail_button_colors,
};
pub use bolt::composites::{
    EmptyState, EmptyStateAction, Island, IslandInspection, ListRow, MenuItem, SplitButton,
    docked_island_frame, document_tab, document_tab_bar, document_tab_title, field_row,
    field_row_vertical, field_row_vertical_with_desc, field_row_with_desc,
    field_row_with_label_width, inline_tab, inline_tab_bar, inspect_island, island_edge_ring,
    island_fill, menu_frame, menu_row, render_menu_container, render_menu_header, render_menu_item,
    render_menu_items, render_menu_overlay, render_separator, result_tab, result_tab_bar,
};

pub use breadcrumb::{Breadcrumb, BreadcrumbSegment};
pub use control_shell::control_shell;
pub use header::{
    collapsible_bar, page_header, page_header_with_action, panel_header, panel_header_with_actions,
    section_header,
};
pub use master_detail_list::{
    MasterDetailAction, MasterDetailActionKind, MasterDetailItem, MasterDetailListConfig, RowKind,
    master_detail_row_kind, render_master_detail_list,
};
pub use notification_center::{
    NotificationFilterChip, NotificationGroupSection, NotificationIconTone, NotificationPopover,
    NotificationRow,
};
pub use rail_nav::{
    RailHandler, RailMark, RailMenuEntry, RailMoveHandler, RailNav, RailOutcome, RailOwner,
    RailRow, RailTarget, rail_command, rail_scroll_area, render_rail_menu,
};
pub use refresh_split_button::{refresh_policy_label, refresh_split_button};
pub use wizard_rail::{
    RailItem, WIZARD_MODAL_HEIGHT_FRACTION, WIZARD_MODAL_WIDTH, render_wizard_progress_bar,
    render_wizard_rail, render_wizard_stepper, wizard_progress_fraction,
};
