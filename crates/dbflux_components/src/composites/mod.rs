mod activity_rail;
mod breadcrumb;
mod control_shell;
mod header;
mod island;
mod master_detail_list;
mod notification_center;
mod rail_nav;
mod refresh_split_button;
mod shell_bar;
mod wizard_rail;

// The generic composites moved to `bolt`; re-exported at these paths so call
// sites keep compiling.
pub use bolt::composites::{
    EmptyState, EmptyStateAction, ListRow, MenuItem, SplitButton, document_tab, document_tab_bar,
    document_tab_title, field_row, field_row_vertical, field_row_vertical_with_desc,
    field_row_with_desc, field_row_with_label_width, inline_tab, inline_tab_bar, menu_frame,
    menu_row, render_menu_container, render_menu_header, render_menu_item, render_menu_items,
    render_menu_overlay, render_separator, result_tab, result_tab_bar,
};

pub use activity_rail::{
    ActivityRail, RailButtonColors, RailEntry, RailPlacement, rail_button_colors,
};
pub use breadcrumb::{Breadcrumb, BreadcrumbSegment};
pub use control_shell::control_shell;
pub use header::{
    collapsible_bar, page_header, page_header_with_action, panel_header, panel_header_with_actions,
    section_header,
};
pub use island::{
    Island, IslandInspection, docked_island_frame, inspect_island, island_edge_ring, island_fill,
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
pub use shell_bar::{BellUrgency, CommandSearch, NotificationBell};
pub use wizard_rail::{
    RailItem, WIZARD_MODAL_HEIGHT_FRACTION, WIZARD_MODAL_WIDTH, render_wizard_progress_bar,
    render_wizard_rail, render_wizard_stepper, wizard_progress_fraction,
};
