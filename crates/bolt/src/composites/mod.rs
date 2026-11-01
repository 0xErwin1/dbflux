//! Composites: the panel headers, the control shell, the list row, the tabs,
//! menus, empty states, split buttons, splitters, field rows and islands a
//! screen builds out of the primitives and controls, and the shell chrome: the
//! activity rail, the title bar's command search and notification bell, and the
//! status bar's chips.

pub mod control_shell;
pub mod header;
pub mod list_row;

pub mod activity_rail;
mod empty_state;
mod field_row;
mod island;
mod menu_item;
mod menu_popup;
mod shell_bar;
mod split_button;
mod splitter;
mod status_chip;
mod tabs;

pub use activity_rail::{
    ACTIVITY_RAIL_KEY_CONTEXT, ActivityRail, RailButtonColors, RailEntry, RailPlacement,
    rail_button_colors,
};
pub use empty_state::{EmptyState, EmptyStateAction};
pub use field_row::{
    field_row, field_row_vertical, field_row_vertical_with_desc, field_row_with_desc,
    field_row_with_label_width,
};
pub use island::{
    Island, IslandInspection, docked_island_frame, inspect_island, island_edge_ring, island_fill,
};
pub use list_row::ListRow;
pub use menu_item::{
    MenuItem, menu_frame, menu_row, render_menu_container, render_menu_header, render_menu_item,
    render_separator,
};
pub use menu_popup::{render_menu_items, render_menu_overlay};
pub use shell_bar::{
    BellUrgency, COMMAND_SEARCH_KEY_CONTEXT, CommandSearch, NotificationBell, ShellIcons,
};
pub use split_button::SplitButton;
pub use splitter::{SplitAxis, Splitter};
pub use status_chip::StatusChip;
pub use tabs::{
    document_tab, document_tab_bar, document_tab_title, inline_tab, inline_tab_bar, result_tab,
    result_tab_bar,
};
