//! The master-detail shell both products build by hand: the activity rail,
//! the header strip, the master-detail frame and the list row the sidebar's
//! tree drives.
//!
//! The module owns only what both products genuinely share. It never knows
//! what a rail entry opens, what a tree row represents or what the detail
//! pane hosts: the host lists entries and rows from its own state and wires
//! the handlers. Cursor movement, expansion state and keybindings stay with
//! the host's navigation state; these primitives render rows and frames from
//! that state.

mod frame;
mod header;
mod rail;
mod row;

pub use frame::{DEFAULT_LIST_WIDTH, MasterDetail};
pub use header::HeaderStrip;
pub use rail::{Rail, RailButtonColors, RailEntry, RailPlacement, rail_button_colors};
pub use row::{Chevron, NavRow, NavRowColors, inline_edit, nav_row_colors};
