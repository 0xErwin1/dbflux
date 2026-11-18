//! Cascading (flyout) menus.
//!
//! A menu is a tree of [`MenuNode`]s. A node with children opens a flyout
//! beside its row, and that flyout is itself a menu, so the cascade nests to
//! any depth. The module has no product type and stores no closure: activation
//! hands the row's [`MenuNode::id`] back to the caller, which maps it to its own
//! action.
//!
//! The component is a plain state value plus render functions, not a GPUI
//! entity — the same shape as the other shared surfaces a product owns and
//! re-renders. A surface keeps one [`MenuNav`] and one [`HoverIntent`] per open
//! menu, feeds pointer and keyboard events into them, and renders the cascade
//! with [`render_menu_tree`].
//!
//! Four pieces are pure and tested without a window:
//!
//! - [`model`]: the row tree a product builds from its own state.
//! - [`nav`]: the cursor path, and the keyboard transitions across levels.
//! - [`placement`]: where a flyout goes, and when it flips or scrolls.
//! - [`hover`]: when a hovered row opens, and when leaving keeps it open.

mod hover;
mod model;
mod nav;
mod placement;
mod render;
#[cfg(test)]
mod tests;

pub use hover::{HOVER_CLOSE_GRACE, HOVER_OPEN_DELAY, HoverIntent, HoverTick, in_corridor};
pub use model::MenuNode;
pub use nav::{BackAction, ChildAction, MenuCursor, MenuNav};
pub use placement::{
    FlyoutPlacement, FlyoutSide, FlyoutSpec, LevelMetrics, level_height, place_flyout,
};
pub use render::{MenuTreeEvent, MenuTreeLayout, render_menu_tree};
