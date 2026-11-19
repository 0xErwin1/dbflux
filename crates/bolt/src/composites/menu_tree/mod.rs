//! Cascading (flyout) menus: a tree of [`MenuNode`]s where a row with children
//! opens a flyout beside it.
//!
//! The component is state plus pure render functions, not a GPUI entity, so the
//! surface keeps ownership: it holds one [`MenuNav`] and one [`HoverIntent`] per
//! open menu and maps the activated row's id to its own action.

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
