//! Keyboard focus in the side panel beside the grid.
//!
//! The value panel, the row inspector, a document collection's Document
//! panel and the query builder share the workspace inspector rail. Ctrl+L
//! (`Command::FocusRight`) moves the keyboard from the grid into whichever of
//! them is open, and the grid then reports `ContextId::Inspector`: J and K
//! scroll the panel, Enter edits the value panel's text, and Ctrl+H or Escape
//! go back to the grid. The panels are the grid's own entities, so the grid
//! routes these keys itself and the workspace stays unaware of their kinds.
//!
//! The two query builders have keys of their own: the grid reports
//! `ContextId::QueryBuilder` or `ContextId::DocumentBuilder` (or the context
//! menu while the builder's action menu is open) and hands every key to the
//! builder, which moves its cursor over its rows and works their fields.

use super::DataGridPanel;
use crate::DataViewMode;
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::composites::RailOutcome;
use dbflux_components::tokens::Heights;
use gpui::*;

/// One step of keyboard scrolling inside a side panel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IslandScroll {
    LineUp,
    LineDown,
    PageUp,
    PageDown,
    Top,
    Bottom,
}

/// Distance one line step scrolls a list without a line height of its own.
const SCROLL_LINE: Pixels = Heights::ROW_COMPACT;

/// Moves `handle` by `step`, kept between the top and the end of the content.
pub(crate) fn scroll_by(handle: &ScrollHandle, step: IslandScroll) {
    let offset = handle.offset();
    let max_scroll = handle.max_offset().y.max(Pixels::ZERO);
    let page = (handle.bounds().size.height - SCROLL_LINE).max(SCROLL_LINE);

    let target = match step {
        IslandScroll::LineUp => offset.y + SCROLL_LINE,
        IslandScroll::LineDown => offset.y - SCROLL_LINE,
        IslandScroll::PageUp => offset.y + page,
        IslandScroll::PageDown => offset.y - page,
        IslandScroll::Top => Pixels::ZERO,
        IslandScroll::Bottom => -max_scroll,
    };

    handle.set_offset(point(offset.x, target.max(-max_scroll).min(Pixels::ZERO)));
}

/// The side panel the keyboard is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SideIsland {
    ValuePanel,
    RowInspector,
    DocumentInspector,
    QueryBuilder,
    DocumentBuilder,
}

impl DataGridPanel {
    /// The side panel this grid shows in the inspector rail, if any.
    fn open_side_island(&self, cx: &App) -> Option<SideIsland> {
        if self.builder.builder_open && self.builder.builder_panel.is_some() {
            return Some(SideIsland::QueryBuilder);
        }

        if self.collection.builder.open && self.collection.builder.panel.is_some() {
            return Some(SideIsland::DocumentBuilder);
        }

        if self.inspector.value_panel_open && self.inspector.value_panel.is_some() {
            return Some(SideIsland::ValuePanel);
        }

        if !self.row_inspector_is_open() {
            return None;
        }

        if self.collection.raw.is_some() && self.is_document_collection(cx) {
            return self
                .inspector
                .document_inspector_content
                .as_ref()
                .map(|_| SideIsland::DocumentInspector);
        }

        self.inspector
            .row_inspector_content
            .as_ref()
            .map(|_| SideIsland::RowInspector)
    }

    fn side_island_focus_handle(&self, island: SideIsland, cx: &App) -> Option<FocusHandle> {
        match island {
            SideIsland::ValuePanel => self
                .inspector
                .value_panel
                .as_ref()
                .map(|panel| panel.read(cx).focus_handle().clone()),
            SideIsland::RowInspector => self
                .inspector
                .row_inspector_content
                .as_ref()
                .map(|content| content.focus_handle(cx)),
            SideIsland::DocumentInspector => self
                .inspector
                .document_inspector_content
                .as_ref()
                .map(|content| content.focus_handle(cx)),
            SideIsland::QueryBuilder => self
                .builder
                .builder_panel
                .as_ref()
                .and_then(|panel| panel.read(cx).focus_handle.clone()),
            SideIsland::DocumentBuilder => self
                .collection
                .builder
                .panel
                .as_ref()
                .map(|panel| panel.read(cx).focus_handle(cx)),
        }
    }

    /// The side panel holding the keyboard, while it is still open.
    pub(super) fn focused_side_island(&self, cx: &App) -> Option<SideIsland> {
        self.focus
            .side_island
            .filter(|island| self.open_side_island(cx) == Some(*island))
    }

    /// The context a builder rail holding the keyboard reports, or `None`
    /// when the keyboard is in another side panel or none.
    pub(super) fn builder_rail_context(&self, cx: &App) -> Option<ContextId> {
        match self.focused_side_island(cx)? {
            SideIsland::QueryBuilder => {
                let panel = self.builder.builder_panel.as_ref()?.read(cx);
                Some(if panel.keyboard_menu_is_open() {
                    ContextId::ContextMenu
                } else {
                    ContextId::QueryBuilder
                })
            }
            SideIsland::DocumentBuilder => {
                let panel = self.collection.builder.panel.as_ref()?.read(cx);
                Some(if panel.keyboard_menu_is_open() {
                    ContextId::ContextMenu
                } else {
                    ContextId::DocumentBuilder
                })
            }
            _ => None,
        }
    }

    /// Whether the keyboard is in the document builder rail.
    pub(super) fn keyboard_in_document_builder(&self, cx: &App) -> bool {
        self.focused_side_island(cx) == Some(SideIsland::DocumentBuilder)
    }

    /// Hands a key to the builder rail holding the keyboard. `None` when
    /// the keyboard is in another side panel.
    fn dispatch_builder_rail_command(
        &mut self,
        island: SideIsland,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<bool> {
        if cmd == Command::FocusLeft {
            self.leave_side_island(window, cx);
            return Some(true);
        }

        let outcome = match island {
            SideIsland::QueryBuilder => self
                .builder
                .builder_panel
                .clone()?
                .update(cx, |panel, cx| panel.keyboard_command(cmd, window, cx)),
            SideIsland::DocumentBuilder => self
                .collection
                .builder
                .panel
                .clone()?
                .update(cx, |panel, cx| panel.keyboard_command(cmd, window, cx)),
            _ => return None,
        };

        match outcome {
            RailOutcome::Handled => Some(true),
            RailOutcome::Leave => {
                self.leave_side_island(window, cx);
                Some(true)
            }
            RailOutcome::Unhandled if cmd == Command::FocusRight => Some(true),
            RailOutcome::Unhandled => None,
        }
    }

    /// Moves the keyboard into the open side panel (`Command::FocusRight`).
    /// Returns false, leaving focus where it is, when no panel is open.
    pub(super) fn enter_side_island(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(island) = self.open_side_island(cx) else {
            return false;
        };

        let Some(handle) = self.side_island_focus_handle(island, cx) else {
            return false;
        };

        handle.focus(window, cx);
        self.focus.side_island = Some(island);

        // A click elsewhere, Tab or a pane move takes the keyboard out of the
        // panel without passing through the grid; the panel's context must
        // not outlive it.
        self.focus._side_island_blur =
            Some(
                cx.on_focus_out(&handle, window, |this, _event, _window, cx| {
                    this.focus.side_island = None;
                    cx.notify();
                }),
            );

        cx.notify();
        true
    }

    /// Hands the keyboard back from the side panel to the grid.
    fn leave_side_island(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus.side_island = None;
        self.focus._side_island_blur = None;

        let is_document_view = self.view_config.mode == DataViewMode::Document;
        self.restore_focus_after_context_menu(is_document_view, window, cx);
        cx.notify();
    }

    /// Handles the Inspector keys while the keyboard is in a side panel.
    /// Returns `None` for a command the panel does not take, which then runs
    /// as it would from the grid.
    pub(super) fn dispatch_side_island_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<bool> {
        self.focus.side_island?;

        let Some(island) = self.focused_side_island(cx) else {
            self.focus.side_island = None;
            self.focus._side_island_blur = None;
            return None;
        };

        if matches!(
            island,
            SideIsland::QueryBuilder | SideIsland::DocumentBuilder
        ) {
            return self.dispatch_builder_rail_command(island, cmd, window, cx);
        }

        let scroll = match cmd {
            Command::SelectNext => Some(IslandScroll::LineDown),
            Command::SelectPrev => Some(IslandScroll::LineUp),
            Command::PageDown => Some(IslandScroll::PageDown),
            Command::PageUp => Some(IslandScroll::PageUp),
            Command::SelectFirst => Some(IslandScroll::Top),
            Command::SelectLast => Some(IslandScroll::Bottom),
            _ => None,
        };

        if let Some(step) = scroll {
            self.scroll_side_island(island, step, cx);
            return Some(true);
        }

        match cmd {
            Command::FocusLeft => {
                self.leave_side_island(window, cx);
                Some(true)
            }
            Command::Cancel => {
                let editing_value = island == SideIsland::ValuePanel
                    && self
                        .inspector
                        .value_panel
                        .as_ref()
                        .is_some_and(|panel| panel.read(cx).editor_has_focus());

                match self.inspector.value_panel.clone() {
                    Some(panel) if editing_value => {
                        panel.update(cx, |panel, cx| panel.leave_editor(window, cx));
                    }
                    _ => self.leave_side_island(window, cx),
                }
                Some(true)
            }
            Command::FocusRight => Some(true),
            Command::Execute => {
                if island == SideIsland::ValuePanel
                    && let Some(panel) = self.inspector.value_panel.clone()
                {
                    panel.update(cx, |panel, cx| panel.focus_editor(window, cx));
                }
                Some(true)
            }
            _ => None,
        }
    }

    fn scroll_side_island(&self, island: SideIsland, step: IslandScroll, cx: &mut Context<Self>) {
        match island {
            SideIsland::ValuePanel => {
                if let Some(panel) = &self.inspector.value_panel {
                    panel.update(cx, |panel, cx| panel.scroll(step, cx));
                }
            }
            SideIsland::RowInspector => {
                if let Some(content) = &self.inspector.row_inspector_content {
                    content.update(cx, |content, cx| content.scroll(step, cx));
                }
            }
            SideIsland::DocumentInspector => {
                if let Some(content) = &self.inspector.document_inspector_content {
                    content.update(cx, |content, cx| content.scroll(step, cx));
                }
            }
            // The builder rails move a cursor instead.
            SideIsland::QueryBuilder | SideIsland::DocumentBuilder => {}
        }
    }
}
