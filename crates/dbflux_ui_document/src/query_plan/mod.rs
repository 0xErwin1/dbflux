//! The Plan result tab: a query's execution plan next to its result tabs.
//!
//! [`PlanView`] is the entity a code document hosts for the Plan tab. It
//! holds the plan the driver returned ([`PlanModel`]), which of the three
//! views shows it (Diagram, Tree, Raw), the selected node and the diagram's
//! camera, and it draws the view switch, the totals, the warnings and the
//! footer around the view. The plan never runs the query: the code document
//! asks the driver for it and hands the answer, or the reason there is none,
//! to the view.
//!
//! - [`model`]: the plan with the numbers the views show worked out.
//! - [`diagram`]: nodes and edges on the shared diagram canvas.
//! - [`tree`], [`raw`]: the list and the engine's own text.
//! - [`inspector`]: the node detail the workspace rail shows.
//! - [`chrome`], [`states`]: the switch row, the footer and the states a plan
//!   can be in besides ready.
//! - [`actions`]: the pane actions, the keyboard path of every control.

mod actions;
mod chrome;
mod diagram;
mod ids;
mod inspector;
pub(crate) mod model;
mod raw;
mod states;
mod tooltip;
mod tree;

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::time::Duration;

use dbflux_app::keymap::Command;
use dbflux_components::diagram as canvas;
use dbflux_components::primitives::SegmentBounds;
use dbflux_core::QueryPlan;
use dbflux_diagram::Direction;
use gpui::prelude::*;
use gpui::{
    AnyView, App, ClipboardItem, Context, Entity, EventEmitter, FocusHandle, Focusable, Pixels,
    Point, ScrollHandle, SharedString, Size, Subscription, Window, div, px,
};
use gpui_component::ActiveTheme;
use uuid::Uuid;

use self::ids::tagged;
use self::inspector::{PlanInspector, PlanInspectorEvent};
use self::model::{NodeId, PlanModel};

pub(crate) use actions::pane_actions;

/// Whether the connection's driver says it has no query plans. A driver that
/// declares no query capabilities is not taken to lack them. Read from the
/// declared capabilities, so no driver is named.
pub(crate) fn declares_no_plans(connected: &dbflux_core::ConnectedProfile) -> bool {
    connected
        .connection
        .metadata()
        .query
        .as_ref()
        .is_some_and(|query| !query.supports_explain)
}

/// The three ways to read a plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum PlanViewMode {
    #[default]
    Diagram,
    Tree,
    Raw,
}

impl PlanViewMode {
    pub(crate) const ALL: [Self; 3] = [Self::Diagram, Self::Tree, Self::Raw];

    /// The id suffix of the mode's switch segment and pane action.
    pub(crate) fn key(self) -> &'static str {
        match self {
            Self::Diagram => "diagram",
            Self::Tree => "tree",
            Self::Raw => "raw",
        }
    }

    fn next(self) -> Self {
        match self {
            Self::Diagram => Self::Tree,
            Self::Tree => Self::Raw,
            Self::Raw => Self::Diagram,
        }
    }
}

/// A failed plan, with what the inline error offers: the correlation id
/// `View in Audit` opens and the text Copy puts on the clipboard.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlanFailure {
    pub summary: String,
    pub correlation_id: Uuid,
    pub clipboard: String,
}

/// What the tab shows.
pub(crate) enum PlanState {
    /// The driver is producing the plan.
    Loading,
    Ready(Box<PlanModel>),
    /// The connection's driver has no query plans.
    Unsupported {
        connection: String,
    },
    /// The text sent held more than one statement; `message` is the driver's.
    MultiStatement {
        message: String,
    },
    Failed(PlanFailure),
    Cancelled,
}

/// What the view asks of the document that hosts it.
pub(crate) enum PlanViewEvent {
    /// The user acted in the plan, so the keyboard belongs to it.
    Focused,
    /// Mount the node inspector in the workspace rail.
    OpenInspector {
        title: SharedString,
        content: AnyView,
    },
    CloseInspector,
    /// Run the plan again for the same text.
    RetryRequested,
    /// Stop the plan that is loading.
    CancelRequested,
}

impl EventEmitter<PlanViewEvent> for PlanView {}

/// Pan and zoom of the diagram, and the viewport they are measured in.
struct Camera {
    zoom: f32,
    pan: Point<Pixels>,
    viewport_origin: Point<Pixels>,
    viewport_size: Size<Pixels>,
    /// A fit is due once the viewport has a size.
    pending_fit: bool,
    /// Where the pointer was at the last pan step, while a pan drag runs.
    pan_anchor: Option<Point<Pixels>>,
}

impl Camera {
    fn new() -> Self {
        Self {
            zoom: 1.0,
            pan: Point::default(),
            viewport_origin: Point::default(),
            viewport_size: Size::default(),
            pending_fit: true,
            pan_anchor: None,
        }
    }
}

struct InspectorMount {
    content: Entity<PlanInspector>,
    _subscription: Subscription,
}

pub(crate) struct PlanView {
    state: PlanState,
    mode: PlanViewMode,
    /// What the plan covers, as the info slot says it ("Statement L7–16").
    scope: String,
    /// How long the driver took to produce the plan.
    elapsed: Option<Duration>,
    selected: Option<NodeId>,
    /// Tree rows whose children are hidden.
    collapsed: HashSet<NodeId>,
    geometry: Option<diagram::DiagramGeometry>,
    camera: Camera,
    switch_bounds: SegmentBounds,
    switch_origin: Point<Pixels>,
    inspector: Option<InspectorMount>,
    inspector_open: bool,
    focus_handle: FocusHandle,
    tree_scroll: ScrollHandle,
}

impl PlanView {
    /// A view waiting for its plan.
    pub(crate) fn new(scope: String, mode: PlanViewMode, cx: &mut Context<Self>) -> Self {
        Self {
            state: PlanState::Loading,
            mode,
            scope,
            elapsed: None,
            selected: None,
            collapsed: HashSet::new(),
            geometry: None,
            camera: Camera::new(),
            switch_bounds: SegmentBounds::default(),
            switch_origin: Point::default(),
            inspector: None,
            inspector_open: false,
            focus_handle: cx.focus_handle(),
            tree_scroll: ScrollHandle::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn scope(&self) -> &str {
        &self.scope
    }

    pub(crate) fn mode(&self) -> PlanViewMode {
        self.mode
    }

    pub(crate) fn state(&self) -> &PlanState {
        &self.state
    }

    #[cfg(test)]
    pub(crate) fn selected(&self) -> Option<NodeId> {
        self.selected
    }

    pub(crate) fn is_loading(&self) -> bool {
        matches!(self.state, PlanState::Loading)
    }

    #[cfg(test)]
    pub(crate) fn inspector_open(&self) -> bool {
        self.inspector_open
    }

    pub(crate) fn model(&self) -> Option<&PlanModel> {
        match &self.state {
            PlanState::Ready(model) => Some(model),
            _ => None,
        }
    }

    // ── States ───────────────────────────────────────────────────────────────

    pub(crate) fn set_ready(
        &mut self,
        plan: &QueryPlan,
        elapsed: Duration,
        cx: &mut Context<Self>,
    ) {
        let model = PlanModel::from_plan(plan);
        self.geometry = Some(diagram::DiagramGeometry::build(&model));
        self.camera = Camera::new();
        self.selected = None;
        self.collapsed.clear();
        self.elapsed = Some(elapsed);
        self.state = PlanState::Ready(Box::new(model));
        self.close_inspector(cx);
        cx.notify();
    }

    pub(crate) fn set_unsupported(&mut self, connection: String, cx: &mut Context<Self>) {
        self.set_without_plan(PlanState::Unsupported { connection }, cx);
    }

    pub(crate) fn set_multi_statement(&mut self, message: String, cx: &mut Context<Self>) {
        self.set_without_plan(PlanState::MultiStatement { message }, cx);
    }

    pub(crate) fn set_failed(&mut self, failure: PlanFailure, cx: &mut Context<Self>) {
        self.set_without_plan(PlanState::Failed(failure), cx);
    }

    pub(crate) fn set_cancelled(&mut self, cx: &mut Context<Self>) {
        self.set_without_plan(PlanState::Cancelled, cx);
    }

    fn set_without_plan(&mut self, state: PlanState, cx: &mut Context<Self>) {
        self.geometry = None;
        self.selected = None;
        self.state = state;
        self.close_inspector(cx);
        cx.notify();
    }

    // ── View switch ──────────────────────────────────────────────────────────

    pub(crate) fn set_mode(&mut self, mode: PlanViewMode, cx: &mut Context<Self>) {
        if self.mode == mode {
            return;
        }

        self.mode = mode;
        if mode == PlanViewMode::Diagram {
            self.camera.pending_fit = true;
        }
        cx.emit(PlanViewEvent::Focused);
        cx.notify();
    }

    // ── Selection and inspector ──────────────────────────────────────────────

    /// Selects a node and shows it in the inspector.
    pub(crate) fn select(&mut self, id: NodeId, cx: &mut Context<Self>) {
        let Some(model) = self.model() else {
            return;
        };
        if model.node(id).is_none() {
            return;
        }

        self.selected = Some(id);
        self.reveal_in_tree(id);
        self.scroll_tree_to(id);
        self.open_inspector(cx);
        cx.emit(PlanViewEvent::Focused);
        cx.notify();
    }

    fn scroll_tree_to(&self, id: NodeId) {
        let Some(model) = self.model() else {
            return;
        };

        if let Some(position) = tree::visible_rows(model, &self.collapsed)
            .iter()
            .position(|row| *row == id)
        {
            self.tree_scroll.scroll_to_item(position);
        }
    }

    fn reveal_in_tree(&mut self, id: NodeId) {
        let Some(model) = self.model() else {
            return;
        };

        let mut ancestors = Vec::new();
        let mut parent = model.node(id).and_then(|node| node.parent);
        while let Some(ancestor) = parent {
            ancestors.push(ancestor);
            parent = model.node(ancestor).and_then(|node| node.parent);
        }

        for ancestor in ancestors {
            self.collapsed.remove(&ancestor);
        }
    }

    fn open_inspector(&mut self, cx: &mut Context<Self>) {
        let Some(node) = self
            .selected
            .and_then(|id| self.model().and_then(|model| model.node(id)))
            .cloned()
        else {
            return;
        };
        let title = SharedString::from(node.operation.clone());

        let content = match &self.inspector {
            Some(mount) => {
                mount
                    .content
                    .update(cx, |inspector, cx| inspector.show(node, cx));
                mount.content.clone()
            }
            None => {
                let content = cx.new(|cx| PlanInspector::new(node, cx));
                let subscription = cx.subscribe(
                    &content,
                    |this, _, event: &PlanInspectorEvent, cx| match event {
                        PlanInspectorEvent::Close => this.dismiss_inspector(cx),
                    },
                );
                self.inspector = Some(InspectorMount {
                    content: content.clone(),
                    _subscription: subscription,
                });
                content
            }
        };

        self.inspector_open = true;
        cx.emit(PlanViewEvent::OpenInspector {
            title,
            content: content.into(),
        });
    }

    /// The inspector's own close button: the rail goes, the selection stays.
    fn dismiss_inspector(&mut self, cx: &mut Context<Self>) {
        self.inspector_open = false;
        cx.emit(PlanViewEvent::CloseInspector);
        cx.notify();
    }

    pub(crate) fn close_inspector(&mut self, cx: &mut Context<Self>) {
        if self.inspector_open {
            self.inspector_open = false;
            cx.emit(PlanViewEvent::CloseInspector);
        }
    }

    /// The workspace rail was dismissed from outside the plan.
    pub(crate) fn mark_inspector_closed(&mut self) {
        self.inspector_open = false;
    }

    /// The tab was left: hide the rail but remember that it was open.
    pub(crate) fn hide_inspector(&mut self, cx: &mut Context<Self>) {
        if self.inspector_open {
            cx.emit(PlanViewEvent::CloseInspector);
        }
    }

    /// The tab was shown again: put the rail back if it was open.
    pub(crate) fn restore_inspector(&mut self, cx: &mut Context<Self>) {
        if self.inspector_open {
            self.open_inspector(cx);
        }
    }

    // ── Camera ───────────────────────────────────────────────────────────────

    fn fit_to_view(&mut self, max_zoom: f32) {
        let Some(bounds) = self
            .geometry
            .as_ref()
            .and_then(|geometry| geometry.bounds())
        else {
            return;
        };
        let Some((zoom, pan)) = diagram::fit_view(bounds, self.camera.viewport_size, max_zoom)
        else {
            return;
        };

        self.camera.zoom = zoom;
        self.camera.pan = pan;
    }

    /// Runs the fit a new plan or a return to the diagram asked for, once the
    /// viewport has a size.
    fn apply_pending_fit(&mut self, cx: &mut Context<Self>) {
        if !self.camera.pending_fit || self.geometry.is_none() {
            return;
        }

        let size = self.camera.viewport_size;
        if size.width <= px(0.0) || size.height <= px(0.0) {
            return;
        }

        self.camera.pending_fit = false;
        self.fit_to_view(canvas::OPEN_FIT_MAX_ZOOM);
        cx.notify();
    }

    pub(crate) fn fit(&mut self, cx: &mut Context<Self>) {
        self.fit_to_view(canvas::FIT_MAX_ZOOM);
        cx.notify();
    }

    pub(crate) fn zoom_in(&mut self, cx: &mut Context<Self>) {
        self.zoom_to(canvas::zoom_in(self.camera.zoom), cx);
    }

    pub(crate) fn zoom_out(&mut self, cx: &mut Context<Self>) {
        self.zoom_to(canvas::zoom_out(self.camera.zoom), cx);
    }

    /// The zoom readout: back to 100% with the camera at the origin.
    pub(crate) fn reset_view(&mut self, cx: &mut Context<Self>) {
        self.camera.zoom = 1.0;
        self.camera.pan = Point::default();
        cx.notify();
    }

    /// Zooms around the middle of the viewport, so what the user looks at
    /// stays put.
    fn zoom_to(&mut self, zoom: f32, cx: &mut Context<Self>) {
        let old = self.camera.zoom;
        if (zoom - old).abs() < f32::EPSILON {
            return;
        }

        let center = Point::new(
            self.camera.viewport_size.width / 2.0,
            self.camera.viewport_size.height / 2.0,
        );
        self.camera.pan = Point::new(
            center.x - (center.x - self.camera.pan.x) * (zoom / old),
            center.y - (center.y - self.camera.pan.y) * (zoom / old),
        );
        self.camera.zoom = zoom;
        cx.notify();
    }

    /// Pans so the selected node is inside the viewport, when it is not.
    fn reveal_selected(&mut self) {
        let Some(bounds) = self
            .selected
            .and_then(|id| self.geometry.as_ref()?.nodes.get(id).copied())
        else {
            return;
        };

        let zoom = self.camera.zoom;
        let size = self.camera.viewport_size;
        if size.width <= px(0.0) || size.height <= px(0.0) {
            return;
        }

        let left = bounds.x * zoom + f32::from(self.camera.pan.x);
        let top = bounds.y * zoom + f32::from(self.camera.pan.y);
        let right = left + bounds.width * zoom;
        let bottom = top + bounds.height * zoom;
        let inside = left >= 0.0
            && top >= 0.0
            && right <= f32::from(size.width)
            && bottom <= f32::from(size.height);

        if !inside {
            self.camera.pan = canvas::center_on(
                bounds.x + bounds.width / 2.0,
                bounds.y + bounds.height / 2.0,
                size,
                zoom,
            );
        }
    }

    // ── Clipboard and audit ──────────────────────────────────────────────────

    pub(crate) fn copy_raw(&self, cx: &mut Context<Self>) {
        if let Some(model) = self.model() {
            cx.write_to_clipboard(ClipboardItem::new_string(model.raw.clone()));
        }
    }

    pub(crate) fn copy_error(&self, cx: &mut Context<Self>) {
        if let PlanState::Failed(failure) = &self.state {
            cx.write_to_clipboard(ClipboardItem::new_string(failure.clipboard.clone()));
        }
    }

    pub(crate) fn view_error_in_audit(&self, cx: &mut Context<Self>) {
        let PlanState::Failed(failure) = &self.state else {
            return;
        };

        let correlation_id = failure.correlation_id;
        if let Some(global) = cx.try_global::<dbflux_ui_base::AppStateGlobal>() {
            let app_state = global.entity.clone();
            app_state.update(cx, |state, cx| {
                state.request_open_audit(Some(correlation_id), cx);
            });
        }
    }

    // ── Keyboard ─────────────────────────────────────────────────────────────

    /// Takes the keyboard commands of the Results context that mean
    /// something here. `false` leaves the command to the document.
    pub(crate) fn dispatch_command(
        &mut self,
        command: Command,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match command {
            Command::CycleResultView if self.model().is_some() => {
                self.set_mode(self.mode.next(), cx);
                true
            }
            Command::Cancel if self.inspector_open => {
                self.dismiss_inspector(cx);
                true
            }
            Command::SelectNext => self.step_selection(Step::Down, cx),
            Command::SelectPrev => self.step_selection(Step::Up, cx),
            Command::ColumnLeft => self.step_selection(Step::Left, cx),
            Command::ColumnRight => self.step_selection(Step::Right, cx),
            Command::SelectFirst => self.select_end(false, cx),
            Command::SelectLast => self.select_end(true, cx),
            Command::Execute if self.selected.is_some() => {
                self.open_inspector(cx);
                true
            }
            _ => false,
        }
    }

    fn select_end(&mut self, last: bool, cx: &mut Context<Self>) -> bool {
        let Some(count) = self.model().map(|model| model.nodes.len()) else {
            return false;
        };
        if count == 0 || self.mode == PlanViewMode::Raw {
            return false;
        }

        let id = if last { count - 1 } else { 0 };
        self.select(id, cx);
        self.reveal_selected();
        true
    }

    fn step_selection(&mut self, step: Step, cx: &mut Context<Self>) -> bool {
        // Borrowing the state field, not the whole view, lets the tree arms
        // change `collapsed` while the model is read.
        let PlanState::Ready(model) = &self.state else {
            return false;
        };

        let target = match (self.mode, self.selected) {
            (PlanViewMode::Raw, _) => return false,
            (_, None) => model.entry_node(),
            (PlanViewMode::Diagram, Some(current)) => self.geometry.as_ref().and_then(|geometry| {
                diagram::geometry::navigate(&geometry.nodes, current, step.direction())
            }),
            (PlanViewMode::Tree, Some(current)) => {
                match tree::row_move(model, &self.collapsed, current, step) {
                    Some(tree::RowMove::Select(id)) => Some(id),
                    Some(tree::RowMove::Collapse(id)) => {
                        self.collapsed.insert(id);
                        cx.notify();
                        return true;
                    }
                    Some(tree::RowMove::Expand(id)) => {
                        self.collapsed.remove(&id);
                        cx.notify();
                        return true;
                    }
                    None => None,
                }
            }
        };

        let Some(target) = target else {
            return false;
        };

        self.select(target, cx);
        self.reveal_selected();
        true
    }
}

/// A move of the selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    Up,
    Down,
    Left,
    Right,
}

impl Step {
    fn direction(self) -> Direction {
        match self {
            Self::Up => Direction::Up,
            Self::Down => Direction::Down,
            Self::Left => Direction::Left,
            Self::Right => Direction::Right,
        }
    }
}

impl Focusable for PlanView {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl gpui::Render for PlanView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let background = cx.theme().background;
        let warnings = self.render_warnings(cx);
        let switch_row = self.render_switch_row(cx);
        let body = self.render_body(window, cx);
        let footer = self.render_footer(cx);

        tagged("plan-view")
            .size_full()
            .flex()
            .flex_col()
            .min_h_0()
            .bg(background)
            .track_focus(&self.focus_handle)
            .child(switch_row)
            .children(warnings)
            .child(div().flex_1().min_h_0().overflow_hidden().child(body))
            .child(footer)
    }
}
