use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Weak},
    time::Instant,
};

use gpui::{
    AccessibilityFrame, App, BorderStyle, Bounds, DrawnFrame, FrameAction, FrameNode,
    FrameObserver, Hsla, Pixels, SharedString, TextAlign, ViewDrawOutcome, Window,
    accesskit::{self, Action, Role as AccessibleRole, Toggled},
    fill, point, px, quad, rgba, size, transparent_black,
};
use gpui_mcp_protocol::{
    AnnotationStyle, AnnotationTarget, NodeAction, NodeState, Rect, Role, TextInfo, UiNode,
    ValueInfo, ViewRenderCause,
};

use crate::annotations::PaintItem;
use crate::registry::{SharedState, ViewRecord, rect_from_gpui};

const LABEL_FONT_SIZE: f32 = 11.0;
const LABEL_HEIGHT: f32 = 16.0;
const LABEL_PADDING: f32 = 4.0;
const OUTLINE_WIDTH: f32 = 2.0;

pub(crate) struct BridgeObserver {
    state: Weak<SharedState>,
}

impl BridgeObserver {
    pub(crate) fn new(state: &Arc<SharedState>) -> Arc<Self> {
        Arc::new(Self {
            state: Arc::downgrade(state),
        })
    }
}

impl FrameObserver for BridgeObserver {
    fn frame_started(&self, window: &Window) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        state.begin_frame();
        let mut content_bounds = window.bounds();
        content_bounds.size = window.viewport_size();
        state.set_window_geometry(rect_from_gpui(content_bounds), window.scale_factor());
    }

    fn accessibility_frame(&self, frame: &Arc<AccessibilityFrame>) {
        if let Some(state) = self.state.upgrade() {
            state.observe_semantics(frame);
            state.keep_overlay_frame(state.has_annotations().then_some(frame));
        }
    }

    fn paint_started(&self) {
        if let Some(state) = self.state.upgrade() {
            state.begin_root_paint();
        }
    }

    fn paint_overlay(&self, window: &mut Window, cx: &mut App) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        if !state.has_annotations() {
            return;
        }
        let (items, frame, next_expiry) = state.annotation_paint(cx.background_executor().now());
        if let Some(deadline) = next_expiry {
            self.schedule_expiry(&state, deadline, window, cx);
        }
        if items.is_empty() {
            return;
        }
        let viewport = window.viewport_size();
        let viewport = Rect {
            x: 0.0,
            y: 0.0,
            width: f32::from(viewport.width),
            height: f32::from(viewport.height),
        };
        let resolved = resolve_targets(&items, frame.as_deref(), viewport);
        if state.paints_annotations() {
            for item in &items {
                if let Some(Some(rect)) = resolved.get(&item.id) {
                    paint_annotation(item, *rect, viewport, window, cx);
                }
            }
        }
        state.record_resolved(&resolved);
    }

    fn frame_finished(&self) {
        if let Some(state) = self.state.upgrade() {
            state.finish_root_paint();
        }
    }

    fn frame_drawn(&self, frame: &DrawnFrame<'_>) {
        if let Some(state) = self.state.upgrade() {
            state.finish_draw(
                frame.draw_duration(),
                frame.observation_duration(),
                frame.views().iter().map(|view| ViewRecord {
                    entity_id: view.entity_id().as_u64(),
                    type_name: view.type_name(),
                    cause: match view.outcome() {
                        ViewDrawOutcome::Reused => None,
                        ViewDrawOutcome::Rendered(cause) => Some(render_cause(cause)),
                    },
                }),
            );
        }
    }
}

impl BridgeObserver {
    /// Draw a frame when the next annotation expires, so it disappears on time
    /// even when nothing else redraws the window.
    fn schedule_expiry(
        &self,
        state: &SharedState,
        deadline: Instant,
        window: &mut Window,
        cx: &mut App,
    ) {
        if !state.claim_expiry_timer(deadline) {
            return;
        }
        let weak = self.state.clone();
        window
            .spawn(cx, async move |cx| {
                let executor = cx.background_executor().clone();
                let delay = deadline.saturating_duration_since(executor.now());
                executor.timer(delay).await;
                if let Some(state) = weak.upgrade() {
                    state.expiry_timer_fired(deadline);
                }
                let _ = cx.update(|window, _| window.request_frame());
            })
            .detach();
    }
}

/// Resolve every annotation to the part of its target visible this frame.
///
/// Node targets are looked up in the frame's semantic nodes, so they follow
/// layout, scrolling and zoom. A node that is missing, hidden, or scrolled
/// entirely out of its scroll containers resolves to `None`.
pub(crate) fn resolve_targets(
    items: &[PaintItem],
    frame: Option<&AccessibilityFrame>,
    viewport: Rect,
) -> BTreeMap<String, Option<Rect>> {
    let index: Option<HashMap<&str, &FrameNode>> = frame
        .filter(|_| {
            items
                .iter()
                .any(|item| matches!(item.target, AnnotationTarget::Node { .. }))
        })
        .map(|frame| frame.nodes().map(|(_, node)| (node.id(), node)).collect());
    items
        .iter()
        .map(|item| {
            let rect = match &item.target {
                AnnotationTarget::Rect { rect } => Some(*rect),
                AnnotationTarget::Node { node_id } => frame
                    .zip(index.as_ref())
                    .and_then(|(frame, index)| visible_node_rect(frame, index, node_id)),
            };
            (
                item.id.clone(),
                rect.and_then(|rect| intersect(rect, viewport)),
            )
        })
        .collect()
}

fn visible_node_rect(
    frame: &AccessibilityFrame,
    index: &HashMap<&str, &FrameNode>,
    node_id: &str,
) -> Option<Rect> {
    let hidden = |node: &FrameNode| {
        frame
            .accessibility_node(node)
            .is_some_and(accesskit::Node::is_hidden)
    };
    let node = *index.get(node_id)?;
    if hidden(node) {
        return None;
    }
    let mut rect = rect_from_gpui(node.bounds());
    let mut current = node;
    // Parent links come from one finished frame, but stop on a cycle anyway.
    for _ in 0..index.len() {
        let Some(parent) = current.parent().and_then(|id| index.get(id).copied()) else {
            break;
        };
        if hidden(parent) {
            return None;
        }
        if parent.actions().contains(&FrameAction::Scroll) {
            rect = intersect(rect, rect_from_gpui(parent.bounds()))?;
        }
        current = parent;
    }
    Some(rect)
}

/// The overlap of two rectangles, or `None` when they do not overlap.
pub(crate) fn intersect(a: Rect, b: Rect) -> Option<Rect> {
    let left = a.x.max(b.x);
    let top = a.y.max(b.y);
    let right = (a.x + a.width).min(b.x + b.width);
    let bottom = (a.y + a.height).min(b.y + b.height);
    (right > left && bottom > top).then_some(Rect {
        x: left,
        y: top,
        width: right - left,
        height: bottom - top,
    })
}

fn bounds(rect: Rect) -> Bounds<Pixels> {
    Bounds::new(
        point(px(rect.x), px(rect.y)),
        size(px(rect.width), px(rect.height)),
    )
}

fn paint_annotation(
    item: &PaintItem,
    rect: Rect,
    viewport: Rect,
    window: &mut Window,
    cx: &mut App,
) {
    let color = hsla(item.color);
    let outline = |window: &mut Window| {
        window.paint_quad(quad(
            bounds(rect),
            px(0.0),
            transparent_black(),
            px(OUTLINE_WIDTH),
            color,
            BorderStyle::Solid,
        ));
    };
    match item.style {
        AnnotationStyle::Outline => outline(window),
        AnnotationStyle::Fill => window.paint_quad(fill(bounds(rect), color)),
        AnnotationStyle::OutlineFill => {
            window.paint_quad(fill(bounds(rect), hsla(quarter_alpha(item.color))));
            outline(window);
        }
    }
    if let Some(label) = item.label.as_deref().filter(|label| !label.is_empty()) {
        paint_label(label, item.color, rect, viewport, window, cx);
    }
}

/// Draw `label` as a solid chip above the annotation's top-left corner, or just
/// inside it when there is no room above, in black or white for contrast.
fn paint_label(
    label: &str,
    color: u32,
    rect: Rect,
    viewport: Rect,
    window: &mut Window,
    cx: &mut App,
) {
    let chip_color = hsla(color | 0xff);
    let text_color = hsla(contrasting_text(color));
    let mut run = window.text_style().to_run(label.len());
    run.color = text_color;
    run.background_color = None;
    run.underline = None;
    run.strikethrough = None;
    let line = window.text_system().shape_line(
        SharedString::from(label.to_owned()),
        px(LABEL_FONT_SIZE),
        &[run],
        None,
    );
    let width = f32::from(line.width) + LABEL_PADDING * 2.0;
    let x = rect.x.min(viewport.width - width).max(0.0);
    let y = if rect.y >= LABEL_HEIGHT {
        rect.y - LABEL_HEIGHT
    } else {
        rect.y
    };
    let chip = Rect {
        x,
        y,
        width,
        height: LABEL_HEIGHT,
    };
    window.paint_quad(quad(
        bounds(chip),
        px(3.0),
        chip_color,
        px(0.0),
        transparent_black(),
        BorderStyle::Solid,
    ));
    let _ = line.paint(
        point(px(x + LABEL_PADDING), px(y)),
        px(LABEL_HEIGHT),
        TextAlign::Left,
        None,
        window,
        cx,
    );
}

#[cfg(feature = "gpui-ce")]
fn hsla(color: u32) -> Hsla {
    gpui::rgb_to_hsla(rgba(color))
}

#[cfg(not(feature = "gpui-ce"))]
fn hsla(color: u32) -> Hsla {
    Hsla::from(rgba(color))
}

/// `color` with a quarter of its alpha.
pub(crate) const fn quarter_alpha(color: u32) -> u32 {
    (color & 0xffff_ff00) | ((color & 0xff) / 4)
}

/// Black or white, whichever reads better on the opaque form of `color`.
pub(crate) const fn contrasting_text(color: u32) -> u32 {
    let red = (color >> 24) & 0xff;
    let green = (color >> 16) & 0xff;
    let blue = (color >> 8) & 0xff;
    // ITU-R BT.601 luma, scaled by 1000.
    if red * 299 + green * 587 + blue * 114 > 150_000 {
        0x0000_00ff
    } else {
        0xffff_ffff
    }
}

const fn render_cause(cause: gpui::ViewRenderCause) -> ViewRenderCause {
    match cause {
        gpui::ViewRenderCause::Uncached => ViewRenderCause::Uncached,
        gpui::ViewRenderCause::CachingDisabled => ViewRenderCause::CachingDisabled,
        gpui::ViewRenderCause::Refresh => ViewRenderCause::Refresh,
        gpui::ViewRenderCause::FirstDraw => ViewRenderCause::FirstDraw,
        gpui::ViewRenderCause::Notified => ViewRenderCause::Notified,
        gpui::ViewRenderCause::AncestorRendered => ViewRenderCause::AncestorRendered,
        gpui::ViewRenderCause::LayoutChanged => ViewRenderCause::LayoutChanged,
        gpui::ViewRenderCause::ObservationChanged => ViewRenderCause::ObservationChanged,
    }
}

/// Convert one observed frame into semantic nodes, parents before children.
///
/// This runs when a client reads the tree rather than during the draw, so its
/// cost never lands inside a measured frame.
pub(crate) fn semantic_nodes(frame: &AccessibilityFrame) -> Vec<UiNode> {
    let mut nodes = frame.nodes().map(|(_, node)| node).collect::<Vec<_>>();
    nodes.sort_by(|left, right| left.path().cmp(right.path()));
    let mut hidden = HashSet::<String>::new();
    nodes
        .into_iter()
        .map(|node| {
            let inherited_hidden = node.parent().is_some_and(|parent| hidden.contains(parent));
            let own_hidden = frame
                .accessibility_node(node)
                .is_some_and(accesskit::Node::is_hidden);
            let mut result = to_node(frame, node);
            if inherited_hidden {
                result.state.visible = false;
            }
            if inherited_hidden || own_hidden {
                hidden.insert(result.id.clone());
            }
            result
        })
        .collect()
}

fn to_node(frame: &AccessibilityFrame, rendered: &FrameNode) -> UiNode {
    let accessible = frame.accessibility_node(rendered);
    let accessible_role =
        accessible.map_or_else(|| rendered.fallback_role(), accesskit::Node::role);
    let focused = frame.tree().focus == rendered.accessibility_id();
    let mut metadata = rendered.metadata().clone();
    if rendered.path() != rendered.id() {
        metadata.insert("gpui_path".to_owned(), rendered.path().to_owned());
    }

    let value = accessible.and_then(accesskit::Node::value);
    let text_value = if rendered.is_redacted() {
        Some(String::new())
    } else {
        value.map(ToOwned::to_owned).or_else(|| {
            (!rendered.content_text().is_empty()).then(|| rendered.content_text().to_owned())
        })
    };
    let text = is_text_role(accessible_role).then(|| TextInfo {
        text: text_value.clone().unwrap_or_default(),
        caret: None,
        selection: None,
        redacted: rendered.is_redacted(),
    });
    let numeric_value = accessible.and_then(accesskit::Node::numeric_value);
    let value = (numeric_value.is_some() || value.is_some()).then(|| ValueInfo {
        value: if rendered.is_redacted() {
            String::new()
        } else {
            numeric_value
                .map(|value| value.to_string())
                .or(text_value)
                .unwrap_or_default()
        },
        min: accessible.and_then(accesskit::Node::min_numeric_value),
        max: accessible.and_then(accesskit::Node::max_numeric_value),
        step: accessible.and_then(accesskit::Node::numeric_value_step),
    });

    UiNode {
        id: rendered.id().to_owned(),
        parent: rendered.parent().map(ToOwned::to_owned),
        children: Vec::new(),
        role: role(accessible_role),
        label: accessible
            .and_then(accesskit::Node::label)
            .map(ToOwned::to_owned)
            .or_else(|| label_from_content(accessible_role, rendered.content_text())),
        description: accessible
            .and_then(accesskit::Node::description)
            .map(ToOwned::to_owned),
        bounds: Some(rect_from_gpui(rendered.bounds())),
        state: NodeState {
            visible: !rendered.bounds().is_empty()
                && !accessible.is_some_and(accesskit::Node::is_hidden),
            enabled: !accessible.is_some_and(accesskit::Node::is_disabled),
            focused,
            read_only: accessible.map(accesskit::Node::is_read_only),
            checked: accessible.and_then(|node| match node.toggled() {
                Some(Toggled::True) => Some(true),
                Some(Toggled::False) => Some(false),
                Some(Toggled::Mixed) | None => None,
            }),
            selected: accessible.and_then(accesskit::Node::is_selected),
            expanded: accessible.and_then(accesskit::Node::is_expanded),
        },
        actions: actions(accessible, rendered),
        text,
        value,
        metadata,
    }
}

fn actions(accessible: Option<&accesskit::Node>, rendered: &FrameNode) -> Vec<NodeAction> {
    let mut actions = Vec::new();
    let supports = |action| accessible.is_some_and(|node| node.supports_action(action));
    push_action(&mut actions, supports(Action::Click), NodeAction::Click);
    push_action(&mut actions, supports(Action::Focus), NodeAction::Focus);
    push_action(
        &mut actions,
        supports(Action::ReplaceSelectedText)
            || (is_editable_text_role(
                accessible.map_or_else(|| rendered.fallback_role(), accesskit::Node::role),
            ) && supports(Action::SetValue)),
        NodeAction::SetText,
    );
    push_action(
        &mut actions,
        supports(Action::SetValue),
        NodeAction::SetValue,
    );
    push_action(
        &mut actions,
        [
            Action::ScrollDown,
            Action::ScrollLeft,
            Action::ScrollRight,
            Action::ScrollUp,
            Action::ScrollIntoView,
            Action::ScrollToPoint,
            Action::SetScrollOffset,
        ]
        .into_iter()
        .any(supports),
        NodeAction::Scroll,
    );
    for action in rendered.actions() {
        let action = match action {
            FrameAction::Hover => NodeAction::Hover,
            FrameAction::Drag => NodeAction::Drag,
            FrameAction::Scroll => NodeAction::Scroll,
            FrameAction::SetText => NodeAction::SetText,
            FrameAction::SetValue => NodeAction::SetValue,
        };
        push_action(&mut actions, true, action);
    }
    actions
}

fn push_action(actions: &mut Vec<NodeAction>, condition: bool, action: NodeAction) {
    if condition && !actions.contains(&action) {
        actions.push(action);
    }
}

fn label_from_content(role: AccessibleRole, content: &str) -> Option<String> {
    (!content.is_empty()
        && matches!(
            role,
            AccessibleRole::Button
                | AccessibleRole::DefaultButton
                | AccessibleRole::CheckBox
                | AccessibleRole::RadioButton
                | AccessibleRole::Switch
                | AccessibleRole::Link
                | AccessibleRole::MenuItem
                | AccessibleRole::MenuItemCheckBox
                | AccessibleRole::MenuItemRadio
                | AccessibleRole::ListBoxOption
                | AccessibleRole::MenuListOption
                | AccessibleRole::Tab
        ))
    .then(|| content.to_owned())
}

const fn is_text_role(role: AccessibleRole) -> bool {
    matches!(
        role,
        AccessibleRole::TextInput
            | AccessibleRole::MultilineTextInput
            | AccessibleRole::SearchInput
            | AccessibleRole::EmailInput
            | AccessibleRole::PasswordInput
            | AccessibleRole::PhoneNumberInput
            | AccessibleRole::UrlInput
            | AccessibleRole::Label
            | AccessibleRole::TextRun
            | AccessibleRole::Paragraph
            | AccessibleRole::Heading
            | AccessibleRole::Legend
            | AccessibleRole::Caption
            | AccessibleRole::FigureCaption
            | AccessibleRole::Term
            | AccessibleRole::Code
            | AccessibleRole::Emphasis
            | AccessibleRole::Strong
    )
}

const fn is_editable_text_role(role: AccessibleRole) -> bool {
    matches!(
        role,
        AccessibleRole::TextInput
            | AccessibleRole::MultilineTextInput
            | AccessibleRole::SearchInput
            | AccessibleRole::EmailInput
            | AccessibleRole::PasswordInput
            | AccessibleRole::PhoneNumberInput
            | AccessibleRole::UrlInput
    )
}

const fn role(role: AccessibleRole) -> Role {
    match role {
        AccessibleRole::Application => Role::Application,
        AccessibleRole::Window | AccessibleRole::RootWebArea => Role::Window,
        AccessibleRole::Button
        | AccessibleRole::DefaultButton
        | AccessibleRole::DisclosureTriangle => Role::Button,
        AccessibleRole::CheckBox => Role::Checkbox,
        AccessibleRole::RadioButton => Role::Radio,
        AccessibleRole::Switch => Role::Switch,
        AccessibleRole::Link => Role::Link,
        AccessibleRole::Label
        | AccessibleRole::TextRun
        | AccessibleRole::Paragraph
        | AccessibleRole::Heading
        | AccessibleRole::Legend
        | AccessibleRole::Caption
        | AccessibleRole::FigureCaption
        | AccessibleRole::Term
        | AccessibleRole::Code
        | AccessibleRole::Emphasis
        | AccessibleRole::Strong => Role::Text,
        AccessibleRole::TextInput
        | AccessibleRole::MultilineTextInput
        | AccessibleRole::EmailInput
        | AccessibleRole::PasswordInput
        | AccessibleRole::PhoneNumberInput
        | AccessibleRole::UrlInput => Role::TextInput,
        AccessibleRole::SearchInput | AccessibleRole::Search => Role::SearchInput,
        AccessibleRole::Slider | AccessibleRole::SpinButton => Role::Slider,
        AccessibleRole::ProgressIndicator | AccessibleRole::Meter => Role::Progress,
        AccessibleRole::Image | AccessibleRole::GraphicsSymbol => Role::Image,
        AccessibleRole::List | AccessibleRole::ListBox => Role::List,
        AccessibleRole::ListItem => Role::ListItem,
        AccessibleRole::Tree => Role::Tree,
        AccessibleRole::TreeItem => Role::TreeItem,
        AccessibleRole::Table
        | AccessibleRole::Grid
        | AccessibleRole::TreeGrid
        | AccessibleRole::ListGrid => Role::Table,
        AccessibleRole::Row | AccessibleRole::LayoutTableRow => Role::Row,
        AccessibleRole::Cell
        | AccessibleRole::GridCell
        | AccessibleRole::LayoutTableCell
        | AccessibleRole::RowHeader
        | AccessibleRole::ColumnHeader => Role::Cell,
        AccessibleRole::Menu | AccessibleRole::MenuBar | AccessibleRole::MenuListPopup => {
            Role::Menu
        }
        AccessibleRole::MenuItem
        | AccessibleRole::MenuItemCheckBox
        | AccessibleRole::MenuItemRadio => Role::MenuItem,
        AccessibleRole::ComboBox | AccessibleRole::EditableComboBox => Role::Combobox,
        AccessibleRole::ListBoxOption | AccessibleRole::MenuListOption => Role::Option,
        AccessibleRole::Splitter => Role::Separator,
        AccessibleRole::Tooltip => Role::Tooltip,
        AccessibleRole::TabList => Role::TabList,
        AccessibleRole::Tab => Role::Tab,
        AccessibleRole::Toolbar => Role::Toolbar,
        AccessibleRole::Dialog | AccessibleRole::AlertDialog => Role::Dialog,
        AccessibleRole::Alert => Role::Alert,
        AccessibleRole::ScrollBar | AccessibleRole::ScrollView => Role::ScrollArea,
        AccessibleRole::Group
        | AccessibleRole::Pane
        | AccessibleRole::RadioGroup
        | AccessibleRole::TabPanel => Role::Group,
        _ => Role::Generic,
    }
}

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use gpui::{
        AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _,
        Render, Role, SharedString, StatefulInteractiveElement as _, StyleRefinement, Styled as _,
        StyledText, TestAppContext, Window, div, px, rgb,
    };
    use gpui_mcp_protocol::{
        MouseButton, NodeAction, Point, PointerCommand, Role as McpRole, ViewOutcome,
        ViewRenderCause,
    };

    use crate::{Automation, input::dispatch_pointer};

    struct SemanticFixture {
        clicked: Rc<Cell<bool>>,
    }

    impl Render for SemanticFixture {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .id("root")
                .role(Role::Application)
                .size_full()
                .child(
                    div()
                        .id("save")
                        .w(px(100.0))
                        .h(px(40.0))
                        .on_click({
                            let clicked = self.clicked.clone();
                            move |_, _, _| clicked.set(true)
                        })
                        .child(div().id("save-label").child(StyledText::new("Save"))),
                )
                .child(
                    div()
                        .id("hover-target")
                        .on_mouse_move(|_, _, _| {})
                        .child("Hover target"),
                )
                .child(div().id("status").child("Ready"))
        }
    }

    #[gpui::test]
    fn observes_real_gpui_ids_text_hierarchy_and_handlers(cx: &mut TestAppContext) {
        let automation = Automation::isolated();
        let clicked = Rc::new(Cell::new(false));
        let automation_for_window = automation.clone();
        let clicked_by_handler = clicked.clone();
        let (_view, visual) = cx.add_window_view(move |window, _| {
            automation_for_window.attach(window);
            SemanticFixture {
                clicked: clicked_by_handler,
            }
        });
        visual.run_until_parked();

        let tree = automation.snapshot();
        assert_eq!(tree.roots, ["root"]);
        assert_eq!(tree.nodes["root"].role, McpRole::Application);
        assert_eq!(tree.nodes["save"].parent.as_deref(), Some("root"));
        assert_eq!(tree.nodes["save-label"].parent.as_deref(), Some("save"));
        assert_eq!(tree.nodes["save"].role, McpRole::Button);
        assert_eq!(tree.nodes["save"].label.as_deref(), Some("Save"));
        assert!(tree.nodes["save"].actions.contains(&NodeAction::Click));
        assert!(
            tree.nodes["hover-target"]
                .actions
                .contains(&NodeAction::Hover)
        );
        assert_eq!(tree.nodes["status"].label.as_deref(), None);

        assert!(tree.nodes["save"].bounds.is_some());
        let save = tree.nodes["save"].bounds.unwrap_or_default();
        visual.update(|window, cx| {
            let point = save.center();
            assert_eq!(
                dispatch_pointer(
                    &PointerCommand::MouseDown {
                        point: Point {
                            x: point.x,
                            y: point.y
                        },
                        button: MouseButton::Left,
                        click_count: 1,
                    },
                    window,
                    cx,
                ),
                Ok(())
            );
            assert_eq!(
                dispatch_pointer(
                    &PointerCommand::MouseUp {
                        point: Point {
                            x: point.x,
                            y: point.y
                        },
                        button: MouseButton::Left,
                        click_count: 1,
                    },
                    window,
                    cx,
                ),
                Ok(())
            );
        });
        assert!(clicked.get());
    }

    struct ReadOnlyFixture;

    impl Render for ReadOnlyFixture {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            div()
                .id("root")
                .role(Role::Application)
                .child(
                    div()
                        .id("editable")
                        .role(Role::TextInput)
                        .aria_read_only(false),
                )
                .child(
                    div()
                        .id("read-only")
                        .role(Role::TextInput)
                        .aria_read_only(true),
                )
                .child(
                    div()
                        .id("disabled")
                        .role(Role::TextInput)
                        .aria_disabled(true),
                )
        }
    }

    #[gpui::test]
    fn read_only_and_disabled_are_distinct_reported_states(cx: &mut TestAppContext) {
        let automation = Automation::isolated();
        let observed = automation.clone();
        let (_, visual) = cx.add_window_view(move |window, _| {
            observed.attach(window);
            ReadOnlyFixture
        });
        visual.run_until_parked();
        let tree = automation.snapshot();
        assert_eq!(tree.nodes["editable"].state.read_only, Some(false));
        assert!(tree.nodes["editable"].state.enabled);
        assert_eq!(tree.nodes["read-only"].state.read_only, Some(true));
        assert!(tree.nodes["read-only"].state.enabled);
        assert_eq!(tree.nodes["disabled"].state.read_only, Some(false));
        assert!(!tree.nodes["disabled"].state.enabled);
    }

    struct HiddenAndRedactedFixture;

    impl Render for HiddenAndRedactedFixture {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .id("root")
                .role(Role::Application)
                .size_full()
                .child(
                    div()
                        .id("hidden-container")
                        .aria_hidden(true)
                        .w(px(120.))
                        .h(px(40.))
                        .child(
                            div()
                                .id("hidden-action")
                                .role(Role::Button)
                                .w(px(100.))
                                .h(px(30.))
                                .child("Hidden action"),
                        ),
                )
                .child(
                    div()
                        .id("redacted-button")
                        .role(Role::Button)
                        .frame_redacted(true)
                        .child("Private label"),
                )
                .child(
                    div()
                        .id("redacted-slider")
                        .role(Role::Slider)
                        .aria_numeric_value(42.)
                        .frame_redacted(true),
                )
        }
    }

    #[gpui::test]
    fn hidden_subtrees_and_redacted_bridge_values(cx: &mut TestAppContext) {
        let automation = Automation::isolated();
        let automation_for_window = automation.clone();
        let (_view, visual) = cx.add_window_view(move |window, _| {
            automation_for_window.attach(window);
            HiddenAndRedactedFixture
        });
        visual.run_until_parked();

        let tree = automation.snapshot();
        assert_eq!(tree.nodes["hidden-container"].role, McpRole::Group);
        assert!(!tree.nodes["hidden-container"].state.visible);
        assert!(
            tree.nodes["hidden-action"]
                .bounds
                .is_some_and(|bounds| bounds.width > 0.)
        );
        assert!(!tree.nodes["hidden-action"].state.visible);
        assert_eq!(tree.nodes["redacted-button"].label, None);
        assert!(
            tree.nodes["redacted-slider"]
                .value
                .as_ref()
                .is_some_and(|value| value.value.is_empty())
        );
    }

    struct RedactedChildFixture;

    impl Render for RedactedChildFixture {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            div()
                .id("container")
                .role(Role::Button)
                .child("Public")
                .child(
                    div()
                        .id("secret")
                        .role(Role::TextInput)
                        .aria_value("private value")
                        .frame_redacted(true)
                        .child("private value"),
                )
        }
    }

    #[gpui::test]
    fn redacted_child_text_does_not_become_an_ancestor_label(cx: &mut TestAppContext) {
        let automation = Automation::isolated();
        let observed = automation.clone();
        let (_, visual) = cx.add_window_view(move |window, _| {
            observed.attach(window);
            RedactedChildFixture
        });
        visual.run_until_parked();
        let tree = automation.snapshot();
        assert_eq!(tree.nodes["container"].label.as_deref(), Some("Public"));
        assert!(
            tree.nodes["secret"]
                .text
                .as_ref()
                .is_some_and(|text| text.redacted && text.text.is_empty())
        );
        assert!(
            tree.nodes["secret"]
                .value
                .as_ref()
                .is_some_and(|value| value.value.is_empty())
        );
    }

    struct CachedPrivateText;

    impl Render for CachedPrivateText {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            div().id("cached-content").child("Cached content")
        }
    }

    struct RedactedCacheFixture {
        content: Entity<CachedPrivateText>,
        redacted: bool,
    }

    impl Render for RedactedCacheFixture {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            div()
                .id("container")
                .role(Role::Button)
                .child("Public")
                .child(
                    div().id("boundary").frame_redacted(self.redacted).child(
                        self.content
                            .clone()
                            .cached(StyleRefinement::default().w(px(160.)).h(px(40.))),
                    ),
                )
        }
    }

    #[gpui::test]
    fn cached_text_respects_changed_redaction_boundaries(cx: &mut TestAppContext) {
        let automation = Automation::isolated();
        let observed = automation.clone();
        let (root, visual) = cx.add_window_view(move |window, cx| {
            observed.attach(window);
            RedactedCacheFixture {
                content: cx.new(|_| CachedPrivateText),
                redacted: false,
            }
        });
        visual.run_until_parked();
        let cached = root.read_with(visual, |root, _| root.content.entity_id());
        assert_eq!(
            automation.snapshot().nodes["container"].label.as_deref(),
            Some("Public Cached content")
        );

        for redacted in [true, false] {
            root.update(visual, |root, cx| {
                root.redacted = redacted;
                cx.notify();
            });
            visual.run_until_parked();
            let expected = if redacted {
                "Public"
            } else {
                "Public Cached content"
            };
            assert_eq!(
                automation.snapshot().nodes["container"].label.as_deref(),
                Some(expected)
            );
            automation.mark_frames();
            visual.update(|window, _| window.request_frame());
            visual.run_until_parked();
            assert_eq!(
                outcome(&automation.frame_report(None, 16), cached),
                Some((ViewOutcome::Reused, None))
            );
            assert_eq!(
                automation.snapshot().nodes["container"].label.as_deref(),
                Some(expected)
            );
        }
    }

    /// One dock panel, rendered as its own view exactly as
    /// `gpui_component::dock::TabPanel` is. Every instance renders the same
    /// element ids, so the instances are distinguished only by the view segment
    /// their entity contributes to the GPUI element path.
    struct DockPanel {
        title: SharedString,
    }

    impl Render for DockPanel {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().id("tab-panel").size_full().child(
                div()
                    .id("tab")
                    .on_click(|_, _, _| {})
                    .child(StyledText::new(self.title.clone())),
            )
        }
    }

    struct DockFixture {
        panels: Vec<Entity<DockPanel>>,
    }

    impl Render for DockFixture {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .id("dock-area")
                .role(Role::Group)
                .size_full()
                .children(self.panels.iter().cloned())
                // An element ID is free to contain the path separator, so a
                // node that needs no qualifying must not be cut at one.
                .child(div().id("recent-project-local.e2etest"))
        }
    }

    #[gpui::test]
    fn repeated_element_ids_in_sibling_views_stay_in_the_tree(cx: &mut TestAppContext) {
        let automation = Automation::isolated();
        let automation_for_window = automation.clone();
        let (_view, visual) = cx.add_window_view(move |window, cx| {
            automation_for_window.attach(window);
            DockFixture {
                panels: vec![
                    cx.new(|_| DockPanel {
                        title: "Hierarchy".into(),
                    }),
                    cx.new(|_| DockPanel {
                        title: "Console".into(),
                    }),
                ],
            }
        });
        visual.run_until_parked();

        let tree = automation.snapshot();
        assert_eq!(
            tree.diagnostics,
            [],
            "repeating an element id in a sibling view is ordinary GPUI, not a tree defect"
        );

        let dock = &tree.nodes["dock-area"];
        assert_eq!(
            dock.children.len(),
            3,
            "the dock area must list both panels, got {:?}",
            dock.children
        );
        assert!(
            tree.nodes.contains_key("recent-project-local.e2etest"),
            "an unambiguous element ID keeps its own name whatever it contains"
        );

        let mut titles = dock
            .children
            .iter()
            .filter(|child| child.as_str() != "recent-project-local.e2etest")
            .map(|panel| {
                assert!(
                    panel.ends_with(".tab-panel"),
                    "a repeated element ID is qualified by its element path, got {panel}"
                );
                let panel = &tree.nodes[panel];
                assert_eq!(
                    panel.children.len(),
                    1,
                    "each panel must list its tab, got {:?}",
                    panel.children
                );
                tree.nodes[&panel.children[0]]
                    .label
                    .clone()
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        titles.sort();
        assert_eq!(titles, ["Console", "Hierarchy"]);
    }

    /// One workbench region drawn as its own cached view, with a control that
    /// restyles itself on hover.
    struct HoverPanel {
        target: &'static str,
        label: &'static str,
    }

    impl Render for HoverPanel {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().size_full().child(
                div()
                    .id(self.target)
                    .w(px(120.))
                    .h(px(40.))
                    .bg(rgb(0x20_20_20))
                    .hover(|style| style.bg(rgb(0x40_40_40)))
                    .active(|style| style.bg(rgb(0x60_60_60)))
                    .on_click(|_, _, _| {})
                    .tooltip(|_, cx| cx.new(|_| PanelTooltip).into())
                    .child(self.label),
            )
        }
    }

    struct PanelTooltip;

    impl Render for PanelTooltip {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().id("panel-tooltip").child("Tooltip")
        }
    }

    /// Two cached regions, one of them inside a button whose label is the
    /// region's text, so the label depends on text a replayed region supplies.
    struct CachedRegions {
        left: Entity<HoverPanel>,
        right: Entity<HoverPanel>,
    }

    impl Render for CachedRegions {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            let region = || StyleRefinement::default().w(px(200.)).h(px(100.));
            div()
                .id("root")
                .role(Role::Application)
                .size_full()
                .flex()
                .child(self.left.clone().cached(region()))
                .child(
                    div()
                        .id("right-group")
                        .role(Role::Button)
                        .child(self.right.clone().cached(region())),
                )
                .child(div().id("parking").w(px(100.)).h(px(100.)))
        }
    }

    fn move_pointer(visual: &mut gpui::VisualTestContext, point: Point) {
        visual.update(|window, cx| {
            assert_eq!(
                dispatch_pointer(
                    &PointerCommand::MouseMove {
                        point,
                        pressed_button: None,
                    },
                    window,
                    cx,
                ),
                Ok(())
            );
        });
        visual.run_until_parked();
    }

    fn outcome(
        report: &gpui_mcp_protocol::FrameReport,
        entity_id: gpui::EntityId,
    ) -> Option<(ViewOutcome, Option<ViewRenderCause>)> {
        report
            .last_frame_views
            .iter()
            .find(|view| view.entity_id == entity_id.as_u64())
            .map(|view| (view.outcome, view.cause))
    }

    fn press(visual: &mut gpui::VisualTestContext, point: Point, down: bool) {
        visual.update(|window, cx| {
            let command = if down {
                PointerCommand::MouseDown {
                    point,
                    button: MouseButton::Left,
                    click_count: 1,
                }
            } else {
                PointerCommand::MouseUp {
                    point,
                    button: MouseButton::Left,
                    click_count: 1,
                }
            };
            assert_eq!(dispatch_pointer(&command, window, cx), Ok(()));
        });
        visual.run_until_parked();
    }

    /// Open the two-region fixture, park the pointer, and return the root and
    /// the two region entities with the centers of the parking spot and the
    /// left region's control.
    fn open_cached_regions<'a>(
        cx: &'a mut TestAppContext,
        automation: &Automation,
    ) -> (
        &'a mut gpui::VisualTestContext,
        [gpui::EntityId; 3],
        Point,
        Point,
    ) {
        let automation_for_window = automation.clone();
        let (root, visual) = cx.add_window_view(move |window, cx| {
            automation_for_window.attach(window);
            CachedRegions {
                left: cx.new(|_| HoverPanel {
                    target: "left-target",
                    label: "Left",
                }),
                right: cx.new(|_| HoverPanel {
                    target: "right-target",
                    label: "Right",
                }),
            }
        });
        visual.run_until_parked();
        let (left, right) = root.read_with(visual, |regions, _| {
            (regions.left.entity_id(), regions.right.entity_id())
        });
        let tree = automation.snapshot();
        let center = |id: &str| tree.nodes[id].bounds.unwrap_or_default().center();
        let parking = center("parking");
        let target = center("left-target");
        move_pointer(visual, parking);
        (visual, [root.entity_id(), left, right], parking, target)
    }

    /// Every frame since the mark rendered `rendered` because it was notified
    /// and replayed `reused`, and none refreshed the window.
    fn only_notified(
        report: &gpui_mcp_protocol::FrameReport,
        rendered: gpui::EntityId,
        reused: gpui::EntityId,
    ) {
        assert!(report.summary.frames > 0, "the interaction drew no frame");
        let view = |entity: gpui::EntityId| {
            report
                .views
                .iter()
                .find(|view| view.entity_id == entity.as_u64())
                .cloned()
                .unwrap_or_default()
        };
        let rendered = view(rendered);
        assert_eq!(
            rendered.causes.keys().copied().collect::<Vec<_>>(),
            [ViewRenderCause::Notified],
            "{:?}",
            report.views
        );
        let reused = view(reused);
        assert_eq!(
            (reused.rendered, reused.reused),
            (0, report.summary.frames),
            "the untouched region replays in every frame: {:?}",
            report.views
        );
        assert!(
            report
                .views
                .iter()
                .all(|view| !view.causes.contains_key(&ViewRenderCause::Refresh)),
            "{:?}",
            report.views
        );
    }

    #[gpui::test]
    fn a_click_redraws_only_the_clicked_cached_view(cx: &mut TestAppContext) {
        let automation = Automation::isolated();
        let (visual, [_, left, right], _, target) = open_cached_regions(cx, &automation);
        move_pointer(visual, target);

        automation.mark_frames();
        press(visual, target, true);
        press(visual, target, false);
        only_notified(&automation.frame_report(None, 16), left, right);
    }

    #[gpui::test]
    fn a_tooltip_redraws_only_the_view_that_shows_it(cx: &mut TestAppContext) {
        let executor = cx.executor();
        let automation = Automation::isolated();
        let (visual, [_, left, right], parking, target) = open_cached_regions(cx, &automation);
        move_pointer(visual, target);

        automation.mark_frames();
        executor.advance_clock(std::time::Duration::from_secs(2));
        visual.run_until_parked();
        assert!(
            automation.snapshot().nodes.contains_key("panel-tooltip"),
            "the tooltip must have been shown"
        );
        only_notified(&automation.frame_report(None, 16), left, right);

        automation.mark_frames();
        move_pointer(visual, parking);
        executor.advance_clock(std::time::Duration::from_secs(2));
        visual.run_until_parked();
        assert!(
            !automation.snapshot().nodes.contains_key("panel-tooltip"),
            "the tooltip must have been hidden"
        );
        only_notified(&automation.frame_report(None, 16), left, right);
    }

    #[gpui::test]
    fn a_hover_redraws_only_the_hovered_cached_view(cx: &mut TestAppContext) {
        let automation = Automation::isolated();
        let automation_for_window = automation.clone();
        let (root, visual) = cx.add_window_view(move |window, cx| {
            automation_for_window.attach(window);
            CachedRegions {
                left: cx.new(|_| HoverPanel {
                    target: "left-target",
                    label: "Left",
                }),
                right: cx.new(|_| HoverPanel {
                    target: "right-target",
                    label: "Right",
                }),
            }
        });
        visual.run_until_parked();
        let (left, right) = root.read_with(visual, |regions, _| {
            (regions.left.entity_id(), regions.right.entity_id())
        });

        let tree = automation.snapshot();
        let center = |id: &str| tree.nodes[id].bounds.unwrap_or_default().center();
        let parking = center("parking");
        let left_target = center("left-target");
        assert_eq!(tree.nodes["right-group"].label.as_deref(), Some("Right"));

        // Park the pointer inside the window first, so the measured move only
        // changes which control is hovered.
        move_pointer(visual, parking);
        automation.mark_frames();
        move_pointer(visual, left_target);

        let report = automation.frame_report(None, 16);
        assert_eq!(
            report.summary.frames, 1,
            "one hover is one frame, got {:?}",
            report.frames
        );
        assert_eq!(
            outcome(&report, left),
            Some((ViewOutcome::Rendered, Some(ViewRenderCause::Notified))),
            "the hovered region renders because its hover state changed"
        );
        assert_eq!(
            outcome(&report, right),
            Some((ViewOutcome::Reused, None)),
            "the other region replays its previous frame: {:?}",
            report.last_frame_views
        );
        assert_eq!(
            outcome(&report, root.entity_id()),
            Some((ViewOutcome::Rendered, Some(ViewRenderCause::Uncached)))
        );
        let frame = &report.frames[0];
        assert_eq!(frame.views_rendered, 2);
        assert_eq!(frame.views_reused, 1);
        assert!(frame.draw_ms > 0.0);
        assert!(frame.bridge_ms <= frame.draw_ms);
        assert!(
            report
                .views
                .iter()
                .any(|view| view.entity_id == left.as_u64()
                    && view.type_name.ends_with("HoverPanel")),
            "views are named by their Render type: {:?}",
            report.views
        );

        let tree = automation.snapshot();
        assert_eq!(
            tree.nodes["right-group"].label.as_deref(),
            Some("Right"),
            "a replayed region still supplies its text to the nodes around it"
        );

        // A full refresh, which injected input used to request, renders both.
        automation.mark_frames();
        visual.update(|window, _| window.refresh());
        visual.run_until_parked();
        let report = automation.frame_report(None, 16);
        assert_eq!(
            outcome(&report, right),
            Some((ViewOutcome::Rendered, Some(ViewRenderCause::Refresh)))
        );
        assert_eq!(
            automation.snapshot().nodes["right-group"].label.as_deref(),
            Some("Right")
        );
    }

    #[gpui::test]
    fn a_requested_frame_replays_every_cached_view(cx: &mut TestAppContext) {
        let automation = Automation::isolated();
        let automation_for_window = automation.clone();
        let (root, visual) = cx.add_window_view(move |window, cx| {
            automation_for_window.attach(window);
            CachedRegions {
                left: cx.new(|_| HoverPanel {
                    target: "left-target",
                    label: "Left",
                }),
                right: cx.new(|_| HoverPanel {
                    target: "right-target",
                    label: "Right",
                }),
            }
        });
        visual.run_until_parked();
        let (left, right) = root.read_with(visual, |regions, _| {
            (regions.left.entity_id(), regions.right.entity_id())
        });

        automation.mark_frames();
        visual.update(|window, _| {
            assert!(!window.frame_pending());
            window.request_frame();
            assert!(window.frame_pending());
        });
        visual.run_until_parked();
        visual.update(|window, _| assert!(!window.frame_pending()));

        let report = automation.frame_report(None, 16);
        assert_eq!(report.summary.frames, 1);
        assert_eq!(outcome(&report, left), Some((ViewOutcome::Reused, None)));
        assert_eq!(outcome(&report, right), Some((ViewOutcome::Reused, None)));
    }

    /// A view whose two nested element ids spell, when joined, an element id
    /// another node already owns outright.
    struct NestedPanel;

    impl Render for NestedPanel {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().id("q").child(div().id("y"))
        }
    }

    struct SeparatorFixture {
        nested: Entity<NestedPanel>,
    }

    impl Render for SeparatorFixture {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .id("dock-area")
                .role(Role::Group)
                .size_full()
                .child(div().id("q.y"))
                .child(self.nested.clone())
                .child(div().id("other").child(div().id("y")))
        }
    }

    #[gpui::test]
    fn a_qualified_identity_never_spells_one_already_given_out(cx: &mut TestAppContext) {
        let automation = Automation::isolated();
        let automation_for_window = automation.clone();
        let (_view, visual) = cx.add_window_view(move |window, cx| {
            automation_for_window.attach(window);
            SeparatorFixture {
                nested: cx.new(|_| NestedPanel),
            }
        });
        visual.run_until_parked();

        let tree = automation.snapshot();
        assert_eq!(tree.diagnostics, []);
        assert_eq!(tree.nodes["dock-area"].children.len(), 3);
        assert!(
            tree.nodes.contains_key("q.y"),
            "the element that owns this id outright must keep it"
        );
        assert!(
            tree.nodes.contains_key("other.y"),
            "a repeated id is qualified by its parent when that separates it"
        );
        assert!(
            tree.nodes
                .keys()
                .any(|id| id.ends_with(".q.y") && id != "q.y"),
            "the nested pair must be pushed past the id it would otherwise spell, got {:?}",
            tree.nodes.keys().collect::<Vec<_>>()
        );
    }
}

#[cfg(test)]
mod overlay_math_tests {
    use gpui_mcp_protocol::Rect;

    use super::{contrasting_text, intersect, quarter_alpha};

    fn rect(x: f32, y: f32, width: f32, height: f32) -> Rect {
        Rect {
            x,
            y,
            width,
            height,
        }
    }

    #[test]
    fn rectangles_clip_to_their_overlap() {
        assert_eq!(
            intersect(rect(0., 0., 10., 10.), rect(5., 5., 10., 10.)),
            Some(rect(5., 5., 5., 5.))
        );
        assert_eq!(intersect(rect(0., 0., 5., 5.), rect(5., 0., 5., 5.)), None);
    }

    #[test]
    fn labels_contrast_with_their_chip() {
        assert_eq!(contrasting_text(0xffff_00ff), 0x0000_00ff);
        assert_eq!(contrasting_text(0x0000_80ff), 0xffff_ffff);
        assert_eq!(quarter_alpha(0x1122_33ff), 0x1122_333f);
    }
}

#[cfg(all(test, feature = "test-support"))]
mod annotation_tests {
    use std::time::Duration;

    use gpui::{
        Context, InteractiveElement as _, IntoElement, ParentElement as _, Render,
        StatefulInteractiveElement as _, Styled as _, TestAppContext, Window, div, px,
    };
    use gpui_mcp_protocol::{AnnotationSource, AnnotationSpec, Rect};

    use crate::Automation;

    struct Moving {
        offset: f32,
        show_target: bool,
        scrolled_away: bool,
    }

    impl Render for Moving {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            let list = div()
                .id("list")
                .absolute()
                .top(px(200.))
                .left(px(0.))
                .w(px(100.))
                .h(px(50.))
                .overflow_y_scroll()
                .child(
                    div()
                        .id("inside")
                        .mt(px(if self.scrolled_away { 500. } else { 10. }))
                        .w(px(40.))
                        .h(px(20.)),
                );
            let mut root = div().id("root").size_full().child(list);
            if self.show_target {
                root = root.child(
                    div()
                        .id("target")
                        .absolute()
                        .top(px(self.offset))
                        .left(px(30.))
                        .w(px(50.))
                        .h(px(20.)),
                );
            }
            root
        }
    }

    fn resolved(automation: &Automation, id: &str) -> Option<Rect> {
        automation
            .annotations()
            .into_iter()
            .find(|annotation| annotation.id == id)
            .and_then(|annotation| annotation.resolved)
    }

    #[gpui::test]
    fn node_annotations_follow_their_element(cx: &mut TestAppContext) {
        let automation = Automation::isolated();
        let for_window = automation.clone();
        let (view, visual) = cx.add_window_view(move |window, _| {
            for_window.attach(window);
            Moving {
                offset: 40.,
                show_target: true,
                scrolled_away: false,
            }
        });
        visual.run_until_parked();

        let applied = visual
            .update(|window, _| {
                automation.set_annotations(
                    vec![
                        AnnotationSpec::node("target")
                            .with_id("t")
                            .with_label("Save button"),
                        AnnotationSpec::node("inside").with_id("in-scroll"),
                        AnnotationSpec::node("missing").with_id("m"),
                    ],
                    None,
                    window,
                )
            })
            .map_err(|error| error.message);
        assert_eq!(
            applied.map(|applied| applied[0].source),
            Ok(AnnotationSource::App)
        );
        visual.run_until_parked();

        let first = resolved(&automation, "t");
        assert!(first.is_some(), "the target is drawn");
        assert_eq!(automation.snapshot().nodes["target"].bounds, first);
        assert!(resolved(&automation, "in-scroll").is_some());
        assert_eq!(resolved(&automation, "m"), None);

        view.update(visual, |view, cx| {
            view.offset = 90.;
            view.scrolled_away = true;
            cx.notify();
        });
        visual.run_until_parked();
        let moved = resolved(&automation, "t");
        assert!(
            first
                .zip(moved)
                .is_some_and(|(first, moved)| (moved.y - first.y - 50.).abs() < 0.5),
            "{first:?} -> {moved:?}"
        );
        assert_eq!(
            resolved(&automation, "in-scroll"),
            None,
            "a node scrolled out of its container is not drawn"
        );

        view.update(visual, |view, cx| {
            view.show_target = false;
            cx.notify();
        });
        visual.run_until_parked();
        assert_eq!(resolved(&automation, "t"), None);
        assert_eq!(
            automation.annotations().len(),
            3,
            "absence hides, not removes"
        );
    }

    #[gpui::test]
    fn annotations_expire_without_other_redraws(cx: &mut TestAppContext) {
        let executor = cx.executor();
        let automation = Automation::isolated();
        let for_window = automation.clone();
        let (_view, visual) = cx.add_window_view(move |window, _| {
            for_window.attach(window);
            Moving {
                offset: 0.,
                show_target: true,
                scrolled_away: false,
            }
        });
        visual.run_until_parked();
        let annotated = visual.update(|window, _| {
            automation.annotate(AnnotationSpec::node("target").with_ttl_ms(500), window)
        });
        assert!(annotated.is_ok());
        visual.run_until_parked();
        let revision = automation.annotation_revision();
        assert_eq!(automation.annotations().len(), 1);

        executor.advance_clock(Duration::from_millis(400));
        visual.run_until_parked();
        assert_eq!(automation.annotations().len(), 1);

        executor.advance_clock(Duration::from_millis(200));
        visual.run_until_parked();
        assert!(automation.annotations().is_empty());
        assert!(automation.annotation_revision() > revision);
    }

    #[gpui::test]
    fn the_app_can_stop_the_bridge_drawing(cx: &mut TestAppContext) {
        let automation = Automation::isolated();
        let for_window = automation.clone();
        let (_view, visual) = cx.add_window_view(move |window, _| {
            for_window.attach(window);
            Moving {
                offset: 0.,
                show_target: true,
                scrolled_away: false,
            }
        });
        visual.run_until_parked();
        visual.update(|window, _| {
            automation.paint_annotations(false, window);
            assert!(
                automation
                    .annotate(AnnotationSpec::node("target").with_id("t"), window)
                    .is_ok()
            );
        });
        visual.run_until_parked();
        assert!(
            resolved(&automation, "t").is_some(),
            "bounds still resolve for an app that draws annotations itself"
        );
        assert_eq!(
            visual.update(|window, _| automation.clear_annotations(None, window)),
            1
        );
    }
}
