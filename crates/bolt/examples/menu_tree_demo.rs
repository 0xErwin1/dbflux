//! A runnable tour of the cascading-menu component (`bolt::composites`).
//!
//! This is the API example for a product mounting a cascading menu: it owns
//! every piece of state the component does not, and wires the events the
//! component reports into that state. Run one scenario at a time:
//!
//! ```text
//! cargo run -p bolt --example menu_tree_demo -- nested
//! ```
//!
//! The scenarios pick where the root panel sits and which rows it shows:
//!
//! - `nested` — a menu near the left whose submenu holds another submenu, so a
//!   two-deep cascade sits on screen beside its parents.
//! - `flip` — the root panel near the right edge, so its flyout must flip left
//!   to stay inside the window.
//! - `bottom` — the root panel near the bottom with a submenu taller than the
//!   room below it, so the flyout slides up and scrolls internally.
//! - `checks` — a submenu of checkable and radio rows; clicking one toggles the
//!   demo's own state and re-renders the mark with the menu still open.
//! - `keyboard` — the `nested` tree driven only by keys.
//!
//! What the demo owns, per the component's contract:
//!
//! - the row tree (`Vec<MenuNode>`), rebuilt from the scenario and from the
//!   checkable state the demo keeps itself, so `checks` really re-renders;
//! - one `MenuNav` and one `HoverIntent` for the whole open menu;
//! - the hover timer: `MenuTreeEvent::Hovered` arms a `Timer` of the
//!   component's `HOVER_OPEN_DELAY`, and when it fires the demo runs
//!   `HoverIntent::tick` and, on `HoverTick::Open`, calls `MenuNav::hover` to
//!   highlight the row and open its flyout, then `HoverIntent::mark_open`;
//! - the keyboard path on the window's focused view: Down/Up step, Right or
//!   Enter open or activate, Left/Escape back out, with
//!   `MenuTreeLayout::keyboard_focus` set so the cursor row wears the ring;
//! - the full-size layer that mounts `render_menu_tree` over
//!   `render_menu_overlay`, so a click outside dismisses the cascade.
//!
//! The component reports no hover-leave, so this demo never feeds
//! `HoverIntent::leave`/`pointer_moved`: a flyout closes by switching to
//! another parent row, by the keyboard, or by an outside click. A product with
//! window-wide pointer tracking can drive the close grace period and the
//! travel corridor the same way it drives the open delay here.

use std::borrow::Cow;
use std::time::Instant;

use bolt::composites::{
    BackAction, ChildAction, HOVER_OPEN_DELAY, HoverIntent, HoverTick, MenuNav, MenuNode,
    MenuTreeEvent, MenuTreeLayout, render_menu_overlay, render_menu_tree,
};
use bolt::{
    brand, density, fonts, icon::IconSource, palette, theme, tokens::MenuMetrics, typography,
};
use gpui::prelude::*;
use gpui::{
    App, AssetSource, Bounds, Context, Div, FocusHandle, KeyDownEvent, Pixels, Point, SharedString,
    Size, Stateful, TitlebarOptions, Window, WindowBounds, WindowOptions, div, point, px, size,
};
use gpui_component::Root;

/// An icon the demo's own asset source serves, by the path `IconSource::Svg`
/// asks for. The component's check mark and chevron resolve through
/// `IconName` into the same `icons/<name>.svg` paths; a product with its own
/// icon set names its icons like this, or aliases `IconName` onto its set.
fn svg_icon(path: &str) -> IconSource {
    IconSource::Svg(path.into())
}

/// The demo window: wide enough that a flipped or shifted flyout still lands
/// inside it, tall enough for a flyout that scrolls.
const WINDOW: Size<Pixels> = size(px(900.0), px(700.0));

/// The root panel's width. Submenu rows set their own flyouts' width through
/// `MenuNode::children_width`; this one is the demo's choice.
const ROOT_WIDTH: Pixels = MenuMetrics::SUBMENU_WIDTH;

const TITLE: &str = "Bolt menu tree demo";

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Nested,
    Flip,
    Bottom,
    Checks,
    Keyboard,
}

impl Scenario {
    fn from_arg(name: &str) -> Option<Self> {
        match name {
            "nested" => Some(Self::Nested),
            "flip" => Some(Self::Flip),
            "bottom" => Some(Self::Bottom),
            "checks" => Some(Self::Checks),
            "keyboard" => Some(Self::Keyboard),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Nested => "nested",
            Self::Flip => "flip",
            Self::Bottom => "bottom",
            Self::Checks => "checks",
            Self::Keyboard => "keyboard",
        }
    }

    fn hint(self) -> &'static str {
        match self {
            Self::Nested => {
                "hover \"Export\", then hover \"Formats\": each flyout opens after 150 ms"
            }
            Self::Flip => "hover \"Move to\": the flyout has no room right, so it flips left",
            Self::Bottom => {
                "hover \"Jump to row\": the flyout slides up and scrolls when taller than the window"
            }
            Self::Checks => {
                "click \"View\", then click \"Word wrap\": the check toggles with the menu open"
            }
            Self::Keyboard => {
                "Down opens the root; Down/Up move; Right opens; Left/Escape backs out"
            }
        }
    }

    /// Where the root panel sits, chosen to exercise one placement rule.
    fn origin(self) -> Point<Pixels> {
        match self {
            Self::Nested | Self::Keyboard => point(px(40.0), px(150.0)),
            Self::Flip => point(px(620.0), px(150.0)),
            Self::Bottom => point(px(40.0), px(588.0)),
            Self::Checks => point(px(240.0), px(150.0)),
        }
    }
}

// ---------------------------------------------------------------------------
// The row trees
// ---------------------------------------------------------------------------

/// The choice of one radio group, kept by the demo so `checks` re-renders its
/// mark from real state.
#[derive(Clone, Copy, PartialEq)]
enum RowSort {
    Name,
    Size,
    Modified,
}

/// The state behind the `checks` scenario: the demo owns it, and activation
/// toggles it and rebuilds the tree from it.
struct CheckState {
    line_numbers: bool,
    word_wrap: bool,
    sort_by: RowSort,
}

impl Default for CheckState {
    fn default() -> Self {
        Self {
            line_numbers: false,
            word_wrap: false,
            sort_by: RowSort::Name,
        }
    }
}

impl CheckState {
    fn summary(&self) -> SharedString {
        let sort = match self.sort_by {
            RowSort::Name => "name",
            RowSort::Size => "size",
            RowSort::Modified => "modified",
        };
        format!(
            "state: line-numbers {} · word-wrap {} · sort by {sort}",
            on_off(self.line_numbers),
            on_off(self.word_wrap),
        )
        .into()
    }
}

fn on_off(value: bool) -> &'static str {
    if value { "on" } else { "off" }
}

/// Builds the row tree of one scenario. `state` feeds the checkable rows, so
/// the `checks` scenario draws its marks from the demo's own state.
fn build_tree(scenario: Scenario, state: &CheckState) -> Vec<MenuNode> {
    match scenario {
        // Two submenus nested inside each other, plus a danger section: the
        // cascade has to keep every parent open and highlighted beside its
        // flyout. The `keyboard` scenario drives this same tree by keys.
        Scenario::Nested | Scenario::Keyboard => vec![
            MenuNode::action("open", "Open table")
                .icon(svg_icon("icons/table.svg"))
                .shortcut("Ctrl+T"),
            MenuNode::action("copy", "Copy as INSERT")
                .icon(svg_icon("icons/copy.svg"))
                .shortcut("Ctrl+Shift+C"),
            MenuNode::row("Export")
                .icon(svg_icon("icons/download.svg"))
                .children(vec![
                    MenuNode::row("Formats")
                        .icon(svg_icon("icons/download.svg"))
                        .children(vec![
                            MenuNode::action("export.csv", "CSV"),
                            MenuNode::action("export.json", "JSON"),
                            MenuNode::action("export.parquet", "Parquet"),
                            MenuNode::action("export.xml", "XML (coming soon)").disabled(),
                        ]),
                    MenuNode::separator(),
                    MenuNode::action("export.all", "Export all rows"),
                ]),
            MenuNode::separator(),
            MenuNode::row("Danger zone").children(vec![
                MenuNode::action("drop", "Drop table")
                    .danger()
                    .icon(svg_icon("icons/trash.svg")),
            ]),
            MenuNode::action("close", "Close").shortcut("Esc"),
        ],

        // The root panel hugs the right edge, so the flyout cannot open to the
        // right of it and must flip left.
        Scenario::Flip => vec![
            MenuNode::action("rename", "Rename").shortcut("F2"),
            MenuNode::separator(),
            MenuNode::row("Move to").children(vec![
                MenuNode::action("move.archive", "Archive").icon(svg_icon("icons/archive.svg")),
                MenuNode::action("move.trash", "Trash").icon(svg_icon("icons/trash.svg")),
                MenuNode::action("move.inbox", "Inbox").icon(svg_icon("icons/inbox.svg")),
            ]),
            MenuNode::action("delete", "Delete")
                .danger()
                .icon(svg_icon("icons/trash.svg")),
        ],

        // A submenu far taller than the window: the flyout slides up to the
        // margin and scrolls inside its capped height.
        Scenario::Bottom => vec![
            MenuNode::action("refresh", "Refresh")
                .icon(svg_icon("icons/refresh-cw.svg"))
                .shortcut("Ctrl+R"),
            MenuNode::action("filter", "Filter rows").icon(svg_icon("icons/funnel.svg")),
            MenuNode::row("Jump to row").children(
                (1..=26)
                    .map(|row| MenuNode::action(format!("jump.{row}"), format!("Row {row}")))
                    .collect(),
            ),
        ],

        // Checkable and radio rows. The marks come from `state`, activation
        // toggles it, and the rows keep the menu open, so the mark re-renders
        // in place. The last row shows the `keep_open` override on an action
        // row, which closes nothing either.
        Scenario::Checks => vec![
            MenuNode::row("View")
                .icon(svg_icon("icons/eye.svg"))
                .children(vec![
                    MenuNode::checked("view.line-numbers", "Line numbers", state.line_numbers),
                    MenuNode::checked("view.word-wrap", "Word wrap", state.word_wrap),
                    MenuNode::separator(),
                    MenuNode::radio(
                        "view.sort.name",
                        "Sort by name",
                        state.sort_by == RowSort::Name,
                    ),
                    MenuNode::radio(
                        "view.sort.size",
                        "Sort by size",
                        state.sort_by == RowSort::Size,
                    ),
                    MenuNode::radio(
                        "view.sort.modified",
                        "Sort by modified",
                        state.sort_by == RowSort::Modified,
                    ),
                    MenuNode::action("view.randomize", "Randomize order").keep_open(true),
                ]),
            MenuNode::action("view.reset", "Reset defaults").icon(svg_icon("icons/undo-2.svg")),
        ],
    }
}

/// The row a path points at, resolved by walking the tree. The component's
/// path lookup helpers are crate-private, which is deliberate: a product maps
/// `Activated` to its own action through its own ids, and only ever needs the
/// row itself to consult `keeps_menu_open`.
fn node_at_path<'a>(nodes: &'a [MenuNode], path: &[usize]) -> Option<&'a MenuNode> {
    let (last, parents) = path.split_last()?;
    let level = parents.iter().try_fold(nodes, |level, index| {
        level.get(*index).map(|node| node.children.as_slice())
    })?;

    level.get(*last)
}

// ---------------------------------------------------------------------------
// The demo surface
// ---------------------------------------------------------------------------

struct MenuTreeDemo {
    scenario: Scenario,
    nodes: Vec<MenuNode>,
    check_state: CheckState,
    nav: MenuNav,
    hover: HoverIntent,
    /// Whether a hover timer task is in flight, so `Hovered` arms exactly one.
    hover_timer_armed: bool,
    /// Set while driving by keys, so the cursor row strokes the focus ring.
    keyboard_focus: bool,
    /// The last report the menu made, shown at the bottom of the window.
    last_event: Option<SharedString>,
    focus_handle: FocusHandle,
}

impl MenuTreeDemo {
    fn new(scenario: Scenario, cx: &mut Context<Self>) -> Self {
        let check_state = CheckState::default();
        let nodes = build_tree(scenario, &check_state);

        Self {
            scenario,
            nodes,
            check_state,
            nav: MenuNav::new(),
            hover: HoverIntent::new(),
            hover_timer_armed: false,
            keyboard_focus: false,
            last_event: None,
            focus_handle: cx.focus_handle(),
        }
    }

    /// Opens the root panel on its first selectable row.
    fn open_root(&mut self, by_keyboard: bool, cx: &mut Context<Self>) {
        self.nav.reset(&self.nodes);
        self.keyboard_focus = by_keyboard;
        self.last_event = Some(if by_keyboard {
            "keyboard: opened the root menu".into()
        } else {
            "pointer: opened the root menu".into()
        });
        cx.notify();
    }

    /// Dismisses the whole cascade, from an outside click or a key that backs
    /// out of the root.
    fn close_menu(&mut self, cx: &mut Context<Self>) {
        self.nav.close_all();
        self.hover = HoverIntent::new();
        self.hover_timer_armed = false;
        self.keyboard_focus = false;
        cx.notify();
    }

    /// Rebuilds the row tree from the demo's own state, so a toggled check
    /// re-renders its mark while the menu stays open.
    fn rebuild_tree(&mut self) {
        self.nodes = build_tree(self.scenario, &self.check_state);
    }

    /// One report from the cascade: the demo's whole pointer-side wiring.
    fn on_menu_event(&mut self, event: MenuTreeEvent, cx: &mut Context<Self>) {
        match event {
            // The pointer entered a row: start the open delay for it.
            MenuTreeEvent::Hovered { level, index } => {
                self.keyboard_focus = false;
                self.hover.enter(level, index, Instant::now());
                self.arm_hover_open(cx);
            }
            // A click on a submenu row: open its flyout right away, without
            // the hover delay.
            MenuTreeEvent::Opened { level, index } => {
                self.keyboard_focus = false;
                self.nav.hover(level, index, &self.nodes);
                self.hover.mark_open(level, Some(index));
                self.last_event = Some(format!("click: opened level {level} row {index}").into());
                cx.notify();
            }
            // A leaf row was activated: map the id to the demo's own state.
            MenuTreeEvent::Activated { id, path } => self.activate(id, &path, cx),
        }
    }

    /// Arms the hover-open timer: one `Timer` of the component's
    /// `HOVER_OPEN_DELAY` per pending row, re-armed while a newer row is still
    /// resting under the pointer.
    fn arm_hover_open(&mut self, cx: &mut Context<Self>) {
        if self.hover_timer_armed {
            return;
        }
        self.hover_timer_armed = true;

        cx.spawn(async move |this, cx| {
            cx.background_executor().timer(HOVER_OPEN_DELAY).await;

            cx.update(|cx| {
                if let Some(demo) = this.upgrade() {
                    demo.update(cx, |demo, cx| demo.on_hover_tick(cx));
                }
            });
        })
        .detach();
    }

    /// What the timer decided when it fired.
    fn on_hover_tick(&mut self, cx: &mut Context<Self>) {
        self.hover_timer_armed = false;

        match self.hover.tick(Instant::now()) {
            HoverTick::Open(level, index) => {
                self.nav.hover(level, index, &self.nodes);
                self.hover.mark_open(level, Some(index));
                self.last_event = Some(format!("hover: opened level {level} row {index}").into());
                cx.notify();
            }
            HoverTick::CloseLevel(_) => {
                // This demo never reports a hover-leave (see the module docs),
                // so a level cannot close through the timer here. A product
                // that feeds `leave` would now shrink the cursor to `level`.
            }
            HoverTick::Nothing => {
                // A newer row is still inside its delay: keep waiting for it.
                if self.hover.pending().is_some() {
                    self.arm_hover_open(cx);
                }
            }
        }
    }

    /// Activation, shared by the pointer path and the keyboard path: map the
    /// row's id to the demo's state, then keep the menu open only when the row
    /// says it does.
    fn activate(&mut self, id: Option<SharedString>, path: &[usize], cx: &mut Context<Self>) {
        let Some(node) = node_at_path(&self.nodes, path) else {
            return;
        };
        let id = id.or_else(|| node.id.clone());
        let keeps_menu_open = node.keeps_menu_open();

        match id.as_deref() {
            Some("view.line-numbers") => {
                self.check_state.line_numbers = !self.check_state.line_numbers;
                self.rebuild_tree();
                self.last_event = Some(
                    format!(
                        "activate \"view.line-numbers\" → line-numbers {}",
                        on_off(self.check_state.line_numbers,)
                    )
                    .into(),
                );
            }
            Some("view.word-wrap") => {
                self.check_state.word_wrap = !self.check_state.word_wrap;
                self.rebuild_tree();
                self.last_event = Some(
                    format!(
                        "activate \"view.word-wrap\" → word-wrap {}",
                        on_off(self.check_state.word_wrap,)
                    )
                    .into(),
                );
            }
            Some("view.sort.name") => {
                self.check_state.sort_by = RowSort::Name;
                self.rebuild_tree();
                self.last_event = Some("activate \"view.sort.name\"".into());
            }
            Some("view.sort.size") => {
                self.check_state.sort_by = RowSort::Size;
                self.rebuild_tree();
                self.last_event = Some("activate \"view.sort.size\"".into());
            }
            Some("view.sort.modified") => {
                self.check_state.sort_by = RowSort::Modified;
                self.rebuild_tree();
                self.last_event = Some("activate \"view.sort.modified\"".into());
            }
            Some(other) => {
                self.last_event = Some(format!("activate \"{other}\"").into());
            }
            None => {
                self.last_event = Some("activate a row without an id".into());
            }
        }

        if keeps_menu_open {
            cx.notify();
        } else {
            self.close_menu(cx);
        }
    }

    /// The keyboard path. Any opening key opens the closed root; otherwise the
    /// component's cursor vocabulary maps one to one onto the keys.
    fn on_key(&mut self, event: &KeyDownEvent, cx: &mut Context<Self>) {
        let open = self.nav.cursor().depth() > 0;
        let key = event.keystroke.key.as_str();

        match (key, open) {
            ("down", false) | ("right", false) | ("enter", false) => self.open_root(true, cx),

            ("down" | "up", true) => {
                // `step` wraps within the deepest open level and skips
                // separators, headers and disabled rows.
                let moved = if key == "down" {
                    self.nav.step(&self.nodes, true)
                } else {
                    self.nav.step(&self.nodes, false)
                };

                if moved {
                    self.keyboard_focus = true;
                    cx.notify();
                }
            }
            ("right" | "enter", true) => match self.nav.open_child(&self.nodes) {
                ChildAction::Opened => {
                    self.keyboard_focus = true;
                    self.last_event = Some(
                        format!("keyboard: opened level {}", self.nav.cursor().depth()).into(),
                    );
                    cx.notify();
                }
                ChildAction::Activate => {
                    let path = self.nav.cursor().path().to_vec();
                    let id = self
                        .nav
                        .current(&self.nodes)
                        .and_then(|node| node.id.clone());
                    self.activate(id, &path, cx);
                }
                ChildAction::None => {}
            },

            ("left" | "escape", true) => match self.nav.back() {
                BackAction::ClosedLevel => {
                    self.keyboard_focus = true;
                    self.last_event = Some("keyboard: closed one level".into());
                    cx.notify();
                }
                BackAction::Dismissed => {
                    self.last_event = Some("keyboard: dismissed the menu".into());
                    self.close_menu(cx);
                }
            },

            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

impl Render for MenuTreeDemo {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let open = self.nav.cursor().depth() > 0;

        let layer = div()
            .size_full()
            .relative()
            .track_focus(&self.focus_handle)
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, _window, cx| {
                this.on_key(event, cx);
            }))
            .child(render_chrome(self, cx));

        if !open {
            return layer;
        }

        // The cascade mounts in a full-size layer, with the overlay behind it:
        // the panels occlude the overlay where they sit, so a click inside a
        // panel reaches its rows and a click anywhere else dismisses.
        let dismiss = cx.weak_entity();
        let overlay = render_menu_overlay("demo-tree-overlay", move |_, cx| {
            if let Some(demo) = dismiss.upgrade() {
                demo.update(cx, |demo, cx| {
                    demo.last_event = Some("outside click: dismissed the menu".into());
                    demo.close_menu(cx);
                });
            }
        });

        let events = cx.weak_entity();
        let layout = MenuTreeLayout {
            origin: self.scenario.origin(),
            width: ROOT_WIDTH,
            viewport: WINDOW,
            keyboard_focus: self.keyboard_focus,
        };

        layer.child(overlay).child(render_menu_tree(
            "demo-tree",
            &self.nodes,
            &self.nav,
            layout,
            move |event, cx| {
                if let Some(demo) = events.upgrade() {
                    demo.update(cx, |demo, cx| demo.on_menu_event(event, cx));
                }
            },
            cx,
        ))
    }
}

/// The demo's own chrome: scenario name, driving hints, the trigger that opens
/// the root panel, and the last report the menu made.
fn render_chrome(demo: &MenuTreeDemo, cx: &Context<MenuTreeDemo>) -> Stateful<Div> {
    let colors = bolt::colors(cx);
    let title_size = fonts::ui_px(cx, bolt::tokens::ui(15.0));
    let hint_size = fonts::ui_px(cx, bolt::tokens::ui(12.5));

    div()
        .id("demo-chrome")
        .size_full()
        .bg(colors.desk)
        .text_color(colors.foreground)
        .font_family(fonts::ui_family(cx))
        .child(
            div()
                .absolute()
                .left(px(24.0))
                .top(px(20.0))
                .flex()
                .flex_col()
                .gap(px(4.0))
                .child(
                    div()
                        .text_size(title_size)
                        .text_color(colors.strong)
                        .child(format!(
                            "Cascading menu — {} scenario",
                            demo.scenario.name()
                        )),
                )
                .child(
                    div()
                        .text_size(hint_size)
                        .text_color(colors.muted)
                        .child(demo.scenario.hint()),
                ),
        )
        .child(render_trigger(cx))
        .child(
            div()
                .absolute()
                .right(px(24.0))
                .bottom(px(18.0))
                .flex()
                .flex_col()
                .items_end()
                .gap(px(4.0))
                .text_size(hint_size)
                .child(
                    div()
                        .text_color(colors.muted)
                        .child(demo.check_state.summary()),
                )
                .child(
                    div().text_color(colors.foreground).child(
                        demo.last_event
                            .clone()
                            .unwrap_or_else(|| "no report yet".into()),
                    ),
                ),
        )
}

/// The trigger that opens the root panel by pointer, standing in for whatever
/// control a product hangs its menu off.
fn render_trigger(cx: &Context<MenuTreeDemo>) -> Stateful<Div> {
    let colors = bolt::colors(cx);
    let open = cx.weak_entity();

    div()
        .id("demo-trigger")
        .absolute()
        .left(px(24.0))
        .top(px(96.0))
        .w(px(160.0))
        .h(px(30.0))
        .flex()
        .items_center()
        .justify_center()
        .bg(colors.raised)
        .border_1()
        .border_color(colors.line_2)
        .text_size(fonts::ui_px(cx, bolt::tokens::ui(12.5)))
        .text_color(colors.strong)
        .hover(|style| style.bg(colors.hover))
        .cursor_pointer()
        .child("Open menu")
        .on_click(move |_, _, cx| {
            if let Some(demo) = open.upgrade() {
                demo.update(cx, |demo, cx| demo.open_root(false, cx));
            }
        })
}

// ---------------------------------------------------------------------------
// Assets
// ---------------------------------------------------------------------------

/// The Lucide frame every one of these icons shares: 24×24, no fill, a 2 px
/// stroke in the text color.
const ICON_FRAME: &str = concat!(
    r#"<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24""#,
    r#" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round""#,
    r#" stroke-linejoin="round">"#,
    "{body}",
    "</svg>",
);

/// Every icon this example draws, as `(asset name, svg body)` pairs: the two
/// the menu rows draw by themselves (check, chevron-right) and the leading
/// icons the demo's rows carry. A product serves these from its own icon set
/// (see its own `Assets`), the same `icons/<name>.svg` paths `IconName`
/// resolves to.
const ICONS: &[(&str, &str)] = &[
    ("check", r#"<path d="M20 6 9 17l-5-5" />"#),
    ("chevron-right", r#"<path d="m9 18 6-6-6-6" />"#),
    (
        "table",
        r#"<path d="M12 3v18" /><rect width="18" height="18" x="3" y="3" rx="2" /><path d="M3 9h18" /><path d="M3 15h18" />"#,
    ),
    (
        "copy",
        r#"<rect width="14" height="14" x="8" y="8" rx="2" ry="2" /><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2" />"#,
    ),
    (
        "download",
        r#"<path d="M12 15V3" /><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /><path d="m7 10 5 5 5-5" />"#,
    ),
    (
        "trash",
        r#"<path d="M10 11v6" /><path d="M14 11v6" /><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" /><path d="M3 6h18" /><path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />"#,
    ),
    (
        "eye",
        r#"<path d="M2.062 12.348a1 1 0 0 1 0-.696 10.75 10.75 0 0 1 19.876 0 1 1 0 0 1 0 .696 10.75 10.75 0 0 1-19.876 0" /><circle cx="12" cy="12" r="3" />"#,
    ),
    (
        "undo-2",
        r#"<path d="M9 14 4 9l5-5" /><path d="M4 9h10.5a5.5 5.5 0 0 1 5.5 5.5a5.5 5.5 0 0 1-5.5 5.5H11" />"#,
    ),
    (
        "refresh-cw",
        r#"<path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8" /><path d="M21 3v5h-5" /><path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16" /><path d="M8 16H3v5" />"#,
    ),
    (
        "funnel",
        r#"<path d="M10 20a1 1 0 0 0 .553.895l2 1A1 1 0 0 0 14 21v-7a2 2 0 0 1 .517-1.341L21.74 4.67A1 1 0 0 0 21 3H3a1 1 0 0 0-.742 1.67l7.225 7.989A2 2 0 0 1 10 14z" />"#,
    ),
    (
        "archive",
        r#"<rect width="20" height="5" x="2" y="3" rx="1" /><path d="M4 8v11a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8" /><path d="M10 12h4" />"#,
    ),
    (
        "inbox",
        r#"<polyline points="22 12 16 12 14 15 10 15 8 12 2 12" /><path d="M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z" />"#,
    ),
];

/// The demo's asset source: it serves the icon paths the rows resolve. `()`
/// would leave the check marks and chevrons unpainted, so a runnable example
/// needs at least this much of a product's asset layer.
struct Assets;

impl AssetSource for Assets {
    fn load(&self, path: &str) -> gpui::Result<Option<Cow<'static, [u8]>>> {
        let body = path
            .strip_prefix("icons/")
            .and_then(|file| file.strip_suffix(".svg"))
            .and_then(|name| ICONS.iter().find(|(icon, _)| *icon == name))
            .map(|(_, body)| ICON_FRAME.replace("{body}", body));

        Ok(body.map(|svg| Cow::Owned(svg.into_bytes())))
    }

    fn list(&self, path: &str) -> gpui::Result<Vec<SharedString>> {
        let names = ICONS
            .iter()
            .map(|(name, _)| SharedString::from(format!("icons/{name}.svg")))
            .filter(|file| file.starts_with(path.trim_end_matches('/')))
            .collect();

        Ok(names)
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    let scenario = std::env::args()
        .nth(1)
        .and_then(|name| Scenario::from_arg(&name))
        .unwrap_or_else(|| {
            eprintln!("usage: menu_tree_demo <nested|flip|bottom|checks|keyboard>");
            std::process::exit(2);
        });

    println!(
        "menu_tree_demo: scenario={} display={:?}",
        scenario.name(),
        std::env::var("DISPLAY").unwrap_or_default(),
    );

    let options = WindowOptions {
        titlebar: Some(TitlebarOptions {
            title: Some(format!("{TITLE} — {}", scenario.name()).into()),
            ..Default::default()
        }),
        window_bounds: Some(WindowBounds::Windowed(Bounds {
            origin: point(px(100.0), px(60.0)),
            size: WINDOW,
        })),
        window_min_size: Some(WINDOW),
        ..Default::default()
    };

    // The product startup path: components first, then the bundled fonts, the
    // font settings and the theme, then the window under `gpui_component`'s
    // `Root`, which is what the design system expects to size the rem and
    // style the text.
    gpui_platform::application()
        .with_assets(Assets)
        .run(move |cx: &mut App| {
            gpui_component::init(cx);
            typography::load_bundled_fonts(cx);
            fonts::init(cx, fonts::FontSettings::default());
            theme::install(
                brand::Brand::neutral(),
                palette::PaletteMode::Dark,
                density::Density::Default,
                cx,
            );

            let window = cx.open_window(options, |window, cx| {
                let demo = cx.new(|cx| MenuTreeDemo::new(scenario, cx));
                let focus = demo.read(cx).focus_handle.clone();
                window.focus(&focus, cx);
                cx.new(|cx| Root::new(demo, window, cx))
            });

            if let Err(error) = window {
                eprintln!("menu_tree_demo: failed to open the demo window: {error}");
            }

            cx.activate(true);
        });
}
