//! Visual and layout proof for the cascading-menu composite
//! (`bolt::composites::menu_tree`), through the public API a surface mounts:
//! `MenuNode`, `MenuNav`, `MenuTreeLayout`, `render_menu_tree` and
//! `render_menu_overlay`.
//!
//! Every case opens the cascade in a real test window, drives the cursor
//! deliberately through `MenuNav` (no hover timers, so each case is
//! deterministic), and asserts the laid-out bounds of the levels and rows
//! through `debug_bounds`. A windowed tour of the same scenarios lives in
//! `examples/menu_tree_demo.rs`; this file is its offscreen counterpart: no
//! display, no GPU, no clock.
//!
//! The panels record an element id (`cascade-level-`*n*) but no debug
//! selector, so each level's frame is read through its rows: see
//! `level_frame` inside. The checkable and radio marks are read through the
//! accessibility frame, and the keyboard focus ring through its selector, the
//! way the crate's own tests read them.
//!
//! Setting `BOLT_MENU_FRAMES_DIR` asks each case to also write its rendered
//! frame as a PNG; see `write_frame` inside for why that cannot happen in
//! this harness today, and what the call answers when it is asked anyway.

// The whole proof sits in a `#[cfg(test)]` module so its helpers may fail a
// broken test with `expect`/`panic!` the way the crate's `#[cfg(test)]`
// fixtures do (see `clippy.toml`); a plain helper in a `tests/` target may
// not. The `#[gpui::test]` cases are exempt either way, because the macro
// defines each of them inside a `#[test]` fn.
#[cfg(test)]
mod menu_tree_visual {
    use std::sync::{Arc, Mutex};

    use bolt::composites::{
        BackAction, ChildAction, MenuNav, MenuNode, MenuTreeEvent, MenuTreeLayout,
        render_menu_overlay, render_menu_tree,
    };
    use bolt::primitives::FOCUS_RING_SELECTOR;
    use bolt::tokens::MenuMetrics;
    use gpui::{
        AccessibilityFrame, App, Bounds, Context, Entity, FrameObserver, IntoElement, Modifiers,
        ParentElement as _, Pixels, Point, Render, Role, ScrollDelta, ScrollWheelEvent,
        Styled as _, TestAppContext, Toggled, TouchPhase, VisualTestContext, Window, div, point,
        px, size,
    };

    /// Environment variable that asks this proof to also write each case's
    /// rendered frame as a PNG, for eyes the layout assertions do not replace.
    const FRAMES_DIR_ENV: &str = "BOLT_MENU_FRAMES_DIR";

    /// The row of the nested tree that opens the first flyout.
    const EXPORT_ROW: usize = 2;
    /// The row under `Export` that opens the second flyout.
    const FORMATS_ROW: usize = 0;
    /// The row under `Export` the cursor steps to, past the separator.
    const EXPORT_ALL_ROW: usize = 2;
    /// The row of the flip tree that opens the flyout.
    const MOVE_TO_ROW: usize = 2;
    /// The row of the bottom tree that opens the tall flyout.
    const JUMP_ROW: usize = 2;

    // ---------------------------------------------------------------------------
    // The proof surface
    // ---------------------------------------------------------------------------

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Scenario {
        /// A two-deep cascade near the left edge.
        Nested,
        /// The root panel hugs the right edge, so its flyout must flip left.
        Flip,
        /// The root panel sits near the bottom edge; `rows` is how many rows the
        /// flyout beside it carries.
        Bottom { rows: usize },
        /// A flyout of checkable and radio rows toggled through the callback.
        Checks,
    }

    /// The choice of the radio group in the `Checks` scenario.
    #[derive(Clone, Copy, PartialEq)]
    enum RowSort {
        Name,
        Size,
        Modified,
    }

    /// The state behind the `Checks` scenario: the surface owns it, activation
    /// changes it, and the tree is rebuilt from it so the marks re-render.
    #[derive(Clone, Copy)]
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
        fn apply(&mut self, id: &str) {
            match id {
                "view.line-numbers" => self.line_numbers = !self.line_numbers,
                "view.word-wrap" => self.word_wrap = !self.word_wrap,
                "view.sort.name" => self.sort_by = RowSort::Name,
                "view.sort.size" => self.sort_by = RowSort::Size,
                "view.sort.modified" => self.sort_by = RowSort::Modified,
                _ => {}
            }
        }
    }

    fn build_tree(scenario: Scenario, checks: &CheckState) -> Vec<MenuNode> {
        match scenario {
            Scenario::Nested => vec![
                MenuNode::action("open", "Open table"),
                MenuNode::action("copy", "Copy as INSERT"),
                MenuNode::row("Export").children(vec![
                    MenuNode::row("Formats").children(vec![
                        MenuNode::action("export.csv", "CSV"),
                        MenuNode::action("export.json", "JSON"),
                        MenuNode::action("export.parquet", "Parquet"),
                        MenuNode::action("export.xml", "XML").disabled(),
                    ]),
                    MenuNode::separator(),
                    MenuNode::action("export.all", "Export all rows"),
                ]),
                MenuNode::separator(),
                MenuNode::row("Danger zone")
                    .children(vec![MenuNode::action("drop", "Drop table").danger()]),
                MenuNode::action("close", "Close"),
            ],
            Scenario::Flip => vec![
                MenuNode::action("rename", "Rename"),
                MenuNode::separator(),
                MenuNode::row("Move to").children(vec![
                    MenuNode::action("move.archive", "Archive"),
                    MenuNode::action("move.trash", "Trash"),
                    MenuNode::action("move.inbox", "Inbox"),
                ]),
                MenuNode::action("delete", "Delete").danger(),
            ],
            Scenario::Bottom { rows } => vec![
                MenuNode::action("refresh", "Refresh"),
                MenuNode::action("filter", "Filter rows"),
                MenuNode::row("Jump to row").children(
                    (1..=rows)
                        .map(|row| MenuNode::action(format!("jump.{row}"), format!("Row {row}")))
                        .collect(),
                ),
            ],
            Scenario::Checks => vec![
                MenuNode::row("View").children(vec![
                    MenuNode::checked("view.line-numbers", "Line numbers", checks.line_numbers)
                        .element_id("line-numbers"),
                    MenuNode::checked("view.word-wrap", "Word wrap", checks.word_wrap)
                        .element_id("word-wrap"),
                    MenuNode::separator(),
                    MenuNode::radio(
                        "view.sort.name",
                        "Sort by name",
                        checks.sort_by == RowSort::Name,
                    )
                    .element_id("sort-name"),
                    MenuNode::radio(
                        "view.sort.size",
                        "Sort by size",
                        checks.sort_by == RowSort::Size,
                    )
                    .element_id("sort-size"),
                    MenuNode::radio(
                        "view.sort.modified",
                        "Sort by modified",
                        checks.sort_by == RowSort::Modified,
                    )
                    .element_id("sort-modified"),
                    MenuNode::action("view.randomize", "Randomize order").keep_open(true),
                ]),
                MenuNode::action("view.reset", "Reset defaults"),
            ],
        }
    }

    fn root_origin(scenario: Scenario) -> Point<Pixels> {
        match scenario {
            Scenario::Nested => point(px(40.0), px(150.0)),
            Scenario::Flip => point(px(1640.0), px(150.0)),
            Scenario::Bottom { .. } => point(px(40.0), px(588.0)),
            Scenario::Checks => point(px(240.0), px(150.0)),
        }
    }

    /// The row a path points at, resolved by walking the tree: the component's
    /// own lookup is crate-private, so a surface resolves the path itself, the way
    /// the API example does.
    fn node_at_path<'a>(nodes: &'a [MenuNode], path: &[usize]) -> Option<&'a MenuNode> {
        let (last, parents) = path.split_last()?;
        let level = parents.iter().try_fold(nodes, |level, index| {
            level.get(*index).map(|node| node.children.as_slice())
        })?;

        level.get(*last)
    }

    /// The host surface: everything the component leaves to its owner, in one
    /// renderable view. The cascade is mounted the way the component's docs
    /// prescribe: a full-size layer, the dismiss overlay behind the tree, one
    /// event callback that reports back into the host's own state.
    struct CascadeHost {
        scenario: Scenario,
        checks: CheckState,
        nodes: Vec<MenuNode>,
        nav: MenuNav,
        origin: Point<Pixels>,
        keyboard_focus: bool,
        reports: Arc<Mutex<Vec<String>>>,
    }

    impl CascadeHost {
        fn new(scenario: Scenario) -> Self {
            let checks = CheckState::default();
            let nodes = build_tree(scenario, &checks);

            Self {
                scenario,
                checks,
                nodes,
                nav: MenuNav::new(),
                origin: root_origin(scenario),
                keyboard_focus: false,
                reports: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Rebuilds the row tree from the host's own state, so a toggled check
        /// re-renders its mark while the menu stays open.
        fn rebuild(&mut self) {
            self.nodes = build_tree(self.scenario, &self.checks);
        }

        fn report(&self, entry: String) {
            self.reports.lock().expect("report log lock").push(entry);
        }

        /// One report from the cascade: the wiring a surface owns. The hover
        /// report only lands in the log here — the cases drive `MenuNav`
        /// deliberately instead of through a timer.
        fn on_menu_event(&mut self, event: MenuTreeEvent, cx: &mut Context<Self>) {
            match event {
                MenuTreeEvent::Activated { id, path } => {
                    let keeps_menu_open =
                        node_at_path(&self.nodes, &path).is_some_and(MenuNode::keeps_menu_open);
                    self.report(format!("activated {}", id.as_deref().unwrap_or("<no id>")));

                    if let Some(id) = id.as_deref() {
                        self.checks.apply(id);
                        self.rebuild();
                    }

                    if keeps_menu_open {
                        cx.notify();
                    } else {
                        self.nav.close_all();
                        cx.notify();
                    }
                }
                MenuTreeEvent::Opened { level, index } => {
                    self.report(format!("opened level {level} row {index}"));
                    self.nav.hover(level, index, &self.nodes);
                    cx.notify();
                }
                MenuTreeEvent::Hovered { level, index } => {
                    self.report(format!("hovered level {level} row {index}"));
                }
            }
        }
    }

    impl Render for CascadeHost {
        fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            let overlay_entity = cx.weak_entity();
            let tree_entity = cx.weak_entity();
            let layout = MenuTreeLayout {
                origin: self.origin,
                width: MenuMetrics::SUBMENU_WIDTH,
                viewport: window.viewport_size(),
                keyboard_focus: self.keyboard_focus,
            };

            div()
                .size_full()
                .relative()
                .child(render_menu_overlay("cascade-overlay", move |_, cx| {
                    if let Some(host) = overlay_entity.upgrade() {
                        host.update(cx, |host, cx| {
                            host.nav.close_all();
                            host.report("dismissed".into());
                            cx.notify();
                        });
                    }
                }))
                .child(render_menu_tree(
                    "cascade",
                    &self.nodes,
                    &self.nav,
                    layout,
                    move |event, cx: &mut App| {
                        if let Some(host) = tree_entity.upgrade() {
                            host.update(cx, |host, cx| host.on_menu_event(event, cx));
                        }
                    },
                    cx,
                ))
        }
    }

    // ---------------------------------------------------------------------------
    // Reading the frames
    // ---------------------------------------------------------------------------

    /// Keeps the latest accessibility frame of the window it observes, the way the
    /// crate's own checked-row test does.
    #[derive(Default)]
    struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

    impl FrameObserver for FrameCapture {
        fn accessibility_updated(&self, frame: &AccessibilityFrame) {
            *self.0.lock().expect("frame capture lock") = Some(frame.clone());
        }
    }

    impl FrameCapture {
        /// Starts observing the frames of `window`, which draws one frame.
        fn observe(window: &mut VisualTestContext) -> Arc<Self> {
            let capture = Arc::new(Self::default());
            let capture_for_window = capture.clone();

            window.update(move |window, _| {
                window.observe_frames(&capture_for_window);
                window.refresh();
            });
            window.run_until_parked();

            capture
        }

        /// Draws a new frame of `window` and returns it.
        fn frame(&self, window: &mut VisualTestContext) -> AccessibilityFrame {
            window.update(|window, _| window.refresh());
            window.run_until_parked();

            self.0
                .lock()
                .expect("frame capture lock")
                .clone()
                .expect("the observed window has drawn a frame")
        }
    }

    /// The accessibility node of the element `id`, from a captured frame.
    fn node<'a>(frame: &'a AccessibilityFrame, id: &str) -> &'a gpui::accesskit::Node {
        let (_, frame_node) = frame
            .nodes()
            .find(|(_, node)| node.id() == id)
            .unwrap_or_else(|| panic!("{id} is in the frame"));

        frame
            .accessibility_node(frame_node)
            .unwrap_or_else(|| panic!("{id} is accessible"))
    }

    /// The frame of one level's panel, read through its first and last row.
    ///
    /// The panel records an element id but no debug selector, so a test cannot
    /// address it directly. The first row of a level sits
    /// `MenuMetrics::ROW_INSET` inside the panel's left edge and
    /// `MenuMetrics::PADDING_Y` under its top, the last row's bottom sits
    /// `MenuMetrics::PADDING_Y` above the panel's bottom, and the panel is as wide
    /// as the layout made it — so the frame follows from the rows. A panel capped
    /// to scroll inside lays its rows out past the cap, so for a capped level the
    /// height this returns is the content's, not the panel's; the capped case
    /// measures its cap by scrolling instead.
    fn level_frame(
        window: &mut VisualTestContext,
        first_row: &'static str,
        last_row: &'static str,
        width: Pixels,
    ) -> Bounds<Pixels> {
        let first = window
            .debug_bounds(first_row)
            .unwrap_or_else(|| panic!("{first_row} is laid out"));
        let last = window
            .debug_bounds(last_row)
            .unwrap_or_else(|| panic!("{last_row} is laid out"));
        let origin = point(
            first.origin.x - MenuMetrics::ROW_INSET,
            first.origin.y - MenuMetrics::PADDING_Y,
        );

        Bounds {
            origin,
            size: size(width, last.bottom() + MenuMetrics::PADDING_Y - origin.y),
        }
    }

    /// The bounds of the element `selector`, which must be laid out.
    fn bounds_of(window: &mut VisualTestContext, selector: &'static str) -> Bounds<Pixels> {
        window
            .debug_bounds(selector)
            .unwrap_or_else(|| panic!("{selector} is laid out"))
    }

    /// Asserts the keyboard focus ring sits on the row `selector`.
    fn assert_ring_on(window: &mut VisualTestContext, row: &'static str) {
        let ring = window
            .debug_bounds(FOCUS_RING_SELECTOR)
            .unwrap_or_else(|| panic!("the cursor row {row} wears the focus ring"));
        let row = bounds_of(window, row);

        assert!(
            row.contains(&ring.center()),
            "the ring at {ring:?} sits on the row {row:?}"
        );
    }

    /// Sends `lines` scroll-wheel lines at `over`, as a wheel over the cascade.
    fn scroll_wheel(window: &mut VisualTestContext, over: Point<Pixels>, lines: f32) {
        window.simulate_event(ScrollWheelEvent {
            position: over,
            delta: ScrollDelta::Lines(point(0.0, lines)),
            modifiers: Default::default(),
            touch_phase: TouchPhase::Moved,
        });
        window.run_until_parked();
    }

    /// Writes `name.png`, the rendered frame of one case, under
    /// [`FRAMES_DIR_ENV`] when the runner set it.
    ///
    /// Unset, nothing is written and nothing leaves the test, so the committed
    /// proof stays hermetic. Set, the frame still cannot be written in this
    /// harness: `Window::render_to_image` needs a headless renderer, and the
    /// `#[gpui::test]` context builds its test platform with no renderer factory
    /// while the crate's test build has no renderer to give it — the wgpu headless
    /// renderer sits behind the platform crate's `test-support` feature, which the
    /// dev-dependencies do not enable. The call answers `render_to_image not
    /// available: no HeadlessRenderer configured`, reported on stderr so a runner
    /// that asked for frames sees exactly why none appeared; the layout
    /// assertions carry the proof. The save path stays wired, so the day a
    /// renderer is reachable the frames appear without another change here.
    fn write_frame(name: &str, window: &mut VisualTestContext) {
        let Some(dir) = std::env::var_os(FRAMES_DIR_ENV) else {
            return;
        };

        match window.update(|window, _| window.render_to_image()) {
            Ok(frame) => {
                let path = std::path::Path::new(&dir).join(format!("{name}.png"));

                match frame.save(&path) {
                    Ok(()) => eprintln!("menu_tree_visual: wrote {}", path.display()),
                    Err(error) => {
                        eprintln!("menu_tree_visual: {name}: the frame did not save: {error}")
                    }
                }
            }
            Err(error) => eprintln!("menu_tree_visual: {name}: no frame to write: {error}"),
        }
    }

    // ---------------------------------------------------------------------------
    // The harness
    // ---------------------------------------------------------------------------

    /// Opens one scenario in a themed test window and returns its host view.
    fn open(
        cx: &mut TestAppContext,
        scenario: Scenario,
    ) -> (Entity<CascadeHost>, &mut VisualTestContext) {
        cx.update(gpui_component::init);
        cx.update(|cx| {
            bolt::theme::install(
                bolt::brand::Brand::neutral(),
                bolt::palette::PaletteMode::Dark,
                bolt::density::Density::Default,
                cx,
            );
        });

        let (host, window) = cx.add_window_view(|_, _| CascadeHost::new(scenario));
        window.run_until_parked();

        (host, window)
    }

    /// Applies one deliberate state change and re-renders the window, so the next
    /// `debug_bounds` read sees the frame the change drew.
    fn drive(
        host: &Entity<CascadeHost>,
        window: &mut VisualTestContext,
        change: impl FnOnce(&mut CascadeHost, &mut Context<CascadeHost>),
    ) {
        window.update(|window, cx| {
            host.update(cx, |host, cx| {
                change(host, cx);
                cx.notify();
            });
            window.refresh();
        });
        window.run_until_parked();
    }

    /// The cursor path of the host's cascade.
    fn cursor_path(host: &Entity<CascadeHost>, window: &mut VisualTestContext) -> Vec<usize> {
        window.update(|_, cx| host.read(cx).nav.cursor().path().to_vec())
    }

    // ---------------------------------------------------------------------------
    // The cases
    // ---------------------------------------------------------------------------

    #[gpui::test]
    fn a_nested_flyout_sits_beside_its_parent_and_keeps_it_open(cx: &mut TestAppContext) {
        let (host, window) = open(cx, Scenario::Nested);
        drive(&host, window, |host, _| {
            host.nav.reset(&host.nodes);
            while host.nav.cursor().highlight(0) != Some(EXPORT_ROW) {
                assert!(
                    host.nav.step(&host.nodes, true),
                    "the submenu row is reachable"
                );
            }
            assert_eq!(host.nav.open_child(&host.nodes), ChildAction::Opened);
            assert_eq!(host.nav.open_child(&host.nodes), ChildAction::Opened);
        });

        let level_0 = level_frame(
            window,
            "cascade-0-0",
            "cascade-0-5",
            MenuMetrics::SUBMENU_WIDTH,
        );
        let level_1 = level_frame(
            window,
            "cascade-1-0",
            "cascade-1-2",
            MenuMetrics::SUBMENU_WIDTH,
        );
        let level_2 = level_frame(
            window,
            "cascade-2-0",
            "cascade-2-3",
            MenuMetrics::SUBMENU_WIDTH,
        );
        let export_row = bounds_of(window, "cascade-0-2");
        let level_1_first_row = bounds_of(window, "cascade-1-0");

        // Beside the parent: the flyout's leading edge meets the parent's
        // trailing edge, with the overlap the two frames join on.
        assert!(
            level_1.origin.x >= level_0.right() - MenuMetrics::SUBMENU_OVERLAP,
            "level 1 at {} does not meet level 0's right edge {}",
            level_1.origin.x,
            level_0.right(),
        );

        // Level with the row that opened it: the flyout's top is the parent
        // panel's top plus the rows above that row, and its first row sits level
        // with the opening row itself.
        let rows_above = export_row.origin.y - level_0.origin.y - MenuMetrics::PADDING_Y;
        assert!(
            (level_1.origin.y - (level_0.origin.y + rows_above)).abs() <= px(2.0),
            "level 1's top {} is not level with the row that opened it ({})",
            level_1.origin.y,
            level_0.origin.y + rows_above,
        );
        assert!(
            (level_1_first_row.origin.y - export_row.origin.y).abs() <= px(2.0),
            "the flyout's first row {} does not sit level with the opening row {}",
            level_1_first_row.origin.y,
            export_row.origin.y,
        );

        // The second flyout opens beside the first, the same way.
        assert!(level_2.origin.x > level_1.origin.x);
        assert!(
            level_2.origin.x >= level_1.right() - MenuMetrics::SUBMENU_OVERLAP,
            "level 2 at {} does not meet level 1's right edge {}",
            level_2.origin.x,
            level_1.right(),
        );

        // The parent panel stayed open, on screen, with its submenu row
        // highlighted: the cursor — the highlight — rests on the row that opened
        // the flyout. (The wash the highlight draws is the same `selected` flag
        // whose ring the keyboard case asserts.)
        let viewport = window.update(|window, _| window.viewport_size());
        assert!(
            level_0.origin.x >= px(0.0)
                && level_0.origin.y >= px(0.0)
                && level_0.right() <= viewport.width
                && level_0.bottom() <= viewport.height,
            "the parent panel {level_0:?} left the surface"
        );
        assert_eq!(
            window.update(|_, cx| host.read(cx).nav.cursor().highlight(0)),
            Some(EXPORT_ROW),
            "the submenu row stays highlighted beside its open flyout"
        );
        assert_eq!(
            window.update(|_, cx| host.read(cx).nav.cursor().open_flyouts()),
            2,
            "both flyouts are open"
        );

        write_frame("nested", window);
    }

    #[gpui::test]
    fn a_flyout_flips_left_at_the_right_edge(cx: &mut TestAppContext) {
        let (host, window) = open(cx, Scenario::Flip);
        drive(&host, window, |host, _| {
            host.nav.reset(&host.nodes);
            assert!(host.nav.hover(0, MOVE_TO_ROW, &host.nodes));
        });

        let level_0 = level_frame(
            window,
            "cascade-0-0",
            "cascade-0-3",
            MenuMetrics::SUBMENU_WIDTH,
        );
        let level_1 = level_frame(
            window,
            "cascade-1-0",
            "cascade-1-2",
            MenuMetrics::SUBMENU_WIDTH,
        );
        let move_to_row = bounds_of(window, "cascade-0-2");
        let level_1_first_row = bounds_of(window, "cascade-1-0");
        let viewport = window.update(|window, _| window.viewport_size());

        assert!(
            level_1.origin.x < level_0.origin.x,
            "the flyout at {} did not flip left of the parent panel at {}",
            level_1.origin.x,
            level_0.origin.x,
        );
        assert!(
            level_1.right() <= viewport.width - MenuMetrics::FLYOUT_MARGIN,
            "the flipped flyout's right edge {} leaves the surface",
            level_1.right(),
        );
        // Mirrored, the flyout's trailing edge overlaps the parent's leading
        // edge, the same join the nested case asserts on the other side.
        assert!(
            level_1.right() >= level_0.origin.x,
            "the flipped flyout does not overlap the parent panel it mirrors"
        );
        // The flip is horizontal only: the flyout's first row stays level with the
        // row that opened it.
        assert!(
            (level_1_first_row.origin.y - move_to_row.origin.y).abs() <= px(2.0),
            "the flipped flyout's first row {} left the opening row {}",
            level_1_first_row.origin.y,
            move_to_row.origin.y,
        );

        write_frame("flip", window);
    }

    #[gpui::test]
    fn a_tall_flyout_shifts_up_and_a_taller_one_scrolls_inside_its_cap(cx: &mut TestAppContext) {
        // Taller than the room below it, shorter than the surface: it slides up
        // just enough to keep its bottom edge inside.
        let (host, window) = open(cx, Scenario::Bottom { rows: 26 });
        drive(&host, window, |host, _| {
            host.nav.reset(&host.nodes);
            assert!(host.nav.hover(0, JUMP_ROW, &host.nodes));
        });

        let jump_row = bounds_of(window, "cascade-0-2");
        let level_1 = level_frame(
            window,
            "cascade-1-0",
            "cascade-1-25",
            MenuMetrics::SUBMENU_WIDTH,
        );
        let aligned = jump_row.origin.y - MenuMetrics::PADDING_Y;
        let viewport = window.update(|window, _| window.viewport_size());

        assert!(
            level_1.origin.y < aligned,
            "the flyout's top {} did not slide above its aligned position {aligned}",
            level_1.origin.y,
        );
        assert!(
            level_1.bottom() <= viewport.height - MenuMetrics::FLYOUT_MARGIN,
            "the shifted flyout's bottom {} leaves the surface",
            level_1.bottom(),
        );

        write_frame("bottom", window);

        // Taller than the surface: the panel is capped, slides to the top margin,
        // and its content scrolls inside it.
        drive(&host, window, |host, _| {
            host.scenario = Scenario::Bottom { rows: 40 };
            host.rebuild();
            host.nav.reset(&host.nodes);
            assert!(host.nav.hover(0, JUMP_ROW, &host.nodes));
        });

        let first = "cascade-1-0";
        let last = "cascade-1-39";
        let first_before = bounds_of(window, first);
        let last_before = bounds_of(window, last);
        let viewport = window.update(|window, _| window.viewport_size());

        assert_eq!(
            first_before.origin.y - MenuMetrics::PADDING_Y,
            MenuMetrics::FLYOUT_MARGIN,
            "the capped panel slides up to the top margin"
        );
        assert!(
            last_before.bottom() > viewport.height,
            "the flyout's content is not taller than the surface"
        );

        // A wheel over the flyout scrolls its rows, which a panel drawn at its
        // full content height could not do: there would be no scroll range. The
        // panel is the only scroll container under the wheel.
        let over = bounds_of(window, "cascade-1-10").center();
        scroll_wheel(window, over, -20.0);
        let first_scrolled = bounds_of(window, first);
        let last_scrolled = bounds_of(window, last);
        let shift = first_before.origin.y - first_scrolled.origin.y;

        assert!(shift > px(0.0), "the capped flyout does not scroll");
        assert!(
            last_scrolled.bottom() <= viewport.height - MenuMetrics::FLYOUT_MARGIN,
            "the capped panel's scrolled content {} leaves the surface",
            last_scrolled.bottom(),
        );

        // The scroll range is the cap: another wheel moves the rows no further.
        scroll_wheel(window, over, -20.0);
        assert_eq!(
            bounds_of(window, first).origin.y,
            first_scrolled.origin.y,
            "the scroll did not saturate at the cap"
        );

        // The same flyout in a window tall enough for it: no cap, no slide, and a
        // wheel over it moves nothing — the panel is its full content height.
        window.simulate_resize(size(px(1920.0), px(1880.0)));
        drive(&host, window, |host, _| {
            host.nav.reset(&host.nodes);
            assert!(host.nav.hover(0, JUMP_ROW, &host.nodes));
        });

        let viewport = window.update(|window, _| window.viewport_size());
        let jump_row = bounds_of(window, "cascade-0-2");
        let first_fits = bounds_of(window, first);
        let last_fits = bounds_of(window, last);

        assert!(
            (first_fits.origin.y - jump_row.origin.y).abs() <= px(2.0),
            "a flyout that fits does not slide: its first row {} stays level with the row that opened it {}",
            first_fits.origin.y,
            jump_row.origin.y,
        );
        assert!(
            last_fits.bottom() <= viewport.height - MenuMetrics::FLYOUT_MARGIN,
            "the uncapped flyout's bottom {} leaves the surface",
            last_fits.bottom(),
        );
        let over = bounds_of(window, "cascade-1-10").center();
        scroll_wheel(window, over, -20.0);
        assert_eq!(
            bounds_of(window, first).origin.y,
            first_fits.origin.y,
            "a flyout that fits its surface does not scroll"
        );
    }

    #[gpui::test]
    fn checkable_rows_toggle_with_the_menu_open(cx: &mut TestAppContext) {
        let (host, window) = open(cx, Scenario::Checks);
        drive(&host, window, |host, _| {
            host.nav.reset(&host.nodes);
            assert_eq!(host.nav.open_child(&host.nodes), ChildAction::Opened);
        });

        let capture = FrameCapture::observe(window);

        // Before the toggle: the checkable rows report themselves unchecked, the
        // radio row reports the group's choice, and no mark is drawn.
        let before = capture.frame(window);
        assert_eq!(node(&before, "word-wrap").role(), Role::MenuItemCheckBox);
        assert_eq!(node(&before, "word-wrap").toggled(), Some(Toggled::False));
        assert_eq!(
            node(&before, "line-numbers").toggled(),
            Some(Toggled::False)
        );
        assert_eq!(node(&before, "sort-name").role(), Role::MenuItemRadio);
        assert_eq!(node(&before, "sort-name").toggled(), Some(Toggled::True));
        assert!(
            window.debug_bounds("word-wrap-check").is_none(),
            "an unchecked row draws no mark"
        );

        // Activate the checkable row through the component's own callback: a real
        // click on the row, which reports `Activated` back into the host, flips
        // the host's state and rebuilds the tree from it.
        let row = bounds_of(window, "word-wrap");
        window.simulate_click(row.center(), Modifiers::default());
        window.run_until_parked();

        let after = capture.frame(window);
        assert_eq!(node(&after, "word-wrap").toggled(), Some(Toggled::True));
        let mark = bounds_of(window, "word-wrap-check");
        assert!(
            row.contains(&mark.center()),
            "the new mark {mark:?} is drawn inside its row {row:?}"
        );
        assert!(
            window.debug_bounds("line-numbers-check").is_none(),
            "the other checkable row keeps its state"
        );
        assert_eq!(
            node(&after, "sort-name").toggled(),
            Some(Toggled::True),
            "the radio group keeps its choice"
        );
        assert_eq!(
            window.update(|_, cx| host
                .read(cx)
                .reports
                .lock()
                .expect("report log lock")
                .clone()),
            ["activated view.word-wrap"],
            "the click reached the host through the activation callback"
        );
        assert_eq!(
            window.update(|_, cx| host.read(cx).nav.cursor().path().to_vec()),
            [0, 0],
            "the cursor keeps its path through the toggle"
        );
        assert_eq!(
            window.update(|_, cx| host.read(cx).nav.cursor().open_flyouts()),
            1,
            "a checkable row keeps the menu open"
        );

        write_frame("checks", window);
    }

    #[gpui::test]
    fn the_keyboard_walks_the_levels_and_rings_the_cursor_row(cx: &mut TestAppContext) {
        let (host, window) = open(cx, Scenario::Nested);
        drive(&host, window, |host, _| {
            host.keyboard_focus = true;
            host.nav.reset(&host.nodes);
        });

        // A keystroke makes focus visible in the window, which is what strokes
        // the ring; from here on every step re-renders with the ring on the
        // cursor row of the deepest open level.
        window.simulate_keystrokes("down");
        assert_eq!(cursor_path(&host, window), [0]);
        assert_ring_on(window, "cascade-0-0");

        // Down to the submenu row, one row at a time.
        drive(&host, window, |host, _| {
            assert!(host.nav.step(&host.nodes, true));
        });
        assert_eq!(cursor_path(&host, window), [1]);
        drive(&host, window, |host, _| {
            assert!(host.nav.step(&host.nodes, true));
        });
        assert_eq!(cursor_path(&host, window), [EXPORT_ROW]);
        assert_ring_on(window, "cascade-0-2");

        // Right opens the flyout and lands on its first row.
        drive(&host, window, |host, _| {
            assert_eq!(host.nav.open_child(&host.nodes), ChildAction::Opened);
        });
        assert_eq!(cursor_path(&host, window), [EXPORT_ROW, FORMATS_ROW]);
        assert_ring_on(window, "cascade-1-0");

        // Down inside the flyout steps over its separator.
        drive(&host, window, |host, _| {
            assert!(
                host.nav.step(&host.nodes, true),
                "the step skips the separator"
            );
        });
        assert_eq!(cursor_path(&host, window), [EXPORT_ROW, EXPORT_ALL_ROW]);
        assert_ring_on(window, "cascade-1-2");

        write_frame("keyboard", window);

        // Left closes the deepest level and puts the ring back on the row that
        // opened it.
        drive(&host, window, |host, _| {
            assert_eq!(host.nav.back(), BackAction::ClosedLevel);
        });
        assert_eq!(cursor_path(&host, window), [EXPORT_ROW]);
        assert_ring_on(window, "cascade-0-2");

        // Left at the root reports dismissal, and the surface closes the cascade.
        drive(&host, window, |host, _| {
            assert_eq!(host.nav.back(), BackAction::Dismissed);
        });
        assert_eq!(
            cursor_path(&host, window),
            [EXPORT_ROW],
            "the root keeps its highlight until the surface closes"
        );
        drive(&host, window, |host, _| host.nav.close_all());
        assert!(
            window.debug_bounds("cascade-0-0").is_none(),
            "the closed cascade draws no rows"
        );
        assert!(
            window.debug_bounds(FOCUS_RING_SELECTOR).is_none(),
            "the closed cascade draws no ring"
        );
    }
}
