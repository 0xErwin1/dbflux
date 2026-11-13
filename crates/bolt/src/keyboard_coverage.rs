//! The keyboard-coverage rule as a reusable registry.
//!
//! The rule: every element that runs an action on click must have a keyboard
//! path, and a test fails when one does not. A product fills the registry —
//! its surfaces, the interactive element ids each draws, and the commands
//! each binds — and the checks run over that data, in both directions: an
//! element with no registry row fails, and a registered command with no key
//! (and no palette entry) fails too.
//!
//! Bolt cannot know a product's surfaces, command names or ids, so the
//! registry is generic over the product's [`CoverageVocabulary`]: the command
//! and context types its keymap dispatches, the stable id strings that name
//! them in failure messages, and the live lookup that answers whether a
//! command is bound. A product declares one vocabulary type and its registries
//! as plain consts; type aliases keep the declarations terse.
//!
//! Every element that runs an action on click shows up in the accessibility
//! tree of a rendered frame as a node supporting `AccessibleAction::Click`
//! (GPUI gives that action to every element with an `on_click` handler). A
//! surface test renders the surface, captures a frame with
//! [`FrameCapture`](crate::accessibility::FrameCapture) and checks every such
//! node with [`Coverage`] against the surface's [`SurfaceRegistry`]: each
//! element id must name the keyboard path that runs the same action.
//!
//! - [`KeyboardPath::Command`]: a command bound in one of the surface's key
//!   contexts (or a context they inherit from), or listed in the command
//!   palette when the test supplies it.
//! - [`KeyboardPath::TabStop`]: a control of a dialog, which keeps Tab inside
//!   it; the element must take focus.
//! - [`KeyboardPath::Menu`]: an entry of a keyboard-openable menu. The test
//!   builds the menu and passes its entry ids, so a removed entry fails the
//!   check.
//! - [`KeyboardPath::MouseOnly`]: an element with no keyboard path, with the
//!   reason. Reasons that start with `gap:` mark a known missing path.
//!
//! An element id the registry does not list fails the test, which is how a
//! new mouse-only action is stopped. A frame that draws another surface (a
//! document inside a shell) hands that subtree to the other surface's test
//! with [`Coverage::delegate`]. Handlers that react to the mouse press
//! instead of the click never reach the accessibility tree; a static lint
//! over the press handlers covers those.

use gpui::AccessibilityFrame;
use std::collections::BTreeSet;

/// A product's keyboard vocabulary for the coverage registry.
///
/// The registry speaks of commands and key contexts without knowing their
/// types; the product supplies them here, together with the stable id
/// strings failure messages name and the live lookup that answers whether a
/// command is bound. All methods are associated functions: the vocabulary is
/// a type-level fact, and [`Coverage::new`] needs no instance of it.
pub trait CoverageVocabulary: Copy + Clone + PartialEq + Eq + std::fmt::Debug + 'static {
    /// The command type the product's keymap dispatches.
    type Command: Copy + Eq + std::fmt::Debug + 'static;
    /// The key-context type the product's keymap resolves.
    type Context: Copy + Eq + std::fmt::Debug + 'static;

    /// The stable id string that names `command` in a failure message.
    fn command_id(command: Self::Command) -> &'static str;

    /// The stable id string that names `context` in a failure message.
    fn context_id(context: Self::Context) -> &'static str;

    /// Whether the live keymap — the product's bindings with the user's
    /// overrides applied — binds `command` in `context`, or in a context
    /// `context` inherits from.
    fn is_bound(context: Self::Context, command: Self::Command) -> bool;
}

/// How the action of a clickable element is reached without the pointer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyboardPath<V: CoverageVocabulary> {
    /// The command bound in the surface's key contexts that runs the action.
    Command(V::Command),
    /// A control of a dialog, reached with Tab (a dialog keeps Tab inside
    /// it) and pressed with Space or Enter. The element must take focus.
    TabStop,
    /// The id of the menu entry that runs the action. A trailing `*` accepts
    /// any entry id with that prefix, for rows whose entries are generated
    /// (one per toast action, one per task).
    Menu(&'static str),
    /// No keyboard path, with the reason (`gap: ...` for a known gap).
    MouseOnly(&'static str),
}

impl<V: CoverageVocabulary> KeyboardPath<V> {
    /// Whether this is a mouse-only entry recorded as a known gap.
    pub fn is_gap(&self) -> bool {
        matches!(self, KeyboardPath::MouseOnly(reason) if reason.starts_with("gap:"))
    }
}

/// One registry row: an element id pattern and its keyboard path.
///
/// A `*` in a pattern stands for any run of characters (`pane-action-*`,
/// `qb-*-rm*`); a pattern without one matches the id exactly. A pattern
/// with a `.` matches the end of the element path instead, for an element
/// whose own id is shared (`sql-auto-refresh.*` names the trigger of the
/// `sql-auto-refresh` dropdown).
pub type CoverageEntry<V> = (&'static str, KeyboardPath<V>);

/// The number of registry rows of each kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CoverageCounts {
    pub command: usize,
    pub tab_stop: usize,
    pub menu: usize,
    pub mouse_only: usize,
    pub gap: usize,
}

/// The coverage registry of one surface: its element id patterns, and the
/// key contexts whose bindings (or their parents') its commands must have.
#[derive(Clone, Copy, Debug)]
pub struct SurfaceRegistry<V: CoverageVocabulary> {
    pub name: &'static str,
    pub contexts: &'static [V::Context],
    pub entries: &'static [CoverageEntry<V>],
}

impl<V: CoverageVocabulary> SurfaceRegistry<V> {
    /// The number of registry rows of each kind.
    pub fn counts(&self) -> CoverageCounts {
        let mut counts = CoverageCounts::default();

        for (_, path) in self.entries {
            match path {
                KeyboardPath::Command(_) => counts.command += 1,
                KeyboardPath::TabStop => counts.tab_stop += 1,
                KeyboardPath::Menu(_) => counts.menu += 1,
                KeyboardPath::MouseOnly(_) if path.is_gap() => counts.gap += 1,
                KeyboardPath::MouseOnly(_) => counts.mouse_only += 1,
            }
        }

        counts
    }

    fn context_names(&self) -> String {
        self.contexts
            .iter()
            .map(|context| V::context_id(*context))
            .collect::<Vec<_>>()
            .join(" / ")
    }
}

/// The checks a rendered frame is run against, with the menus the test
/// built and the subtrees other surface tests own.
pub struct Coverage<V: CoverageVocabulary> {
    surfaces: Vec<SurfaceRegistry<V>>,
    delegated: Vec<(&'static str, &'static str)>,
    menu_entries: BTreeSet<String>,
    palette: Vec<V::Command>,
}

impl<V: CoverageVocabulary> Coverage<V> {
    /// Checks frames against `surface`.
    pub fn new(surface: SurfaceRegistry<V>) -> Self {
        Self {
            surfaces: vec![surface],
            delegated: Vec::new(),
            menu_entries: BTreeSet::new(),
            palette: Vec::new(),
        }
    }

    /// Also checks against `surface`, for a frame that draws several
    /// surfaces. An element takes the first registry that lists it.
    pub fn with_surface(mut self, surface: SurfaceRegistry<V>) -> Self {
        self.surfaces.push(surface);
        self
    }

    /// Skips every element inside an element whose id matches `pattern`:
    /// the test of `owner` covers that subtree.
    pub fn delegate(mut self, pattern: &'static str, owner: &'static str) -> Self {
        self.delegated.push((pattern, owner));
        self
    }

    /// Adds the entry ids of a menu the test built, for the registries'
    /// [`KeyboardPath::Menu`] rows.
    pub fn with_menu_entries<I, S>(mut self, entries: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.menu_entries
            .extend(entries.into_iter().map(Into::into));
        self
    }

    /// Adds the commands the command palette lists: a registry command that
    /// has no key in the surface's contexts still counts when listed here.
    pub fn with_palette(mut self, commands: impl IntoIterator<Item = V::Command>) -> Self {
        self.palette.extend(commands);
        self
    }

    /// Checks every clickable element of `frame`, returning one line per
    /// problem found.
    pub fn problems(&self, frame: &AccessibilityFrame) -> Vec<String> {
        let mut problems = Vec::new();

        for element in clickable_elements(frame) {
            if self.is_delegated(&element) {
                continue;
            }

            let found = self.surfaces.iter().find_map(|surface| {
                surface
                    .entries
                    .iter()
                    .find(|(pattern, _)| matches_element(pattern, &element))
                    .map(|(pattern, path)| (surface, *pattern, *path))
            });

            let Some((surface, pattern, path)) = found else {
                problems.push(format!(
                    "clickable element `{}` (path `{}`) has no keyboard path: add a Command \
                     bound in the surface's key context or an entry in its menu that runs the \
                     same action, then register the id in the coverage registry of the surface \
                     that draws it ({}); list it as MouseOnly(\"reason\") only for window \
                     chrome or pointer gestures",
                    element.id,
                    element.path,
                    self.surface_names(),
                ));
                continue;
            };

            match path {
                KeyboardPath::Command(command) => {
                    let bound = surface
                        .contexts
                        .iter()
                        .any(|context| V::is_bound(*context, command));

                    if !bound && !self.palette.contains(&command) {
                        problems.push(format!(
                            "`{}` (pattern `{pattern}` of {}, path `{}`) names command `{}`, \
                             which has no key in {} and is not in the command palette",
                            element.id,
                            surface.name,
                            element.path,
                            V::command_id(command),
                            surface.context_names(),
                        ));
                    }
                }
                KeyboardPath::TabStop => {
                    if !element.focusable {
                        problems.push(format!(
                            "`{}` (pattern `{pattern}` of {}, path `{}`) is a TabStop but \
                             takes no focus, so Tab never reaches it",
                            element.id, surface.name, element.path,
                        ));
                    }
                }
                KeyboardPath::Menu(entry) => {
                    let listed = self
                        .menu_entries
                        .iter()
                        .any(|built| matches_pattern(entry, built));

                    if !listed {
                        problems.push(format!(
                            "`{}` (pattern `{pattern}` of {}, path `{}`) names menu entry \
                             `{entry}`, which the menus built by the test do not contain",
                            element.id, surface.name, element.path,
                        ));
                    }
                }
                KeyboardPath::MouseOnly(reason) => {
                    if reason.trim().is_empty() {
                        problems.push(format!(
                            "`{}` (pattern `{pattern}` of {}) is MouseOnly without a reason",
                            element.id, surface.name,
                        ));
                    }
                }
            }
        }

        problems
    }

    /// Panics listing every clickable element of `frame` that has no
    /// keyboard path in the registries. Returns the ids of the elements it
    /// checked, so a test can also prove the surface it meant to check was
    /// drawn.
    #[track_caller]
    #[expect(
        clippy::panic,
        reason = "assert_covered is a coverage-assertion helper: failing the check is its contract, and the panic carries the full problem list"
    )]
    pub fn assert_covered(&self, frame: &AccessibilityFrame) -> Vec<String> {
        let problems = self.problems(frame);

        if problems.is_empty() {
            return clickable_elements(frame)
                .into_iter()
                .filter(|element| !self.is_delegated(element))
                .map(|element| element.id)
                .collect();
        }

        let mut message = format!(
            "keyboard coverage of {} failed ({} problem(s)):",
            self.surface_names(),
            problems.len()
        );
        for problem in problems {
            message.push_str("\n  - ");
            message.push_str(&problem);
        }

        panic!("{message}");
    }

    fn is_delegated(&self, element: &ClickableElement) -> bool {
        let Some(ancestors) = element.path.strip_suffix(element.id.as_str()) else {
            return false;
        };

        ancestors.split('.').any(|segment| {
            self.delegated
                .iter()
                .any(|(pattern, _)| matches_pattern(pattern, segment))
        })
    }

    fn surface_names(&self) -> String {
        self.surfaces
            .iter()
            .map(|surface| surface.name)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// A rendered element that runs an action on click.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClickableElement {
    pub id: String,
    pub path: String,
    /// Whether the element takes keyboard focus.
    pub focusable: bool,
}

/// Every element of `frame` whose accessibility node supports a click, in
/// element path order.
pub fn clickable_elements(frame: &AccessibilityFrame) -> Vec<ClickableElement> {
    use gpui::AccessibleAction;

    let mut elements: Vec<ClickableElement> = frame
        .nodes()
        .filter_map(|(_, node)| {
            let accessible = frame.accessibility_node(node)?;

            accessible
                .supports_action(AccessibleAction::Click)
                .then(|| ClickableElement {
                    id: node.id().to_string(),
                    path: node.path().to_string(),
                    focusable: accessible.supports_action(AccessibleAction::Focus),
                })
        })
        .collect();

    elements.sort();
    elements
}

/// Whether the registry `pattern` names `element`. A pattern with a `.` names
/// an element by the end of its element path (`sql-auto-refresh.*` for the
/// trigger inside the `sql-auto-refresh` dropdown), whatever its frame
/// identity is; any other pattern is matched with [`matches_pattern`].
fn matches_element(pattern: &str, element: &ClickableElement) -> bool {
    if !pattern.contains('.') {
        return matches_pattern(pattern, &element.id);
    }

    glob_matches(pattern, &element.path)
        || element
            .path
            .match_indices('.')
            .any(|(index, _)| glob_matches(pattern, &element.path[index + 1..]))
}

/// Whether `pattern` matches `id`, or the trailing run of `id` after one of
/// its `.` separators (a repeated element id is named by its path suffix).
fn matches_pattern(pattern: &str, id: &str) -> bool {
    glob_matches(pattern, id)
        || id
            .match_indices('.')
            .any(|(index, _)| glob_matches(pattern, &id[index + 1..]))
}

/// Whether `candidate` matches `pattern`, where each `*` stands for any run
/// of characters, possibly empty.
fn glob_matches(pattern: &str, candidate: &str) -> bool {
    let mut parts = pattern.split('*');
    let first = parts.next().unwrap_or_default();

    let Some(mut rest) = candidate.strip_prefix(first) else {
        return false;
    };

    let parts: Vec<&str> = parts.collect();
    let Some((last, middle)) = parts.split_last() else {
        return rest.is_empty();
    };

    for part in middle {
        match rest.find(part) {
            Some(index) => rest = &rest[index + part.len()..],
            None => return false,
        }
    }

    rest.ends_with(last)
}

/// The rendered-frame observation the surface tests capture with. The
/// identical helper lives in [`crate::accessibility`]; re-exported here so a
/// product's keyboard-coverage module has one import source.
#[cfg(any(test, feature = "test-support"))]
pub use crate::accessibility::FrameCapture;

#[cfg(test)]
mod tests {
    use super::{
        ClickableElement, Coverage, CoverageVocabulary, FrameCapture, KeyboardPath,
        SurfaceRegistry, matches_element, matches_pattern,
    };
    use gpui::prelude::FluentBuilder as _;
    use gpui::{
        Context, InteractiveElement as _, IntoElement, ParentElement as _, Render,
        StatefulInteractiveElement as _, Styled as _, TestAppContext, Window, div,
    };
    use std::cell::RefCell;
    use std::collections::HashSet;

    // The test vocabulary: two commands, two contexts, and the bound set a
    // test fills to answer `is_bound`. Empty by default, so a test that
    // needs a command unbound clears the set first.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct TestKeymap;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    enum TestCommand {
        Copy,
        Select,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    enum TestContext {
        Panel,
        Tree,
    }

    thread_local! {
        static BOUND: RefCell<HashSet<(TestContext, TestCommand)>> =
            RefCell::new(HashSet::new());
    }

    fn bind(context: TestContext, command: TestCommand) {
        BOUND.with(|bound| bound.borrow_mut().insert((context, command)));
    }

    fn clear_bindings() {
        BOUND.with(|bound| bound.borrow_mut().clear());
    }

    impl CoverageVocabulary for TestKeymap {
        type Command = TestCommand;
        type Context = TestContext;

        fn command_id(command: Self::Command) -> &'static str {
            match command {
                TestCommand::Copy => "copy",
                TestCommand::Select => "select",
            }
        }

        fn context_id(context: Self::Context) -> &'static str {
            match context {
                TestContext::Panel => "panel",
                TestContext::Tree => "tree",
            }
        }

        fn is_bound(context: Self::Context, command: Self::Command) -> bool {
            BOUND.with(|bound| bound.borrow().contains(&(context, command)))
        }
    }

    struct Surface {
        extra: bool,
    }

    impl Render for Surface {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .size_full()
                .child(div().id("run").on_click(|_, _, _| {}).child("Run"))
                .child(div().id("row-1").on_click(|_, _, _| {}).child("Row"))
                .child(div().id("grip").on_click(|_, _, _| {}).child("Grip"))
                .child(div().id("label").child("No click"))
                .child(
                    div()
                        .id("embedded")
                        .child(div().id("embedded-button").on_click(|_, _, _| {})),
                )
                .when(self.extra, |this| {
                    this.child(div().id("new-button").on_click(|_, _, _| {}).child("New"))
                })
        }
    }

    const SURFACE: SurfaceRegistry<TestKeymap> = SurfaceRegistry {
        name: "test surface",
        contexts: &[TestContext::Panel],
        entries: &[
            ("run", KeyboardPath::Command(TestCommand::Copy)),
            ("row-*", KeyboardPath::Menu("open-row")),
            ("grip", KeyboardPath::MouseOnly("resize grip")),
        ],
    };

    const SIDEBAR_SURFACE: SurfaceRegistry<TestKeymap> = SurfaceRegistry {
        name: "sidebar surface",
        contexts: &[TestContext::Tree],
        entries: SURFACE.entries,
    };

    fn render(cx: &mut TestAppContext, extra: bool) -> gpui::AccessibilityFrame {
        let (_, window) = cx.add_window_view(|_, _| Surface { extra });
        let capture = FrameCapture::observe(window);
        capture.frame(window)
    }

    fn coverage() -> Coverage<TestKeymap> {
        Coverage::new(SURFACE)
            .delegate("embedded", "the embedded surface")
            .with_menu_entries(["open-row"])
    }

    #[test]
    fn patterns_match_exact_ids_prefixes_and_path_suffixes() {
        assert!(matches_pattern("run", "run"));
        assert!(!matches_pattern("run", "running"));
        assert!(matches_pattern("row-*", "row-12"));
        assert!(matches_pattern("close", "tab-3.close"));
        assert!(!matches_pattern("close", "tab-3.closed"));
        assert!(matches_pattern("qb-*-rm*", "qb-pred-rm-0"));
        assert!(matches_pattern("qb-*-rm*", "qb-agg-rm"));
        assert!(!matches_pattern("qb-*-rm*", "qb-rm-sort"));
        assert!(matches_pattern("*-close", "tab-close"));
        assert!(!matches_pattern("a*b*c", "a-c"));

        let trigger = ClickableElement {
            id: "dropdown-trigger".to_string(),
            path: "root.panel.sql-auto-refresh.dropdown-trigger".to_string(),
            focusable: false,
        };
        assert!(matches_element("sql-auto-refresh.*", &trigger));
        assert!(matches_element("dropdown-trigger", &trigger));
        assert!(!matches_element("panel.dropdown-trigger", &trigger));
    }

    #[gpui::test]
    fn a_registered_surface_passes(cx: &mut TestAppContext) {
        clear_bindings();
        bind(TestContext::Panel, TestCommand::Copy);
        let frame = render(cx, false);

        assert_eq!(coverage().problems(&frame), Vec::<String>::new());
        assert_eq!(coverage().assert_covered(&frame), ["grip", "row-1", "run"]);
    }

    #[gpui::test]
    fn an_unregistered_clickable_element_fails_with_the_fix(cx: &mut TestAppContext) {
        clear_bindings();
        bind(TestContext::Panel, TestCommand::Copy);
        let frame = render(cx, true);

        let problems = coverage().problems(&frame);

        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].starts_with("clickable element `new-button`"),
            "{problems:?}"
        );
        assert!(
            problems[0].contains("register the id in the coverage registry of the surface"),
            "{problems:?}"
        );
    }

    #[gpui::test]
    fn a_delegated_subtree_is_left_to_its_owner(cx: &mut TestAppContext) {
        clear_bindings();
        bind(TestContext::Panel, TestCommand::Copy);
        let frame = render(cx, false);

        let problems = Coverage::new(SURFACE)
            .with_menu_entries(["open-row"])
            .problems(&frame);

        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].starts_with("clickable element `embedded-button`"),
            "{problems:?}"
        );
    }

    #[gpui::test]
    fn a_missing_menu_entry_or_unbound_command_fails(cx: &mut TestAppContext) {
        clear_bindings();
        bind(TestContext::Panel, TestCommand::Copy);
        let frame = render(cx, false);

        let problems = Coverage::new(SIDEBAR_SURFACE)
            .delegate("embedded", "the embedded surface")
            .problems(&frame);

        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("command `copy`")),
            "{problems:?}"
        );
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("menu entry `open-row`")),
            "{problems:?}"
        );

        let with_palette = Coverage::new(SIDEBAR_SURFACE)
            .delegate("embedded", "the embedded surface")
            .with_menu_entries(["open-row"])
            .with_palette([TestCommand::Copy]);
        assert_eq!(with_palette.problems(&frame), Vec::<String>::new());
    }

    #[gpui::test]
    fn an_element_takes_the_first_registry_that_lists_it(cx: &mut TestAppContext) {
        clear_bindings();
        bind(TestContext::Panel, TestCommand::Copy);
        let frame = render(cx, false);

        let problems = Coverage::new(SIDEBAR_SURFACE)
            .with_surface(SURFACE)
            .delegate("embedded", "the embedded surface")
            .with_menu_entries(["open-row"])
            .problems(&frame);

        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("of sidebar surface"), "{problems:?}");
    }

    #[gpui::test]
    fn a_menu_pattern_accepts_a_generated_entry(cx: &mut TestAppContext) {
        const GENERATED: SurfaceRegistry<TestKeymap> = SurfaceRegistry {
            name: "generated menu",
            contexts: &[TestContext::Panel],
            entries: &[
                ("run", KeyboardPath::Command(TestCommand::Copy)),
                ("row-*", KeyboardPath::Menu("open-row-*")),
                ("grip", KeyboardPath::MouseOnly("resize grip")),
            ],
        };
        clear_bindings();
        bind(TestContext::Panel, TestCommand::Copy);
        let frame = render(cx, false);

        let problems = Coverage::new(GENERATED)
            .delegate("embedded", "the embedded surface")
            .with_menu_entries(["open-row-7"])
            .problems(&frame);

        assert_eq!(problems, Vec::<String>::new());
    }

    #[gpui::test]
    fn a_tab_stop_must_take_focus(cx: &mut TestAppContext) {
        const TAB_STOPS: SurfaceRegistry<TestKeymap> = SurfaceRegistry {
            name: "dialog",
            contexts: &[TestContext::Tree],
            entries: &[
                ("run", KeyboardPath::TabStop),
                ("row-*", KeyboardPath::TabStop),
                ("grip", KeyboardPath::MouseOnly("resize grip")),
            ],
        };
        let frame = render(cx, false);

        let problems = Coverage::new(TAB_STOPS)
            .delegate("embedded", "the embedded surface")
            .problems(&frame);

        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(
            problems
                .iter()
                .all(|problem| problem.contains("takes no focus")),
            "{problems:?}"
        );
    }

    #[gpui::test]
    fn a_focusable_tab_stop_passes(cx: &mut TestAppContext) {
        struct Dialog {
            focus: gpui::FocusHandle,
        }

        impl Render for Dialog {
            fn render(
                &mut self,
                _window: &mut Window,
                _cx: &mut Context<Self>,
            ) -> impl IntoElement {
                div().size_full().child(
                    div()
                        .id("apply")
                        .track_focus(&self.focus)
                        .on_click(|_, _, _| {})
                        .child("Apply"),
                )
            }
        }

        const DIALOG: SurfaceRegistry<TestKeymap> = SurfaceRegistry {
            name: "dialog",
            contexts: &[TestContext::Tree],
            entries: &[("apply", KeyboardPath::TabStop)],
        };

        let (_, window) = cx.add_window_view(|_, cx| Dialog {
            focus: cx.focus_handle(),
        });
        let capture = FrameCapture::observe(window);

        assert_eq!(
            Coverage::new(DIALOG).assert_covered(&capture.frame(window)),
            ["apply"]
        );
    }

    #[test]
    fn counts_split_gaps_from_other_mouse_only_rows() {
        const ROWS: SurfaceRegistry<TestKeymap> = SurfaceRegistry {
            name: "rows",
            contexts: &[],
            entries: &[
                ("a", KeyboardPath::Command(TestCommand::Select)),
                ("b", KeyboardPath::Menu("b")),
                ("c", KeyboardPath::MouseOnly("window chrome")),
                ("d", KeyboardPath::MouseOnly("gap: no key yet")),
                ("e", KeyboardPath::TabStop),
            ],
        };

        let counts = ROWS.counts();

        assert_eq!(
            (
                counts.command,
                counts.tab_stop,
                counts.menu,
                counts.mouse_only,
                counts.gap
            ),
            (1, 1, 1, 1, 1)
        );
    }
}
