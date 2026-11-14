//! Keyboard coverage guard for rendered surfaces.
//!
//! Every element that runs an action on click shows up in the accessibility
//! tree of a rendered frame as a node supporting `AccessibleAction::Click`
//! (GPUI gives that action to every element with an `on_click` handler). A
//! surface test renders the surface, captures a frame with
//! [`FrameCapture`] and checks every such node with [`Coverage`] against the
//! surface's [`SurfaceRegistry`]: each element id must name the keyboard
//! path that runs the same action.
//!
//! The registry machinery — the pattern matching, the checks in both
//! directions, the frame capture — is product-neutral and lives in
//! `bolt::keyboard_coverage`, generic over a product's keyboard vocabulary.
//! This module is DBFlux's registration on it: the [`DbFluxKeymap`]
//! vocabulary (the `Command`/`ContextId` types the keymap dispatches, their
//! id strings, and the live-keymap lookup), the aliases every surface's
//! registry is declared with, and the dialog chrome the `Modal` primitive
//! draws.
//!
//! - [`KeyboardPath::Command`]: a command bound in one of the surface's key
//!   contexts (or a context they inherit from), or listed in the command
//!   palette when the test supplies it.
//! - [`KeyboardPath::TabStop`]: a control of a dialog, which keeps Tab inside
//!   it; the element must take focus.
//! - [`KeyboardPath::Menu`]: an entry of a keyboard-openable menu (the pane
//!   actions, a table or rail menu). The test builds the menu and passes its
//!   entry ids, so a removed entry fails the check.
//! - [`KeyboardPath::MouseOnly`]: an element with no keyboard path, with the
//!   reason. Reasons that start with `gap:` mark a known missing path.
//!
//! An element id the registry does not list fails the test, which is how a
//! new mouse-only action is stopped. A frame that draws another surface (a
//! document inside the workspace) hands that subtree to the other surface's
//! test with [`Coverage::delegate`]. Handlers that react to the mouse press
//! instead of the click never reach the accessibility tree; the static lint
//! (`python3 scripts/lint.py mouse-down`) covers those.

use bolt::keyboard_coverage::CoverageVocabulary;
use dbflux_app::keymap::{Command, ContextId};

/// DBFlux's keyboard vocabulary for the coverage registry.
///
/// The command and context types are the keymap's persisted vocabulary; the
/// bound check reads the effective keymap — the default bindings with the
/// user's overrides applied — the same way key dispatch does, including the
/// fallback to parent contexts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DbFluxKeymap;

impl CoverageVocabulary for DbFluxKeymap {
    type Command = Command;
    type Context = ContextId;

    fn command_id(command: Command) -> &'static str {
        command.id()
    }

    fn context_id(context: ContextId) -> &'static str {
        context.id()
    }

    fn is_bound(context: ContextId, command: Command) -> bool {
        crate::keymap::effective_keymap()
            .keys_for_command(context, command)
            .is_some()
    }
}

/// How the action of a clickable element is reached without the pointer.
pub type KeyboardPath = bolt::keyboard_coverage::KeyboardPath<DbFluxKeymap>;

/// One registry row: an element id pattern and its keyboard path.
pub type CoverageEntry = bolt::keyboard_coverage::CoverageEntry<DbFluxKeymap>;

/// The number of registry rows of each kind.
pub use bolt::keyboard_coverage::CoverageCounts;

/// The coverage registry of one surface: its element id patterns, and the
/// key contexts whose bindings (or their parents') its commands must have.
pub type SurfaceRegistry = bolt::keyboard_coverage::SurfaceRegistry<DbFluxKeymap>;

/// The checks a rendered frame is run against.
pub type Coverage = bolt::keyboard_coverage::Coverage<DbFluxKeymap>;

/// A rendered element that runs an action on click.
pub use bolt::keyboard_coverage::ClickableElement;

/// Every element of `frame` whose accessibility node supports a click, in
/// element path order.
pub use bolt::keyboard_coverage::clickable_elements;

/// Keeps the latest accessibility frame of the window it observes.
#[cfg(any(test, feature = "test-support"))]
pub use bolt::keyboard_coverage::FrameCapture;

/// The chrome of the `Modal` primitive every dialog is drawn in: its close
/// button answers Escape, which the Modal key context binds to Cancel.
pub const MODAL_CHROME: SurfaceRegistry = SurfaceRegistry {
    name: "dialog chrome",
    contexts: &[ContextId::Modal],
    entries: &[("modal-close", KeyboardPath::Command(Command::Cancel))],
};

#[cfg(test)]
mod tests {
    use super::{Coverage, FrameCapture, KeyboardPath, MODAL_CHROME, SurfaceRegistry};
    use crate::keymap::init_keymap;
    use dbflux_app::keymap::{Command, ContextId};
    use gpui::prelude::FluentBuilder as _;
    use gpui::{
        Context, InteractiveElement as _, IntoElement, ParentElement as _, Render,
        StatefulInteractiveElement as _, Styled as _, TestAppContext, Window, div,
    };

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

    const SURFACE: SurfaceRegistry = SurfaceRegistry {
        name: "test surface",
        contexts: &[ContextId::SqlPreviewModal],
        entries: &[
            ("run", KeyboardPath::Command(Command::CopyPreview)),
            ("row-*", KeyboardPath::Menu("open-row")),
            ("grip", KeyboardPath::MouseOnly("resize grip")),
        ],
    };

    const SIDEBAR_SURFACE: SurfaceRegistry = SurfaceRegistry {
        name: "sidebar surface",
        contexts: &[ContextId::Sidebar],
        entries: SURFACE.entries,
    };

    fn render(cx: &mut TestAppContext, extra: bool) -> gpui::AccessibilityFrame {
        cx.update(init_keymap);
        let (_, window) = cx.add_window_view(|_, _| Surface { extra });
        let capture = FrameCapture::observe(window);
        capture.frame(window)
    }

    fn coverage() -> Coverage {
        Coverage::new(SURFACE)
            .delegate("embedded", "the embedded surface")
            .with_menu_entries(["open-row"])
    }

    #[gpui::test]
    fn a_registered_surface_passes(cx: &mut TestAppContext) {
        let frame = render(cx, false);

        assert_eq!(coverage().problems(&frame), Vec::<String>::new());
        assert_eq!(coverage().assert_covered(&frame), ["grip", "row-1", "run"]);
    }

    #[gpui::test]
    fn an_unregistered_clickable_element_fails_with_the_fix(cx: &mut TestAppContext) {
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
        let frame = render(cx, false);

        let problems = Coverage::new(SIDEBAR_SURFACE)
            .delegate("embedded", "the embedded surface")
            .problems(&frame);

        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("command `copy_preview`")),
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
            .with_palette([Command::CopyPreview]);
        assert_eq!(with_palette.problems(&frame), Vec::<String>::new());
    }

    #[gpui::test]
    fn an_element_takes_the_first_registry_that_lists_it(cx: &mut TestAppContext) {
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
        const GENERATED: SurfaceRegistry = SurfaceRegistry {
            name: "generated menu",
            contexts: &[ContextId::SqlPreviewModal],
            entries: &[
                ("run", KeyboardPath::Command(Command::CopyPreview)),
                ("row-*", KeyboardPath::Menu("open-row-*")),
                ("grip", KeyboardPath::MouseOnly("resize grip")),
            ],
        };
        let frame = render(cx, false);

        let problems = Coverage::new(GENERATED)
            .delegate("embedded", "the embedded surface")
            .with_menu_entries(["open-row-7"])
            .problems(&frame);

        assert_eq!(problems, Vec::<String>::new());
    }

    #[gpui::test]
    fn a_tab_stop_must_take_focus(cx: &mut TestAppContext) {
        const TAB_STOPS: SurfaceRegistry = SurfaceRegistry {
            name: "dialog",
            contexts: &[ContextId::Modal],
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

        const DIALOG: SurfaceRegistry = SurfaceRegistry {
            name: "dialog",
            contexts: &[ContextId::Modal],
            entries: &[("apply", KeyboardPath::TabStop)],
        };

        cx.update(init_keymap);
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
        const ROWS: SurfaceRegistry = SurfaceRegistry {
            name: "rows",
            contexts: &[],
            entries: &[
                ("a", KeyboardPath::Command(Command::RunQuery)),
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

    #[test]
    fn the_dialog_chrome_registry_names_the_modal_cancel_command() {
        assert_eq!(MODAL_CHROME.name, "dialog chrome");
        assert_eq!(MODAL_CHROME.contexts, &[ContextId::Modal]);
        assert_eq!(
            MODAL_CHROME.entries,
            &[("modal-close", KeyboardPath::Command(Command::Cancel))]
        );
    }
}
