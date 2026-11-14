//! Connection-environment, keymap and copy extensions over bolt's generic
//! [`Dropdown`].
//!
//! The generic dropdown — its state machine, menu, triggers and the
//! [`bolt::keymap::CommandSet`] key seam — lives in `bolt` and is
//! re-exported at this crate's `controls` path so call sites keep compiling.
//! What stays here is the product half: the `ConnectionEnvironment` badge
//! and the keymap action the dropdowns answer. The translated placeholder
//! is the text seam's job: `theme::init` installs the product's translator
//! and the dropdown reads `bolt::strings`.

use bolt::controls::{BadgeLabel, Dropdown};
use bolt::keymap::{Command as BoltCommand, CommandSet, KeyCommand, attach_key_commands};
use dbflux_core::ConnectionEnvironment;
use dbflux_core::keymap_types::{Command, ContextId};
use gpui::{App, Context};

use crate::actions::RunCommand;
use crate::primitives::{environment_badge_tone, environment_short_label};

/// Connection-environment methods over the generic dropdown, so call sites
/// keep naming the environment instead of badge data.
pub trait DropdownEnvironment {
    /// Draws the environment tag between the toolbar trigger's label and its
    /// chevron (the connection selector), or removes it with `None`.
    fn set_label_environment(
        &mut self,
        environment: Option<ConnectionEnvironment>,
        cx: &mut Context<Self>,
    ) where
        Self: Sized;
}

impl DropdownEnvironment for Dropdown {
    fn set_label_environment(
        &mut self,
        environment: Option<ConnectionEnvironment>,
        cx: &mut Context<Self>,
    ) {
        let badge = environment.map(environment_badge);
        self.set_label_badge(badge, cx);
    }
}

/// The dropdown tag of a connection environment: its short label (PROD,
/// STAGING, DEV) in the environment's tone.
fn environment_badge(environment: ConnectionEnvironment) -> BadgeLabel {
    BadgeLabel {
        label: environment_short_label(environment).into(),
        tone: environment_badge_tone(environment),
    }
}

/// The product's keymap action, for the bolt keymap seam: the command id it
/// carries names the command a key binding runs.
impl KeyCommand for RunCommand {
    fn key_command_id(&self) -> &str {
        &self.command
    }
}

/// Installs the seams the product's components read: the keymap's
/// [`CommandSet`] (the `RunCommand` action and the semantic commands mapped
/// onto the product's `Command` ids).
///
/// The set carries the dropdown's five menu keys, the vim leader
/// commands the editors' hosts answer (`FocusSearch` in `vim/binding.rs`,
/// the dialog save in the `VimHost` impls) and the palette's new-tab
/// confirm, so one table serves every component and a running app cannot
/// have a dropdown with one table and an editor with another.
///
/// Call it once at startup, before the first window; `theme::init` does. A
/// component rendered before it runs answers no commands.
pub fn install_keymap_seams(cx: &mut App) {
    cx.set_global(
        CommandSet::new(
            ContextId::Dropdown.as_gpui_context(),
            attach_key_commands::<RunCommand>,
        )
        // The dropdown's menu keys (the keymap layer in `dbflux_ui_base`).
        .map(BoltCommand::SelectNext, Command::SelectNext.id())
        .map(BoltCommand::SelectPrev, Command::SelectPrev.id())
        .map(BoltCommand::Execute, Command::Execute.id())
        .map(BoltCommand::ExpandCollapse, Command::ExpandCollapse.id())
        .map(BoltCommand::Cancel, Command::Cancel.id())
        // The vim leader commands (`vim/binding.rs`).
        .map(BoltCommand::FocusSearch, Command::FocusSearch.id())
        .map(BoltCommand::Save, Command::SaveQuery.id())
        // The command palette's new-tab confirm (`command_palette.rs`).
        .map(BoltCommand::OpenInNewTab, Command::RunQueryInNewTab.id()),
    );
}

/// The keys the app keymap binds in the `Dropdown` context (see the dropdown
/// layer in `dbflux_ui_base::keymap`), for component tests that run without
/// the app keymap. Also installs [`install_keymap_seams`], which the app
/// does at startup.
#[cfg(test)]
pub(crate) fn bind_dropdown_keys_for_tests(cx: &mut gpui::TestAppContext) {
    use gpui::KeyBinding;

    let context = Some(ContextId::Dropdown.as_gpui_context());
    let run = |command: Command| RunCommand::new(command.id());

    cx.update(|cx| {
        install_keymap_seams(cx);
        cx.bind_keys([
            KeyBinding::new("j", run(Command::SelectNext), context),
            KeyBinding::new("down", run(Command::SelectNext), context),
            KeyBinding::new("k", run(Command::SelectPrev), context),
            KeyBinding::new("up", run(Command::SelectPrev), context),
            KeyBinding::new("enter", run(Command::Execute), context),
            KeyBinding::new("space", run(Command::ExpandCollapse), context),
            KeyBinding::new("escape", run(Command::Cancel), context),
        ]);
    });
}

#[cfg(test)]
mod tests {
    use dbflux_core::ConnectionEnvironment;
    use gpui::{Entity, SharedString, TestAppContext};

    /// A bare window body holding the dropdown under test.
    struct Bare {
        dropdown: Entity<Dropdown>,
    }

    impl gpui::Render for Bare {
        fn render(
            &mut self,
            _window: &mut gpui::Window,
            _cx: &mut gpui::Context<Self>,
        ) -> impl gpui::IntoElement {
            use gpui::{ParentElement as _, Styled as _};

            gpui::div().size_full().child(self.dropdown.clone())
        }
    }

    use super::{DropdownEnvironment, environment_badge, environment_short_label};
    use crate::controls::{Dropdown, bind_dropdown_keys_for_tests};
    use crate::primitives::BadgeTone;

    #[test]
    fn environments_map_to_their_badge_tones() {
        assert_eq!(
            environment_badge(ConnectionEnvironment::Production).tone,
            BadgeTone::Danger
        );
        assert_eq!(
            environment_badge(ConnectionEnvironment::Staging).tone,
            BadgeTone::Warning
        );
        assert_eq!(
            environment_badge(ConnectionEnvironment::Development).tone,
            BadgeTone::Info
        );
        // The label is the environment's translated short tag; assert it
        // against the same translation the trigger draws.
        assert_eq!(
            environment_badge(ConnectionEnvironment::Staging).label,
            SharedString::from(environment_short_label(ConnectionEnvironment::Staging))
        );
    }

    /// Keyboard tests against a real window: the keys the app keymap binds
    /// in the `Dropdown` context are bound here the same way (see
    /// `bind_dropdown_keys_for_tests`), and an owner view stands in for the
    /// pane hosting the dropdown and records every command that reaches it.
    mod keyboard {
        use super::bind_dropdown_keys_for_tests;
        use crate::actions::RunCommand;
        use crate::controls::{Dropdown, DropdownItem, DropdownSelectionChanged};
        use dbflux_core::keymap_types::Command;
        use gpui::{
            AppContext as _, Context, Entity, FocusHandle, InteractiveElement as _, IntoElement,
            ParentElement as _, Render, Styled as _, TestAppContext, VisualTestContext, Window,
            div,
        };

        struct Owner {
            focus: FocusHandle,
            dropdown: Entity<Dropdown>,
            commands: Vec<Command>,
            selections: Vec<usize>,
            _subscription: gpui::Subscription,
        }

        impl Render for Owner {
            fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
                div()
                    .size_full()
                    .key_context("Owner")
                    .track_focus(&self.focus)
                    .on_action(cx.listener(|this, action: &RunCommand, _window, _cx| {
                        if let Some(command) = Command::from_action_id(&action.command) {
                            this.commands.push(command);
                        }
                    }))
                    .child(self.dropdown.clone())
            }
        }

        struct Setup<'a> {
            owner: Entity<Owner>,
            window: &'a mut VisualTestContext,
        }

        fn setup(cx: &mut TestAppContext) -> Setup<'_> {
            cx.update(gpui_component::init);
            bind_dropdown_keys_for_tests(cx);

            // The owner's own bindings on the same keys, like a pane whose
            // key handling drives a dropdown it keeps unfocused.
            cx.update(|cx| {
                cx.bind_keys([
                    gpui::KeyBinding::new(
                        "j",
                        RunCommand::new(Command::SelectNext.id()),
                        Some("Owner"),
                    ),
                    gpui::KeyBinding::new(
                        "enter",
                        RunCommand::new(Command::Execute.id()),
                        Some("Owner"),
                    ),
                ]);
            });

            let (owner, window) = cx.add_window_view(|window, cx| {
                let dropdown = cx.new(|_cx| {
                    Dropdown::new("keyboard-dropdown").items(vec![
                        DropdownItem::new("One"),
                        DropdownItem::new("Two"),
                        DropdownItem::new("Three"),
                    ])
                });

                let subscription = cx.subscribe_in(
                    &dropdown,
                    window,
                    |this: &mut Owner, _, event: &DropdownSelectionChanged, _window, _cx| {
                        this.selections.push(event.index);
                    },
                );

                Owner {
                    focus: cx.focus_handle(),
                    dropdown,
                    commands: Vec::new(),
                    selections: Vec::new(),
                    _subscription: subscription,
                }
            });
            window.run_until_parked();

            let setup = Setup { owner, window };
            let owner = setup.owner.clone();
            setup.window.update(|window, cx| {
                let focus = owner.read(cx).focus.clone();
                focus.focus(window, cx);
            });
            setup.window.run_until_parked();

            setup
        }

        impl Setup<'_> {
            fn dropdown(&mut self) -> Entity<Dropdown> {
                let owner = self.owner.clone();
                self.window.update(|_, cx| owner.read(cx).dropdown.clone())
            }

            fn focus_dropdown(&mut self) {
                let dropdown = self.dropdown();
                self.window.update(|window, cx| {
                    dropdown.update(cx, |dropdown, cx| dropdown.focus(window, cx));
                });
                self.window.run_until_parked();
            }

            fn keys(&mut self, keystrokes: &str) {
                self.window.simulate_keystrokes(keystrokes);
                self.window.run_until_parked();
            }

            fn is_open(&mut self) -> bool {
                let dropdown = self.dropdown();
                self.window.update(|_, cx| dropdown.read(cx).is_open())
            }

            fn highlighted(&mut self) -> Option<usize> {
                let dropdown = self.dropdown();
                self.window
                    .update(|_, cx| dropdown.read(cx).highlighted_index())
            }

            fn selected(&mut self) -> Option<usize> {
                let dropdown = self.dropdown();
                self.window.update(|_, cx| dropdown.read(cx).selected())
            }

            fn dropdown_focused(&mut self) -> bool {
                let dropdown = self.dropdown();
                self.window
                    .update(|window, cx| dropdown.read(cx).is_focused(window))
            }

            fn owner_focused(&mut self) -> bool {
                let owner = self.owner.clone();
                self.window
                    .update(|window, cx| owner.read(cx).focus.is_focused(window))
            }

            fn commands(&mut self) -> Vec<Command> {
                let owner = self.owner.clone();
                self.window.update(|_, cx| owner.read(cx).commands.clone())
            }

            fn selections(&mut self) -> Vec<usize> {
                let owner = self.owner.clone();
                self.window
                    .update(|_, cx| owner.read(cx).selections.clone())
            }
        }

        #[gpui::test]
        fn a_focused_dropdown_opens_moves_and_selects_by_keys(cx: &mut TestAppContext) {
            let mut setup = setup(cx);
            setup.focus_dropdown();
            assert!(setup.dropdown_focused(), "the dropdown takes focus");

            setup.keys("enter");
            assert!(setup.is_open(), "Enter opens the menu");
            assert_eq!(setup.highlighted(), Some(0));

            setup.keys("j down");
            assert_eq!(setup.highlighted(), Some(2), "j and Down move down");

            setup.keys("k");
            assert_eq!(setup.highlighted(), Some(1), "k moves up");

            setup.keys("enter");
            assert!(!setup.is_open(), "Enter confirms and closes");
            assert_eq!(setup.selected(), Some(1));
            assert_eq!(setup.selections(), vec![1]);
            assert!(setup.owner_focused(), "focus returns to where it came from");
            assert!(setup.commands().is_empty(), "{:?}", setup.commands());
        }

        #[gpui::test]
        fn space_opens_and_escape_closes_without_selecting(cx: &mut TestAppContext) {
            let mut setup = setup(cx);
            setup.focus_dropdown();

            setup.keys("space");
            assert!(setup.is_open(), "Space opens the menu");

            setup.keys("down escape");
            assert!(!setup.is_open(), "Escape closes the menu");
            assert_eq!(setup.selected(), None, "Escape selects nothing");
            assert!(setup.selections().is_empty());
            assert!(setup.owner_focused(), "Escape hands focus back");
            assert!(setup.commands().is_empty(), "{:?}", setup.commands());
        }

        #[gpui::test]
        fn a_closed_focused_dropdown_lets_other_keys_through(cx: &mut TestAppContext) {
            let mut setup = setup(cx);
            setup.focus_dropdown();

            setup.keys("j");

            assert!(!setup.is_open());
            assert_eq!(setup.commands(), vec![Command::SelectNext]);
        }

        #[gpui::test]
        fn an_unfocused_open_dropdown_leaves_the_keys_to_its_owner(cx: &mut TestAppContext) {
            let mut setup = setup(cx);
            let dropdown = setup.dropdown();
            setup
                .window
                .update(|_, cx| dropdown.update(cx, |dropdown, cx| dropdown.open(cx)));

            setup.keys("j enter");

            assert!(setup.is_open(), "the owner drives a dropdown it keeps");
            assert_eq!(setup.highlighted(), Some(0));
            assert_eq!(
                setup.commands(),
                vec![Command::SelectNext, Command::Execute]
            );
        }

        /// Without the installed seam a dropdown answers no keys at all, so a
        /// host that never installs it keeps full control of its keys.
        #[gpui::test]
        fn a_dropdown_without_the_seam_answers_no_keys(cx: &mut TestAppContext) {
            use super::Bare;

            cx.update(gpui_component::init);
            cx.update(|cx| {
                cx.bind_keys([gpui::KeyBinding::new(
                    "enter",
                    RunCommand::new(Command::Execute.id()),
                    Some("Dropdown"),
                )]);
            });

            let (bare, window) = cx.add_window_view(|_window, cx| Bare {
                dropdown: cx.new(|_cx| {
                    Dropdown::new("seamless-dropdown").items(vec![DropdownItem::new("One")])
                }),
            });
            window.run_until_parked();

            window.update(|window, cx| {
                let dropdown = bare.read(cx).dropdown.clone();
                dropdown.update(cx, |dropdown, cx| dropdown.focus(window, cx));
            });
            window.run_until_parked();
            window.simulate_keystrokes("enter");
            window.run_until_parked();

            let open = window.update(|_, cx| bare.read(cx).dropdown.read(cx).is_open());
            assert!(!open, "no installed seam, no key answered");
        }
    }

    #[test]
    fn dropdown_placeholder_key_resolves() {
        let en = dbflux_i18n::t!("controls.dropdown.placeholder", locale = "en");
        let es = dbflux_i18n::t!("controls.dropdown.placeholder", locale = "es");

        assert!(!en.is_empty() && en != "controls.dropdown.placeholder");
        assert!(!es.is_empty() && es != "controls.dropdown.placeholder");
        assert_ne!(en, es);
    }

    /// A trigger that draws only a chevron has no text of its own, so it needs
    /// the name the owner sets; anything else is named by its placeholder.
    mod naming {
        use super::super::install_keymap_seams;
        use crate::accessibility::{FrameCapture, accessible_name};
        use crate::controls::{ButtonVariant, Dropdown, DropdownItem};
        use gpui::{
            AppContext as _, Context, Entity, IntoElement, ParentElement as _, Render, Styled as _,
            TestAppContext, Window, div,
        };

        struct Owner {
            dropdown: Entity<Dropdown>,
        }

        impl Render for Owner {
            fn render(
                &mut self,
                _window: &mut Window,
                _cx: &mut Context<Self>,
            ) -> impl IntoElement {
                div()
                    .w_full()
                    .child(div().w(gpui::px(200.0)).child(self.dropdown.clone()))
            }
        }

        /// Renders one dropdown under the id `naming-dropdown` and returns the
        /// name of its trigger.
        fn trigger_name(
            build: impl FnOnce() -> Dropdown + 'static,
            cx: &mut TestAppContext,
        ) -> Option<String> {
            cx.update(crate::theme::init);
            cx.update(install_keymap_seams);

            let capture = std::sync::Arc::new(FrameCapture::default());
            let capture_for_window = capture.clone();
            let (_, window) = cx.add_window_view(move |window, cx| {
                window.observe_frames(&capture_for_window);
                window.refresh();

                Owner {
                    dropdown: cx.new(|_cx| build()),
                }
            });
            window.run_until_parked();
            let frame = capture.frame(window);

            // A node keeps its own element id only while that id names it
            // alone, so the trigger may be filed under its path in the control.
            let id = frame
                .nodes()
                .map(|(_, node)| node.id().to_string())
                .find(|id| id == "dropdown-trigger" || id.ends_with(".dropdown-trigger"))?;

            accessible_name(&frame, &id)
        }

        #[gpui::test]
        fn an_icon_only_trigger_is_named_by_its_aria_label(cx: &mut TestAppContext) {
            let name = trigger_name(
                || {
                    Dropdown::new("naming-dropdown")
                        .items(vec![DropdownItem::new("Manual")])
                        .aria_label("Auto-refresh interval")
                        .chevron_trigger(ButtonVariant::Secondary)
                },
                cx,
            );

            assert_eq!(name.as_deref(), Some("Auto-refresh interval"));
        }

        #[gpui::test]
        fn a_trigger_without_an_explicit_name_takes_the_placeholder(cx: &mut TestAppContext) {
            let placeholder = dbflux_i18n::t!("controls.dropdown.placeholder").to_string();
            let name = trigger_name(
                || {
                    Dropdown::new("naming-dropdown")
                        .items(vec![DropdownItem::new("Manual")])
                        .chevron_trigger(ButtonVariant::Secondary)
                },
                cx,
            );

            assert_eq!(name, Some(placeholder));
        }

        /// Once something is selected, the trigger draws that value, and the
        /// name follows the text on screen rather than the placeholder.
        #[gpui::test]
        fn a_trigger_with_a_selection_is_named_by_the_value_it_shows(cx: &mut TestAppContext) {
            let name = trigger_name(
                || {
                    Dropdown::new("naming-dropdown")
                        .items(vec![DropdownItem::new("Manual"), DropdownItem::new("30s")])
                        .selected_index(Some(1))
                },
                cx,
            );

            assert_eq!(name.as_deref(), Some("30s"));
        }
    }

    /// The toolbar trigger draws the environment badge the trait sets: the
    /// trait maps the environment to bolt's `BadgeLabel` and stores it on the
    /// dropdown, and setting `None` clears it again.
    #[gpui::test]
    fn set_label_environment_stores_and_clears_the_environment_badge(cx: &mut TestAppContext) {
        use gpui::AppContext as _;

        cx.update(gpui_component::init);

        let (bare, window) = cx.add_window_view(|_window, cx| Bare {
            dropdown: cx.new(|_cx| Dropdown::new("env-dropdown").toolbar_style(true)),
        });
        window.run_until_parked();

        window.update(|_, cx| {
            let dropdown = bare.read(cx).dropdown.clone();
            dropdown.update(cx, |dropdown, cx| {
                dropdown.set_label_environment(Some(ConnectionEnvironment::Staging), cx);
            });
        });

        let badge = window
            .update(|_, cx| bare.read(cx).dropdown.read(cx).label_badge().cloned())
            .expect("the environment badge is stored");
        assert_eq!(badge.tone, BadgeTone::Warning);
        assert_eq!(
            badge.label,
            SharedString::from(environment_short_label(ConnectionEnvironment::Staging))
        );

        window.update(|_, cx| {
            let dropdown = bare.read(cx).dropdown.clone();
            dropdown.update(cx, |dropdown, cx| {
                dropdown.set_label_environment(None, cx);
            });
        });
        assert!(window.update(|_, cx| bare.read(cx).dropdown.read(cx).label_badge().is_none()));
    }
}
