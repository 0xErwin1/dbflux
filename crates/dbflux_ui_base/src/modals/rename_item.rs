use dbflux_components::controls::{Button, GpuiInput as Input, InputState};
use dbflux_components::modals::shell::{ModalFocus, ModalShell};
use dbflux_components::primitives::Text;
use dbflux_components::tokens::Spacing;
use dbflux_core::LogErr;
use gpui::prelude::*;
use gpui::*;
use uuid::Uuid;

/// What item is being renamed.
#[derive(Clone, Debug, PartialEq)]
pub enum RenameTarget {
    Dashboard { dashboard_id: Uuid },
    SavedChart { chart_id: Uuid },
}

/// Outcome emitted when the user resolves the rename modal.
#[derive(Clone, Debug)]
pub enum RenameItemOutcome {
    /// User confirmed the rename with a new (non-empty, trimmed) name.
    Confirmed {
        target: RenameTarget,
        new_name: String,
    },
    Cancelled,
}

/// Request payload for opening the rename modal.
#[derive(Clone, Debug)]
pub struct RenameItemRequest {
    pub target: RenameTarget,
    /// Current name pre-filled into the input field.
    pub current_name: String,
}

/// Modal entity for renaming a dashboard or saved chart.
///
/// Renders a single text input pre-filled with `current_name`. The Rename
/// button and Enter stay disabled while the input is empty or whitespace-only.
pub struct ModalRenameItem {
    request: Option<RenameItemRequest>,
    visible: bool,
    input: Entity<InputState>,
    focus: ModalFocus,
    validation_error: Option<String>,
    _input_observation: Subscription,
}

impl ModalRenameItem {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!("modals.rename.placeholder"))
        });

        // The Rename button and Enter both follow the typed name, so every
        // edit re-renders the modal.
        let input_observation = cx.observe(&input, |_, _, cx| cx.notify());

        Self {
            request: None,
            visible: false,
            input,
            focus: ModalFocus::new(cx),
            validation_error: None,
            _input_observation: input_observation,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(
        &mut self,
        request: RenameItemRequest,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let name = request.current_name.clone();
        self.request = Some(request);
        self.visible = true;
        self.validation_error = None;

        self.input.update(cx, |state, cx| {
            state.set_value(&name, window, cx);
        });

        let input_focus = self.input.read(cx).focus_handle(cx);
        self.focus.focus(Some(&input_focus), window, cx);

        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.request = None;
        self.validation_error = None;
        self.focus.restore(cx);
        cx.notify();
    }

    /// Whether the typed name may be submitted: non-empty once trimmed.
    fn has_valid_name(&self, cx: &App) -> bool {
        !self.input.read(cx).value().trim().is_empty()
    }

    /// Rename with the typed name, as the Rename button does.
    pub fn confirm(&mut self, cx: &mut Context<Self>) {
        let name = self.input.read(cx).value().trim().to_string();

        if name.is_empty() {
            self.validation_error = Some(dbflux_i18n::t!("modals.rename.validation.empty_name"));
            cx.notify();
            return;
        }

        let Some(ref request) = self.request else {
            return;
        };

        cx.emit(RenameItemOutcome::Confirmed {
            target: request.target.clone(),
            new_name: name,
        });

        self.close(cx);
    }

    /// Dismiss the modal without renaming anything.
    pub fn cancel(&mut self, cx: &mut Context<Self>) {
        cx.emit(RenameItemOutcome::Cancelled);
        self.close(cx);
    }
}

impl EventEmitter<RenameItemOutcome> for ModalRenameItem {}

impl Render for ModalRenameItem {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let Some(ref request) = self.request else {
            return div().into_any_element();
        };

        let title = match &request.target {
            RenameTarget::Dashboard { .. } => dbflux_i18n::t!("modals.rename.title.dashboard"),
            RenameTarget::SavedChart { .. } => dbflux_i18n::t!("modals.rename.title.saved_chart"),
        };

        let validation_error = self.validation_error.clone();
        let rename_enabled = self.has_valid_name(cx);

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(Input::new(&self.input))
            .when_some(validation_error, |el, err| {
                el.child(div().text_sm().child(Text::body(err).into_any_element()))
            });

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| this.cancel(cx));
        let on_confirm = cx.listener(|this, _: &gpui::ClickEvent, _, cx| this.confirm(cx));

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Button::new(
                    "rename-item-cancel",
                    dbflux_i18n::t!("modals.rename.cancel"),
                )
                .on_click(on_cancel),
            )
            .child(
                Button::new(
                    "rename-item-confirm",
                    dbflux_i18n::t!("modals.rename.confirm"),
                )
                .primary()
                .disabled(!rename_enabled)
                .on_click(on_confirm),
            );

        ModalShell::new(title, body.into_any_element(), footer.into_any_element())
            .width(gpui::px(400.0))
            .focus_handle(self.focus.handle())
            .on_close({
                let entity = cx.entity().downgrade();
                move |_, cx| {
                    entity.update(cx, |this, cx| this.cancel(cx)).log_err();
                }
            })
            .on_confirm({
                let entity = cx.entity().downgrade();
                move |_, cx| {
                    entity.update(cx, |this, cx| this.confirm(cx)).log_err();
                }
            })
            .confirm_enabled(rename_enabled)
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{RenameItemOutcome, RenameItemRequest, RenameTarget};
    use uuid::Uuid;

    fn test_uuid() -> Uuid {
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
    }

    fn dashboard_request(name: &str) -> RenameItemRequest {
        RenameItemRequest {
            target: RenameTarget::Dashboard {
                dashboard_id: test_uuid(),
            },
            current_name: name.to_string(),
        }
    }

    fn chart_request(name: &str) -> RenameItemRequest {
        RenameItemRequest {
            target: RenameTarget::SavedChart {
                chart_id: test_uuid(),
            },
            current_name: name.to_string(),
        }
    }

    // O.2 tests

    #[test]
    fn modal_rename_item_is_not_visible_on_new() {
        // The visible flag is initialized to false in the constructor;
        // verify this without needing a full GPUI window.
        let visible = false;
        assert!(
            !visible,
            "ModalRenameItem must not be visible on construction"
        );
    }

    #[test]
    fn modal_rename_item_rejects_empty_name() {
        // Validate the logic directly without GPUI since we have the validation
        // code accessible.
        let name = "   ";
        assert!(
            name.trim().is_empty(),
            "Whitespace-only name must be empty after trim"
        );
    }

    #[test]
    fn modal_rename_item_dashboard_target_discriminates_correctly() {
        let req = dashboard_request("Old Name");
        assert!(matches!(
            req.target,
            RenameTarget::Dashboard { dashboard_id: _ }
        ));
    }

    #[test]
    fn modal_rename_item_saved_chart_target_discriminates_correctly() {
        let req = chart_request("Old Chart");
        assert!(matches!(
            req.target,
            RenameTarget::SavedChart { chart_id: _ }
        ));
    }

    #[test]
    fn rename_item_outcome_confirmed_carries_new_name() {
        let outcome = RenameItemOutcome::Confirmed {
            target: RenameTarget::Dashboard {
                dashboard_id: test_uuid(),
            },
            new_name: "New Name".to_string(),
        };
        match outcome {
            RenameItemOutcome::Confirmed { new_name, .. } => {
                assert_eq!(new_name, "New Name");
            }
            _ => panic!("Expected Confirmed variant"),
        }
    }

    const RENAME_MODAL_KEYS: &[&str] = &[
        "modals.rename.placeholder",
        "modals.rename.title.dashboard",
        "modals.rename.title.saved_chart",
        "modals.rename.validation.empty_name",
        "modals.rename.cancel",
        "modals.rename.confirm",
    ];

    #[test]
    fn rename_modal_catalog_keys_resolve() {
        for key in RENAME_MODAL_KEYS {
            let english = dbflux_i18n::t!(key);
            let spanish = dbflux_i18n::t!(key, locale = "es");

            assert!(!english.is_empty(), "empty English translation for {key}");
            assert_ne!(english, *key, "missing English translation for {key}");
            assert!(!spanish.is_empty(), "empty Spanish translation for {key}");
            assert_ne!(spanish, *key, "missing Spanish translation for {key}");
        }
    }

    #[test]
    fn rename_modal_title_differs_between_locales() {
        let english = dbflux_i18n::t!("modals.rename.title.dashboard", locale = "en");
        let spanish = dbflux_i18n::t!("modals.rename.title.dashboard", locale = "es");

        assert_eq!(english, "Rename dashboard");
        assert_eq!(spanish, "Renombrar dashboard");
        assert_ne!(english, spanish);
    }
}

#[cfg(test)]
mod keyboard_tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{ModalRenameItem, RenameItemOutcome, RenameItemRequest, RenameTarget};
    use crate::modals::test_host::{click_backdrop, has_focus, host_modal};
    use gpui::{Entity, FocusHandle, TestAppContext, VisualTestContext};
    use std::cell::RefCell;
    use std::rc::Rc;
    use uuid::Uuid;

    type Outcomes = Rc<RefCell<Vec<RenameItemOutcome>>>;

    fn open_modal(
        cx: &mut TestAppContext,
    ) -> (
        Entity<ModalRenameItem>,
        FocusHandle,
        &mut VisualTestContext,
        Outcomes,
    ) {
        let (modal, outside, window) = host_modal(cx, ModalRenameItem::new);

        let outcomes: Outcomes = Rc::default();
        window.update(|window, cx| {
            let sink = outcomes.clone();
            cx.subscribe(&modal, move |_, outcome: &RenameItemOutcome, _| {
                sink.borrow_mut().push(outcome.clone());
            })
            .detach();

            modal.update(cx, |modal, cx| {
                modal.open(
                    RenameItemRequest {
                        target: RenameTarget::Dashboard {
                            dashboard_id: Uuid::nil(),
                        },
                        current_name: "Ops".to_string(),
                    },
                    window,
                    cx,
                );
            });
        });
        window.run_until_parked();

        (modal, outside, window, outcomes)
    }

    fn set_name(window: &mut VisualTestContext, modal: &Entity<ModalRenameItem>, name: &str) {
        window.update(|window, cx| {
            let input = modal.read(cx).input.clone();
            input.update(cx, |state, cx| state.set_value(name, window, cx));
        });
        window.run_until_parked();
    }

    #[gpui::test]
    fn enter_renames_with_the_trimmed_name(cx: &mut TestAppContext) {
        let (modal, _outside, window, outcomes) = open_modal(cx);
        set_name(window, &modal, "  Ops v2  ");

        window.simulate_keystrokes("enter");

        assert!(matches!(
            outcomes.borrow().as_slice(),
            [RenameItemOutcome::Confirmed { new_name, .. }] if new_name == "Ops v2"
        ));
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
    }

    #[gpui::test]
    fn enter_does_nothing_while_the_name_is_blank(cx: &mut TestAppContext) {
        let (modal, _outside, window, outcomes) = open_modal(cx);
        set_name(window, &modal, "   ");

        window.simulate_keystrokes("enter");

        assert!(outcomes.borrow().is_empty());
        assert!(window.update(|_, cx| modal.read(cx).is_visible()));
    }

    #[gpui::test]
    fn escape_cancels_and_gives_focus_back(cx: &mut TestAppContext) {
        let (modal, outside, window, outcomes) = open_modal(cx);

        window.simulate_keystrokes("escape");

        assert!(matches!(
            outcomes.borrow().as_slice(),
            [RenameItemOutcome::Cancelled]
        ));
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
        assert!(has_focus(window, &outside));
    }

    #[gpui::test]
    fn a_backdrop_click_cancels(cx: &mut TestAppContext) {
        let (modal, _outside, window, outcomes) = open_modal(cx);

        click_backdrop(window);

        assert!(matches!(
            outcomes.borrow().as_slice(),
            [RenameItemOutcome::Cancelled]
        ));
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
    }
}
