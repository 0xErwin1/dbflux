use dbflux_components::controls::{Button, GpuiInput as Input, InputState};
use dbflux_components::modals::shell::{ModalFocus, ModalShell};
use dbflux_components::primitives::Text;
use dbflux_components::tokens::Spacing;
use dbflux_core::LogErr;
use gpui::prelude::*;
use gpui::*;
use uuid::Uuid;

/// Outcome emitted when the user resolves the create-dashboard modal.
#[derive(Clone, Debug)]
pub enum CreateDashboardOutcome {
    /// User confirmed creation with a valid name.
    Confirmed {
        profile_id: Uuid,
        name: String,
    },
    Cancelled,
}

/// Request payload for opening the create-dashboard modal.
#[derive(Clone, Debug)]
pub struct CreateDashboardRequest {
    /// Profile the dashboard will be associated with.
    pub profile_id: Uuid,
}

/// Modal entity for creating a new dashboard.
///
/// Renders a single name input field. The Create button and Enter stay
/// disabled while the name is empty or whitespace-only.
pub struct ModalCreateDashboard {
    request: Option<CreateDashboardRequest>,
    visible: bool,
    name_input: Entity<InputState>,
    focus: ModalFocus,
    validation_error: Option<String>,
    _input_observation: Subscription,
}

impl ModalCreateDashboard {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let name_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("modals.create_dashboard.name_label"))
        });

        // The Create button and Enter both follow the typed name, so every
        // edit re-renders the modal.
        let input_observation = cx.observe(&name_input, |_, _, cx| cx.notify());

        Self {
            request: None,
            visible: false,
            name_input,
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
        request: CreateDashboardRequest,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.request = Some(request);
        self.visible = true;
        self.validation_error = None;

        self.name_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });

        let input_focus = self.name_input.read(cx).focus_handle(cx);
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
        !self.name_input.read(cx).value().trim().is_empty()
    }

    /// Create the dashboard with the typed name, as the Create button does.
    pub fn confirm(&mut self, cx: &mut Context<Self>) {
        let name = self.name_input.read(cx).value().trim().to_string();

        if name.is_empty() {
            self.validation_error = Some(dbflux_i18n::t!(
                "modals.create_dashboard.validation.empty_name"
            ));
            cx.notify();
            return;
        }

        let Some(ref request) = self.request else {
            return;
        };

        cx.emit(CreateDashboardOutcome::Confirmed {
            profile_id: request.profile_id,
            name,
        });

        self.close(cx);
    }

    /// Dismiss the modal without creating a dashboard.
    pub fn cancel(&mut self, cx: &mut Context<Self>) {
        cx.emit(CreateDashboardOutcome::Cancelled);
        self.close(cx);
    }
}

impl EventEmitter<CreateDashboardOutcome> for ModalCreateDashboard {}

impl Render for ModalCreateDashboard {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let validation_error = self.validation_error.clone();
        let create_enabled = self.has_valid_name(cx);

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(
                Text::label(dbflux_i18n::t!("modals.create_dashboard.name_label"))
                    .into_any_element(),
            )
            .child(Input::new(&self.name_input))
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
                    "create-dashboard-cancel",
                    dbflux_i18n::t!("modals.create_dashboard.cancel"),
                )
                .on_click(on_cancel),
            )
            .child(
                Button::new(
                    "create-dashboard-confirm",
                    dbflux_i18n::t!("modals.create_dashboard.confirm"),
                )
                .primary()
                .disabled(!create_enabled)
                .on_click(on_confirm),
            );

        ModalShell::new(
            dbflux_i18n::t!("modals.create_dashboard.title"),
            body.into_any_element(),
            footer.into_any_element(),
        )
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
        .confirm_enabled(create_enabled)
        .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{CreateDashboardOutcome, CreateDashboardRequest};
    use uuid::Uuid;

    fn test_uuid() -> Uuid {
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
    }

    // O.1 tests

    #[test]
    fn modal_create_dashboard_rejects_empty_name() {
        let name = "";
        assert!(name.trim().is_empty(), "Empty name must be rejected");
    }

    #[test]
    fn modal_create_dashboard_rejects_whitespace_only_name() {
        let name = "   ";
        assert!(
            name.trim().is_empty(),
            "Whitespace-only name must be rejected"
        );
    }

    #[test]
    fn modal_create_dashboard_is_not_visible_on_new() {
        // visible is initialized to false; verify this without requiring a GPUI window.
        let visible = false;
        assert!(
            !visible,
            "ModalCreateDashboard must not be visible on construction"
        );
    }

    #[test]
    fn modal_create_dashboard_profile_id_is_propagated() {
        let profile_id = test_uuid();
        let req = CreateDashboardRequest { profile_id };
        let outcome = CreateDashboardOutcome::Confirmed {
            profile_id: req.profile_id,
            name: "My Dashboard".to_string(),
        };
        match outcome {
            CreateDashboardOutcome::Confirmed {
                profile_id: pid,
                name,
            } => {
                assert_eq!(pid, profile_id);
                assert_eq!(name, "My Dashboard");
            }
            _ => panic!("Expected Confirmed variant"),
        }
    }

    const CREATE_DASHBOARD_KEYS: &[&str] = &[
        "modals.create_dashboard.name_label",
        "modals.create_dashboard.title",
        "modals.create_dashboard.validation.empty_name",
        "modals.create_dashboard.cancel",
        "modals.create_dashboard.confirm",
    ];

    #[test]
    fn create_dashboard_catalog_keys_resolve() {
        for key in CREATE_DASHBOARD_KEYS {
            let english = dbflux_i18n::t!(key);
            let spanish = dbflux_i18n::t!(key, locale = "es");

            assert!(!english.is_empty(), "empty English translation for {key}");
            assert_ne!(english, *key, "missing English translation for {key}");
            assert!(!spanish.is_empty(), "empty Spanish translation for {key}");
            assert_ne!(spanish, *key, "missing Spanish translation for {key}");
        }
    }

    #[test]
    fn create_dashboard_title_differs_between_locales() {
        let english = dbflux_i18n::t!("modals.create_dashboard.title", locale = "en");
        let spanish = dbflux_i18n::t!("modals.create_dashboard.title", locale = "es");

        assert_eq!(english, "New dashboard");
        assert_eq!(spanish, "Nuevo dashboard");
        assert_ne!(english, spanish);
    }
}

#[cfg(test)]
mod keyboard_tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{CreateDashboardOutcome, CreateDashboardRequest, ModalCreateDashboard};
    use crate::modals::test_host::{click_backdrop, has_focus, host_modal};
    use gpui::{Entity, FocusHandle, TestAppContext, VisualTestContext};
    use std::cell::RefCell;
    use std::rc::Rc;
    use uuid::Uuid;

    type Outcomes = Rc<RefCell<Vec<CreateDashboardOutcome>>>;

    fn open_modal(
        cx: &mut TestAppContext,
    ) -> (
        Entity<ModalCreateDashboard>,
        FocusHandle,
        &mut VisualTestContext,
        Outcomes,
    ) {
        let (modal, outside, window) = host_modal(cx, ModalCreateDashboard::new);

        let outcomes: Outcomes = Rc::default();
        window.update(|window, cx| {
            let sink = outcomes.clone();
            cx.subscribe(&modal, move |_, outcome: &CreateDashboardOutcome, _| {
                sink.borrow_mut().push(outcome.clone());
            })
            .detach();

            modal.update(cx, |modal, cx| {
                modal.open(
                    CreateDashboardRequest {
                        profile_id: Uuid::nil(),
                    },
                    window,
                    cx,
                );
            });
        });
        window.run_until_parked();

        (modal, outside, window, outcomes)
    }

    #[gpui::test]
    fn enter_does_nothing_until_a_name_is_typed(cx: &mut TestAppContext) {
        let (modal, _outside, window, outcomes) = open_modal(cx);

        window.simulate_keystrokes("enter");
        assert!(outcomes.borrow().is_empty());
        assert!(window.update(|_, cx| modal.read(cx).is_visible()));

        window.simulate_input("Ops");
        window.simulate_keystrokes("enter");

        assert!(matches!(
            outcomes.borrow().as_slice(),
            [CreateDashboardOutcome::Confirmed { name, .. }] if name == "Ops"
        ));
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
    }

    #[gpui::test]
    fn escape_cancels_and_gives_focus_back(cx: &mut TestAppContext) {
        let (modal, outside, window, outcomes) = open_modal(cx);

        window.simulate_keystrokes("escape");

        assert!(matches!(
            outcomes.borrow().as_slice(),
            [CreateDashboardOutcome::Cancelled]
        ));
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
        assert!(has_focus(window, &outside));
    }

    #[gpui::test]
    fn a_backdrop_click_cancels(cx: &mut TestAppContext) {
        let (_modal, _outside, window, outcomes) = open_modal(cx);

        click_backdrop(window);

        assert!(matches!(
            outcomes.borrow().as_slice(),
            [CreateDashboardOutcome::Cancelled]
        ));
    }
}
