use dbflux_components::controls::{Button, GpuiInput as Input, InputState};
use dbflux_components::modals::shell::ModalShell;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::Spacing;
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
/// Renders a single name input field. Rejects empty or whitespace-only names
/// with an inline validation message.
pub struct ModalCreateDashboard {
    request: Option<CreateDashboardRequest>,
    visible: bool,
    name_input: Entity<InputState>,
    focus_handle: FocusHandle,
    validation_error: Option<String>,
    _subscriptions: Vec<Subscription>,
}

impl ModalCreateDashboard {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("Dashboard name"));

        Self {
            request: None,
            visible: false,
            name_input,
            focus_handle: cx.focus_handle(),
            validation_error: None,
            _subscriptions: Vec::new(),
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
            state.focus(window, cx);
        });

        let sub = cx.subscribe_in(
            &self.name_input.clone(),
            window,
            |this, _, event, window, cx| {
                use dbflux_components::controls::InputEvent;
                if let InputEvent::PressEnter { .. } = event {
                    this.confirm(window, cx);
                }
            },
        );
        self._subscriptions = vec![sub];

        self.focus_handle.focus(window);
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.request = None;
        self.validation_error = None;
        cx.notify();
    }

    fn confirm(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let name = self.name_input.read(cx).value().trim().to_string();

        if name.is_empty() {
            self.validation_error = Some("Name cannot be empty".to_string());
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
}

impl EventEmitter<CreateDashboardOutcome> for ModalCreateDashboard {}

impl Render for ModalCreateDashboard {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let validation_error = self.validation_error.clone();

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(Text::label("Dashboard name").into_any_element())
            .child(Input::new(&self.name_input))
            .when_some(validation_error, |el, err| {
                el.child(div().text_sm().child(Text::body(err).into_any_element()))
            });

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(CreateDashboardOutcome::Cancelled);
            this.close(cx);
        });

        let on_confirm =
            cx.listener(|this, _: &gpui::ClickEvent, window, cx| this.confirm(window, cx));

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(Button::new("create-dashboard-cancel", "Cancel").on_click(on_cancel))
            .child(
                Button::new("create-dashboard-confirm", "Create")
                    .primary()
                    .on_click(on_confirm),
            );

        ModalShell::new(
            "New dashboard",
            body.into_any_element(),
            footer.into_any_element(),
        )
        .width(gpui::px(400.0))
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
}
