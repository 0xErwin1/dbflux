use crate::app_state_entity::{AppStateChanged, AppStateEntity, AuthProfileCreated};
use crate::modal_frame::ModalFrame;
use crate::platform;
use dbflux_components::composites::{RailItem, field_row_vertical, render_wizard_rail};
use dbflux_components::controls::InputState;
use dbflux_components::controls::{Button, Input};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Text;
#[cfg(feature = "aws")]
use dbflux_components::tokens::Radii;
use dbflux_components::tokens::Spacing;
use dbflux_core::AuthProfile;
use gpui::prelude::FluentBuilder;
use gpui::*;
#[cfg(feature = "aws")]
use gpui_component::ActiveTheme;
#[cfg(feature = "aws")]
use gpui_component::scroll::ScrollableElement;
use uuid::Uuid;

#[cfg(feature = "aws")]
use dbflux_aws::{
    AwsSsoAccount, list_sso_account_roles_blocking, list_sso_accounts_blocking, login_sso_blocking,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WizardStep {
    Start,
    Account,
    Role,
    Confirm,
}

impl WizardStep {
    const ALL: [WizardStep; 4] = [
        WizardStep::Start,
        WizardStep::Account,
        WizardStep::Role,
        WizardStep::Confirm,
    ];

    fn label(self) -> String {
        match self {
            WizardStep::Start => dbflux_i18n::t!("sso_wizard.step.start"),
            WizardStep::Account => dbflux_i18n::t!("sso_wizard.step.account"),
            WizardStep::Role => dbflux_i18n::t!("sso_wizard.step.role"),
            WizardStep::Confirm => dbflux_i18n::t!("sso_wizard.step.confirm"),
        }
    }
}

/// Maximum height of the discovered account and role lists; longer lists
/// scroll inside it so the wizard buttons stay in view.
#[cfg(feature = "aws")]
const DISCOVERED_LIST_MAX_HEIGHT: Pixels = px(240.0);

/// Rail entries for the wizard: steps before `current` are completed. The
/// rail is display-only because Back already walks the steps in order.
fn sso_rail_items(current: WizardStep) -> Vec<RailItem> {
    let current_index = WizardStep::ALL
        .iter()
        .position(|step| *step == current)
        .unwrap_or(0);

    WizardStep::ALL
        .iter()
        .enumerate()
        .map(|(index, step)| RailItem {
            label: SharedString::from(step.label()),
            completed: index < current_index,
            current: index == current_index,
        })
        .collect()
}

/// A step 1 input with its visible label above it. The label is also the
/// input's accessible name, and `id` keeps the input addressable across runs.
fn labeled_input(
    id: &'static str,
    label: String,
    state: &Entity<InputState>,
    cx: &App,
) -> gpui::Div {
    field_row_vertical(
        label.clone(),
        Input::new(state).id(id).aria_label(label),
        cx,
    )
}

pub struct SsoWizard {
    app_state: Entity<AppStateEntity>,
    visible: bool,
    step: WizardStep,
    focus_handle: FocusHandle,
    input_profile_name: Entity<InputState>,
    input_start_url: Entity<InputState>,
    input_region: Entity<InputState>,
    input_account_id: Entity<InputState>,
    input_role_name: Entity<InputState>,
    status: Option<String>,

    #[cfg(feature = "aws")]
    discovered_accounts: Vec<AwsSsoAccount>,
    #[cfg(feature = "aws")]
    discovered_roles: Vec<String>,
    #[cfg(feature = "aws")]
    accounts_loading: bool,
    #[cfg(feature = "aws")]
    roles_loading: bool,
}

pub enum SsoWizardEvent {
    ProfileCreated { profile_id: Uuid },
}

impl SsoWizard {
    pub fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        Self {
            app_state,
            visible: false,
            step: WizardStep::Start,
            focus_handle: cx.focus_handle(),
            input_profile_name: cx
                .new(|cx| InputState::new(window, cx).placeholder("profile-name")),
            input_start_url: cx
                .new(|cx| InputState::new(window, cx).placeholder("https://...awsapps.com/start")),
            input_region: cx.new(|cx| InputState::new(window, cx).placeholder("us-east-1")),
            input_account_id: cx.new(|cx| InputState::new(window, cx).placeholder("123456789012")),
            input_role_name: cx.new(|cx| InputState::new(window, cx).placeholder("ReadOnlyRole")),
            status: None,

            #[cfg(feature = "aws")]
            discovered_accounts: Vec::new(),
            #[cfg(feature = "aws")]
            discovered_roles: Vec::new(),
            #[cfg(feature = "aws")]
            accounts_loading: false,
            #[cfg(feature = "aws")]
            roles_loading: false,
        }
    }

    pub fn open(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.visible = true;
        self.step = WizardStep::Start;
        self.status = None;
        self.focus_handle.focus(window, cx);
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        cx.notify();
    }

    fn next(&mut self, cx: &mut Context<Self>) {
        self.step = match self.step {
            WizardStep::Start => WizardStep::Account,
            WizardStep::Account => WizardStep::Role,
            WizardStep::Role => WizardStep::Confirm,
            WizardStep::Confirm => WizardStep::Confirm,
        };
        cx.notify();
    }

    fn back(&mut self, cx: &mut Context<Self>) {
        self.step = match self.step {
            WizardStep::Start => WizardStep::Start,
            WizardStep::Account => WizardStep::Start,
            WizardStep::Role => WizardStep::Account,
            WizardStep::Confirm => WizardStep::Role,
        };
        cx.notify();
    }

    fn save_profile(&mut self, cx: &mut Context<Self>) {
        let profile_name = self.input_profile_name.read(cx).value().trim().to_string();
        let start_url = self.input_start_url.read(cx).value().trim().to_string();
        let region = self.input_region.read(cx).value().trim().to_string();
        let account_id = self.input_account_id.read(cx).value().trim().to_string();
        let role_name = self.input_role_name.read(cx).value().trim().to_string();

        if profile_name.is_empty() || start_url.is_empty() || region.is_empty() {
            self.status = Some(dbflux_i18n::t!("sso_wizard.status.missing_required"));
            cx.notify();
            return;
        }

        let mut fields = std::collections::HashMap::new();
        fields.insert("profile_name".to_string(), profile_name.clone());
        fields.insert("sso_start_url".to_string(), start_url);
        fields.insert("region".to_string(), region);
        if !account_id.is_empty() {
            fields.insert("sso_account_id".to_string(), account_id);
        }
        if !role_name.is_empty() {
            fields.insert("sso_role_name".to_string(), role_name);
        }

        let profile_id = Uuid::new_v4();

        self.app_state.update(cx, |state, cx| {
            state.add_auth_profile(AuthProfile {
                id: profile_id,
                name: profile_name,
                provider_id: "aws-sso".to_string(),
                fields,
                // AWS SSO profiles carry only non-secret config; no key material.
                secret_fields: std::collections::HashMap::new(),
                enabled: true,
                read_only: false,
                dangling_origin: None,
            });
            cx.emit(AuthProfileCreated { profile_id });
            cx.emit(AppStateChanged);
        });

        cx.emit(SsoWizardEvent::ProfileCreated { profile_id });

        self.status = Some(dbflux_i18n::t!("sso_wizard.status.profile_created"));
        self.visible = false;
        cx.notify();
    }

    #[cfg(feature = "aws")]
    fn discover_accounts(&mut self, cx: &mut Context<Self>) {
        if self.accounts_loading {
            return;
        }

        let profile_name = self.input_profile_name.read(cx).value().trim().to_string();
        let start_url = self.input_start_url.read(cx).value().trim().to_string();
        let region = self.input_region.read(cx).value().trim().to_string();

        if profile_name.is_empty() || start_url.is_empty() || region.is_empty() {
            self.status = Some(dbflux_i18n::t!(
                "sso_wizard.status.fill_before_account_discovery"
            ));
            cx.notify();
            return;
        }

        self.accounts_loading = true;
        self.status = Some(dbflux_i18n::t!("sso_wizard.status.discovering_accounts"));
        cx.notify();

        let this = cx.entity().clone();
        let task = cx.background_executor().spawn(async move {
            let _ = login_sso_blocking(Uuid::nil(), &profile_name, &start_url);
            list_sso_accounts_blocking(&profile_name, &region, &start_url)
        });

        cx.spawn(async move |_entity, cx| {
            let result = task.await;
            cx.update(|cx| {
                this.update(cx, |this, cx| {
                    this.accounts_loading = false;
                    match result {
                        Ok(accounts) => {
                            this.discovered_accounts = accounts;
                            this.status =
                                Some(dbflux_i18n::t!("sso_wizard.status.accounts_discovered"));
                        }
                        Err(error) => {
                            this.discovered_accounts.clear();
                            this.status = Some(dbflux_i18n::t!(
                                "sso_wizard.status.accounts_failed",
                                error = error
                            ));
                        }
                    }
                    cx.notify();
                });
            });
        })
        .detach();
    }

    #[cfg(feature = "aws")]
    fn discover_roles_for_selected_account(&mut self, cx: &mut Context<Self>) {
        if self.roles_loading {
            return;
        }

        let profile_name = self.input_profile_name.read(cx).value().trim().to_string();
        let start_url = self.input_start_url.read(cx).value().trim().to_string();
        let region = self.input_region.read(cx).value().trim().to_string();
        let account_id = self.input_account_id.read(cx).value().trim().to_string();

        if profile_name.is_empty()
            || start_url.is_empty()
            || region.is_empty()
            || account_id.is_empty()
        {
            self.status = Some(dbflux_i18n::t!(
                "sso_wizard.status.fill_before_role_discovery"
            ));
            cx.notify();
            return;
        }

        self.roles_loading = true;
        self.status = Some(dbflux_i18n::t!("sso_wizard.status.discovering_roles"));
        cx.notify();

        let this = cx.entity().clone();
        let task = cx.background_executor().spawn(async move {
            list_sso_account_roles_blocking(&profile_name, &region, &start_url, &account_id)
        });

        cx.spawn(async move |_entity, cx| {
            let result = task.await;
            cx.update(|cx| {
                this.update(cx, |this, cx| {
                    this.roles_loading = false;
                    match result {
                        Ok(roles) => {
                            this.discovered_roles = roles;
                            this.status =
                                Some(dbflux_i18n::t!("sso_wizard.status.roles_discovered"));
                        }
                        Err(error) => {
                            this.discovered_roles.clear();
                            this.status = Some(dbflux_i18n::t!(
                                "sso_wizard.status.roles_failed",
                                error = error
                            ));
                        }
                    }
                    cx.notify();
                });
            });
        })
        .detach();
    }
}

impl EventEmitter<SsoWizardEvent> for SsoWizard {}

impl Render for SsoWizard {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let title_text = dbflux_i18n::t!("sso_wizard.title");
        let csd_title_bar = platform::render_csd_title_bar(_window, cx, &title_text);

        let content = if !self.visible {
            div().into_any_element()
        } else {
            self.render_visible(_window, cx)
        };

        div()
            .size_full()
            .when_some(csd_title_bar, |el, title_bar| el.child(title_bar))
            .child(content)
            .into_any_element()
    }
}

impl SsoWizard {
    fn render_visible(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        let close_entity = cx.entity().downgrade();
        let close = move |_window: &mut Window, cx: &mut App| {
            let _ = close_entity.update(cx, |this, cx| this.close(cx));
        };

        let mut frame = ModalFrame::new("sso-wizard", &self.focus_handle, close)
            .title(dbflux_i18n::t!("sso_wizard.title"))
            .icon(AppIcon::Lock)
            .width(px(680.0));

        let body = div()
            .flex()
            .flex_col()
            .flex_1()
            .min_w(px(0.0))
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .child(match self.step {
                WizardStep::Start => div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::SM)
                    .child(labeled_input(
                        "sso-field-profile-name",
                        dbflux_i18n::t!("sso_wizard.field.profile_name"),
                        &self.input_profile_name,
                        cx,
                    ))
                    .child(labeled_input(
                        "sso-field-start-url",
                        dbflux_i18n::t!("sso_wizard.field.start_url"),
                        &self.input_start_url,
                        cx,
                    ))
                    .child(labeled_input(
                        "sso-field-region",
                        dbflux_i18n::t!("sso_wizard.field.region"),
                        &self.input_region,
                        cx,
                    ))
                    .into_any_element(),
                WizardStep::Account => {
                    #[cfg_attr(not(feature = "aws"), allow(unused_mut))]
                    let mut account_step = div()
                        .flex()
                        .flex_col()
                        .gap(Spacing::SM)
                        .child(Input::new(&self.input_account_id))
                        .child(Text::caption(dbflux_i18n::t!("sso_wizard.account.hint")));

                    #[cfg(feature = "aws")]
                    {
                        let query = self.input_account_id.read(cx).value().trim().to_lowercase();

                        let filtered_accounts = self
                            .discovered_accounts
                            .iter()
                            .filter(|account| {
                                if query.is_empty() {
                                    return true;
                                }

                                account.account_id.to_lowercase().contains(&query)
                                    || account.account_name.to_lowercase().contains(&query)
                            })
                            .cloned()
                            .collect::<Vec<_>>();

                        let discover_label = if self.accounts_loading {
                            dbflux_i18n::t!("sso_wizard.account.discovering")
                        } else {
                            dbflux_i18n::t!("sso_wizard.account.discover_button")
                        };

                        account_step = account_step
                            .child(
                                Button::new("sso-wizard-discover-accounts", discover_label)
                                    .ghost()
                                    .disabled(self.accounts_loading)
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.discover_accounts(cx);
                                    })),
                            )
                            .when(
                                !self.accounts_loading
                                    && !self.discovered_accounts.is_empty()
                                    && filtered_accounts.is_empty(),
                                |d| {
                                    d.child(Text::caption(dbflux_i18n::t!(
                                        "sso_wizard.account.no_matches"
                                    )))
                                },
                            )
                            .child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .gap_1()
                                    .max_h(DISCOVERED_LIST_MAX_HEIGHT)
                                    .overflow_y_scrollbar()
                                    .id("sso-wizard-account-list")
                                    .children(filtered_accounts.iter().map(|account| {
                                        let label = format!(
                                            "{} ({})",
                                            account.account_name, account.account_id
                                        );
                                        let account_id = account.account_id.clone();

                                        div()
                                            .px(Spacing::SM)
                                            .py(Spacing::XS)
                                            .rounded(Radii::SM)
                                            .border_1()
                                            .border_color(cx.theme().border)
                                            .cursor_pointer()
                                            .on_mouse_down(
                                                MouseButton::Left,
                                                cx.listener(move |this, _, window, cx| {
                                                    this.input_account_id.update(
                                                        cx,
                                                        |state, cx| {
                                                            state.set_value(
                                                                account_id.clone(),
                                                                window,
                                                                cx,
                                                            );
                                                        },
                                                    );
                                                    cx.notify();
                                                }),
                                            )
                                            .child(label)
                                    })),
                            );
                    }

                    account_step.into_any_element()
                }
                WizardStep::Role => {
                    #[cfg_attr(not(feature = "aws"), allow(unused_mut))]
                    let mut role_step = div()
                        .flex()
                        .flex_col()
                        .gap(Spacing::SM)
                        .child(Input::new(&self.input_role_name))
                        .child(Text::caption(dbflux_i18n::t!("sso_wizard.role.hint")));

                    #[cfg(feature = "aws")]
                    {
                        let query = self.input_role_name.read(cx).value().trim().to_lowercase();

                        let filtered_roles = self
                            .discovered_roles
                            .iter()
                            .filter(|role| {
                                if query.is_empty() {
                                    return true;
                                }

                                role.to_lowercase().contains(&query)
                            })
                            .cloned()
                            .collect::<Vec<_>>();

                        let discover_label = if self.roles_loading {
                            dbflux_i18n::t!("sso_wizard.role.discovering")
                        } else {
                            dbflux_i18n::t!("sso_wizard.role.discover_button")
                        };

                        role_step = role_step
                            .child(
                                Button::new("sso-wizard-discover-roles", discover_label)
                                    .ghost()
                                    .disabled(self.roles_loading)
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.discover_roles_for_selected_account(cx);
                                    })),
                            )
                            .when(
                                !self.roles_loading
                                    && !self.discovered_roles.is_empty()
                                    && filtered_roles.is_empty(),
                                |d| {
                                    d.child(Text::caption(dbflux_i18n::t!(
                                        "sso_wizard.role.no_matches"
                                    )))
                                },
                            )
                            .child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .gap_1()
                                    .max_h(DISCOVERED_LIST_MAX_HEIGHT)
                                    .overflow_y_scrollbar()
                                    .id("sso-wizard-role-list")
                                    .children(filtered_roles.iter().map(|role| {
                                        let role_name = role.clone();

                                        div()
                                            .px(Spacing::SM)
                                            .py(Spacing::XS)
                                            .rounded(Radii::SM)
                                            .border_1()
                                            .border_color(cx.theme().border)
                                            .cursor_pointer()
                                            .on_mouse_down(
                                                MouseButton::Left,
                                                cx.listener(move |this, _, window, cx| {
                                                    this.input_role_name.update(cx, |state, cx| {
                                                        state.set_value(
                                                            role_name.clone(),
                                                            window,
                                                            cx,
                                                        );
                                                    });
                                                    cx.notify();
                                                }),
                                            )
                                            .child(role.clone())
                                    })),
                            );
                    }

                    role_step.into_any_element()
                }
                WizardStep::Confirm => div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(Text::body(dbflux_i18n::t!(
                        "sso_wizard.confirm.profile",
                        value = self.input_profile_name.read(cx).value()
                    )))
                    .child(Text::body(dbflux_i18n::t!(
                        "sso_wizard.confirm.start_url",
                        value = self.input_start_url.read(cx).value()
                    )))
                    .child(Text::body(dbflux_i18n::t!(
                        "sso_wizard.confirm.region",
                        value = self.input_region.read(cx).value()
                    )))
                    .child(Text::body(dbflux_i18n::t!(
                        "sso_wizard.confirm.account",
                        value = self.input_account_id.read(cx).value()
                    )))
                    .child(Text::body(dbflux_i18n::t!(
                        "sso_wizard.confirm.role",
                        value = self.input_role_name.read(cx).value()
                    )))
                    .into_any_element(),
            })
            .when_some(self.status.clone(), |d, status| {
                d.child(Text::caption(status))
            })
            .child(
                div()
                    .flex()
                    .justify_end()
                    .gap(Spacing::SM)
                    .child(
                        Button::new("sso-wizard-back", dbflux_i18n::t!("sso_wizard.back"))
                            .ghost()
                            .disabled(matches!(self.step, WizardStep::Start))
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.back(cx);
                            })),
                    )
                    .child({
                        let next_label = if matches!(self.step, WizardStep::Confirm) {
                            dbflux_i18n::t!("sso_wizard.save")
                        } else {
                            dbflux_i18n::t!("sso_wizard.next")
                        };
                        Button::new("sso-wizard-next", next_label)
                            .ghost()
                            .on_click(cx.listener(|this, _, _, cx| {
                                if matches!(this.step, WizardStep::Confirm) {
                                    this.save_profile(cx);
                                } else {
                                    this.next(cx);
                                }
                            }))
                    }),
            );

        frame = frame.child(
            div()
                .flex()
                .flex_row()
                .child(render_wizard_rail(
                    &sso_rail_items(self.step),
                    None::<fn(usize, &mut Window, &mut App)>,
                    cx,
                ))
                .child(body),
        );
        frame.render(cx)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "aws")]
    use super::{AwsSsoAccount, DISCOVERED_LIST_MAX_HEIGHT};
    use super::{SsoWizard, WizardStep, sso_rail_items};
    #[cfg(feature = "aws")]
    use gpui::FrameAction;
    use gpui::{AccessibilityFrame, FrameObserver, Role, TestAppContext};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// Keeps the latest rendered accessibility frame of the window it observes.
    #[derive(Default)]
    struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

    impl FrameObserver for FrameCapture {
        fn accessibility_updated(&self, frame: &AccessibilityFrame) {
            *self.0.lock().expect("frame capture lock") = Some(frame.clone());
        }
    }

    /// Opens the wizard in a test window, moves it to `step`, lets `prepare`
    /// adjust its state, and returns the latest rendered frame.
    fn render_wizard(
        step: WizardStep,
        prepare: impl FnOnce(&mut SsoWizard) + 'static,
        cx: &mut TestAppContext,
    ) -> AccessibilityFrame {
        cx.update(gpui_component::init);
        let app_state = crate::object_tree::test_support::test_app_state(cx);

        let capture = Arc::new(FrameCapture::default());
        let capture_for_window = capture.clone();
        let (_wizard, visual) = cx.add_window_view(move |window, cx| {
            window.observe_frames(&capture_for_window);

            let mut wizard = SsoWizard::new(app_state, window, cx);
            wizard.open(window, cx);
            wizard.step = step;
            prepare(&mut wizard);

            window.refresh();
            wizard
        });
        visual.run_until_parked();

        capture
            .0
            .lock()
            .expect("frame capture lock")
            .clone()
            .expect("the window rendered a frame")
    }

    #[gpui::test]
    fn start_step_inputs_have_visible_labels_ids_and_names(cx: &mut TestAppContext) {
        let frame = render_wizard(WizardStep::Start, |_| {}, cx);

        let text_inputs: HashMap<String, Option<String>> = frame
            .nodes()
            .filter_map(|(_, node)| {
                let accessible = frame.accessibility_node(node)?;
                (accessible.role() == Role::TextInput).then(|| {
                    (
                        node.id().to_owned(),
                        accessible.label().map(ToOwned::to_owned),
                    )
                })
            })
            .collect();

        let expected = [
            (
                "sso-field-profile-name",
                dbflux_i18n::t!("sso_wizard.field.profile_name"),
            ),
            (
                "sso-field-start-url",
                dbflux_i18n::t!("sso_wizard.field.start_url"),
            ),
            (
                "sso-field-region",
                dbflux_i18n::t!("sso_wizard.field.region"),
            ),
        ];

        for (id, label) in expected {
            assert_eq!(
                text_inputs.get(id),
                Some(&Some(label.clone())),
                "input {id} in {text_inputs:?}"
            );
            assert!(
                frame
                    .nodes()
                    .any(|(_, node)| node.content_text().contains(label.as_str())),
                "label {label} is not visible"
            );
        }
    }

    #[gpui::test]
    fn steps_render_in_the_shared_wizard_rail(cx: &mut TestAppContext) {
        let frame = render_wizard(WizardStep::Role, |_| {}, cx);

        for (index, step) in WizardStep::ALL.iter().enumerate() {
            let rail_id = format!("wizard-rail-{index}");
            let entry = frame
                .nodes()
                .find(|(_, node)| node.id() == rail_id)
                .map(|(_, node)| node)
                .unwrap_or_else(|| panic!("rail entry {rail_id} is rendered"));

            assert!(
                entry.content_text().contains(step.label().as_str()),
                "rail entry {rail_id} shows {}",
                step.label()
            );
        }
    }

    #[test]
    fn rail_marks_steps_before_the_current_one_completed() {
        let items = sso_rail_items(WizardStep::Role);

        let states: Vec<(bool, bool)> = items
            .iter()
            .map(|item| (item.completed, item.current))
            .collect();

        assert_eq!(
            states,
            vec![(true, false), (true, false), (false, true), (false, false)]
        );
    }

    /// Checks that the scrollable list `id`, filled with more rows than fit,
    /// stops growing at the list height cap and that its scroll area takes
    /// scrolling.
    #[cfg(feature = "aws")]
    fn assert_bounded_scroll_list(frame: &AccessibilityFrame, id: &str) {
        let node = |node_id: String| {
            frame
                .nodes()
                .map(|(_, node)| node)
                .find(|node| node.id() == node_id)
                .unwrap_or_else(|| panic!("{node_id} is rendered"))
        };

        let list_height = node(id.to_string()).bounds().size.height;
        assert_eq!(
            list_height, DISCOVERED_LIST_MAX_HEIGHT,
            "{id} should fill the height cap and scroll the rest"
        );

        let area = node(format!("{id}-area"));
        assert!(
            area.actions().contains(&FrameAction::Scroll),
            "{id} does not scroll: {:?}",
            area.actions()
        );
    }

    #[cfg(feature = "aws")]
    #[gpui::test]
    fn discovered_accounts_scroll_inside_a_bounded_list(cx: &mut TestAppContext) {
        let frame = render_wizard(
            WizardStep::Account,
            |wizard| {
                wizard.discovered_accounts = (0..60)
                    .map(|index| AwsSsoAccount {
                        account_id: format!("{index:012}"),
                        account_name: format!("account-{index}"),
                        email_address: None,
                    })
                    .collect();
            },
            cx,
        );

        assert_bounded_scroll_list(&frame, "sso-wizard-account-list");
    }

    #[cfg(feature = "aws")]
    #[gpui::test]
    fn discovered_roles_scroll_inside_a_bounded_list(cx: &mut TestAppContext) {
        let frame = render_wizard(
            WizardStep::Role,
            |wizard| {
                wizard.discovered_roles = (0..60).map(|index| format!("Role{index}")).collect();
            },
            cx,
        );

        assert_bounded_scroll_list(&frame, "sso-wizard-role-list");
    }

    const SSO_WIZARD_KEYS: &[&str] = &[
        "sso_wizard.title",
        "sso_wizard.step.start",
        "sso_wizard.step.account",
        "sso_wizard.step.role",
        "sso_wizard.step.confirm",
        "sso_wizard.field.profile_name",
        "sso_wizard.field.start_url",
        "sso_wizard.field.region",
        "sso_wizard.account.hint",
        "sso_wizard.account.discovering",
        "sso_wizard.account.discover_button",
        "sso_wizard.account.no_matches",
        "sso_wizard.role.hint",
        "sso_wizard.role.discovering",
        "sso_wizard.role.discover_button",
        "sso_wizard.role.no_matches",
        "sso_wizard.confirm.profile",
        "sso_wizard.confirm.start_url",
        "sso_wizard.confirm.region",
        "sso_wizard.confirm.account",
        "sso_wizard.confirm.role",
        "sso_wizard.back",
        "sso_wizard.next",
        "sso_wizard.save",
        "sso_wizard.status.missing_required",
        "sso_wizard.status.fill_before_account_discovery",
        "sso_wizard.status.discovering_accounts",
        "sso_wizard.status.accounts_discovered",
        "sso_wizard.status.accounts_failed",
        "sso_wizard.status.fill_before_role_discovery",
        "sso_wizard.status.discovering_roles",
        "sso_wizard.status.roles_discovered",
        "sso_wizard.status.roles_failed",
        "sso_wizard.status.profile_created",
    ];

    #[test]
    fn sso_wizard_catalog_keys_resolve() {
        for locale in ["en", "es", "ko", "zh_Hans"] {
            for key in SSO_WIZARD_KEYS.iter().copied() {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "empty {locale} translation for {key}");
                assert_ne!(value, key, "missing {locale} translation for {key}");
            }
        }
    }

    #[test]
    fn sso_wizard_step_title_differs_between_locales() {
        let english = dbflux_i18n::t!("sso_wizard.step.start");
        let spanish = dbflux_i18n::t!("sso_wizard.step.start", locale = "es");

        assert_ne!(english, spanish);
    }
}
