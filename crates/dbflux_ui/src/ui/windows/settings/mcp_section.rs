use super::layout;
use super::section_trait::SectionFocusEvent;
use super::{SettingsSection, SettingsSectionId};
use crate::app::{AppStateChanged, AppStateEntity, McpRuntimeEventRaised};
use crate::keymap::{KeyChord, Modifiers, key_chord_from_gpui};
use crate::ui::components::dropdown::DropdownItem;
use crate::ui::components::multi_select::MultiSelect;
use dbflux_components::controls::{Button, Checkbox, Input};
use dbflux_components::primitives::{Label, Text};
use dbflux_mcp::{PolicyRoleDto, ToolPolicyDto, TrustedClientDto};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::{InputEvent, InputState};
use gpui_component::scroll::ScrollableElement;
use std::collections::HashSet;

/// Tool display metadata: (id, label, description)
const TOOL_META: &[(&str, &str, &str)] = &[
    (
        "list_connections",
        "List Connections",
        "Enumerate all configured database connections",
    ),
    (
        "get_connection",
        "Get Connection",
        "Retrieve full details of a specific connection",
    ),
    (
        "get_connection_metadata",
        "Connection Metadata",
        "Fetch driver capabilities and metadata for a connection",
    ),
    (
        "list_databases",
        "List Databases",
        "List all databases accessible on a connection",
    ),
    (
        "list_schemas",
        "List Schemas",
        "List schemas within a database",
    ),
    (
        "list_tables",
        "List Tables",
        "List tables and collections within a schema",
    ),
    (
        "list_collections",
        "List Collections",
        "List MongoDB collections or similar document stores",
    ),
    (
        "describe_object",
        "Describe Object",
        "Get column/field definitions and indexes for a table or collection",
    ),
    (
        "read_query",
        "Read Query",
        "Execute a SELECT or equivalent read-only query",
    ),
    (
        "explain_query",
        "Explain Query",
        "Show the query execution plan without running it",
    ),
    (
        "preview_mutation",
        "Preview Mutation",
        "Dry-run a write/delete query and report affected rows",
    ),
    (
        "list_scripts",
        "List Scripts",
        "List saved scripts in the scripts directory",
    ),
    (
        "get_script",
        "Get Script",
        "Retrieve the source of a specific saved script",
    ),
    (
        "create_script",
        "Create Script",
        "Save a new script to the scripts directory",
    ),
    (
        "update_script",
        "Update Script",
        "Overwrite an existing saved script",
    ),
    (
        "delete_script",
        "Delete Script",
        "Permanently remove a script from the scripts directory",
    ),
    (
        "run_script",
        "Run Script",
        "Execute a saved Lua/SQL/shell script against a connection",
    ),
    (
        "request_execution",
        "Request Execution",
        "Submit a mutation for human approval before it runs",
    ),
    (
        "list_pending_executions",
        "List Pending",
        "View all executions awaiting approval",
    ),
    (
        "get_pending_execution",
        "Get Pending",
        "Retrieve details of a specific pending execution",
    ),
    (
        "approve_execution",
        "Approve Execution",
        "Approve and trigger a pending mutation",
    ),
    (
        "reject_execution",
        "Reject Execution",
        "Reject and discard a pending mutation",
    ),
    (
        "query_audit_logs",
        "Query Audit Logs",
        "Search and filter the audit trail",
    ),
    (
        "get_audit_entry",
        "Get Audit Entry",
        "Retrieve a single audit log entry by ID",
    ),
    (
        "export_audit_logs",
        "Export Audit Logs",
        "Download audit log entries as a file",
    ),
];

/// Execution class display metadata: (id, label, description)
const CLASS_META: &[(&str, &str, &str)] = &[
    (
        "metadata",
        "Metadata",
        "Schema inspection — listing databases, tables, and describing objects",
    ),
    (
        "read",
        "Read",
        "Running read-only queries and fetching data",
    ),
    (
        "write",
        "Write",
        "Inserting, updating, or running scripts that modify data",
    ),
    (
        "destructive",
        "Destructive",
        "DELETE, DROP, TRUNCATE and other irreversible operations",
    ),
    (
        "admin",
        "Admin",
        "Approving executions, exporting audit logs, and privileged actions",
    ),
];

/// Tool groups for the Policies form checkboxes.
const TOOL_GROUPS: &[(&str, &[&str])] = &[
    (
        "Discovery",
        &[
            "list_connections",
            "get_connection",
            "get_connection_metadata",
        ],
    ),
    (
        "Schema",
        &[
            "list_databases",
            "list_schemas",
            "list_tables",
            "list_collections",
            "describe_object",
        ],
    ),
    (
        "Query",
        &["read_query", "explain_query", "preview_mutation"],
    ),
    (
        "Scripts",
        &[
            "list_scripts",
            "get_script",
            "create_script",
            "update_script",
            "delete_script",
            "run_script",
        ],
    ),
    (
        "Approval",
        &[
            "request_execution",
            "list_pending_executions",
            "get_pending_execution",
            "approve_execution",
            "reject_execution",
        ],
    ),
    (
        "Audit",
        &["query_audit_logs", "get_audit_entry", "export_audit_logs"],
    ),
];

fn tool_label(id: &str) -> &str {
    TOOL_META
        .iter()
        .find(|(t, _, _)| *t == id)
        .map(|(_, l, _)| *l)
        .unwrap_or(id)
}

fn tool_description(id: &str) -> &'static str {
    TOOL_META
        .iter()
        .find(|(t, _, _)| *t == id)
        .map(|(_, _, d)| *d)
        .unwrap_or("")
}

use dbflux_mcp::builtin_display_name;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum McpSectionVariant {
    Clients,
    Roles,
    Policies,
}

pub(super) struct McpSection {
    app_state: Entity<AppStateEntity>,
    variant: McpSectionVariant,

    // Client tab
    input_client_id: Entity<InputState>,
    input_client_name: Entity<InputState>,
    input_client_issuer: Entity<InputState>,
    selected_client_id: Option<String>,
    draft_active: bool,

    // Role tab
    input_role_id: Entity<InputState>,
    role_policies_multiselect: Entity<MultiSelect>,
    selected_role_id: Option<String>,

    // Policy tab
    input_policy_id: Entity<InputState>,
    draft_policy_classes: HashSet<String>,
    draft_policy_tools: HashSet<String>,
    selected_policy_id: Option<String>,

    // Common
    content_focused: bool,
    switching_input: bool,
    pending_sync_from_state: bool,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<SectionFocusEvent> for McpSection {}

impl McpSection {
    pub(super) fn new(
        app_state: Entity<AppStateEntity>,
        variant: McpSectionVariant,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let input_client_id = cx.new(|cx| InputState::new(window, cx).placeholder("client-id"));
        let input_client_name =
            cx.new(|cx| InputState::new(window, cx).placeholder("Agent / integration name"));
        let input_client_issuer =
            cx.new(|cx| InputState::new(window, cx).placeholder("Issuer (optional)"));
        let input_role_id = cx.new(|cx| InputState::new(window, cx).placeholder("role-id"));
        let initial_policy_items = {
            let policies = app_state.read(cx).list_mcp_policies().unwrap_or_default();
            Self::build_policy_multiselect_items(&policies)
        };
        let role_policies_multiselect = cx.new(|cx| {
            let mut ms = MultiSelect::new("mcp-role-policies").placeholder("No policies selected");
            ms.set_items(initial_policy_items, cx);
            ms
        });
        let input_policy_id = cx.new(|cx| InputState::new(window, cx).placeholder("policy-id"));

        let state_sub = cx.subscribe(&app_state, |this, _, _: &AppStateChanged, cx| {
            this.pending_sync_from_state = true;
            cx.notify();
        });

        fn make_blur_sub(cx: &mut Context<McpSection>, input: &Entity<InputState>) -> Subscription {
            cx.subscribe(input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Blur) {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            })
        }

        let subs = vec![
            state_sub,
            make_blur_sub(cx, &input_client_id),
            make_blur_sub(cx, &input_client_name),
            make_blur_sub(cx, &input_client_issuer),
            make_blur_sub(cx, &input_role_id),
            make_blur_sub(cx, &input_policy_id),
        ];

        Self {
            app_state,
            variant,

            input_client_id,
            input_client_name,
            input_client_issuer,
            selected_client_id: None,
            draft_active: true,

            input_role_id,
            role_policies_multiselect,
            selected_role_id: None,

            input_policy_id,
            draft_policy_classes: HashSet::new(),
            draft_policy_tools: HashSet::new(),
            selected_policy_id: None,

            content_focused: false,
            switching_input: false,
            pending_sync_from_state: true,
            _subscriptions: subs,
        }
    }

    // ─── Client helpers ──────────────────────────────────────────────────────

    fn trusted_clients(&self, cx: &App) -> Vec<TrustedClientDto> {
        self.app_state
            .read(cx)
            .list_mcp_trusted_clients()
            .unwrap_or_default()
    }

    fn select_client(&mut self, client_id: &str, window: &mut Window, cx: &mut Context<Self>) {
        let Some(client) = self
            .trusted_clients(cx)
            .into_iter()
            .find(|item| item.id == client_id)
        else {
            return;
        };

        self.selected_client_id = Some(client.id.clone());
        self.draft_active = client.active;
        self.input_client_id
            .update(cx, |i, cx| i.set_value(client.id, window, cx));
        self.input_client_name
            .update(cx, |i, cx| i.set_value(client.name, window, cx));
        self.input_client_issuer.update(cx, |i, cx| {
            i.set_value(client.issuer.unwrap_or_default(), window, cx)
        });
    }

    fn clear_client_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.selected_client_id = None;
        self.draft_active = true;
        self.input_client_id
            .update(cx, |i, cx| i.set_value("", window, cx));
        self.input_client_name
            .update(cx, |i, cx| i.set_value("", window, cx));
        self.input_client_issuer
            .update(cx, |i, cx| i.set_value("", window, cx));
    }

    fn draft_client(&self, cx: &App) -> TrustedClientDto {
        let id = self.input_client_id.read(cx).value().trim().to_string();
        let name = self.input_client_name.read(cx).value().trim().to_string();
        let issuer = self.input_client_issuer.read(cx).value().trim().to_string();

        TrustedClientDto {
            id,
            name,
            issuer: (!issuer.is_empty()).then_some(issuer),
            active: self.draft_active,
        }
    }

    fn selected_client(&self, cx: &App) -> Option<TrustedClientDto> {
        let id = self.selected_client_id.as_ref()?;
        self.trusted_clients(cx).into_iter().find(|c| &c.id == id)
    }

    fn client_has_unsaved_changes(&self, cx: &App) -> bool {
        let draft = self.draft_client(cx);
        if draft.id.is_empty() && draft.name.is_empty() && draft.issuer.is_none() && draft.active {
            return false;
        }
        match self.selected_client(cx) {
            Some(existing) => existing != draft,
            None => true,
        }
    }

    fn save_client(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::components::toast::ToastExt;

        let draft = self.draft_client(cx);
        if draft.id.is_empty() || draft.name.is_empty() {
            cx.toast_error("Client ID and name are required", window);
            return;
        }

        self.app_state.update(cx, |state, cx| {
            if let Err(e) = state.upsert_mcp_trusted_client(draft.clone()) {
                log::warn!("failed to upsert trusted client '{}': {}", draft.id, e);
                return;
            }
            for event in state.drain_mcp_runtime_events() {
                cx.emit(McpRuntimeEventRaised { event });
            }
            cx.emit(AppStateChanged);
        });

        self.selected_client_id = Some(draft.id);
        cx.toast_info("Trusted client saved", window);
    }

    fn delete_selected_client(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::components::toast::ToastExt;

        let Some(client_id) = self.selected_client_id.clone() else {
            cx.toast_warning("Select a trusted client first", window);
            return;
        };

        self.app_state.update(cx, |state, cx| {
            if let Err(e) = state.delete_mcp_trusted_client(&client_id) {
                log::warn!("failed to delete trusted client '{}': {}", client_id, e);
                return;
            }
            for event in state.drain_mcp_runtime_events() {
                cx.emit(McpRuntimeEventRaised { event });
            }
            cx.emit(AppStateChanged);
        });

        self.clear_client_form(window, cx);
        cx.toast_info("Trusted client deleted", window);
    }

    fn toggle_selected_client_active(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::components::toast::ToastExt;

        let Some(mut selected) = self.selected_client(cx) else {
            cx.toast_warning("Select a trusted client first", window);
            return;
        };

        selected.active = !selected.active;
        self.draft_active = selected.active;

        self.app_state.update(cx, |state, cx| {
            if let Err(e) = state.upsert_mcp_trusted_client(selected.clone()) {
                log::warn!("failed to toggle trusted client: {}", e);
                return;
            }
            for event in state.drain_mcp_runtime_events() {
                cx.emit(McpRuntimeEventRaised { event });
            }
            cx.emit(AppStateChanged);
        });

        let msg = if selected.active {
            "Trusted client activated"
        } else {
            "Trusted client deactivated"
        };
        cx.toast_info(msg, window);
    }

    // ─── Role helpers ─────────────────────────────────────────────────────────

    fn roles(&self, cx: &App) -> Vec<PolicyRoleDto> {
        self.app_state.read(cx).list_mcp_roles().unwrap_or_default()
    }

    fn select_role(&mut self, role_id: &str, window: &mut Window, cx: &mut Context<Self>) {
        let Some(role) = self.roles(cx).into_iter().find(|r| r.id == role_id) else {
            return;
        };

        self.selected_role_id = Some(role.id.clone());
        self.input_role_id
            .update(cx, |i, cx| i.set_value(role.id.clone(), window, cx));

        let policy_items = Self::build_policy_multiselect_items(&self.policies(cx));
        self.role_policies_multiselect.update(cx, |ms, cx| {
            ms.set_items(policy_items, cx);
            ms.set_selected_values(&role.policy_ids, cx);
        });
    }

    fn clear_role_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.selected_role_id = None;
        self.input_role_id
            .update(cx, |i, cx| i.set_value("", window, cx));

        let policy_items = Self::build_policy_multiselect_items(&self.policies(cx));
        self.role_policies_multiselect.update(cx, |ms, cx| {
            ms.set_items(policy_items, cx);
            ms.clear_selection(cx);
        });
    }

    fn collect_role_policy_ids(&self, cx: &App) -> Vec<String> {
        self.role_policies_multiselect
            .read(cx)
            .selected_values()
            .iter()
            .map(|v| v.to_string())
            .collect()
    }

    fn save_role(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::components::toast::ToastExt;

        let id = self.input_role_id.read(cx).value().trim().to_string();
        if id.is_empty() {
            cx.toast_error("Role ID is required", window);
            return;
        }
        if dbflux_mcp::is_builtin(&id) {
            cx.toast_error("Built-in roles cannot be modified", window);
            return;
        }

        let policy_ids = self.collect_role_policy_ids(cx);
        let dto = PolicyRoleDto {
            id: id.clone(),
            policy_ids,
        };

        self.app_state.update(cx, |state, cx| {
            if let Err(e) = state.upsert_mcp_role(dto) {
                log::warn!("failed to upsert role '{}': {}", id, e);
                return;
            }
            for event in state.drain_mcp_runtime_events() {
                cx.emit(McpRuntimeEventRaised { event });
            }
            cx.emit(AppStateChanged);
        });

        self.selected_role_id = Some(id);
        cx.toast_info("Role saved", window);
    }

    fn delete_selected_role(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::components::toast::ToastExt;

        let Some(role_id) = self.selected_role_id.clone() else {
            cx.toast_warning("Select a role first", window);
            return;
        };

        self.app_state.update(cx, |state, cx| {
            if let Err(e) = state.delete_mcp_role(&role_id) {
                log::warn!("failed to delete role '{}': {}", role_id, e);
                return;
            }
            for event in state.drain_mcp_runtime_events() {
                cx.emit(McpRuntimeEventRaised { event });
            }
            cx.emit(AppStateChanged);
        });

        self.clear_role_form(window, cx);
        cx.toast_info("Role deleted", window);
    }

    fn build_policy_multiselect_items(policies: &[ToolPolicyDto]) -> Vec<DropdownItem> {
        policies
            .iter()
            .map(|p| {
                let label = builtin_display_name(&p.id)
                    .unwrap_or(p.id.as_str())
                    .to_string();
                DropdownItem::with_value(label, p.id.clone())
            })
            .collect()
    }

    // ─── Policy helpers ───────────────────────────────────────────────────────

    fn policies(&self, cx: &App) -> Vec<ToolPolicyDto> {
        self.app_state
            .read(cx)
            .list_mcp_policies()
            .unwrap_or_default()
    }

    fn select_policy(&mut self, policy_id: &str, window: &mut Window, cx: &mut Context<Self>) {
        let policies: Vec<ToolPolicyDto> = self
            .app_state
            .read(cx)
            .list_mcp_policies()
            .unwrap_or_default();

        let Some(policy) = policies.into_iter().find(|p| p.id == policy_id) else {
            return;
        };

        self.selected_policy_id = Some(policy.id.clone());
        self.input_policy_id
            .update(cx, |i, cx| i.set_value(policy.id.clone(), window, cx));
        self.draft_policy_classes = policy.allowed_classes.into_iter().collect();
        self.draft_policy_tools = policy.allowed_tools.into_iter().collect();
    }

    fn clear_policy_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.selected_policy_id = None;
        self.input_policy_id
            .update(cx, |i, cx| i.set_value("", window, cx));
        self.draft_policy_classes.clear();
        self.draft_policy_tools.clear();
    }

    fn save_policy(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::components::toast::ToastExt;

        let id = self.input_policy_id.read(cx).value().trim().to_string();
        if id.is_empty() {
            cx.toast_error("Policy ID is required", window);
            return;
        }
        if dbflux_mcp::is_builtin(&id) {
            cx.toast_error("Built-in policies cannot be modified", window);
            return;
        }

        let mut tools: Vec<String> = self.draft_policy_tools.iter().cloned().collect();
        tools.sort();
        let mut classes: Vec<String> = self.draft_policy_classes.iter().cloned().collect();
        classes.sort();

        let dto = ToolPolicyDto {
            id: id.clone(),
            allowed_tools: tools,
            allowed_classes: classes,
        };

        self.app_state.update(cx, |state, cx| {
            if let Err(e) = state.upsert_mcp_policy(dto) {
                log::warn!("failed to upsert policy '{}': {}", id, e);
                return;
            }
            for event in state.drain_mcp_runtime_events() {
                cx.emit(McpRuntimeEventRaised { event });
            }
            cx.emit(AppStateChanged);
        });

        self.selected_policy_id = Some(id);
        cx.toast_info("Policy saved", window);
    }

    fn delete_selected_policy(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::components::toast::ToastExt;

        let Some(policy_id) = self.selected_policy_id.clone() else {
            cx.toast_warning("Select a policy first", window);
            return;
        };

        self.app_state.update(cx, |state, cx| {
            if let Err(e) = state.delete_mcp_policy(&policy_id) {
                log::warn!("failed to delete policy '{}': {}", policy_id, e);
                return;
            }
            for event in state.drain_mcp_runtime_events() {
                cx.emit(McpRuntimeEventRaised { event });
            }
            cx.emit(AppStateChanged);
        });

        self.clear_policy_form(window, cx);
        cx.toast_info("Policy deleted", window);
    }

    // ─── Render helpers ───────────────────────────────────────────────────────

    fn render_clients_content(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let clients = self.trusted_clients(cx);
        let selected = self.selected_client_id.clone();

        let list = div()
            .w(px(300.0))
            .h_full()
            .border_r_1()
            .border_color(theme.border)
            .p_3()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                Button::new("mcp-client-new", "New Trusted Client")
                    .small()
                    .ghost()
                    .on_click(
                        cx.listener(|this, _, window, cx| this.clear_client_form(window, cx)),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .when(clients.is_empty(), |r| {
                        r.child(Text::muted("No trusted clients configured."))
                    })
                    .children(clients.iter().map(|client| {
                        let id = client.id.clone();
                        let is_selected = selected.as_deref() == Some(client.id.as_str());

                        div()
                            .id(SharedString::from(format!("client-{}", client.id)))
                            .p_2()
                            .rounded(px(4.0))
                            .border_1()
                            .border_color(if is_selected {
                                theme.primary
                            } else {
                                transparent_black()
                            })
                            .bg(if is_selected {
                                theme.secondary
                            } else {
                                transparent_black()
                            })
                            .cursor_pointer()
                            .hover({
                                let s = theme.secondary;
                                move |d| d.bg(s)
                            })
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.select_client(&id, window, cx);
                            }))
                            .child(
                                div()
                                    .flex()
                                    .justify_between()
                                    .items_center()
                                    .child(Label::new(client.name.clone()))
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(if client.active {
                                                theme.success
                                            } else {
                                                theme.muted_foreground
                                            })
                                            .child(if client.active {
                                                "active"
                                            } else {
                                                "inactive"
                                            }),
                                    ),
                            )
                            .child(Text::caption(client.id.clone()))
                    })),
            );

        let save_label = if self.selected_client(cx).is_some() {
            "Update Client"
        } else {
            "Create Client"
        };
        let active_label = if self.draft_active {
            "Deactivate"
        } else {
            "Activate"
        };

        let form = div()
            .flex_1()
            .h_full()
            .flex()
            .flex_col()
            .child(layout::section_header(
                "Trusted Clients",
                "AI agent identities allowed to connect via MCP",
                &theme,
            ))
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_3()
                    .child(Label::new("Client ID"))
                    .child(Input::new(&self.input_client_id).small())
                    .child(Label::new("Name"))
                    .child(Input::new(&self.input_client_name).small())
                    .child(Label::new("Issuer (optional)"))
                    .child(Input::new(&self.input_client_issuer).small())
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(
                                Checkbox::new("mcp-client-active")
                                    .checked(self.draft_active)
                                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                        this.draft_active = *checked;
                                        cx.notify();
                                    })),
                            )
                            .child(Text::body("Active")),
                    ),
            )
            .child(
                div()
                    .p_4()
                    .border_t_1()
                    .border_color(theme.border)
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap_2()
                    .child(Text::caption(if self.client_has_unsaved_changes(cx) {
                        "Unsaved form changes"
                    } else {
                        "All changes applied"
                    }))
                    .child(
                        div()
                            .flex()
                            .gap_2()
                            .child(
                                Button::new("mcp-client-toggle-active", active_label)
                                    .small()
                                    .ghost()
                                    .disabled(self.selected_client(cx).is_none())
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.toggle_selected_client_active(window, cx);
                                    })),
                            )
                            .child(
                                Button::new("mcp-client-delete", "Delete")
                                    .small()
                                    .danger()
                                    .disabled(self.selected_client(cx).is_none())
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.delete_selected_client(window, cx);
                                    })),
                            )
                            .child(
                                Button::new("mcp-client-save", save_label)
                                    .small()
                                    .primary()
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.save_client(window, cx);
                                    })),
                            ),
                    ),
            );

        div()
            .size_full()
            .flex()
            .overflow_hidden()
            .child(list)
            .child(form)
    }

    fn render_roles_content(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let roles = self.roles(cx);
        let selected = self.selected_role_id.clone();

        let list = div()
            .w(px(300.0))
            .h_full()
            .border_r_1()
            .border_color(theme.border)
            .p_3()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                Button::new("mcp-role-new", "New Role")
                    .small()
                    .ghost()
                    .on_click(cx.listener(|this, _, window, cx| this.clear_role_form(window, cx))),
            )
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .when(roles.is_empty(), |r| {
                        r.child(Text::muted("No roles configured."))
                    })
                    .children(roles.iter().map(|role| {
                        let id = role.id.clone();
                        let is_selected = selected.as_deref() == Some(role.id.as_str());

                        div()
                            .id(SharedString::from(format!("role-{}", role.id)))
                            .p_2()
                            .rounded(px(4.0))
                            .border_1()
                            .border_color(if is_selected {
                                theme.primary
                            } else {
                                transparent_black()
                            })
                            .bg(if is_selected {
                                theme.secondary
                            } else {
                                transparent_black()
                            })
                            .cursor_pointer()
                            .hover({
                                let s = theme.secondary;
                                move |d| d.bg(s)
                            })
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.select_role(&id, window, cx);
                            }))
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .justify_between()
                                    .gap_2()
                                    .child(Label::new(
                                        builtin_display_name(&role.id)
                                            .unwrap_or(role.id.as_str())
                                            .to_string(),
                                    ))
                                    .when(dbflux_mcp::is_builtin(&role.id), |d| {
                                        d.child(
                                            div()
                                                .px_1p5()
                                                .py_0p5()
                                                .rounded_sm()
                                                .text_xs()
                                                .bg(theme.accent.opacity(0.2))
                                                .text_color(theme.accent_foreground)
                                                .child("built-in"),
                                        )
                                    }),
                            )
                            .child(Text::caption(format!(
                                "{} {}",
                                role.policy_ids.len(),
                                if role.policy_ids.len() == 1 {
                                    "policy"
                                } else {
                                    "policies"
                                }
                            )))
                    })),
            );

        let role_is_builtin = self
            .selected_role_id
            .as_deref()
            .map(dbflux_mcp::is_builtin)
            .unwrap_or(false);

        let save_label = if self.selected_role_id.is_some() {
            "Update Role"
        } else {
            "Create Role"
        };

        let form = div()
            .flex_1()
            .h_full()
            .flex()
            .flex_col()
            .child(layout::section_header(
                "Roles",
                "Group policies into named roles assigned to actors per connection",
                &theme,
            ))
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_3()
                    .child(Label::new("Role ID"))
                    .child(Input::new(&self.input_role_id).small())
                    .child(Label::new("Policies"))
                    .child(Text::caption("Select policies defined in the Policies tab"))
                    .child(self.role_policies_multiselect.clone()),
            )
            .child(
                div()
                    .p_4()
                    .border_t_1()
                    .border_color(theme.border)
                    .flex()
                    .items_center()
                    .justify_end()
                    .gap_2()
                    .when(role_is_builtin, |d| {
                        d.child(Text::caption("Built-in roles cannot be modified"))
                    })
                    .child(
                        Button::new("mcp-role-delete", "Delete")
                            .small()
                            .danger()
                            .disabled(self.selected_role_id.is_none() || role_is_builtin)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.delete_selected_role(window, cx);
                            })),
                    )
                    .child(
                        Button::new("mcp-role-save", save_label)
                            .small()
                            .primary()
                            .disabled(role_is_builtin)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.save_role(window, cx);
                            })),
                    ),
            );

        div()
            .size_full()
            .flex()
            .overflow_hidden()
            .child(list)
            .child(form)
    }

    fn render_policies_content(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let policies: Vec<ToolPolicyDto> = self
            .app_state
            .read(cx)
            .list_mcp_policies()
            .unwrap_or_default();
        let selected = self.selected_policy_id.clone();

        let list = div()
            .w(px(300.0))
            .h_full()
            .border_r_1()
            .border_color(theme.border)
            .p_3()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                Button::new("mcp-policy-new", "New Policy")
                    .small()
                    .ghost()
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.clear_policy_form(window, cx);
                    })),
            )
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .when(policies.is_empty(), |r| {
                        r.child(Text::muted("No policies configured."))
                    })
                    .children(policies.iter().map(|policy| {
                        let id = policy.id.clone();
                        let is_selected = selected.as_deref() == Some(policy.id.as_str());

                        div()
                            .id(SharedString::from(format!("policy-{}", policy.id)))
                            .p_2()
                            .rounded(px(4.0))
                            .border_1()
                            .border_color(if is_selected {
                                theme.primary
                            } else {
                                transparent_black()
                            })
                            .bg(if is_selected {
                                theme.secondary
                            } else {
                                transparent_black()
                            })
                            .cursor_pointer()
                            .hover({
                                let s = theme.secondary;
                                move |d| d.bg(s)
                            })
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.select_policy(&id, window, cx);
                            }))
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .justify_between()
                                    .gap_2()
                                    .child(Label::new(
                                        builtin_display_name(&policy.id)
                                            .unwrap_or(policy.id.as_str())
                                            .to_string(),
                                    ))
                                    .when(dbflux_mcp::is_builtin(&policy.id), |d| {
                                        d.child(
                                            div()
                                                .px_1p5()
                                                .py_0p5()
                                                .rounded_sm()
                                                .text_xs()
                                                .bg(theme.accent.opacity(0.2))
                                                .text_color(theme.accent_foreground)
                                                .child("built-in"),
                                        )
                                    }),
                            )
                            .child(Text::caption(format!(
                                "{} tools · {} classes",
                                policy.allowed_tools.len(),
                                policy.allowed_classes.len()
                            )))
                    })),
            );

        let policy_is_builtin = self
            .selected_policy_id
            .as_deref()
            .map(dbflux_mcp::is_builtin)
            .unwrap_or(false);

        let save_label = if self.selected_policy_id.is_some() {
            "Update Policy"
        } else {
            "Create Policy"
        };

        let form = div()
            .flex_1()
            .h_full()
            .flex()
            .flex_col()
            .child(layout::section_header(
                "Policies",
                "Define which tools and execution classes are allowed",
                &theme,
            ))
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_3()
                    .child(Label::new("Policy ID"))
                    .child(Input::new(&self.input_policy_id).small())
                    .child(Label::new("Allowed Execution Classes"))
                    .child(
                        div()
                            .flex()
                            .flex_wrap()
                            .gap_3()
                            .children(CLASS_META.iter().map(|&(class, label, description)| {
                                let checked = self.draft_policy_classes.contains(class);
                                div()
                                    .flex()
                                    .items_start()
                                    .gap_2()
                                    .child(
                                        div().pt(px(2.0)).child(
                                            Checkbox::new(SharedString::from(format!(
                                                "policy-class-{}",
                                                class
                                            )))
                                            .checked(checked)
                                            .on_click(cx.listener(
                                                move |this, checked: &bool, _, cx| {
                                                    if *checked {
                                                        this.draft_policy_classes
                                                            .insert(class.to_string());
                                                    } else {
                                                        this.draft_policy_classes.remove(class);
                                                    }
                                                    cx.notify();
                                                },
                                            )),
                                        ),
                                    )
                                    .child(
                                        div()
                                            .flex()
                                            .flex_col()
                                            .gap_0p5()
                                            .child(Label::new(label))
                                            .child(Text::caption(description)),
                                    )
                            })),
                    )
                    .child(Label::new("Allowed Tools"))
                    .children(TOOL_GROUPS.iter().map(|(group_name, tools)| {
                        div()
                            .flex()
                            .flex_col()
                            .gap_2()
                            .child(Text::caption(*group_name).font_weight(FontWeight::MEDIUM))
                            .child(div().flex().flex_col().gap_2().pl_2().children(
                                tools.iter().map(|&tool| {
                                    let checked = self.draft_policy_tools.contains(tool);
                                    let label = tool_label(tool);
                                    let description = tool_description(tool);
                                    div()
                                        .flex()
                                        .items_start()
                                        .gap_2()
                                        .child(
                                            div().pt(px(2.0)).child(
                                                Checkbox::new(SharedString::from(format!(
                                                    "policy-tool-{}",
                                                    tool
                                                )))
                                                .checked(checked)
                                                .on_click(cx.listener(
                                                    move |this, checked: &bool, _, cx| {
                                                        if *checked {
                                                            this.draft_policy_tools
                                                                .insert(tool.to_string());
                                                        } else {
                                                            this.draft_policy_tools.remove(tool);
                                                        }
                                                        cx.notify();
                                                    },
                                                )),
                                            ),
                                        )
                                        .child(
                                            div()
                                                .flex()
                                                .flex_col()
                                                .gap_0p5()
                                                .child(Label::new(label))
                                                .child(Text::caption(description)),
                                        )
                                }),
                            ))
                    })),
            )
            .child(
                div()
                    .p_4()
                    .border_t_1()
                    .border_color(theme.border)
                    .flex()
                    .items_center()
                    .justify_end()
                    .gap_2()
                    .when(policy_is_builtin, |d| {
                        d.child(Text::caption("Built-in policies cannot be modified"))
                    })
                    .child(
                        Button::new("mcp-policy-delete", "Delete")
                            .small()
                            .danger()
                            .disabled(self.selected_policy_id.is_none() || policy_is_builtin)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.delete_selected_policy(window, cx);
                            })),
                    )
                    .child(
                        Button::new("mcp-policy-save", save_label)
                            .small()
                            .primary()
                            .disabled(policy_is_builtin)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.save_policy(window, cx);
                            })),
                    ),
            );

        div()
            .size_full()
            .flex()
            .overflow_hidden()
            .child(list)
            .child(form)
    }

    // ─── Keyboard navigation ──────────────────────────────────────────────────

    pub(super) fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.content_focused {
            return;
        }

        let chord = key_chord_from_gpui(&event.keystroke);

        match self.variant {
            McpSectionVariant::Clients => self.handle_clients_nav(chord, window, cx),
            McpSectionVariant::Roles => self.handle_roles_nav(chord, window, cx),
            McpSectionVariant::Policies => self.handle_policies_nav(chord, window, cx),
        }
    }

    fn handle_clients_nav(&mut self, chord: KeyChord, window: &mut Window, cx: &mut Context<Self>) {
        let clients = self.trusted_clients(cx);

        match (chord.key.as_str(), chord.modifiers) {
            ("j", m) | ("down", m) if m == Modifiers::none() => {
                let next_id = match &self.selected_client_id {
                    None => clients.first().map(|c| c.id.clone()),
                    Some(current) => {
                        let idx = clients.iter().position(|c| &c.id == current);
                        idx.and_then(|i| clients.get(i + 1))
                            .or_else(|| clients.first())
                            .map(|c| c.id.clone())
                    }
                };

                if let Some(id) = next_id {
                    self.select_client(&id, window, cx);
                }

                cx.notify();
            }

            ("k", m) | ("up", m) if m == Modifiers::none() => {
                let prev_id = match &self.selected_client_id {
                    None => clients.last().map(|c| c.id.clone()),
                    Some(current) => {
                        let idx = clients.iter().position(|c| &c.id == current);
                        idx.and_then(|i| i.checked_sub(1).and_then(|i| clients.get(i)))
                            .or_else(|| clients.last())
                            .map(|c| c.id.clone())
                    }
                };

                if let Some(id) = prev_id {
                    self.select_client(&id, window, cx);
                }

                cx.notify();
            }

            ("escape", m) if m == Modifiers::none() => {
                if self.selected_client_id.is_some() {
                    self.clear_client_form(window, cx);
                } else {
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            }

            _ => {}
        }
    }

    fn handle_roles_nav(&mut self, chord: KeyChord, window: &mut Window, cx: &mut Context<Self>) {
        let roles = self.roles(cx);

        match (chord.key.as_str(), chord.modifiers) {
            ("j", m) | ("down", m) if m == Modifiers::none() => {
                let next_id = match &self.selected_role_id {
                    None => roles.first().map(|r| r.id.clone()),
                    Some(current) => {
                        let idx = roles.iter().position(|r| &r.id == current);
                        idx.and_then(|i| roles.get(i + 1))
                            .or_else(|| roles.first())
                            .map(|r| r.id.clone())
                    }
                };

                if let Some(id) = next_id {
                    self.select_role(&id, window, cx);
                }

                cx.notify();
            }

            ("k", m) | ("up", m) if m == Modifiers::none() => {
                let prev_id = match &self.selected_role_id {
                    None => roles.last().map(|r| r.id.clone()),
                    Some(current) => {
                        let idx = roles.iter().position(|r| &r.id == current);
                        idx.and_then(|i| i.checked_sub(1).and_then(|i| roles.get(i)))
                            .or_else(|| roles.last())
                            .map(|r| r.id.clone())
                    }
                };

                if let Some(id) = prev_id {
                    self.select_role(&id, window, cx);
                }

                cx.notify();
            }

            ("escape", m) if m == Modifiers::none() => {
                if self.selected_role_id.is_some() {
                    self.clear_role_form(window, cx);
                } else {
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            }

            _ => {}
        }
    }

    fn handle_policies_nav(
        &mut self,
        chord: KeyChord,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let policies = self.policies(cx);

        match (chord.key.as_str(), chord.modifiers) {
            ("j", m) | ("down", m) if m == Modifiers::none() => {
                let next_id = match &self.selected_policy_id {
                    None => policies.first().map(|p| p.id.clone()),
                    Some(current) => {
                        let idx = policies.iter().position(|p| &p.id == current);
                        idx.and_then(|i| policies.get(i + 1))
                            .or_else(|| policies.first())
                            .map(|p| p.id.clone())
                    }
                };

                if let Some(id) = next_id {
                    self.select_policy(&id, window, cx);
                }

                cx.notify();
            }

            ("k", m) | ("up", m) if m == Modifiers::none() => {
                let prev_id = match &self.selected_policy_id {
                    None => policies.last().map(|p| p.id.clone()),
                    Some(current) => {
                        let idx = policies.iter().position(|p| &p.id == current);
                        idx.and_then(|i| i.checked_sub(1).and_then(|i| policies.get(i)))
                            .or_else(|| policies.last())
                            .map(|p| p.id.clone())
                    }
                };

                if let Some(id) = prev_id {
                    self.select_policy(&id, window, cx);
                }

                cx.notify();
            }

            ("escape", m) if m == Modifiers::none() => {
                if self.selected_policy_id.is_some() {
                    self.clear_policy_form(window, cx);
                } else {
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            }

            _ => {}
        }
    }
}

impl SettingsSection for McpSection {
    fn section_id(&self) -> SettingsSectionId {
        match self.variant {
            McpSectionVariant::Clients => SettingsSectionId::McpClients,
            McpSectionVariant::Roles => SettingsSectionId::McpRoles,
            McpSectionVariant::Policies => SettingsSectionId::McpPolicies,
        }
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        McpSection::handle_key_event(self, event, window, cx);
    }

    fn focus_in(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = true;
        cx.notify();
    }

    fn focus_out(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = false;
        cx.notify();
    }

    fn is_dirty(&self, cx: &App) -> bool {
        match self.variant {
            McpSectionVariant::Clients => self.client_has_unsaved_changes(cx),
            _ => false,
        }
    }
}

impl Render for McpSection {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.pending_sync_from_state {
            self.pending_sync_from_state = false;

            if let Some(id) = self.selected_client_id.clone() {
                self.select_client(&id, window, cx);
            }

            // Refresh items first so select_role sees a current list.
            let policy_items = Self::build_policy_multiselect_items(&self.policies(cx));
            self.role_policies_multiselect
                .update(cx, |ms, cx| ms.set_items(policy_items, cx));

            if let Some(id) = self.selected_role_id.clone() {
                self.select_role(&id, window, cx);
            }

            if let Some(id) = self.selected_policy_id.clone() {
                self.select_policy(&id, window, cx);
            }
        }

        let content: AnyElement = match self.variant {
            McpSectionVariant::Clients => self.render_clients_content(cx).into_any_element(),
            McpSectionVariant::Roles => self.render_roles_content(cx).into_any_element(),
            McpSectionVariant::Policies => self.render_policies_content(cx).into_any_element(),
        };

        div().h_full().overflow_hidden().child(content)
    }
}
