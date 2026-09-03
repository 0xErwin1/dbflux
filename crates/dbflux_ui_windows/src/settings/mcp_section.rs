use super::form_section::{FormSection, create_blur_subscription};
use super::section_trait::SectionFocusEvent;
use super::{SettingsSection, SettingsSectionId, layout};
use crate::labels::{mcp_policy_tools_classes_summary, mcp_role_policy_count};
use dbflux_app::keymap::Modifiers;
use dbflux_components::components::multi_select::MultiSelect;
use dbflux_components::composites::{
    BadgeTone, MasterDetailAction, MasterDetailActionKind, MasterDetailItem,
    MasterDetailListConfig, render_master_detail_list,
};
use dbflux_components::controls::DropdownItem;
use dbflux_components::controls::InputState;
use dbflux_components::controls::{Button, Checkbox, Input};
use dbflux_components::primitives::{Label, focus_frame};
use dbflux_components::tokens::Widths;
use dbflux_components::typography::{Body, FieldLabel, SubSectionLabel};
use dbflux_mcp::{PolicyRoleDto, ToolPolicyDto, TrustedClientDto};
use dbflux_ui_base::keymap::key_chord_from_gpui;
use dbflux_ui_base::toast::{Toast, copy_action, now_hms};
use dbflux_ui_base::{AppStateChanged, AppStateEntity, McpRuntimeEventRaised};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;
use std::collections::HashSet;

/// Tool ids in their stable display order. Each id doubles as the catalog key
/// segment for its translated name and description: `settings.mcp.tool.<id>.name`
/// and `settings.mcp.tool.<id>.description`.
const TOOL_IDS: &[&str] = &[
    "list_connections",
    "get_connection",
    "get_connection_metadata",
    "list_databases",
    "list_schemas",
    "list_tables",
    "list_collections",
    "describe_object",
    "read_query",
    "explain_query",
    "preview_mutation",
    "list_scripts",
    "get_script",
    "create_script",
    "update_script",
    "delete_script",
    "run_script",
    "request_execution",
    "list_pending_executions",
    "get_pending_execution",
    "approve_execution",
    "reject_execution",
    "query_audit_logs",
    "get_audit_entry",
    "export_audit_logs",
];

/// Resolves the tool display metadata for the active locale: (id, translated
/// name, translated description). Call once per render and reuse the result
/// across the tool checkbox rows instead of re-resolving per row.
fn tool_meta() -> Vec<(&'static str, String, String)> {
    TOOL_IDS
        .iter()
        .map(|&id| {
            let name = dbflux_i18n::t!(&format!("settings.mcp.tool.{id}.name"));
            let description = dbflux_i18n::t!(&format!("settings.mcp.tool.{id}.description"));
            (id, name, description)
        })
        .collect()
}

/// Execution class ids in their stable display order. Each id doubles as the
/// catalog key segment for its translated label and description:
/// `settings.mcp.class.<id>.label` and `settings.mcp.class.<id>.description`.
const CLASS_IDS: &[&str] = &["metadata", "read", "write", "destructive", "admin"];

/// Resolves the execution class display metadata for the active locale:
/// (id, translated label, translated description). Call once per render and
/// reuse the result across the class checkbox rows.
fn class_meta() -> Vec<(&'static str, String, String)> {
    CLASS_IDS
        .iter()
        .map(|&id| {
            let label = dbflux_i18n::t!(&format!("settings.mcp.class.{id}.label"));
            let description = dbflux_i18n::t!(&format!("settings.mcp.class.{id}.description"));
            (id, label, description)
        })
        .collect()
}

/// Tool groups for the Policies form checkboxes. Each group id doubles as the
/// catalog key segment for its translated name: `settings.mcp.group.<id>`.
const TOOL_GROUPS: &[(&str, &[&str])] = &[
    (
        "discovery",
        &[
            "list_connections",
            "get_connection",
            "get_connection_metadata",
        ],
    ),
    (
        "schema",
        &[
            "list_databases",
            "list_schemas",
            "list_tables",
            "list_collections",
            "describe_object",
        ],
    ),
    (
        "query",
        &["read_query", "explain_query", "preview_mutation"],
    ),
    (
        "scripts",
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
        "approval",
        &[
            "request_execution",
            "list_pending_executions",
            "get_pending_execution",
            "approve_execution",
            "reject_execution",
        ],
    ),
    (
        "audit",
        &["query_audit_logs", "get_audit_entry", "export_audit_logs"],
    ),
];

/// Resolves the translated display name for a tool group id.
fn tool_group_label(group_id: &str) -> String {
    dbflux_i18n::t!(&format!("settings.mcp.group.{group_id}"))
}

fn tool_label(meta: &[(&'static str, String, String)], id: &str) -> String {
    meta.iter()
        .find(|(t, _, _)| *t == id)
        .map(|(_, name, _)| name.clone())
        .unwrap_or_else(|| id.to_string())
}

fn tool_description(meta: &[(&'static str, String, String)], id: &str) -> String {
    meta.iter()
        .find(|(t, _, _)| *t == id)
        .map(|(_, _, description)| description.clone())
        .unwrap_or_default()
}

use dbflux_mcp::builtin_display_name;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum McpSectionVariant {
    Clients,
    Roles,
    Policies,
}

/// One focusable form field across the three MCP variants. `PolicyClass`/
/// `PolicyTool` carry the row index into `mcp_policy_class_ids()` /
/// `mcp_policy_tool_ids()`, keeping the field enum `Copy`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum McpFormField {
    ClientId,
    ClientName,
    ClientIssuer,
    ClientActive,
    ClientToggleActive,
    RoleId,
    RolePolicies,
    PolicyId,
    PolicyClass(usize),
    PolicyTool(usize),
    DeleteButton,
    SaveButton,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum McpFocus {
    List,
    Form,
}

/// The ordered ids `PolicyClass(index)` refers to; the render layout and this
/// row table must stay in the same order.
pub(super) fn mcp_policy_class_ids() -> Vec<&'static str> {
    CLASS_IDS.to_vec()
}

/// The ordered ids `PolicyTool(index)` refers to: `TOOL_GROUPS` flattened in
/// display order, which is the same order as `TOOL_IDS`.
pub(super) fn mcp_policy_tool_ids() -> Vec<&'static str> {
    TOOL_GROUPS
        .iter()
        .flat_map(|(_, tools)| tools.iter().copied())
        .collect()
}

/// The row table a `McpSection` form walks with `j`/`k`/`tab`. Builtin roles
/// and policies drop the save/delete button row so a stale field cursor can
/// never land on a mutating control for a read-only item.
pub(super) fn mcp_form_rows(
    variant: McpSectionVariant,
    is_builtin: bool,
    class_count: usize,
    tool_count: usize,
) -> Vec<Vec<McpFormField>> {
    match variant {
        McpSectionVariant::Clients => vec![
            vec![McpFormField::ClientId],
            vec![McpFormField::ClientName],
            vec![McpFormField::ClientIssuer],
            vec![McpFormField::ClientActive],
            vec![
                McpFormField::ClientToggleActive,
                McpFormField::DeleteButton,
                McpFormField::SaveButton,
            ],
        ],
        McpSectionVariant::Roles => {
            let mut rows = vec![vec![McpFormField::RoleId], vec![McpFormField::RolePolicies]];
            if !is_builtin {
                rows.push(vec![McpFormField::DeleteButton, McpFormField::SaveButton]);
            }
            rows
        }
        McpSectionVariant::Policies => {
            let mut rows = vec![vec![McpFormField::PolicyId]];
            rows.push((0..class_count).map(McpFormField::PolicyClass).collect());
            rows.extend((0..tool_count).map(|i| vec![McpFormField::PolicyTool(i)]));
            if !is_builtin {
                rows.push(vec![McpFormField::DeleteButton, McpFormField::SaveButton]);
            }
            rows
        }
    }
}

/// Whether `field` is a text input that `focus_current_field` should move
/// keyboard focus into (as opposed to a checkbox, button, or multiselect).
pub(super) fn mcp_is_input_field(field: McpFormField) -> bool {
    matches!(
        field,
        McpFormField::ClientId
            | McpFormField::ClientName
            | McpFormField::ClientIssuer
            | McpFormField::RoleId
            | McpFormField::PolicyId
    )
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
    mcp_focus: McpFocus,
    mcp_form_field: McpFormField,
    editing_field: bool,
    list_scroll_handle: ScrollHandle,
    content_focused: bool,
    switching_input: bool,
    pending_sync_from_state: bool,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<SectionFocusEvent> for McpSection {}

impl McpSection {
    fn section_header_copy(&self) -> (String, String) {
        match self.variant {
            McpSectionVariant::Clients => (
                dbflux_i18n::t!("settings.mcp.trusted_clients_title"),
                dbflux_i18n::t!("settings.mcp.trusted_clients_description"),
            ),
            McpSectionVariant::Roles => (
                dbflux_i18n::t!("settings.mcp.roles_title"),
                dbflux_i18n::t!("settings.mcp.roles_description"),
            ),
            McpSectionVariant::Policies => (
                dbflux_i18n::t!("settings.mcp.policies_title"),
                dbflux_i18n::t!("settings.mcp.policies_description"),
            ),
        }
    }

    pub(super) fn new(
        app_state: Entity<AppStateEntity>,
        variant: McpSectionVariant,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let input_client_id = cx.new(|cx| InputState::new(window, cx).placeholder("client-id"));
        let input_client_name = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("settings.mcp.placeholder.client_name"))
        });
        let input_client_issuer = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("settings.mcp.field.issuer_optional"))
        });
        let input_role_id = cx.new(|cx| InputState::new(window, cx).placeholder("role-id"));
        let initial_policy_items = {
            let policies = app_state.read(cx).list_mcp_policies().unwrap_or_default();
            Self::build_policy_multiselect_items(&policies)
        };
        let role_policies_multiselect = cx.new(|cx| {
            let mut ms = MultiSelect::new("mcp-role-policies").placeholder(dbflux_i18n::t!(
                "settings.mcp.placeholder.no_policies_selected"
            ));
            ms.set_items(initial_policy_items, cx);
            ms
        });
        let input_policy_id = cx.new(|cx| InputState::new(window, cx).placeholder("policy-id"));

        let state_sub = cx.subscribe(&app_state, |this, _, _: &AppStateChanged, cx| {
            this.pending_sync_from_state = true;
            cx.notify();
        });

        let subs = vec![
            state_sub,
            create_blur_subscription(cx, &input_client_id),
            create_blur_subscription(cx, &input_client_name),
            create_blur_subscription(cx, &input_client_issuer),
            create_blur_subscription(cx, &input_role_id),
            create_blur_subscription(cx, &input_policy_id),
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

            mcp_focus: McpFocus::List,
            mcp_form_field: Self::first_field_for(variant),
            editing_field: false,
            list_scroll_handle: ScrollHandle::new(),
            content_focused: false,
            switching_input: false,
            pending_sync_from_state: true,
            _subscriptions: subs,
        }
    }

    fn first_field_for(variant: McpSectionVariant) -> McpFormField {
        match variant {
            McpSectionVariant::Clients => McpFormField::ClientId,
            McpSectionVariant::Roles => McpFormField::RoleId,
            McpSectionVariant::Policies => McpFormField::PolicyId,
        }
    }

    fn role_is_builtin(&self) -> bool {
        self.selected_role_id
            .as_deref()
            .map(dbflux_mcp::is_builtin)
            .unwrap_or(false)
    }

    fn policy_is_builtin(&self) -> bool {
        self.selected_policy_id
            .as_deref()
            .map(dbflux_mcp::is_builtin)
            .unwrap_or(false)
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

    fn save_client(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let draft = self.draft_client(cx);
        if draft.id.is_empty() || draft.name.is_empty() {
            let msg = dbflux_i18n::t!("settings.mcp.error.client_id_name_required");
            Toast::error(msg.clone())
                .meta_right(now_hms())
                .action(copy_action(msg))
                .push(cx);
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
        Toast::info(dbflux_i18n::t!("settings.mcp.toast.client_saved"))
            .meta_right(now_hms())
            .push(cx);
    }

    fn delete_selected_client(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(client_id) = self.selected_client_id.clone() else {
            Toast::warning(dbflux_i18n::t!("settings.mcp.toast.select_client_first"))
                .meta_right(now_hms())
                .push(cx);
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
        Toast::info(dbflux_i18n::t!("settings.mcp.toast.client_deleted"))
            .meta_right(now_hms())
            .push(cx);
    }

    fn toggle_selected_client_active(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let Some(mut selected) = self.selected_client(cx) else {
            Toast::warning(dbflux_i18n::t!("settings.mcp.toast.select_client_first"))
                .meta_right(now_hms())
                .push(cx);
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
            dbflux_i18n::t!("settings.mcp.toast.client_activated")
        } else {
            dbflux_i18n::t!("settings.mcp.toast.client_deactivated")
        };
        Toast::info(msg).meta_right(now_hms()).push(cx);
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

    fn save_role(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let id = self.input_role_id.read(cx).value().trim().to_string();
        if id.is_empty() {
            let msg = dbflux_i18n::t!("settings.mcp.error.role_id_required");
            Toast::error(msg.clone())
                .meta_right(now_hms())
                .action(copy_action(msg))
                .push(cx);
            return;
        }
        if dbflux_mcp::is_builtin(&id) {
            let msg = dbflux_i18n::t!("settings.mcp.error.builtin_role_readonly");
            Toast::error(msg.clone())
                .meta_right(now_hms())
                .action(copy_action(msg))
                .push(cx);
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
        Toast::info(dbflux_i18n::t!("settings.mcp.toast.role_saved"))
            .meta_right(now_hms())
            .push(cx);
    }

    fn delete_selected_role(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(role_id) = self.selected_role_id.clone() else {
            Toast::warning(dbflux_i18n::t!("settings.mcp.toast.select_role_first"))
                .meta_right(now_hms())
                .push(cx);
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
        Toast::info(dbflux_i18n::t!("settings.mcp.toast.role_deleted"))
            .meta_right(now_hms())
            .push(cx);
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

    fn save_policy(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let id = self.input_policy_id.read(cx).value().trim().to_string();
        if id.is_empty() {
            let msg = dbflux_i18n::t!("settings.mcp.error.policy_id_required");
            Toast::error(msg.clone())
                .meta_right(now_hms())
                .action(copy_action(msg))
                .push(cx);
            return;
        }
        if dbflux_mcp::is_builtin(&id) {
            let msg = dbflux_i18n::t!("settings.mcp.error.builtin_policy_readonly");
            Toast::error(msg.clone())
                .meta_right(now_hms())
                .action(copy_action(msg))
                .push(cx);
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
        Toast::info(dbflux_i18n::t!("settings.mcp.toast.policy_saved"))
            .meta_right(now_hms())
            .push(cx);
    }

    fn delete_selected_policy(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(policy_id) = self.selected_policy_id.clone() else {
            Toast::warning(dbflux_i18n::t!("settings.mcp.toast.select_policy_first"))
                .meta_right(now_hms())
                .push(cx);
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
        Toast::info(dbflux_i18n::t!("settings.mcp.toast.policy_deleted"))
            .meta_right(now_hms())
            .push(cx);
    }

    // ─── Render helpers ───────────────────────────────────────────────────────

    fn render_clients_content(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let clients = self.trusted_clients(cx);
        let selected = self.selected_client_id.clone();
        let is_list_focused = self.mcp_focus == McpFocus::List;

        let ids: Vec<String> = clients.iter().map(|c| c.id.clone()).collect();
        let items: Vec<MasterDetailItem> = clients
            .iter()
            .map(|client| {
                let is_selected = selected.as_deref() == Some(client.id.as_str());
                MasterDetailItem {
                    id: SharedString::from(client.id.clone()),
                    label: SharedString::from(client.name.clone()),
                    detail: Some(SharedString::from(client.id.clone())),
                    badge: Some(if client.active {
                        (SharedString::from("active"), BadgeTone::Success)
                    } else {
                        (SharedString::from("inactive"), BadgeTone::Neutral)
                    }),
                    selected: is_selected,
                    focused: is_list_focused && is_selected,
                }
            })
            .collect();

        let config = MasterDetailListConfig {
            id: SharedString::from("mcp-clients-list"),
            width: Widths::SETTINGS_LIST_PANEL,
            new_action: Some(MasterDetailAction {
                label: SharedString::from(dbflux_i18n::t!("settings.mcp.new_client")),
                enabled: true,
                focused: is_list_focused && selected.is_none(),
            }),
            secondary_action: None,
            empty_message: Some(SharedString::from(dbflux_i18n::t!(
                "settings.mcp.empty.clients"
            ))),
        };

        let entity = cx.entity();
        let entity_for_action = entity.clone();

        let form = div()
            .flex_1()
            .h_full()
            .flex()
            .flex_col()
            .child(dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.mcp.trusted_clients_title"),
                dbflux_i18n::t!("settings.mcp.trusted_clients_form_description"),
                cx,
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
                    .child(Label::new(dbflux_i18n::t!("settings.mcp.field.client_id")))
                    .child(self.render_mcp_input_field(
                        &self.input_client_id,
                        McpFormField::ClientId,
                        theme.primary,
                        cx,
                    ))
                    .child(Label::new(dbflux_i18n::t!("settings.mcp.field.name")))
                    .child(self.render_mcp_input_field(
                        &self.input_client_name,
                        McpFormField::ClientName,
                        theme.primary,
                        cx,
                    ))
                    .child(Label::new(dbflux_i18n::t!(
                        "settings.mcp.field.issuer_optional"
                    )))
                    .child(self.render_mcp_input_field(
                        &self.input_client_issuer,
                        McpFormField::ClientIssuer,
                        theme.primary,
                        cx,
                    ))
                    .child(focus_frame(
                        self.mcp_focus == McpFocus::Form
                            && self.mcp_form_field == McpFormField::ClientActive,
                        Some(theme.primary),
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
                            .child(Body::new(dbflux_i18n::t!("settings.mcp.field.active"))),
                        cx,
                    )),
            );

        let list = render_master_detail_list(
            &config,
            &items,
            &self.list_scroll_handle,
            move |index: usize, window: &mut Window, cx: &mut App| {
                let Some(id) = ids.get(index).cloned() else {
                    return;
                };
                entity.update(cx, |this, cx| {
                    this.select_client(&id, window, cx);
                    this.enter_form(window, cx);
                });
            },
            move |_kind: MasterDetailActionKind, window: &mut Window, cx: &mut App| {
                entity_for_action.update(cx, |this, cx| this.new_current_item(window, cx));
            },
            cx,
        );

        div()
            .size_full()
            .flex()
            .overflow_hidden()
            .child(list)
            .child(form)
    }

    fn render_mcp_input_field(
        &self,
        input: &Entity<InputState>,
        field: McpFormField,
        primary: Hsla,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let is_focused = self.mcp_focus == McpFocus::Form && self.mcp_form_field == field;

        focus_frame(is_focused, Some(primary), Input::new(input).small(), cx).on_mouse_down(
            MouseButton::Left,
            cx.listener(move |this, _, window, cx| {
                this.switching_input = true;
                this.mcp_focus = McpFocus::Form;
                this.mcp_form_field = field;
                this.mcp_focus_current_field(window, cx);
                cx.notify();
            }),
        )
    }

    fn render_roles_content(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let roles = self.roles(cx);
        let selected = self.selected_role_id.clone();
        let is_list_focused = self.mcp_focus == McpFocus::List;

        let ids: Vec<String> = roles.iter().map(|r| r.id.clone()).collect();
        let items: Vec<MasterDetailItem> = roles
            .iter()
            .map(|role| {
                let is_selected = selected.as_deref() == Some(role.id.as_str());
                let label = builtin_display_name(&role.id)
                    .unwrap_or(role.id.as_str())
                    .to_string();
                let badge = if dbflux_mcp::is_builtin(&role.id) {
                    Some((
                        SharedString::from(dbflux_i18n::t!("settings.mcp.field.builtin_badge")),
                        BadgeTone::Accent,
                    ))
                } else {
                    None
                };
                MasterDetailItem {
                    id: SharedString::from(role.id.clone()),
                    label: SharedString::from(label),
                    detail: Some(SharedString::from(mcp_role_policy_count(
                        role.policy_ids.len(),
                    ))),
                    badge,
                    selected: is_selected,
                    focused: is_list_focused && is_selected,
                }
            })
            .collect();

        let config = MasterDetailListConfig {
            id: SharedString::from("mcp-roles-list"),
            width: Widths::SETTINGS_LIST_PANEL,
            new_action: Some(MasterDetailAction {
                label: SharedString::from(dbflux_i18n::t!("settings.mcp.new_role")),
                enabled: true,
                focused: is_list_focused && selected.is_none(),
            }),
            secondary_action: None,
            empty_message: Some(SharedString::from(dbflux_i18n::t!(
                "settings.mcp.empty.roles"
            ))),
        };

        let entity = cx.entity();
        let entity_for_action = entity.clone();

        let role_policies_focused =
            self.mcp_focus == McpFocus::Form && self.mcp_form_field == McpFormField::RolePolicies;

        let form = div()
            .flex_1()
            .h_full()
            .flex()
            .flex_col()
            .child(dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.mcp.roles_title"),
                dbflux_i18n::t!("settings.mcp.roles_form_description"),
                cx,
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
                    .child(Label::new(dbflux_i18n::t!("settings.mcp.field.role_id")))
                    .child(self.render_mcp_input_field(
                        &self.input_role_id,
                        McpFormField::RoleId,
                        theme.primary,
                        cx,
                    ))
                    .child(Label::new(dbflux_i18n::t!("settings.mcp.field.policies")))
                    .child(
                        Body::new(dbflux_i18n::t!("settings.mcp.hint.select_policies"))
                            .color(theme.muted_foreground),
                    )
                    .child(focus_frame(
                        role_policies_focused,
                        Some(theme.primary),
                        self.role_policies_multiselect.clone(),
                        cx,
                    )),
            );

        let list = render_master_detail_list(
            &config,
            &items,
            &self.list_scroll_handle,
            move |index: usize, window: &mut Window, cx: &mut App| {
                let Some(id) = ids.get(index).cloned() else {
                    return;
                };
                entity.update(cx, |this, cx| {
                    this.select_role(&id, window, cx);
                    this.enter_form(window, cx);
                });
            },
            move |_kind: MasterDetailActionKind, window: &mut Window, cx: &mut App| {
                entity_for_action.update(cx, |this, cx| this.new_current_item(window, cx));
            },
            cx,
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
        let tool_meta = tool_meta();
        let class_meta = class_meta();
        let policies: Vec<ToolPolicyDto> = self
            .app_state
            .read(cx)
            .list_mcp_policies()
            .unwrap_or_default();
        let selected = self.selected_policy_id.clone();
        let is_list_focused = self.mcp_focus == McpFocus::List;
        let is_form_focused = self.mcp_focus == McpFocus::Form;
        let field = self.mcp_form_field;

        let ids: Vec<String> = policies.iter().map(|p| p.id.clone()).collect();
        let items: Vec<MasterDetailItem> = policies
            .iter()
            .map(|policy| {
                let is_selected = selected.as_deref() == Some(policy.id.as_str());
                let label = builtin_display_name(&policy.id)
                    .unwrap_or(policy.id.as_str())
                    .to_string();
                let badge = if dbflux_mcp::is_builtin(&policy.id) {
                    Some((
                        SharedString::from(dbflux_i18n::t!("settings.mcp.field.builtin_badge")),
                        BadgeTone::Accent,
                    ))
                } else {
                    None
                };
                MasterDetailItem {
                    id: SharedString::from(policy.id.clone()),
                    label: SharedString::from(label),
                    detail: Some(SharedString::from(mcp_policy_tools_classes_summary(
                        policy.allowed_tools.len(),
                        policy.allowed_classes.len(),
                    ))),
                    badge,
                    selected: is_selected,
                    focused: is_list_focused && is_selected,
                }
            })
            .collect();

        let config = MasterDetailListConfig {
            id: SharedString::from("mcp-policies-list"),
            width: Widths::SETTINGS_LIST_PANEL,
            new_action: Some(MasterDetailAction {
                label: SharedString::from(dbflux_i18n::t!("settings.mcp.new_policy")),
                enabled: true,
                focused: is_list_focused && selected.is_none(),
            }),
            secondary_action: None,
            empty_message: Some(SharedString::from(dbflux_i18n::t!(
                "settings.mcp.empty.policies"
            ))),
        };

        let entity = cx.entity();
        let entity_for_action = entity.clone();

        let mut tool_index = 0usize;

        let form = div()
            .flex_1()
            .h_full()
            .flex()
            .flex_col()
            .child(dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.mcp.policies_title"),
                dbflux_i18n::t!("settings.mcp.policies_form_description"),
                cx,
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
                    .child(Label::new(dbflux_i18n::t!("settings.mcp.field.policy_id")))
                    .child(self.render_mcp_input_field(
                        &self.input_policy_id,
                        McpFormField::PolicyId,
                        theme.primary,
                        cx,
                    ))
                    .child(Label::new(dbflux_i18n::t!(
                        "settings.mcp.field.allowed_execution_classes"
                    )))
                    .child(div().flex().flex_wrap().gap_3().children(
                        class_meta.iter().enumerate().map(
                            |(index, (class, label, description))| {
                                let class = *class;
                                let checked = self.draft_policy_classes.contains(class);
                                let is_focused =
                                    is_form_focused && field == McpFormField::PolicyClass(index);

                                focus_frame(
                                    is_focused,
                                    Some(theme.primary),
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
                                                .child(FieldLabel::new(label.clone()))
                                                .child(
                                                    Body::new(description.clone())
                                                        .color(theme.muted_foreground),
                                                ),
                                        ),
                                    cx,
                                )
                            },
                        ),
                    ))
                    .child(Label::new(dbflux_i18n::t!(
                        "settings.mcp.field.allowed_tools"
                    )))
                    .children(TOOL_GROUPS.iter().map(|(group_id, tools)| {
                        div()
                            .flex()
                            .flex_col()
                            .gap_2()
                            .child(SubSectionLabel::new(tool_group_label(group_id)))
                            .child(div().flex().flex_col().gap_2().pl_2().children(
                                tools.iter().map(|&tool| {
                                    let index = tool_index;
                                    tool_index += 1;
                                    let checked = self.draft_policy_tools.contains(tool);
                                    let label = tool_label(&tool_meta, tool);
                                    let description = tool_description(&tool_meta, tool);
                                    let is_focused =
                                        is_form_focused && field == McpFormField::PolicyTool(index);

                                    focus_frame(
                                        is_focused,
                                        Some(theme.primary),
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
                                                                this.draft_policy_tools
                                                                    .remove(tool);
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
                                                    .child(FieldLabel::new(label))
                                                    .child(
                                                        Body::new(description)
                                                            .color(theme.muted_foreground),
                                                    ),
                                            ),
                                        cx,
                                    )
                                }),
                            ))
                    })),
            );

        let list = render_master_detail_list(
            &config,
            &items,
            &self.list_scroll_handle,
            move |index: usize, window: &mut Window, cx: &mut App| {
                let Some(id) = ids.get(index).cloned() else {
                    return;
                };
                entity.update(cx, |this, cx| {
                    this.select_policy(&id, window, cx);
                    this.enter_form(window, cx);
                });
            },
            move |_kind: MasterDetailActionKind, window: &mut Window, cx: &mut App| {
                entity_for_action.update(cx, |this, cx| this.new_current_item(window, cx));
            },
            cx,
        );

        div()
            .size_full()
            .flex()
            .overflow_hidden()
            .child(list)
            .child(form)
    }

    fn render_clients_footer_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let primary = cx.theme().primary;
        let is_form_focused = self.mcp_focus == McpFocus::Form;
        let field = self.mcp_form_field;
        let save_label = if self.selected_client(cx).is_some() {
            dbflux_i18n::t!("settings.mcp.action.update_client")
        } else {
            dbflux_i18n::t!("settings.mcp.action.create_client")
        };
        let active_label = if self.draft_active {
            dbflux_i18n::t!("settings.mcp.action.deactivate")
        } else {
            dbflux_i18n::t!("settings.mcp.action.activate")
        };

        div()
            .flex()
            .items_center()
            .justify_end()
            .gap_3()
            .child(
                Body::new(if self.client_has_unsaved_changes(cx) {
                    dbflux_i18n::t!("settings.mcp.status.unsaved")
                } else {
                    dbflux_i18n::t!("settings.mcp.status.saved")
                })
                .color(cx.theme().muted_foreground),
            )
            .child(layout::footer_action_frame(
                is_form_focused && field == McpFormField::ClientToggleActive,
                primary,
                Button::new("mcp-client-toggle-active", active_label)
                    .small()
                    .ghost()
                    .w_full()
                    .disabled(self.selected_client(cx).is_none())
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.toggle_selected_client_active(window, cx);
                    })),
            ))
            .child(layout::footer_action_frame(
                is_form_focused && field == McpFormField::DeleteButton,
                primary,
                Button::new(
                    "mcp-client-delete",
                    dbflux_i18n::t!("settings.mcp.action.delete"),
                )
                .small()
                .danger()
                .w_full()
                .disabled(self.selected_client(cx).is_none())
                .on_click(cx.listener(|this, _, window, cx| {
                    this.delete_selected_client(window, cx);
                })),
            ))
            .child(layout::footer_action_frame(
                is_form_focused && field == McpFormField::SaveButton,
                primary,
                Button::new("mcp-client-save", save_label)
                    .small()
                    .primary()
                    .w_full()
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.save_client(window, cx);
                    })),
            ))
            .into_any_element()
    }

    fn render_roles_footer_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let primary = cx.theme().primary;
        let is_form_focused = self.mcp_focus == McpFocus::Form;
        let field = self.mcp_form_field;
        let role_is_builtin = self
            .selected_role_id
            .as_deref()
            .map(dbflux_mcp::is_builtin)
            .unwrap_or(false);
        let save_label = if self.selected_role_id.is_some() {
            dbflux_i18n::t!("settings.mcp.action.update_role")
        } else {
            dbflux_i18n::t!("settings.mcp.action.create_role")
        };

        div()
            .flex()
            .items_center()
            .justify_end()
            .gap_3()
            .when(role_is_builtin, |div| {
                div.child(
                    Body::new(dbflux_i18n::t!("settings.mcp.error.builtin_role_readonly"))
                        .color(cx.theme().muted_foreground),
                )
            })
            .child(layout::footer_action_frame(
                is_form_focused && field == McpFormField::DeleteButton,
                primary,
                Button::new(
                    "mcp-role-delete",
                    dbflux_i18n::t!("settings.mcp.action.delete"),
                )
                .small()
                .danger()
                .w_full()
                .disabled(self.selected_role_id.is_none() || role_is_builtin)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.delete_selected_role(window, cx);
                })),
            ))
            .child(layout::footer_action_frame(
                is_form_focused && field == McpFormField::SaveButton,
                primary,
                Button::new("mcp-role-save", save_label)
                    .small()
                    .primary()
                    .w_full()
                    .disabled(role_is_builtin)
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.save_role(window, cx);
                    })),
            ))
            .into_any_element()
    }

    fn render_policies_footer_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let primary = cx.theme().primary;
        let is_form_focused = self.mcp_focus == McpFocus::Form;
        let field = self.mcp_form_field;
        let policy_is_builtin = self
            .selected_policy_id
            .as_deref()
            .map(dbflux_mcp::is_builtin)
            .unwrap_or(false);
        let save_label = if self.selected_policy_id.is_some() {
            dbflux_i18n::t!("settings.mcp.action.update_policy")
        } else {
            dbflux_i18n::t!("settings.mcp.action.create_policy")
        };

        div()
            .flex()
            .items_center()
            .justify_end()
            .gap_3()
            .when(policy_is_builtin, |div| {
                div.child(
                    Body::new(dbflux_i18n::t!(
                        "settings.mcp.error.builtin_policy_readonly"
                    ))
                    .color(cx.theme().muted_foreground),
                )
            })
            .child(layout::footer_action_frame(
                is_form_focused && field == McpFormField::DeleteButton,
                primary,
                Button::new(
                    "mcp-policy-delete",
                    dbflux_i18n::t!("settings.mcp.action.delete"),
                )
                .small()
                .danger()
                .w_full()
                .disabled(self.selected_policy_id.is_none() || policy_is_builtin)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.delete_selected_policy(window, cx);
                })),
            ))
            .child(layout::footer_action_frame(
                is_form_focused && field == McpFormField::SaveButton,
                primary,
                Button::new("mcp-policy-save", save_label)
                    .small()
                    .primary()
                    .w_full()
                    .disabled(policy_is_builtin)
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.save_policy(window, cx);
                    })),
            ))
            .into_any_element()
    }

    // ─── Keyboard navigation ──────────────────────────────────────────────────

    fn select_next(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.variant {
            McpSectionVariant::Clients => {
                let clients = self.trusted_clients(cx);
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
            }
            McpSectionVariant::Roles => {
                let roles = self.roles(cx);
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
            }
            McpSectionVariant::Policies => {
                let policies = self.policies(cx);
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
            }
        }
    }

    fn select_prev(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.variant {
            McpSectionVariant::Clients => {
                let clients = self.trusted_clients(cx);
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
            }
            McpSectionVariant::Roles => {
                let roles = self.roles(cx);
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
            }
            McpSectionVariant::Policies => {
                let policies = self.policies(cx);
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
            }
        }
    }

    fn select_first(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.variant {
            McpSectionVariant::Clients => {
                if let Some(id) = self.trusted_clients(cx).first().map(|c| c.id.clone()) {
                    self.select_client(&id, window, cx);
                }
            }
            McpSectionVariant::Roles => {
                if let Some(id) = self.roles(cx).first().map(|r| r.id.clone()) {
                    self.select_role(&id, window, cx);
                }
            }
            McpSectionVariant::Policies => {
                if let Some(id) = self.policies(cx).first().map(|p| p.id.clone()) {
                    self.select_policy(&id, window, cx);
                }
            }
        }
    }

    fn select_last(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.variant {
            McpSectionVariant::Clients => {
                if let Some(id) = self.trusted_clients(cx).last().map(|c| c.id.clone()) {
                    self.select_client(&id, window, cx);
                }
            }
            McpSectionVariant::Roles => {
                if let Some(id) = self.roles(cx).last().map(|r| r.id.clone()) {
                    self.select_role(&id, window, cx);
                }
            }
            McpSectionVariant::Policies => {
                if let Some(id) = self.policies(cx).last().map(|p| p.id.clone()) {
                    self.select_policy(&id, window, cx);
                }
            }
        }
    }

    fn delete_current_selection(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.variant {
            McpSectionVariant::Clients => self.delete_selected_client(window, cx),
            McpSectionVariant::Roles if !self.role_is_builtin() => {
                self.delete_selected_role(window, cx);
            }
            McpSectionVariant::Policies if !self.policy_is_builtin() => {
                self.delete_selected_policy(window, cx);
            }
            McpSectionVariant::Roles | McpSectionVariant::Policies => {}
        }
    }

    fn clear_current_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.variant {
            McpSectionVariant::Clients => self.clear_client_form(window, cx),
            McpSectionVariant::Roles => self.clear_role_form(window, cx),
            McpSectionVariant::Policies => self.clear_policy_form(window, cx),
        }
    }

    fn new_current_item(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.clear_current_form(window, cx);
        self.enter_form(window, cx);
    }

    fn mcp_focus_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.editing_field = true;

        match self.mcp_form_field {
            McpFormField::ClientId => {
                self.input_client_id
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            McpFormField::ClientName => {
                self.input_client_name
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            McpFormField::ClientIssuer => {
                self.input_client_issuer
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            McpFormField::RoleId => {
                self.input_role_id
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            McpFormField::PolicyId => {
                self.input_policy_id
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            _ => {
                self.editing_field = false;
            }
        }
    }

    fn mcp_activate_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let blocked = match self.variant {
            McpSectionVariant::Roles => self.role_is_builtin(),
            McpSectionVariant::Policies => self.policy_is_builtin(),
            McpSectionVariant::Clients => false,
        };

        // A built-in role/policy stays inspectable (the field cursor and its
        // value ring still move over it) but never enters edit mode: a stale
        // cursor can outlive a selection change, so this guard is checked
        // again here even though `mcp_form_rows` already drops the button
        // row for builtin items.
        if blocked {
            return;
        }

        match self.mcp_form_field {
            McpFormField::ClientActive => {
                self.draft_active = !self.draft_active;
                cx.notify();
            }
            McpFormField::ClientToggleActive => {
                self.toggle_selected_client_active(window, cx);
            }
            McpFormField::RolePolicies => {
                self.role_policies_multiselect
                    .update(cx, |ms, cx| ms.toggle_open(cx));
            }
            McpFormField::PolicyClass(index) => {
                if let Some(&id) = mcp_policy_class_ids().get(index) {
                    if self.draft_policy_classes.contains(id) {
                        self.draft_policy_classes.remove(id);
                    } else {
                        self.draft_policy_classes.insert(id.to_string());
                    }
                    cx.notify();
                }
            }
            McpFormField::PolicyTool(index) => {
                if let Some(&id) = mcp_policy_tool_ids().get(index) {
                    if self.draft_policy_tools.contains(id) {
                        self.draft_policy_tools.remove(id);
                    } else {
                        self.draft_policy_tools.insert(id.to_string());
                    }
                    cx.notify();
                }
            }
            McpFormField::SaveButton => match self.variant {
                McpSectionVariant::Clients => self.save_client(window, cx),
                McpSectionVariant::Roles => self.save_role(window, cx),
                McpSectionVariant::Policies => self.save_policy(window, cx),
            },
            McpFormField::DeleteButton => self.delete_current_selection(window, cx),
            field if mcp_is_input_field(field) => {
                self.mcp_focus_current_field(window, cx);
            }
            _ => {}
        }
    }

    pub(super) fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.content_focused() && !self.editing_field() {
            return;
        }

        if self.handle_editing_keys(event, window, cx) {
            return;
        }

        let chord = key_chord_from_gpui(&event.keystroke);

        match self.mcp_focus {
            McpFocus::List => match (chord.key.as_str(), chord.modifiers) {
                ("j", m) | ("down", m) if m == Modifiers::none() => {
                    self.select_next(window, cx);
                    cx.notify();
                }
                ("k", m) | ("up", m) if m == Modifiers::none() => {
                    self.select_prev(window, cx);
                    cx.notify();
                }
                ("l", m) | ("right", m) | ("enter", m) if m == Modifiers::none() => {
                    self.enter_form(window, cx);
                    cx.notify();
                }
                ("n", m) if m == Modifiers::none() => {
                    self.new_current_item(window, cx);
                    cx.notify();
                }
                ("d", m) if m == Modifiers::none() => {
                    self.delete_current_selection(window, cx);
                    cx.notify();
                }
                ("g", m) if m == Modifiers::none() => {
                    self.select_first(window, cx);
                    cx.notify();
                }
                ("G", m) if m == Modifiers::none() => {
                    self.select_last(window, cx);
                    cx.notify();
                }
                ("escape", m) if m == Modifiers::none() => {
                    let has_selection = match self.variant {
                        McpSectionVariant::Clients => self.selected_client_id.is_some(),
                        McpSectionVariant::Roles => self.selected_role_id.is_some(),
                        McpSectionVariant::Policies => self.selected_policy_id.is_some(),
                    };
                    if has_selection {
                        self.clear_current_form(window, cx);
                    } else {
                        cx.emit(SectionFocusEvent::RequestFocusReturn);
                    }
                }
                _ => {}
            },
            McpFocus::Form => match (chord.key.as_str(), chord.modifiers) {
                ("escape", m) | ("h", m) if m == Modifiers::none() => {
                    self.exit_form(window, cx);
                    cx.notify();
                }
                ("j", m) | ("down", m) if m == Modifiers::none() => {
                    self.move_down();
                    cx.notify();
                }
                ("k", m) | ("up", m) if m == Modifiers::none() => {
                    self.move_up();
                    cx.notify();
                }
                ("left", m) if m == Modifiers::none() => {
                    self.move_left();
                    cx.notify();
                }
                ("l", m) | ("right", m) if m == Modifiers::none() => {
                    self.move_right();
                    cx.notify();
                }
                ("enter", m) if m == Modifiers::none() => {
                    self.activate_current_field(window, cx);
                    cx.notify();
                }
                ("tab", m) if m == Modifiers::none() => {
                    self.tab_next();
                    cx.notify();
                }
                ("tab", m) if m == Modifiers::shift() => {
                    self.tab_prev();
                    cx.notify();
                }
                ("g", m) if m == Modifiers::none() => {
                    self.move_first();
                    cx.notify();
                }
                ("G", m) if m == Modifiers::none() => {
                    self.move_last();
                    cx.notify();
                }
                _ => {}
            },
        }
    }
}

impl FormSection for McpSection {
    type Focus = McpFocus;
    type FormField = McpFormField;

    fn focus_area(&self) -> Self::Focus {
        self.mcp_focus
    }

    fn set_focus_area(&mut self, focus: Self::Focus) {
        self.mcp_focus = focus;
    }

    fn form_field(&self) -> Self::FormField {
        self.mcp_form_field
    }

    fn set_form_field(&mut self, field: Self::FormField) {
        self.mcp_form_field = field;
    }

    fn editing_field(&self) -> bool {
        self.editing_field
    }

    fn set_editing_field(&mut self, editing: bool) {
        self.editing_field = editing;
    }

    fn switching_input(&self) -> bool {
        self.switching_input
    }

    fn set_switching_input(&mut self, switching: bool) {
        self.switching_input = switching;
    }

    fn content_focused(&self) -> bool {
        self.content_focused
    }

    fn list_focus() -> Self::Focus {
        McpFocus::List
    }

    fn form_focus() -> Self::Focus {
        McpFocus::Form
    }

    fn first_form_field() -> Self::FormField {
        McpFormField::ClientId
    }

    fn form_rows(&self) -> Vec<Vec<Self::FormField>> {
        let is_builtin = match self.variant {
            McpSectionVariant::Roles => self.role_is_builtin(),
            McpSectionVariant::Policies => self.policy_is_builtin(),
            McpSectionVariant::Clients => false,
        };
        mcp_form_rows(
            self.variant,
            is_builtin,
            mcp_policy_class_ids().len(),
            mcp_policy_tool_ids().len(),
        )
    }

    fn is_input_field(field: Self::FormField) -> bool {
        mcp_is_input_field(field)
    }

    fn focus_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        McpSection::mcp_focus_current_field(self, window, cx);
    }

    fn activate_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        McpSection::mcp_activate_current_field(self, window, cx);
    }

    fn enter_form(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.mcp_focus = McpFocus::Form;
        self.mcp_form_field = Self::first_field_for(self.variant);
        self.editing_field = false;
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
        self.editing_field = false;
        cx.notify();
    }

    fn is_dirty(&self, cx: &App) -> bool {
        match self.variant {
            McpSectionVariant::Clients => self.client_has_unsaved_changes(cx),
            _ => false,
        }
    }

    fn render_footer_actions(
        &self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        Some(match self.variant {
            McpSectionVariant::Clients => self.render_clients_footer_actions(window, cx),
            McpSectionVariant::Roles => self.render_roles_footer_actions(window, cx),
            McpSectionVariant::Policies => self.render_policies_footer_actions(window, cx),
        })
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

        let (title, description) = self.section_header_copy();

        div()
            .h_full()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(dbflux_components::composites::section_header(
                title,
                description,
                cx,
            ))
            .child(div().flex_1().min_h_0().overflow_hidden().child(content))
    }
}

#[cfg(test)]
mod form_row_tests {
    use super::{
        McpFormField, McpSectionVariant, mcp_form_rows, mcp_is_input_field, mcp_policy_class_ids,
        mcp_policy_tool_ids,
    };

    fn all_fields(rows: &[Vec<McpFormField>]) -> Vec<McpFormField> {
        rows.iter().flatten().copied().collect()
    }

    #[test]
    fn clients_rows_always_include_the_button_row() {
        let rows = mcp_form_rows(McpSectionVariant::Clients, false, 0, 0);
        let fields = all_fields(&rows);

        assert!(fields.contains(&McpFormField::ClientId));
        assert!(fields.contains(&McpFormField::ClientName));
        assert!(fields.contains(&McpFormField::ClientIssuer));
        assert!(fields.contains(&McpFormField::ClientActive));
        assert!(fields.contains(&McpFormField::ClientToggleActive));
        assert!(fields.contains(&McpFormField::DeleteButton));
        assert!(fields.contains(&McpFormField::SaveButton));
        assert_eq!(rows[0], vec![McpFormField::ClientId]);
    }

    #[test]
    fn roles_drop_button_row_when_builtin() {
        let editable = mcp_form_rows(McpSectionVariant::Roles, false, 0, 0);
        let builtin = mcp_form_rows(McpSectionVariant::Roles, true, 0, 0);

        assert!(all_fields(&editable).contains(&McpFormField::SaveButton));
        assert!(all_fields(&editable).contains(&McpFormField::DeleteButton));
        assert!(!all_fields(&builtin).contains(&McpFormField::SaveButton));
        assert!(!all_fields(&builtin).contains(&McpFormField::DeleteButton));
        assert_eq!(editable[0], vec![McpFormField::RoleId]);
    }

    #[test]
    fn policies_class_row_length_matches_class_count() {
        let rows = mcp_form_rows(McpSectionVariant::Policies, false, 5, 0);
        let class_row = &rows[1];

        assert_eq!(class_row.len(), 5);
        assert_eq!(
            class_row,
            &(0..5).map(McpFormField::PolicyClass).collect::<Vec<_>>()
        );
    }

    #[test]
    fn policies_have_one_row_per_tool() {
        let rows = mcp_form_rows(McpSectionVariant::Policies, false, 5, 3);
        let tool_rows: Vec<_> = rows
            .iter()
            .filter(|row| matches!(row.as_slice(), [McpFormField::PolicyTool(_)]))
            .collect();

        assert_eq!(tool_rows.len(), 3);
    }

    #[test]
    fn policies_drop_button_row_when_builtin() {
        let editable = mcp_form_rows(McpSectionVariant::Policies, false, 5, 25);
        let builtin = mcp_form_rows(McpSectionVariant::Policies, true, 5, 25);

        assert!(all_fields(&editable).contains(&McpFormField::SaveButton));
        assert!(!all_fields(&builtin).contains(&McpFormField::SaveButton));
    }

    #[test]
    fn no_row_is_empty_and_no_field_repeats_within_a_variant() {
        for (variant, is_builtin) in [
            (McpSectionVariant::Clients, false),
            (McpSectionVariant::Roles, false),
            (McpSectionVariant::Roles, true),
            (McpSectionVariant::Policies, false),
            (McpSectionVariant::Policies, true),
        ] {
            let rows = mcp_form_rows(variant, is_builtin, 5, 25);

            assert!(rows.iter().all(|row| !row.is_empty()));

            let fields = all_fields(&rows);
            let mut seen = fields.clone();
            seen.sort_by_key(|f| format!("{f:?}"));
            seen.dedup();
            assert_eq!(seen.len(), fields.len());
        }
    }

    #[test]
    fn first_form_field_lands_in_row_zero() {
        assert_eq!(
            mcp_form_rows(McpSectionVariant::Clients, false, 0, 0)[0][0],
            McpFormField::ClientId
        );
        assert_eq!(
            mcp_form_rows(McpSectionVariant::Roles, false, 0, 0)[0][0],
            McpFormField::RoleId
        );
        assert_eq!(
            mcp_form_rows(McpSectionVariant::Policies, false, 5, 25)[0][0],
            McpFormField::PolicyId
        );
    }

    #[test]
    fn policy_class_and_tool_id_lists_are_stable() {
        let classes = mcp_policy_class_ids();
        assert_eq!(
            classes,
            vec!["metadata", "read", "write", "destructive", "admin"]
        );

        let tools = mcp_policy_tool_ids();
        assert_eq!(tools.len(), 25);
        assert_eq!(tools[0], "list_connections");
        assert_eq!(tools[tools.len() - 1], "export_audit_logs");
    }

    #[test]
    fn is_input_field_covers_exactly_the_five_inputs() {
        assert!(mcp_is_input_field(McpFormField::ClientId));
        assert!(mcp_is_input_field(McpFormField::ClientName));
        assert!(mcp_is_input_field(McpFormField::ClientIssuer));
        assert!(mcp_is_input_field(McpFormField::RoleId));
        assert!(mcp_is_input_field(McpFormField::PolicyId));

        assert!(!mcp_is_input_field(McpFormField::ClientActive));
        assert!(!mcp_is_input_field(McpFormField::ClientToggleActive));
        assert!(!mcp_is_input_field(McpFormField::RolePolicies));
        assert!(!mcp_is_input_field(McpFormField::PolicyClass(0)));
        assert!(!mcp_is_input_field(McpFormField::PolicyTool(0)));
        assert!(!mcp_is_input_field(McpFormField::DeleteButton));
        assert!(!mcp_is_input_field(McpFormField::SaveButton));
    }
}

#[cfg(test)]
mod tests {
    use super::{class_meta, tool_meta};

    const CHROME_KEYS: &[&str] = &[
        "settings.mcp.class.metadata.label",
        "settings.mcp.class.metadata.description",
        "settings.mcp.class.read.label",
        "settings.mcp.class.read.description",
        "settings.mcp.class.write.label",
        "settings.mcp.class.write.description",
        "settings.mcp.class.destructive.label",
        "settings.mcp.class.destructive.description",
        "settings.mcp.class.admin.label",
        "settings.mcp.class.admin.description",
        "settings.mcp.group.discovery",
        "settings.mcp.group.schema",
        "settings.mcp.group.query",
        "settings.mcp.group.scripts",
        "settings.mcp.group.approval",
        "settings.mcp.group.audit",
        "settings.mcp.trusted_clients_title",
        "settings.mcp.trusted_clients_description",
        "settings.mcp.trusted_clients_form_description",
        "settings.mcp.roles_title",
        "settings.mcp.roles_description",
        "settings.mcp.roles_form_description",
        "settings.mcp.policies_title",
        "settings.mcp.policies_description",
        "settings.mcp.policies_form_description",
        "settings.mcp.new_client",
        "settings.mcp.new_role",
        "settings.mcp.new_policy",
        "settings.mcp.empty.clients",
        "settings.mcp.empty.roles",
        "settings.mcp.empty.policies",
        "settings.mcp.field.client_id",
        "settings.mcp.field.name",
        "settings.mcp.field.issuer_optional",
        "settings.mcp.field.active",
        "settings.mcp.field.role_id",
        "settings.mcp.field.policies",
        "settings.mcp.field.policy_id",
        "settings.mcp.field.allowed_execution_classes",
        "settings.mcp.field.allowed_tools",
        "settings.mcp.field.builtin_badge",
        "settings.mcp.placeholder.client_name",
        "settings.mcp.placeholder.no_policies_selected",
        "settings.mcp.hint.select_policies",
        "settings.mcp.error.client_id_name_required",
        "settings.mcp.error.role_id_required",
        "settings.mcp.error.builtin_role_readonly",
        "settings.mcp.error.policy_id_required",
        "settings.mcp.error.builtin_policy_readonly",
        "settings.mcp.toast.client_saved",
        "settings.mcp.toast.select_client_first",
        "settings.mcp.toast.client_deleted",
        "settings.mcp.toast.client_activated",
        "settings.mcp.toast.client_deactivated",
        "settings.mcp.toast.role_saved",
        "settings.mcp.toast.select_role_first",
        "settings.mcp.toast.role_deleted",
        "settings.mcp.toast.policy_saved",
        "settings.mcp.toast.select_policy_first",
        "settings.mcp.toast.policy_deleted",
        "settings.mcp.status.unsaved",
        "settings.mcp.status.saved",
        "settings.mcp.action.update_client",
        "settings.mcp.action.create_client",
        "settings.mcp.action.activate",
        "settings.mcp.action.deactivate",
        "settings.mcp.action.delete",
        "settings.mcp.action.update_role",
        "settings.mcp.action.create_role",
        "settings.mcp.action.update_policy",
        "settings.mcp.action.create_policy",
    ];

    const EXPECTED_CLASS_IDS: &[&str] = &["metadata", "read", "write", "destructive", "admin"];

    #[test]
    fn mcp_chrome_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in CHROME_KEYS {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(
                    !value.is_empty(),
                    "key {key} resolved empty for locale {locale}"
                );
                assert_ne!(value, *key, "key {key} did not resolve for locale {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "key {key} fell back to the raw locale-qualified form for locale {locale}"
                );
            }
        }
    }

    #[test]
    fn mcp_trusted_clients_title_differs_between_locales() {
        let english = dbflux_i18n::t!("settings.mcp.trusted_clients_title", locale = "en");
        let spanish = dbflux_i18n::t!("settings.mcp.trusted_clients_title", locale = "es");

        assert_eq!(english, "Trusted Clients");
        assert_eq!(spanish, "Clientes de confianza");
        assert_ne!(english, spanish);
    }

    #[test]
    fn class_meta_ids_unchanged() {
        let meta = class_meta();
        let actual_ids: Vec<&str> = meta.iter().map(|(id, _, _)| *id).collect();

        assert_eq!(actual_ids, EXPECTED_CLASS_IDS);
    }

    const EXPECTED_TOOL_IDS: &[&str] = &[
        "list_connections",
        "get_connection",
        "get_connection_metadata",
        "list_databases",
        "list_schemas",
        "list_tables",
        "list_collections",
        "describe_object",
        "read_query",
        "explain_query",
        "preview_mutation",
        "list_scripts",
        "get_script",
        "create_script",
        "update_script",
        "delete_script",
        "run_script",
        "request_execution",
        "list_pending_executions",
        "get_pending_execution",
        "approve_execution",
        "reject_execution",
        "query_audit_logs",
        "get_audit_entry",
        "export_audit_logs",
    ];

    #[test]
    fn mcp_tool_meta_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for id in EXPECTED_TOOL_IDS {
                for field in ["name", "description"] {
                    let key = format!("settings.mcp.tool.{id}.{field}");
                    let value = dbflux_i18n::t!(&key, locale = locale);

                    assert!(
                        !value.is_empty(),
                        "key {key} resolved empty for locale {locale}"
                    );
                    assert_ne!(value, key, "key {key} did not resolve for locale {locale}");
                    assert_ne!(
                        value,
                        format!("{locale}.{key}"),
                        "key {key} fell back to the raw locale-qualified form for locale {locale}"
                    );
                }
            }
        }
    }

    #[test]
    fn mcp_tool_meta_ids_unchanged() {
        let meta = tool_meta();
        let actual_ids: Vec<&str> = meta.iter().map(|(id, _, _)| *id).collect();

        assert_eq!(actual_ids, EXPECTED_TOOL_IDS);
    }

    #[test]
    fn mcp_tool_list_connections_name_differs_between_locales() {
        let english = dbflux_i18n::t!("settings.mcp.tool.list_connections.name", locale = "en");
        let spanish = dbflux_i18n::t!("settings.mcp.tool.list_connections.name", locale = "es");

        assert_eq!(english, "List Connections");
        assert_eq!(spanish, "Listar conexiones");
        assert_ne!(english, spanish);
    }
}
