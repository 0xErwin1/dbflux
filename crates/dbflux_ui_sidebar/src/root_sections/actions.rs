use dbflux_core::chrono::Utc;
use dbflux_core::observability::{EventCategory, EventOutcome, EventRecord, EventSeverity};
use dbflux_core::{
    Connection, DbError, MutationPolicy, ReadOnlyReason, RootSectionAction,
    RootSectionPresentation, SchemaNodeId,
};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::Context;
use std::sync::Arc;
use uuid::Uuid;

use super::resolve_text;
use crate::{ContextMenuAction, ContextMenuItem, Sidebar, SidebarEvent};

/// A declared item action the user picked, with everything needed to confirm,
/// run and audit it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootSectionActionRequest {
    pub profile_id: Uuid,
    pub section_id: String,
    /// What the section's folder opens; the document showing it is reloaded
    /// after a successful action.
    pub section_presentation: Option<RootSectionPresentation>,
    pub item_id: String,
    pub item_label: String,
    pub action: RootSectionAction,
}

impl RootSectionActionRequest {
    pub fn action_label(&self) -> String {
        resolve_text(&self.action.label)
    }

    /// The confirmation question, followed by the declared description.
    pub fn confirm_message(&self) -> String {
        let question = dbflux_i18n::t!(
            "sidebar.root_section.confirm_message",
            action = self.action_label(),
            item = self.item_label
        );

        match &self.action.description {
            Some(description) => format!("{question} {}", resolve_text(description)),
            None => question,
        }
    }
}

/// A request waiting for confirmation, bound to the connection it was made
/// on: it runs only while that connection is still the profile's.
pub(crate) struct PendingRootSectionAction {
    pub(crate) request: RootSectionActionRequest,
    /// `None` when the profile had no connection at request time.
    pub(crate) connection: Option<Arc<dyn Connection>>,
}

/// What running an action produced: the driver's result, and the error of
/// recording its audit event, reported on its own so it never turns a
/// successful action into a failed one.
pub(super) struct RootSectionActionOutcome {
    pub(super) result: Result<(), DbError>,
    pub(super) audit_error: Option<String>,
}

/// Copy of the confirmation dialog for a root section action.
pub struct RootSectionActionConfirm {
    pub title: String,
    pub message: String,
    pub confirm_label: String,
}

impl Sidebar {
    fn root_section_def(
        &self,
        profile_id: Uuid,
        section_id: &str,
    ) -> Option<&dbflux_core::RootSectionDef> {
        self.root_section_cache
            .get(&profile_id)
            .and_then(|cached| cached.section(section_id))
    }

    /// Opens what a section folder declares, if anything.
    pub(crate) fn activate_root_section(
        &mut self,
        profile_id: Uuid,
        section_id: &str,
        cx: &mut Context<Self>,
    ) {
        let Some(presentation) = self
            .root_section_def(profile_id, section_id)
            .and_then(|section| section.presentation)
        else {
            return;
        };

        cx.emit(SidebarEvent::OpenRootSection {
            profile_id,
            section_id: section_id.to_string(),
            presentation,
        });
    }

    pub(crate) fn activate_root_section_item(
        &mut self,
        profile_id: Uuid,
        section_id: &str,
        item_id: &str,
        cx: &mut Context<Self>,
    ) {
        let Some(presentation) = self
            .root_section_def(profile_id, section_id)
            .map(|section| section.item_presentation)
        else {
            return;
        };

        cx.emit(SidebarEvent::OpenRootSectionItem {
            profile_id,
            section_id: section_id.to_string(),
            item_id: item_id.to_string(),
            presentation,
        });
    }

    /// Opens the section declared to open on connection row activation. When
    /// the declarations have not arrived yet, the activation is remembered and
    /// honored once they land.
    pub(crate) fn activate_profile_root_section(
        &mut self,
        profile_id: Uuid,
        cx: &mut Context<Self>,
    ) {
        if let Some(section_id) = self.profile_activation_section(profile_id) {
            self.activate_root_section(profile_id, &section_id, cx);
        } else if self.root_section_declarations_pending(profile_id, cx) {
            self.pending_root_section_activations.insert(profile_id);
            self.spawn_fetch_root_sections(profile_id, cx);
        }
    }

    /// The section that opens when the connection row is activated.
    pub(crate) fn profile_activation_section(&self, profile_id: Uuid) -> Option<String> {
        self.root_section_cache.get(&profile_id).and_then(|cached| {
            cached
                .sections
                .iter()
                .find(|section| {
                    section.opens_on_profile_activation && section.presentation.is_some()
                })
                .map(|section| section.id.clone())
        })
    }

    /// The context menu of a section item: open, then one entry per declared
    /// action, those that need confirmation styled as dangerous.
    pub(crate) fn root_section_item_menu(&self, item_id: &str) -> Vec<ContextMenuItem> {
        let mut items = Vec::new();

        Self::append_menu_section(
            &mut items,
            [ContextMenuItem::item(
                dbflux_i18n::t!("sidebar.menu.open"),
                ContextMenuAction::Open,
            )],
        );

        let Some(SchemaNodeId::RootSectionItem {
            profile_id,
            section_id,
            ..
        }) = crate::parse_node_id(item_id)
        else {
            return items;
        };

        let Some(section) = self.root_section_def(profile_id, &section_id) else {
            return items;
        };

        let action_entries: Vec<ContextMenuItem> = section
            .item_actions
            .iter()
            .map(|action| {
                let label = resolve_text(&action.label);
                let menu_action = ContextMenuAction::RootSectionAction(action.id.clone());
                if action.requires_confirmation() {
                    ContextMenuItem::danger(label, menu_action)
                } else {
                    ContextMenuItem::item(label, menu_action)
                }
            })
            .collect();

        Self::append_menu_section(&mut items, action_entries);
        items
    }

    /// Runs the declared action `action_id` on the item row `item_id`, after
    /// a confirmation when its classification asks for one.
    pub(crate) fn request_root_section_action(
        &mut self,
        item_id: &str,
        action_id: &str,
        cx: &mut Context<Self>,
    ) {
        let Some(SchemaNodeId::RootSectionItem {
            profile_id,
            section_id,
            item_id,
        }) = crate::parse_node_id(item_id)
        else {
            return;
        };

        let Some(cached) = self.root_section_cache.get(&profile_id) else {
            return;
        };
        let Some(section) = cached.section(&section_id) else {
            return;
        };
        let section_presentation = section.presentation;
        let Some(action) = section.item_action(action_id).cloned() else {
            return;
        };

        let item_label = cached
            .listings
            .get(&section_id)
            .and_then(|listing| listing.items.iter().find(|item| item.id == item_id))
            .map(|item| item.label.clone())
            .unwrap_or_else(|| item_id.clone());

        self.dispatch_root_section_action(
            RootSectionActionRequest {
                profile_id,
                section_id,
                section_presentation,
                item_id,
                item_label,
                action,
            },
            cx,
        );
    }

    /// Binds the request to the profile's current connection, refuses it
    /// when the connection's mutation policy forbids it, then asks for
    /// confirmation when the action's classification requires it, or runs it
    /// right away.
    pub fn dispatch_root_section_action(
        &mut self,
        request: RootSectionActionRequest,
        cx: &mut Context<Self>,
    ) {
        let connected = self
            .app_state
            .read(cx)
            .connections()
            .get(&request.profile_id);
        let connection = connected.map(|connected| connected.connection.clone());
        let refusal = connected.and_then(|connected| {
            root_section_action_refusal(
                &request,
                connected.mutation_policy,
                connected.read_only_reason,
            )
        });

        if let Some(refusal) = refusal {
            report_error(UserFacingError::new(ErrorKind::User, refusal), cx);
            return;
        }

        let pending = PendingRootSectionAction {
            request,
            connection,
        };
        if pending.request.action.requires_confirmation() {
            self.show_root_section_action_confirm(pending, cx);
        } else {
            self.run_root_section_action(pending, cx);
        }
    }

    /// Runs a confirmed action on the background executor and audits it
    /// under `Governance`. It runs only on the connection it was requested on
    /// and only while the connection's mutation policy allows it.
    pub(crate) fn run_root_section_action(
        &mut self,
        pending: PendingRootSectionAction,
        cx: &mut Context<Self>,
    ) {
        let PendingRootSectionAction {
            request,
            connection: requested_connection,
        } = pending;
        let state = self.app_state.read(cx);
        let audit_service = state.audit_service().clone();
        let connected = state.connections().get(&request.profile_id);

        let refusal = match (connected, &requested_connection) {
            (Some(connected), Some(requested)) if Arc::ptr_eq(&connected.connection, requested) => {
                root_section_action_refusal(
                    &request,
                    connected.mutation_policy,
                    connected.read_only_reason,
                )
            }
            (Some(_), Some(_)) => Some(dbflux_i18n::t!(
                "sidebar.toast.root_section_action_connection_changed",
                action = request.action_label(),
                item = request.item_label
            )),
            _ => Some(root_section_action_failed_message(
                &request,
                &dbflux_i18n::t!("connections.toast.no_active_connection"),
            )),
        };
        if let Some(refusal) = refusal {
            report_error(UserFacingError::new(ErrorKind::User, refusal), cx);
            return;
        }
        let Some(connection) = requested_connection else {
            return;
        };

        let background_task = cx.background_executor().spawn({
            let request = request.clone();
            async move {
                let result = match connection.root_section_source() {
                    Some(source) => source.execute_action(
                        &request.section_id,
                        &request.item_id,
                        &request.action.id,
                    ),
                    None => Err(DbError::NotSupported(
                        "connection declares no root sections".to_string(),
                    )),
                };

                let event =
                    root_section_action_audit_event(&request, &connection.metadata().id, &result);
                let audit_error = audit_service
                    .record(event)
                    .err()
                    .map(|error| error.to_string());

                RootSectionActionOutcome {
                    result,
                    audit_error,
                }
            }
        });

        cx.spawn(async move |this, cx| {
            let outcome = background_task.await;
            if let Err(error) = this.update(cx, |sidebar, cx| {
                sidebar.finish_root_section_action(&request, outcome, cx);
            }) {
                log::warn!("Sidebar dropped before a root section action finished: {error}");
            }
        })
        .detach();
    }

    /// Reports what the action produced. On success it lists the section
    /// again and tells the workspace, which reloads the document showing it;
    /// a failed audit write is reported separately and changes neither.
    pub(super) fn finish_root_section_action(
        &mut self,
        request: &RootSectionActionRequest,
        outcome: RootSectionActionOutcome,
        cx: &mut Context<Self>,
    ) {
        if let Some(audit_error) = outcome.audit_error {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!(
                        "sidebar.toast.root_section_action_audit_failed",
                        action = request.action_label(),
                        item = request.item_label,
                        error = audit_error
                    ),
                ),
                cx,
            );
        }

        match outcome.result {
            Ok(()) => {
                self.refresh_root_section(request.profile_id, &request.section_id, cx);
                cx.emit(SidebarEvent::RootSectionChanged {
                    profile_id: request.profile_id,
                    section_id: request.section_id.clone(),
                    presentation: request.section_presentation,
                });
            }
            Err(error) => report_error(
                UserFacingError::new(
                    ErrorKind::Driver,
                    root_section_action_failed_message(request, &error.to_string()),
                ),
                cx,
            ),
        }
    }
}

/// Why the connection's mutation policy refuses `request`, if it does.
/// Actions that only read always pass. Approval routing for these actions is
/// not available yet, so an approval policy refuses the others.
fn root_section_action_refusal(
    request: &RootSectionActionRequest,
    policy: MutationPolicy,
    read_only_reason: Option<ReadOnlyReason>,
) -> Option<String> {
    if request.action.is_read_only() {
        return None;
    }

    let action = request.action_label();
    match policy {
        MutationPolicy::Allowed => None,
        MutationPolicy::ReadOnly => Some(match read_only_reason {
            Some(ReadOnlyReason::ProfileSetting) => dbflux_i18n::t!(
                "sidebar.toast.root_section_action_refused.read_only_profile",
                action = action
            ),
            Some(ReadOnlyReason::ServerEnforced) => dbflux_i18n::t!(
                "sidebar.toast.root_section_action_refused.read_only_server",
                action = action
            ),
            None => dbflux_i18n::t!(
                "sidebar.toast.root_section_action_refused.read_only",
                action = action
            ),
        }),
        MutationPolicy::ApprovalRequired => Some(dbflux_i18n::t!(
            "sidebar.toast.root_section_action_refused.approval_unavailable",
            action = action
        )),
    }
}

fn root_section_action_failed_message(request: &RootSectionActionRequest, error: &str) -> String {
    dbflux_i18n::t!(
        "sidebar.toast.root_section_action_failed",
        action = request.action_label(),
        item = request.item_label,
        error = error
    )
}

/// The `Governance` audit row of one action run.
pub(crate) fn root_section_action_audit_event(
    request: &RootSectionActionRequest,
    driver_id: &str,
    result: &Result<(), DbError>,
) -> EventRecord {
    let (severity, outcome, action_key) = match result {
        Ok(()) => (
            EventSeverity::Info,
            EventOutcome::Success,
            "root_section_action",
        ),
        Err(_) => (
            EventSeverity::Error,
            EventOutcome::Failure,
            "root_section_action_failed",
        ),
    };

    let target = format!(
        "action '{}' on item '{}' of section '{}' (classification {:?}, profile {})",
        request.action.id,
        request.item_id,
        request.section_id,
        request.action.classification,
        request.profile_id
    );
    let summary = match result {
        Ok(()) => format!("Root section {target} executed"),
        Err(error) => format!("Root section {target} failed: {error}"),
    };

    EventRecord::new(
        Utc::now().timestamp_millis(),
        severity,
        EventCategory::Governance,
        outcome,
    )
    .with_action(action_key.to_string())
    .with_summary(summary)
    .with_actor_id("ui:user")
    .with_connection_context(request.profile_id.to_string(), "", driver_id)
    .with_object_ref(
        format!("root_section:{}", request.section_id),
        request.item_id.clone(),
    )
}
