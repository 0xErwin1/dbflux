use dbflux_core::chrono::Utc;
use dbflux_core::observability::{EventCategory, EventOutcome, EventRecord, EventSeverity};
use dbflux_core::{DbError, RootSectionAction, SchemaNodeId};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::Context;
use uuid::Uuid;

use super::resolve_text;
use crate::{ContextMenuAction, ContextMenuItem, Sidebar, SidebarEvent};

/// A declared item action the user picked, with everything needed to confirm,
/// run and audit it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootSectionActionRequest {
    pub profile_id: Uuid,
    pub section_id: String,
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
        let Some(action) = cached
            .section(&section_id)
            .and_then(|section| section.item_action(action_id))
            .cloned()
        else {
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
                item_id,
                item_label,
                action,
            },
            cx,
        );
    }

    /// Asks for confirmation when the action's classification requires it,
    /// otherwise runs it right away.
    pub fn dispatch_root_section_action(
        &mut self,
        request: RootSectionActionRequest,
        cx: &mut Context<Self>,
    ) {
        if request.action.requires_confirmation() {
            self.show_root_section_action_confirm(request, cx);
        } else {
            self.run_root_section_action(request, cx);
        }
    }

    /// Runs a confirmed action on the background executor, audits it under
    /// `Governance`, reports a failure and lists the section again on success.
    pub(crate) fn run_root_section_action(
        &mut self,
        request: RootSectionActionRequest,
        cx: &mut Context<Self>,
    ) {
        let state = self.app_state.read(cx);
        let Some(connection) = state
            .connections()
            .get(&request.profile_id)
            .map(|connected| connected.connection.clone())
        else {
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    root_section_action_failed_message(
                        &request,
                        &dbflux_i18n::t!("connections.toast.no_active_connection"),
                    ),
                ),
                cx,
            );
            return;
        };
        let audit_service = state.audit_service().clone();

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
                if let Err(error) = audit_service.record(event) {
                    log::warn!("Failed to record root section action audit event: {error}");
                }

                result
            }
        });

        cx.spawn(async move |this, cx| match background_task.await {
            Ok(()) => {
                if let Err(error) = this.update(cx, |sidebar, cx| {
                    sidebar.refresh_root_section(request.profile_id, &request.section_id, cx);
                }) {
                    log::warn!("Sidebar dropped before refreshing a root section: {error}");
                }
            }
            Err(error) => report_error_async(
                UserFacingError::new(
                    ErrorKind::Driver,
                    root_section_action_failed_message(&request, &error.to_string()),
                ),
                cx,
            ),
        })
        .detach();
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
