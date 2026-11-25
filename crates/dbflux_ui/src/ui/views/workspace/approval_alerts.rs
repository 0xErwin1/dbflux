//! Alerts for MCP executions that start waiting for approval.
//!
//! The standalone `dbflux mcp` process shares the approval queue with the app
//! through `dbflux.db` and has no channel to push new requests, so the
//! workspace polls the queue. Any change refreshes the open approvals tab; a
//! new request also raises a desktop notification while DBFlux is in the
//! background and a toast while it is focused.

use std::collections::HashSet;
use std::time::Duration;

use super::notifications::PendingApproval;
use super::*;
use dbflux_ui_base::toast::{Toast, ToastAction};
use gpui::{SystemNotification, SystemNotificationAction, SystemNotificationResponse};

const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Tag prefix of a notification about one request; the request id follows.
const SINGLE_TAG_PREFIX: &str = "mcp-approval:";

/// Tag of a notification that announces several requests at once.
const MANY_TAG: &str = "mcp-approvals";

const REVIEW_ACTION_ID: &str = "review";

/// Remembers which pending requests were already announced.
#[derive(Default)]
pub(super) struct ApprovalAlertTracker {
    announced: HashSet<String>,
}

impl ApprovalAlertTracker {
    /// Compares the queue with the previous poll: returns the ids of
    /// `pending_ids` not announced before, in their order, and forgets
    /// announced ids that are no longer pending.
    pub(super) fn observe(&mut self, pending_ids: &[String]) -> QueueChange {
        let previous_len = self.announced.len();
        self.announced.retain(|id| pending_ids.contains(id));
        let removed_any = self.announced.len() != previous_len;

        let new_ids: Vec<String> = pending_ids
            .iter()
            .filter(|id| self.announced.insert((*id).clone()))
            .cloned()
            .collect();

        QueueChange {
            changed: removed_any || !new_ids.is_empty(),
            new_ids,
        }
    }
}

/// What changed in the approval queue since the previous poll.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct QueueChange {
    /// Requests that joined the queue.
    pub(super) new_ids: Vec<String>,
    /// Whether any request joined or left the queue.
    pub(super) changed: bool,
}

/// What a click on an alert opens.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ApprovalAlertTarget {
    /// The approvals tab focused on one request.
    Request(String),
    /// The approvals tab with no particular request focused.
    Queue,
}

impl ApprovalAlertTarget {
    pub(super) fn tag(&self) -> String {
        match self {
            Self::Request(id) => format!("{SINGLE_TAG_PREFIX}{id}"),
            Self::Queue => MANY_TAG.to_string(),
        }
    }

    /// The target of a notification tag, or `None` for a tag this module did
    /// not post.
    pub(super) fn from_tag(tag: &str) -> Option<Self> {
        if tag == MANY_TAG {
            return Some(Self::Queue);
        }

        tag.strip_prefix(SINGLE_TAG_PREFIX)
            .filter(|id| !id.is_empty())
            .map(|id| Self::Request(id.to_string()))
    }
}

/// Title, body and target of one alert.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ApprovalAlert {
    pub(super) title: String,
    pub(super) body: String,
    pub(super) target: ApprovalAlertTarget,
}

impl Workspace {
    /// Starts the poll of the approval queue; the task lives as long as the
    /// workspace.
    pub(super) fn spawn_approval_alerts(window: &mut Window, cx: &mut Context<Self>) -> Task<()> {
        Self::subscribe_approval_notification_clicks(window, cx);

        cx.spawn_in(window, async move |workspace, cx| {
            loop {
                cx.background_executor().timer(POLL_INTERVAL).await;

                let polled = workspace.update_in(cx, |workspace, window, cx| {
                    workspace.poll_approval_alerts(window, cx);
                });

                if polled.is_err() {
                    break;
                }
            }
        })
    }

    /// Focuses this window and opens the approvals tab when the user clicks
    /// a desktop notification this module posted.
    fn subscribe_approval_notification_clicks(window: &mut Window, cx: &mut Context<Self>) {
        let workspace = cx.entity().downgrade();
        let window_handle = window.window_handle();

        cx.on_system_notification_response(move |response: SystemNotificationResponse, cx| {
            let Some(target) = ApprovalAlertTarget::from_tag(&response.tag) else {
                return;
            };
            let Some(workspace) = workspace.upgrade() else {
                return;
            };

            let opened = window_handle.update(cx, |_, window, cx| {
                window.activate_window();
                workspace.update(cx, |workspace, cx| {
                    workspace.open_approval_alert_target(target, window, cx);
                });
            });

            if let Err(error) = opened {
                log::warn!("Failed to open MCP approvals from a notification: {error}");
            }
        });
    }

    fn poll_approval_alerts(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let pending = self.pending_approvals(cx);
        let pending_ids: Vec<String> = pending.iter().map(|request| request.id.clone()).collect();

        // Requests seen while alerts are off still count as announced, so
        // turning alerts on does not replay the whole queue.
        let change = self.approval_alerts.observe(&pending_ids);

        // The standalone `dbflux mcp` process changes the queue without
        // raising runtime events here, so an open approvals tab would keep
        // showing the old list.
        if change.changed {
            self.app_state.update(cx, |_, cx| {
                cx.emit(McpRuntimeEventRaised {
                    event: dbflux_mcp::McpRuntimeEvent::PendingExecutionsUpdated,
                });
            });
        }

        let new_ids = change.new_ids;

        if new_ids.is_empty()
            || !self
                .app_state
                .read(cx)
                .general_settings()
                .mcp_approval_notifications
        {
            return;
        }

        let new_requests: Vec<&PendingApproval> = pending
            .iter()
            .filter(|request| new_ids.contains(&request.id))
            .collect();

        let Some(alert) = self.approval_alert(&new_requests, cx) else {
            return;
        };

        if window.is_window_active() {
            self.show_approval_toast(alert, window, cx);
        } else {
            cx.show_system_notification(SystemNotification {
                tag: alert.target.tag().into(),
                title: alert.title.into(),
                body: alert.body.into(),
                actions: vec![SystemNotificationAction {
                    id: REVIEW_ACTION_ID.into(),
                    label: dbflux_i18n::t!("notifications.approval.review").into(),
                }],
            });
        }
    }

    fn approval_alert(&self, new_requests: &[&PendingApproval], cx: &App) -> Option<ApprovalAlert> {
        let title = dbflux_i18n::t!("notifications.approval.alert_title");

        match new_requests {
            [] => None,
            [request] => Some(ApprovalAlert {
                title,
                body: dbflux_i18n::t!(
                    "notifications.approval.alert_body",
                    client = request.actor_id,
                    tool = request.tool_id,
                    classification = request.classification_label,
                    connection = self.approval_connection_name(&request.connection_id, cx)
                ),
                target: ApprovalAlertTarget::Request(request.id.clone()),
            }),
            many => Some(ApprovalAlert {
                title,
                body: dbflux_i18n::t!("notifications.approval.alert_body_many", count = many.len()),
                target: ApprovalAlertTarget::Queue,
            }),
        }
    }

    fn show_approval_toast(
        &mut self,
        alert: ApprovalAlert,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let workspace = cx.entity().downgrade();
        let window_handle = window.window_handle();
        let target = alert.target;

        Toast::info(alert.title)
            .body(alert.body)
            .action(
                ToastAction::new(
                    "mcp-approval-review",
                    dbflux_i18n::t!("notifications.approval.review"),
                )
                .primary()
                .dismisses()
                .on_click(move |cx| {
                    let Some(workspace) = workspace.upgrade() else {
                        return;
                    };
                    let target = target.clone();

                    let opened = window_handle.update(cx, |_, window, cx| {
                        workspace.update(cx, |workspace, cx| {
                            workspace.open_approval_alert_target(target, window, cx);
                        });
                    });

                    if let Err(error) = opened {
                        log::warn!("Failed to open MCP approvals from a toast: {error}");
                    }
                }),
            )
            .push(cx);
    }

    fn open_approval_alert_target(
        &mut self,
        target: ApprovalAlertTarget,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match target {
            ApprovalAlertTarget::Request(pending_id) => {
                self.review_approval(pending_id, window, cx)
            }
            ApprovalAlertTarget::Queue => self.open_mcp_approvals(window, cx),
        }
    }
}

#[cfg(test)]
mod tests {
    // A glob import would bring in `gpui::test` through the workspace
    // prelude and shadow the built-in `#[test]`.
    use super::{ApprovalAlertTarget, ApprovalAlertTracker, QueueChange, SINGLE_TAG_PREFIX};

    fn ids(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn observe_reports_every_id_the_first_time() {
        let mut tracker = ApprovalAlertTracker::default();

        assert_eq!(
            tracker.observe(&ids(&["a", "b"])),
            QueueChange {
                new_ids: ids(&["a", "b"]),
                changed: true,
            }
        );
    }

    #[test]
    fn observe_skips_ids_already_announced() {
        let mut tracker = ApprovalAlertTracker::default();
        tracker.observe(&ids(&["a"]));

        assert_eq!(tracker.observe(&ids(&["a", "b"])).new_ids, ids(&["b"]));
        assert_eq!(
            tracker.observe(&ids(&["a", "b"])),
            QueueChange {
                new_ids: Vec::new(),
                changed: false,
            }
        );
    }

    #[test]
    fn observe_reports_a_request_leaving_the_queue_as_a_change() {
        let mut tracker = ApprovalAlertTracker::default();
        tracker.observe(&ids(&["a", "b"]));

        assert_eq!(
            tracker.observe(&ids(&["b"])),
            QueueChange {
                new_ids: Vec::new(),
                changed: true,
            }
        );
    }

    #[test]
    fn observe_forgets_ids_that_left_the_queue() {
        let mut tracker = ApprovalAlertTracker::default();
        tracker.observe(&ids(&["a"]));
        tracker.observe(&ids(&[]));

        assert_eq!(tracker.observe(&ids(&["a"])).new_ids, ids(&["a"]));
    }

    #[test]
    fn tag_round_trips_to_its_target() {
        for target in [
            ApprovalAlertTarget::Request("01J-pending".to_string()),
            ApprovalAlertTarget::Queue,
        ] {
            assert_eq!(ApprovalAlertTarget::from_tag(&target.tag()), Some(target));
        }
    }

    #[test]
    fn from_tag_ignores_foreign_and_empty_tags() {
        assert_eq!(ApprovalAlertTarget::from_tag("update-available"), None);
        assert_eq!(ApprovalAlertTarget::from_tag(SINGLE_TAG_PREFIX), None);
    }
}
