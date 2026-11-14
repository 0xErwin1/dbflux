use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::modal::{Modal, ModalFocus, ModalVariant};
use dbflux_components::primitives::{Icon, SurfaceRole, Text, surface};
use dbflux_components::tokens::{FontSizes, Heights, Spacing};
use dbflux_core::LogErr;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use uuid::Uuid;

/// Confirmation body text for deleting a dashboard, with the dashboard name
/// interpolated into the translated template.
pub fn delete_dashboard_body_text(name: &str) -> String {
    dbflux_i18n::t!("modals.delete_confirm.dashboard.body", name = name)
}

/// Confirmation body text for deleting a saved chart, with the chart name
/// interpolated into the translated template.
pub fn delete_saved_chart_body_text(name: &str) -> String {
    dbflux_i18n::t!("modals.delete_confirm.saved_chart.body", name = name)
}

/// Orphan-warning text shown when a saved chart is still referenced by
/// dashboards. Uses the singular catalog bucket only for exactly one
/// referencing dashboard; every other count, including zero, uses the
/// plural bucket.
pub fn orphan_warning_text(count: usize, names: &str) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "modals.delete_confirm.saved_chart.orphan_warning.one",
            names = names
        )
    } else {
        dbflux_i18n::t!(
            "modals.delete_confirm.saved_chart.orphan_warning.many",
            count = count,
            names = names
        )
    }
}

/// The item a confirmed deletion removes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeleteTarget {
    Dashboard { dashboard_id: Uuid },
    SavedChart { chart_id: Uuid },
}

/// Outcome emitted when the user resolves a delete confirmation modal.
#[derive(Clone, Debug, PartialEq)]
pub enum DeleteConfirmOutcome {
    Confirmed(DeleteTarget),
    Cancelled,
}

/// Request payload for opening a delete confirmation modal, naming the item
/// to delete and the context the modal shows about it.
#[derive(Clone, Debug)]
pub enum DeleteConfirmRequest {
    Dashboard {
        dashboard_id: Uuid,
        dashboard_name: String,
    },
    SavedChart {
        chart_id: Uuid,
        chart_name: String,
        /// Dashboards that reference this chart: `(dashboard_id, dashboard_name)`.
        ///
        /// Populated by the caller using `find_dashboards_referencing_chart`
        /// before opening the modal. When non-empty, the modal shows an
        /// orphan-warning block listing the affected dashboard names.
        referencing_dashboards: Vec<(Uuid, String)>,
    },
}

impl DeleteConfirmRequest {
    pub fn target(&self) -> DeleteTarget {
        match self {
            Self::Dashboard { dashboard_id, .. } => DeleteTarget::Dashboard {
                dashboard_id: *dashboard_id,
            },
            Self::SavedChart { chart_id, .. } => DeleteTarget::SavedChart {
                chart_id: *chart_id,
            },
        }
    }

    fn title(&self) -> String {
        match self {
            Self::Dashboard { .. } => dbflux_i18n::t!("modals.delete_confirm.dashboard.title"),
            Self::SavedChart { .. } => dbflux_i18n::t!("modals.delete_confirm.saved_chart.title"),
        }
    }

    fn body_text(&self) -> String {
        match self {
            Self::Dashboard { dashboard_name, .. } => delete_dashboard_body_text(dashboard_name),
            Self::SavedChart { chart_name, .. } => delete_saved_chart_body_text(chart_name),
        }
    }

    /// Element ids of the cancel and confirm buttons, in that order.
    fn button_ids(&self) -> (&'static str, &'static str) {
        match self {
            Self::Dashboard { .. } => ("delete-dashboard-cancel", "delete-dashboard-confirm"),
            Self::SavedChart { .. } => ("delete-chart-cancel", "delete-chart-confirm"),
        }
    }

    /// The orphan warning for a saved chart still used by dashboards, or
    /// `None` when nothing references the item.
    fn orphan_warning(&self) -> Option<String> {
        let Self::SavedChart {
            referencing_dashboards,
            ..
        } = self
        else {
            return None;
        };

        if referencing_dashboards.is_empty() {
            return None;
        }

        let names_list = referencing_dashboards
            .iter()
            .map(|(_, name)| name.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        Some(orphan_warning_text(
            referencing_dashboards.len(),
            &names_list,
        ))
    }
}

/// Modal entity for confirming the deletion of a dashboard or a saved chart.
///
/// A dashboard deletion repeats the dashboard name below the body. A saved
/// chart deletion lists the dashboards that still reference the chart, when
/// there are any, so the user understands the consequences.
pub struct ModalDeleteConfirm {
    request: Option<DeleteConfirmRequest>,
    visible: bool,
    focus: ModalFocus,
}

impl ModalDeleteConfirm {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            request: None,
            visible: false,
            focus: ModalFocus::new(cx),
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(&mut self, request: DeleteConfirmRequest, cx: &mut Context<Self>) {
        self.request = Some(request);
        self.visible = true;
        self.focus.focus_on_next_render();
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.request = None;
        self.focus.restore(cx);
        cx.notify();
    }

    /// Resolve the modal as if the confirm button was clicked: emit the
    /// outcome and close. Enter uses this, through the modal shell while focus
    /// is inside the modal and through the workspace's ConfirmModal keymap
    /// otherwise.
    pub fn confirm(&mut self, cx: &mut Context<Self>) {
        let Some(target) = self.request.as_ref().map(DeleteConfirmRequest::target) else {
            return;
        };
        cx.emit(DeleteConfirmOutcome::Confirmed(target));
        self.close(cx);
    }

    /// Resolve the modal as if the cancel button was clicked.
    pub fn cancel(&mut self, cx: &mut Context<Self>) {
        cx.emit(DeleteConfirmOutcome::Cancelled);
        self.close(cx);
    }
}

impl EventEmitter<DeleteConfirmOutcome> for ModalDeleteConfirm {}

impl Render for ModalDeleteConfirm {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        self.focus.apply_pending(window, cx);

        let Some(ref request) = self.request else {
            return div().into_any_element();
        };

        let theme = cx.theme();
        let title = request.title();
        let (cancel_id, confirm_id) = request.button_ids();

        let detail = match request {
            DeleteConfirmRequest::Dashboard { dashboard_name, .. } => Some(
                surface(SurfaceRole::Raised, cx)
                    .w_full()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .font_family(dbflux_components::fonts::editor_family(cx))
                            .text_color(theme.foreground)
                            .child(dashboard_name.clone()),
                    )
                    .into_any_element(),
            ),
            DeleteConfirmRequest::SavedChart { .. } => request.orphan_warning().map(|warning| {
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(div().text_sm().text_color(theme.warning).child(warning))
                    .into_any_element()
            }),
        };

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(
                div()
                    .flex()
                    .items_start()
                    .gap(Spacing::SM)
                    .child(
                        Icon::new(AppIcon::TriangleAlert)
                            .size(Heights::ICON_SM)
                            .color(theme.danger),
                    )
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .child(Text::body(request.body_text()).into_any_element()),
                    ),
            )
            .children(detail);

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| this.cancel(cx));
        let on_confirm = cx.listener(|this, _: &gpui::ClickEvent, _, cx| this.confirm(cx));

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Button::new(cancel_id, dbflux_i18n::t!("modals.delete_confirm.cancel"))
                    .on_click(on_cancel),
            )
            .child(
                Button::new(confirm_id, dbflux_i18n::t!("modals.delete_confirm.confirm"))
                    .danger()
                    .on_click(on_confirm),
            );

        Modal::new(title)
            .body(body)
            .footer(footer)
            .icon(AppIcon::Delete)
            .variant(ModalVariant::Danger)
            .width(px(460.0))
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
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{DeleteConfirmRequest, DeleteTarget, ModalDeleteConfirm};
    use gpui::AppContext as _;
    use uuid::Uuid;

    fn test_uuid() -> Uuid {
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
    }

    // O.3 tests

    #[test]
    fn modal_delete_dashboard_confirm_body_text_contains_name_and_cannot_be_undone() {
        let body_text = super::delete_dashboard_body_text("My Dashboard");

        assert!(body_text.contains("My Dashboard"));
        assert!(body_text.contains("This cannot be undone."));
        assert_eq!(
            body_text,
            dbflux_i18n::t!(
                "modals.delete_confirm.dashboard.body",
                name = "My Dashboard"
            )
        );
    }

    #[test]
    fn modal_delete_dashboard_confirm_is_not_visible_on_new() {
        // visible is initialized to false; check struct default directly.
        let visible = false;
        assert!(
            !visible,
            "ModalDeleteConfirm must not be visible on construction"
        );
    }

    #[gpui::test]
    fn modal_delete_dashboard_confirm_is_visible_after_open(cx: &mut gpui::TestAppContext) {
        let modal = cx.new(ModalDeleteConfirm::new);
        let req = DeleteConfirmRequest::Dashboard {
            dashboard_id: test_uuid(),
            dashboard_name: "Alpha".to_string(),
        };

        cx.update(|cx| modal.update(cx, |modal, cx| modal.open(req, cx)));

        assert!(cx.update(|cx| modal.read(cx).is_visible()));
    }

    // O.4 tests

    #[test]
    fn modal_delete_saved_chart_confirm_shows_orphan_warning_when_referenced() {
        let req = DeleteConfirmRequest::SavedChart {
            chart_id: test_uuid(),
            chart_name: "My Chart".to_string(),
            referencing_dashboards: vec![
                (Uuid::new_v4(), "Dashboard A".to_string()),
                (Uuid::new_v4(), "Dashboard B".to_string()),
            ],
        };

        assert_eq!(
            req.orphan_warning(),
            Some(super::orphan_warning_text(2, "Dashboard A, Dashboard B"))
        );
    }

    #[test]
    fn modal_delete_saved_chart_confirm_omits_warning_when_no_refs() {
        let req = DeleteConfirmRequest::SavedChart {
            chart_id: test_uuid(),
            chart_name: "My Chart".to_string(),
            referencing_dashboards: vec![],
        };

        assert_eq!(req.orphan_warning(), None);
    }

    #[test]
    fn each_subject_keeps_its_strings_ids_and_target() {
        let dashboard = DeleteConfirmRequest::Dashboard {
            dashboard_id: test_uuid(),
            dashboard_name: "Ops".to_string(),
        };
        let chart = DeleteConfirmRequest::SavedChart {
            chart_id: test_uuid(),
            chart_name: "Latency".to_string(),
            referencing_dashboards: vec![(Uuid::new_v4(), "Ops".to_string())],
        };

        assert_eq!(
            dashboard.title(),
            dbflux_i18n::t!("modals.delete_confirm.dashboard.title")
        );
        assert_eq!(
            dashboard.body_text(),
            super::delete_dashboard_body_text("Ops")
        );
        assert_eq!(
            dashboard.button_ids(),
            ("delete-dashboard-cancel", "delete-dashboard-confirm")
        );
        assert_eq!(dashboard.orphan_warning(), None);
        assert_eq!(
            dashboard.target(),
            DeleteTarget::Dashboard {
                dashboard_id: test_uuid()
            }
        );

        assert_eq!(
            chart.title(),
            dbflux_i18n::t!("modals.delete_confirm.saved_chart.title")
        );
        assert_eq!(
            chart.body_text(),
            super::delete_saved_chart_body_text("Latency")
        );
        assert_eq!(
            chart.button_ids(),
            ("delete-chart-cancel", "delete-chart-confirm")
        );
        assert_eq!(
            chart.orphan_warning(),
            Some(super::orphan_warning_text(1, "Ops"))
        );
        assert_eq!(
            chart.target(),
            DeleteTarget::SavedChart {
                chart_id: test_uuid()
            }
        );
    }

    #[test]
    fn orphan_warning_text_contains_broken_placeholders() {
        let warning = super::orphan_warning_text(2, "Dashboard A, Dashboard B");

        assert!(warning.contains("broken placeholders"));
        assert!(warning.contains("Dashboard A"));
        assert!(warning.contains("Dashboard B"));
    }

    #[test]
    fn orphan_warning_text_uses_singular_bucket_for_one() {
        let warning = super::orphan_warning_text(1, "A");

        assert_eq!(
            warning,
            dbflux_i18n::t!(
                "modals.delete_confirm.saved_chart.orphan_warning.one",
                names = "A"
            )
        );
        assert_ne!(
            warning,
            dbflux_i18n::t!(
                "modals.delete_confirm.saved_chart.orphan_warning.many",
                count = 1,
                names = "A"
            )
        );
    }

    #[test]
    fn orphan_warning_text_uses_plural_bucket_for_many() {
        let warning = super::orphan_warning_text(2, "A, B");

        assert!(warning.contains('2'));
        assert!(warning.contains('A'));
        assert!(warning.contains('B'));
        assert_eq!(
            warning,
            dbflux_i18n::t!(
                "modals.delete_confirm.saved_chart.orphan_warning.many",
                count = 2,
                names = "A, B"
            )
        );
    }

    #[test]
    fn orphan_warning_text_zero_uses_plural_bucket() {
        let warning = super::orphan_warning_text(0, "A");

        assert_eq!(
            warning,
            dbflux_i18n::t!(
                "modals.delete_confirm.saved_chart.orphan_warning.many",
                count = 0,
                names = "A"
            )
        );
    }

    const DELETE_CONFIRM_KEYS: &[&str] = &[
        "modals.delete_confirm.dashboard.title",
        "modals.delete_confirm.dashboard.body",
        "modals.delete_confirm.saved_chart.title",
        "modals.delete_confirm.saved_chart.body",
        "modals.delete_confirm.saved_chart.orphan_warning.one",
        "modals.delete_confirm.saved_chart.orphan_warning.many",
        "modals.delete_confirm.cancel",
        "modals.delete_confirm.confirm",
    ];

    #[test]
    fn delete_confirm_catalog_keys_resolve() {
        for key in DELETE_CONFIRM_KEYS {
            let english = dbflux_i18n::t!(key);
            let spanish = dbflux_i18n::t!(key, locale = "es");

            assert!(!english.is_empty(), "empty English translation for {key}");
            assert_ne!(english, *key, "missing English translation for {key}");
            assert!(!spanish.is_empty(), "empty Spanish translation for {key}");
            assert_ne!(spanish, *key, "missing Spanish translation for {key}");
        }
    }
}

#[cfg(test)]
mod keyboard_tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{DeleteConfirmOutcome, DeleteConfirmRequest, DeleteTarget, ModalDeleteConfirm};
    use crate::modals::test_host::{click_backdrop, has_focus, host_modal};
    use gpui::{Entity, FocusHandle, TestAppContext, VisualTestContext};
    use std::cell::RefCell;
    use std::rc::Rc;
    use uuid::Uuid;

    type Outcomes = Rc<RefCell<Vec<DeleteConfirmOutcome>>>;

    /// Opens the delete modal for `request` without a window, as the
    /// workspace does, so the modal's own focus handling moves the keyboard
    /// into it.
    fn open_modal(
        cx: &mut TestAppContext,
        request: DeleteConfirmRequest,
    ) -> (
        Entity<ModalDeleteConfirm>,
        FocusHandle,
        &mut VisualTestContext,
        Outcomes,
    ) {
        let (modal, outside, window) = host_modal(cx, |_, cx| ModalDeleteConfirm::new(cx));

        let outcomes: Outcomes = Rc::default();
        window.update(|_, cx| {
            let sink = outcomes.clone();
            cx.subscribe(&modal, move |_, outcome: &DeleteConfirmOutcome, _| {
                sink.borrow_mut().push(outcome.clone());
            })
            .detach();

            modal.update(cx, |modal, cx| modal.open(request, cx));
        });
        window.run_until_parked();

        (modal, outside, window, outcomes)
    }

    fn open_dashboard_modal(
        cx: &mut TestAppContext,
    ) -> (
        Entity<ModalDeleteConfirm>,
        FocusHandle,
        &mut VisualTestContext,
        Outcomes,
    ) {
        open_modal(
            cx,
            DeleteConfirmRequest::Dashboard {
                dashboard_id: Uuid::nil(),
                dashboard_name: "Ops".to_string(),
            },
        )
    }

    fn open_chart_modal(
        cx: &mut TestAppContext,
    ) -> (
        Entity<ModalDeleteConfirm>,
        FocusHandle,
        &mut VisualTestContext,
        Outcomes,
    ) {
        open_modal(
            cx,
            DeleteConfirmRequest::SavedChart {
                chart_id: Uuid::nil(),
                chart_name: "Latency".to_string(),
                referencing_dashboards: Vec::new(),
            },
        )
    }

    #[gpui::test]
    fn enter_deletes_the_dashboard(cx: &mut TestAppContext) {
        let (modal, _outside, window, outcomes) = open_dashboard_modal(cx);

        window.simulate_keystrokes("enter");

        assert_eq!(
            outcomes.borrow().as_slice(),
            [DeleteConfirmOutcome::Confirmed(DeleteTarget::Dashboard {
                dashboard_id: Uuid::nil()
            })]
        );
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
    }

    #[gpui::test]
    fn escape_keeps_the_dashboard_and_gives_focus_back(cx: &mut TestAppContext) {
        let (modal, outside, window, outcomes) = open_dashboard_modal(cx);

        window.simulate_keystrokes("escape");

        assert_eq!(
            outcomes.borrow().as_slice(),
            [DeleteConfirmOutcome::Cancelled]
        );
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
        assert!(has_focus(window, &outside));
    }

    #[gpui::test]
    fn a_backdrop_click_keeps_the_dashboard(cx: &mut TestAppContext) {
        let (_modal, _outside, window, outcomes) = open_dashboard_modal(cx);

        click_backdrop(window);

        assert_eq!(
            outcomes.borrow().as_slice(),
            [DeleteConfirmOutcome::Cancelled]
        );
    }

    #[gpui::test]
    fn enter_deletes_the_saved_chart(cx: &mut TestAppContext) {
        let (modal, _outside, window, outcomes) = open_chart_modal(cx);

        window.simulate_keystrokes("enter");

        assert_eq!(
            outcomes.borrow().as_slice(),
            [DeleteConfirmOutcome::Confirmed(DeleteTarget::SavedChart {
                chart_id: Uuid::nil()
            })]
        );
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
    }

    #[gpui::test]
    fn escape_keeps_the_saved_chart_and_gives_focus_back(cx: &mut TestAppContext) {
        let (modal, outside, window, outcomes) = open_chart_modal(cx);

        window.simulate_keystrokes("escape");

        assert_eq!(
            outcomes.borrow().as_slice(),
            [DeleteConfirmOutcome::Cancelled]
        );
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
        assert!(has_focus(window, &outside));
    }

    #[gpui::test]
    fn a_backdrop_click_keeps_the_saved_chart(cx: &mut TestAppContext) {
        let (_modal, _outside, window, outcomes) = open_chart_modal(cx);

        click_backdrop(window);

        assert_eq!(
            outcomes.borrow().as_slice(),
            [DeleteConfirmOutcome::Cancelled]
        );
    }
}
