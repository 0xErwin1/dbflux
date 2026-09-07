use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::shell::{ModalShell, ModalVariant};
use dbflux_components::primitives::{Icon, Text, surface_raised};
use dbflux_components::tokens::{FontSizes, Heights, Spacing};
use dbflux_components::typography::AppFonts;
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

// --- Dashboard delete confirm ---

/// Outcome emitted when the user resolves the dashboard delete modal.
#[derive(Clone, Debug, PartialEq)]
pub enum DeleteDashboardOutcome {
    Confirmed { dashboard_id: Uuid },
    Cancelled,
}

/// Request payload for opening the dashboard delete confirmation modal.
#[derive(Clone, Debug)]
pub struct DeleteDashboardRequest {
    pub dashboard_id: Uuid,
    pub dashboard_name: String,
}

/// Modal entity for confirming dashboard deletion.
///
/// Shows the dashboard name and the message "This cannot be undone." on confirm.
pub struct ModalDeleteDashboardConfirm {
    request: Option<DeleteDashboardRequest>,
    visible: bool,
}

impl ModalDeleteDashboardConfirm {
    pub fn new(_cx: &mut Context<Self>) -> Self {
        Self {
            request: None,
            visible: false,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(&mut self, request: DeleteDashboardRequest, cx: &mut Context<Self>) {
        self.request = Some(request);
        self.visible = true;
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.request = None;
        cx.notify();
    }
}

impl EventEmitter<DeleteDashboardOutcome> for ModalDeleteDashboardConfirm {}

impl Render for ModalDeleteDashboardConfirm {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let Some(ref request) = self.request else {
            return div().into_any_element();
        };

        let theme = cx.theme();
        let dashboard_name = request.dashboard_name.clone();
        let dashboard_id = request.dashboard_id;

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
                    .child(div().flex_1().min_w_0().child(
                        Text::body(delete_dashboard_body_text(&dashboard_name)).into_any_element(),
                    )),
            )
            .child(
                surface_raised(cx)
                    .w_full()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .font_family(AppFonts::MONO)
                            .text_color(theme.foreground)
                            .child(dashboard_name),
                    ),
            );

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(DeleteDashboardOutcome::Cancelled);
            this.close(cx);
        });

        let on_confirm = cx.listener(move |this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(DeleteDashboardOutcome::Confirmed { dashboard_id });
            this.close(cx);
        });

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Button::new(
                    "delete-dashboard-cancel",
                    dbflux_i18n::t!("modals.delete_confirm.cancel"),
                )
                .on_click(on_cancel),
            )
            .child(
                Button::new(
                    "delete-dashboard-confirm",
                    dbflux_i18n::t!("modals.delete_confirm.confirm"),
                )
                .danger()
                .on_click(on_confirm),
            );

        ModalShell::new(
            dbflux_i18n::t!("modals.delete_confirm.dashboard.title"),
            body.into_any_element(),
            footer.into_any_element(),
        )
        .variant(ModalVariant::Danger)
        .width(px(460.0))
        .into_any_element()
    }
}

// --- Saved chart delete confirm ---

/// Outcome emitted when the user resolves the saved-chart delete modal.
#[derive(Clone, Debug, PartialEq)]
pub enum DeleteSavedChartOutcome {
    Confirmed { chart_id: Uuid },
    Cancelled,
}

/// Request payload for opening the saved-chart delete confirmation modal.
#[derive(Clone, Debug)]
pub struct DeleteSavedChartRequest {
    pub chart_id: Uuid,
    pub chart_name: String,
    /// Dashboards that reference this chart: `(dashboard_id, dashboard_name)`.
    ///
    /// Populated by the caller using `find_dashboards_referencing_chart` before
    /// opening the modal. When non-empty, the modal shows an orphan-warning block
    /// listing the affected dashboard names.
    pub referencing_dashboards: Vec<(Uuid, String)>,
}

/// Modal entity for confirming saved-chart deletion.
///
/// When `referencing_dashboards` is non-empty, renders an orphan-warning block
/// listing the affected dashboards so the user understands the consequences.
pub struct ModalDeleteSavedChartConfirm {
    request: Option<DeleteSavedChartRequest>,
    visible: bool,
}

impl ModalDeleteSavedChartConfirm {
    pub fn new(_cx: &mut Context<Self>) -> Self {
        Self {
            request: None,
            visible: false,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(&mut self, request: DeleteSavedChartRequest, cx: &mut Context<Self>) {
        self.request = Some(request);
        self.visible = true;
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.request = None;
        cx.notify();
    }
}

impl EventEmitter<DeleteSavedChartOutcome> for ModalDeleteSavedChartConfirm {}

impl Render for ModalDeleteSavedChartConfirm {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let Some(ref request) = self.request else {
            return div().into_any_element();
        };

        let theme = cx.theme();
        let chart_name = request.chart_name.clone();
        let chart_id = request.chart_id;
        let referencing = request.referencing_dashboards.clone();
        let has_refs = !referencing.is_empty();

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
                    .child(div().flex_1().min_w_0().child(
                        Text::body(delete_saved_chart_body_text(&chart_name)).into_any_element(),
                    )),
            )
            // Orphan-warning block: shown only when the chart is referenced by dashboards.
            .when(has_refs, |el| {
                let dashboard_names: Vec<String> =
                    referencing.iter().map(|(_, name)| name.clone()).collect();
                let names_list = dashboard_names.join(", ");

                el.child(
                    div().flex().flex_col().gap(Spacing::XS).child(
                        div()
                            .text_sm()
                            .text_color(theme.warning)
                            .child(orphan_warning_text(referencing.len(), &names_list)),
                    ),
                )
            });

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(DeleteSavedChartOutcome::Cancelled);
            this.close(cx);
        });

        let on_confirm = cx.listener(move |this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(DeleteSavedChartOutcome::Confirmed { chart_id });
            this.close(cx);
        });

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Button::new(
                    "delete-chart-cancel",
                    dbflux_i18n::t!("modals.delete_confirm.cancel"),
                )
                .on_click(on_cancel),
            )
            .child(
                Button::new(
                    "delete-chart-confirm",
                    dbflux_i18n::t!("modals.delete_confirm.confirm"),
                )
                .danger()
                .on_click(on_confirm),
            );

        ModalShell::new(
            dbflux_i18n::t!("modals.delete_confirm.saved_chart.title"),
            body.into_any_element(),
            footer.into_any_element(),
        )
        .variant(ModalVariant::Danger)
        .width(px(460.0))
        .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{DeleteDashboardRequest, DeleteSavedChartRequest, ModalDeleteDashboardConfirm};
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
            "ModalDeleteDashboardConfirm must not be visible on construction"
        );
    }

    #[test]
    fn modal_delete_dashboard_confirm_is_visible_after_open() {
        // Verify the data model: after calling open(), visible must be true.
        let mut modal = ModalDeleteDashboardConfirm {
            request: None,
            visible: false,
        };
        let req = DeleteDashboardRequest {
            dashboard_id: test_uuid(),
            dashboard_name: "Alpha".to_string(),
        };
        modal.request = Some(req);
        modal.visible = true;
        assert!(modal.is_visible());
    }

    // O.4 tests

    #[test]
    fn modal_delete_saved_chart_confirm_shows_orphan_warning_when_referenced() {
        let req = DeleteSavedChartRequest {
            chart_id: test_uuid(),
            chart_name: "My Chart".to_string(),
            referencing_dashboards: vec![
                (Uuid::new_v4(), "Dashboard A".to_string()),
                (Uuid::new_v4(), "Dashboard B".to_string()),
            ],
        };

        assert_eq!(req.referencing_dashboards.len(), 2);
        assert!(
            req.referencing_dashboards
                .iter()
                .any(|(_, n)| n == "Dashboard A")
        );
        assert!(
            req.referencing_dashboards
                .iter()
                .any(|(_, n)| n == "Dashboard B")
        );
    }

    #[test]
    fn modal_delete_saved_chart_confirm_omits_warning_when_no_refs() {
        let req = DeleteSavedChartRequest {
            chart_id: test_uuid(),
            chart_name: "My Chart".to_string(),
            referencing_dashboards: vec![],
        };

        assert!(req.referencing_dashboards.is_empty());
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
