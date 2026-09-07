use crate::icons::AppIcon;
use crate::modals::shell::{ModalShell, ModalVariant};
use crate::primitives::Icon;
use crate::semantic::BannerColors as SemBannerColors;
use crate::tokens::{FontSizes, Spacing};
use crate::typography::AppFonts;
use dbflux_core::{
    ColumnSnapshot, IndexSnapshot, QueryTableRef, SchemaChange, SchemaDriftDetected,
};
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::button::{Button, ButtonVariants};

/// Event emitted when the user clicks "Refresh and re-run".
#[derive(Clone, Debug)]
pub struct SchemaDriftRefresh;

/// Event emitted when the user clicks "Continue with stale schema".
#[derive(Clone, Debug)]
pub struct SchemaDriftContinue;

/// Event emitted when the user dismisses the modal (close button / ESC).
#[derive(Clone, Debug)]
pub struct SchemaDriftDismissed;

/// Modal body for schema-drift notification.
///
/// Presents a per-table diff table with column-level changes highlighted
/// in amber. Footer offers two primary actions: refresh-and-rerun or
/// continue with the stale schema.
///
/// This is an `Entity<ModalSchemaDrift>` rendered inside a `ModalShell`
/// by the code document's render loop via the `pending_schema_drift` pattern.
pub struct ModalSchemaDrift {
    drift: Option<SchemaDriftDetected>,
    visible: bool,
    loading: bool,
}

impl ModalSchemaDrift {
    pub fn new(_cx: &mut Context<Self>) -> Self {
        Self {
            drift: None,
            visible: false,
            loading: false,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    /// Open the modal with the given drift payload.
    pub fn open(&mut self, drift: SchemaDriftDetected, cx: &mut Context<Self>) {
        self.drift = Some(drift);
        self.visible = true;
        self.loading = false;
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.drift = None;
        self.loading = false;
        cx.notify();
    }

    /// Mark the modal as loading (while refresh is in progress).
    pub fn set_loading(&mut self, loading: bool, cx: &mut Context<Self>) {
        self.loading = loading;
        cx.notify();
    }
}

impl Render for ModalSchemaDrift {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let Some(ref drift) = self.drift else {
            return div().into_any_element();
        };

        let loading = self.loading;
        let theme = cx.theme();
        let border_color = theme.border;
        let muted = theme.muted_foreground;
        let warning_bg = SemBannerColors::for_current(cx).warning_bg;

        // Build the body: one section per drifted table.
        let mut body = div().flex().flex_col().gap(Spacing::MD);

        for diff in &drift.diffs {
            let table_label = format_table_ref(&diff.table);

            let mut section = div().flex().flex_col().gap(Spacing::XS);

            // Table heading row.
            section = section.child(
                div()
                    .text_size(FontSizes::SM)
                    .font_weight(gpui::FontWeight::SEMIBOLD)
                    .text_color(theme.foreground)
                    .child(table_label),
            );

            // Column diff table.
            let header_row = div()
                .flex()
                .items_center()
                .px(Spacing::SM)
                .py(Spacing::XS)
                .border_b_1()
                .border_color(border_color)
                .child(
                    div()
                        .w_1_3()
                        .text_size(FontSizes::XS)
                        .font_weight(gpui::FontWeight::BOLD)
                        .text_color(muted)
                        .child(dbflux_i18n::t!("modals.schema_drift.column_header")),
                )
                .child(
                    div()
                        .w_1_3()
                        .text_size(FontSizes::XS)
                        .font_weight(gpui::FontWeight::BOLD)
                        .text_color(muted)
                        .child(dbflux_i18n::t!("modals.schema_drift.local_header")),
                )
                .child(
                    div()
                        .flex_1()
                        .text_size(FontSizes::XS)
                        .font_weight(gpui::FontWeight::BOLD)
                        .text_color(muted)
                        .child(dbflux_i18n::t!("modals.schema_drift.remote_header")),
                );

            let mut diff_rows = div()
                .flex()
                .flex_col()
                .border_1()
                .border_color(border_color)
                .rounded(px(3.0))
                .overflow_hidden()
                .child(header_row);

            for change in &diff.changes {
                let row = render_change_row(change, warning_bg, theme);
                diff_rows = diff_rows.child(row);
            }

            section = section.child(diff_rows);
            body = body.child(section);
        }

        // Footer: Continue (ghost) | Refresh (primary).
        let on_continue = cx.listener(|this, _event: &gpui::ClickEvent, _, cx| {
            cx.emit(SchemaDriftContinue);
            this.close(cx);
        });

        let on_refresh = cx.listener(|this, _event: &gpui::ClickEvent, _, cx| {
            this.set_loading(true, cx);
            cx.emit(SchemaDriftRefresh);
        });

        let on_close = cx.listener(|this, _event: &gpui::ClickEvent, _, cx| {
            cx.emit(SchemaDriftDismissed);
            this.close(cx);
        });

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Button::new("drift-continue")
                    .label(dbflux_i18n::t!("modals.schema_drift.continue_stale"))
                    .ghost()
                    .on_click(on_continue),
            )
            .child(div().flex_1())
            .child(
                Button::new("drift-close")
                    .label(dbflux_i18n::t!("modals.schema_drift.cancel"))
                    .on_click(on_close),
            )
            .child(if loading {
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .child(Icon::new(AppIcon::Loader).size(px(12.0)).muted()) // guardrail-allow: 12px icon size, no ICON_XS token
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .text_color(theme.muted_foreground)
                            .child(dbflux_i18n::t!("modals.schema_drift.refreshing")),
                    )
                    .into_any_element()
            } else {
                Button::new("drift-refresh")
                    .label(dbflux_i18n::t!("modals.schema_drift.refresh"))
                    .primary()
                    .on_click(on_refresh)
                    .into_any_element()
            });

        ModalShell::new(
            dbflux_i18n::t!("modals.schema_drift.title"),
            body.into_any_element(),
            footer.into_any_element(),
        )
        .variant(ModalVariant::Default)
        .width(px(640.0))
        .into_any_element()
    }
}

impl EventEmitter<SchemaDriftRefresh> for ModalSchemaDrift {}
impl EventEmitter<SchemaDriftContinue> for ModalSchemaDrift {}
impl EventEmitter<SchemaDriftDismissed> for ModalSchemaDrift {}

fn format_table_ref(table_ref: &QueryTableRef) -> String {
    match (&table_ref.database, &table_ref.schema) {
        (Some(db), Some(schema)) => format!("{}.{}.{}", db, schema, table_ref.table),
        (None, Some(schema)) => format!("{}.{}", schema, table_ref.table),
        _ => table_ref.table.clone(),
    }
}

fn render_change_row(
    change: &SchemaChange,
    warning_bg: gpui::Hsla,
    theme: &gpui_component::Theme,
) -> gpui::Div {
    let muted = theme.muted_foreground;

    match change {
        SchemaChange::ColumnAdded(snap) => drift_row(
            warning_bg,
            "—",
            &snap.name,
            &snap_label(snap),
            &dbflux_i18n::t!("modals.schema_drift.change.new_column"),
            muted,
        ),
        SchemaChange::ColumnRemoved(snap) => drift_row(
            warning_bg,
            &snap.name,
            &snap_label(snap),
            "—",
            &dbflux_i18n::t!("modals.schema_drift.change.removed"),
            muted,
        ),
        SchemaChange::ColumnTypeChanged { before, after } => drift_row(
            warning_bg,
            &before.name,
            &snap_label(before),
            &snap_label(after),
            &dbflux_i18n::t!("modals.schema_drift.change.type_changed"),
            muted,
        ),
        SchemaChange::NullabilityChanged {
            column,
            before,
            after,
        } => {
            let nullable_label = dbflux_i18n::t!("modals.schema_drift.change.nullable");
            let before_label = if *before { &nullable_label } else { "NOT NULL" };
            let after_label = if *after { &nullable_label } else { "NOT NULL" };
            drift_row(
                warning_bg,
                column,
                before_label,
                after_label,
                &dbflux_i18n::t!("modals.schema_drift.change.nullability_changed"),
                muted,
            )
        }
        SchemaChange::PrimaryKeyChanged { before, after } => drift_row(
            warning_bg,
            &dbflux_i18n::t!("modals.schema_drift.change.primary_key"),
            &before.join(", "),
            &after.join(", "),
            &dbflux_i18n::t!("modals.schema_drift.change.pk_changed"),
            muted,
        ),
        SchemaChange::ForeignKeyChanged => drift_row(
            warning_bg,
            &dbflux_i18n::t!("modals.schema_drift.change.foreign_keys"),
            &dbflux_i18n::t!("modals.schema_drift.change.cached"),
            &dbflux_i18n::t!("modals.schema_drift.change.changed"),
            &dbflux_i18n::t!("modals.schema_drift.change.fk_changed"),
            muted,
        ),
        SchemaChange::DefaultChanged {
            column,
            before,
            after,
        } => drift_row(
            warning_bg,
            column,
            before.as_deref().unwrap_or("—"),
            after.as_deref().unwrap_or("—"),
            &dbflux_i18n::t!("modals.schema_drift.change.default_changed"),
            muted,
        ),
        SchemaChange::IndexAdded(snap) => drift_row(
            warning_bg,
            &snap.name,
            "—",
            &index_label(snap),
            &dbflux_i18n::t!("modals.schema_drift.change.index_added"),
            muted,
        ),
        SchemaChange::IndexRemoved(snap) => drift_row(
            warning_bg,
            &snap.name,
            &index_label(snap),
            "—",
            &dbflux_i18n::t!("modals.schema_drift.change.index_removed"),
            muted,
        ),
    }
}

fn snap_label(snap: &ColumnSnapshot) -> String {
    let pk = if snap.is_primary_key { " PK" } else { "" };
    let null_mark = if snap.nullable { "" } else { " NOT NULL" };
    format!("{}{}{}", snap.type_name, pk, null_mark)
}

fn index_label(snap: &IndexSnapshot) -> String {
    let unique = if snap.is_unique { " UNIQUE" } else { "" };
    format!("({}){}", snap.columns.join(", "), unique)
}

fn drift_row(
    warning_bg: gpui::Hsla,
    column: &str,
    before: &str,
    after: &str,
    note: &str,
    muted: gpui::Hsla,
) -> gpui::Div {
    div()
        .flex()
        .items_center()
        .px(Spacing::SM)
        .py(Spacing::XS)
        .bg(warning_bg)
        .child(
            div()
                .w_1_3()
                .text_size(FontSizes::XS)
                .font_family(AppFonts::MONO)
                .text_color(muted)
                .child(column.to_string()),
        )
        .child(
            div()
                .w_1_3()
                .text_size(FontSizes::XS)
                .font_family(AppFonts::MONO)
                .text_color(muted)
                .child(before.to_string()),
        )
        .child(
            div()
                .flex_1()
                .flex()
                .items_center()
                .gap(Spacing::XS)
                .child(
                    div()
                        .text_size(FontSizes::XS)
                        .font_family(AppFonts::MONO)
                        .text_color(muted)
                        .child(after.to_string()),
                )
                .child(
                    div()
                        .text_size(FontSizes::XS)
                        .text_color(muted)
                        .italic()
                        .child(format!("· {}", note)),
                ),
        )
}

// ---------------------------------------------------------------------------
// Tests — catalog key resolution
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #[test]
    fn schema_drift_keys_resolve_in_both_locales() {
        let keys = [
            "modals.schema_drift.title",
            "modals.schema_drift.column_header",
            "modals.schema_drift.local_header",
            "modals.schema_drift.remote_header",
            "modals.schema_drift.change.new_column",
            "modals.schema_drift.change.removed",
            "modals.schema_drift.change.type_changed",
            "modals.schema_drift.change.nullability_changed",
            "modals.schema_drift.change.pk_changed",
            "modals.schema_drift.change.fk_changed",
            "modals.schema_drift.change.default_changed",
            "modals.schema_drift.change.index_added",
            "modals.schema_drift.change.index_removed",
            "modals.schema_drift.change.cached",
            "modals.schema_drift.change.changed",
            "modals.schema_drift.change.primary_key",
            "modals.schema_drift.change.foreign_keys",
            "modals.schema_drift.change.nullable",
            "modals.schema_drift.continue_stale",
            "modals.schema_drift.cancel",
            "modals.schema_drift.refreshing",
            "modals.schema_drift.refresh",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(
                !en.is_empty() && !en.starts_with("en."),
                "en missing for {key}, got {en:?}"
            );
            assert!(
                !es.is_empty() && !es.starts_with("es."),
                "es missing for {key}, got {es:?}"
            );
        }
    }

    #[test]
    fn schema_drift_refresh_differs_between_locales() {
        let en = dbflux_i18n::t!("modals.schema_drift.refresh", locale = "en");
        let es = dbflux_i18n::t!("modals.schema_drift.refresh", locale = "es");
        assert_eq!(en, "Refresh and re-run");
        assert_eq!(es, "Actualizar y volver a ejecutar");
        assert_ne!(en, es);
    }

    #[test]
    fn schema_drift_nullable_label_diverges_between_locales() {
        let en = dbflux_i18n::t!("modals.schema_drift.change.nullable", locale = "en");
        let es = dbflux_i18n::t!("modals.schema_drift.change.nullable", locale = "es");
        assert_eq!(en, "nullable");
        assert_eq!(es, "admite NULL");
        assert_ne!(en, es);
    }
}
