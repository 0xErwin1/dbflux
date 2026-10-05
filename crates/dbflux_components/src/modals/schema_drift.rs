use crate::controls::Button;
use crate::icons::AppIcon;
use crate::modals::modal::{Modal, ModalFocus, ModalVariant};
use crate::modals::parts::{modal_frame, modal_lead};
use crate::primitives::{Badge, BadgeTone};
use crate::tokens::{ChromeColors, ModalMetrics};
use dbflux_core::{
    ColumnSnapshot, IndexSnapshot, LogErr, QueryTableRef, SchemaChange, SchemaDriftDetected,
};
use gpui::*;
use gpui_component::ActiveTheme;

/// Width of the schema-drift dialog (P1Modals).
const SCHEMA_DRIFT_WIDTH: Pixels = px(640.0);

/// Width of the column and change-badge columns of the drift table.
const DRIFT_NAME_WIDTH: Pixels = px(140.0);
const DRIFT_NOTE_WIDTH: Pixels = px(150.0);

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
/// This is an `Entity<ModalSchemaDrift>` rendered inside a `Modal`
/// by the code document's render loop via the `pending_schema_drift` pattern.
pub struct ModalSchemaDrift {
    drift: Option<SchemaDriftDetected>,
    visible: bool,
    loading: bool,
    focus: ModalFocus,
}

impl ModalSchemaDrift {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            drift: None,
            visible: false,
            loading: false,
            focus: ModalFocus::new(cx),
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
        self.focus.focus_on_next_render();
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.drift = None;
        self.loading = false;
        self.focus.restore(cx);
        cx.notify();
    }

    /// Start the refresh, as the Refresh button does. Does nothing while a
    /// refresh is already running.
    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        if self.loading {
            return;
        }

        self.set_loading(true, cx);
        cx.emit(SchemaDriftRefresh);
    }

    /// Dismiss the modal, as the close button and Escape do.
    pub fn dismiss(&mut self, cx: &mut Context<Self>) {
        cx.emit(SchemaDriftDismissed);
        self.close(cx);
    }

    /// Mark the modal as loading (while refresh is in progress).
    pub fn set_loading(&mut self, loading: bool, cx: &mut Context<Self>) {
        self.loading = loading;
        cx.notify();
    }
}

impl Render for ModalSchemaDrift {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        self.focus.apply_pending(window, cx);

        let Some(ref drift) = self.drift else {
            return div().into_any_element();
        };

        let loading = self.loading;

        let mut body = div().flex().flex_col().gap(ModalMetrics::BODY_GAP);

        for diff in &drift.diffs {
            body = body
                .child(modal_lead(
                    dbflux_i18n::t!(
                        "modals.schema_drift.lead",
                        table = format_table_ref(&diff.table)
                    ),
                    cx,
                ))
                .child(render_diff_table(&diff.changes, cx));
        }

        let on_continue = cx.listener(|this, _event: &gpui::ClickEvent, _, cx| {
            cx.emit(SchemaDriftContinue);
            this.close(cx);
        });

        let on_refresh = cx.listener(|this, _event: &gpui::ClickEvent, _, cx| {
            this.refresh(cx);
        });

        let on_close = cx.listener(|this, _event: &gpui::ClickEvent, _, cx| {
            this.dismiss(cx);
        });

        let refresh_label = if loading {
            dbflux_i18n::t!("modals.schema_drift.refreshing")
        } else {
            dbflux_i18n::t!("modals.schema_drift.refresh")
        };

        let footer = div()
            .flex()
            .items_center()
            .gap(ModalMetrics::FOOTER_GAP)
            .child(
                Button::new(
                    "drift-continue",
                    dbflux_i18n::t!("modals.schema_drift.continue_stale"),
                )
                .on_click(on_continue),
            )
            .child(
                Button::new("drift-close", dbflux_i18n::t!("modals.schema_drift.cancel"))
                    .on_click(on_close),
            )
            .child(
                Button::new("drift-refresh", refresh_label)
                    .primary()
                    .icon(if loading {
                        AppIcon::Loader
                    } else {
                        AppIcon::RefreshCcw
                    })
                    .disabled(loading)
                    .on_click(on_refresh),
            );

        Modal::new(dbflux_i18n::t!("modals.schema_drift.title"))
            .body(body)
            .footer(footer)
            .icon(AppIcon::CircleAlert)
            .icon_color(cx.theme().warning)
            .variant(ModalVariant::Default)
            .width(SCHEMA_DRIFT_WIDTH)
            .focus_handle(self.focus.handle())
            .on_close({
                let entity = cx.entity().downgrade();
                move |_, cx| {
                    entity.update(cx, |this, cx| this.dismiss(cx)).log_err();
                }
            })
            .on_confirm({
                let entity = cx.entity().downgrade();
                move |_, cx| {
                    entity.update(cx, |this, cx| this.refresh(cx)).log_err();
                }
            })
            .confirm_enabled(!loading)
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

/// Tone of a change's badge: additions read as success, removals as danger,
/// every other change as a warning.
fn change_tone(change: &SchemaChange) -> BadgeTone {
    match change {
        SchemaChange::ColumnAdded(_) | SchemaChange::IndexAdded(_) => BadgeTone::Success,
        SchemaChange::ColumnRemoved(_) | SchemaChange::IndexRemoved(_) => BadgeTone::Danger,
        SchemaChange::ColumnTypeChanged { .. }
        | SchemaChange::NullabilityChanged { .. }
        | SchemaChange::PrimaryKeyChanged { .. }
        | SchemaChange::ForeignKeyChanged
        | SchemaChange::DefaultChanged { .. } => BadgeTone::Warning,
    }
}

/// The name, cached value, current value and note of one change.
fn change_cells(change: &SchemaChange) -> (String, String, String, String) {
    let dash = "\u{2014}".to_string();

    match change {
        SchemaChange::ColumnAdded(snap) => (
            snap.name.clone(),
            dash,
            snap_label(snap),
            dbflux_i18n::t!("modals.schema_drift.change.new_column"),
        ),
        SchemaChange::ColumnRemoved(snap) => (
            snap.name.clone(),
            snap_label(snap),
            dash,
            dbflux_i18n::t!("modals.schema_drift.change.removed"),
        ),
        SchemaChange::ColumnTypeChanged { before, after } => (
            before.name.clone(),
            snap_label(before),
            snap_label(after),
            dbflux_i18n::t!("modals.schema_drift.change.type_changed"),
        ),
        SchemaChange::NullabilityChanged {
            column,
            before,
            after,
        } => {
            let nullability = |nullable: bool| {
                if nullable {
                    dbflux_i18n::t!("modals.schema_drift.change.nullable")
                } else {
                    "NOT NULL".to_string()
                }
            };

            (
                column.clone(),
                nullability(*before),
                nullability(*after),
                dbflux_i18n::t!("modals.schema_drift.change.nullability_changed"),
            )
        }
        SchemaChange::PrimaryKeyChanged { before, after } => (
            dbflux_i18n::t!("modals.schema_drift.change.primary_key"),
            before.join(", "),
            after.join(", "),
            dbflux_i18n::t!("modals.schema_drift.change.pk_changed"),
        ),
        SchemaChange::ForeignKeyChanged => (
            dbflux_i18n::t!("modals.schema_drift.change.foreign_keys"),
            dbflux_i18n::t!("modals.schema_drift.change.cached"),
            dbflux_i18n::t!("modals.schema_drift.change.changed"),
            dbflux_i18n::t!("modals.schema_drift.change.fk_changed"),
        ),
        SchemaChange::DefaultChanged {
            column,
            before,
            after,
        } => (
            column.clone(),
            before.clone().unwrap_or_else(|| dash.clone()),
            after.clone().unwrap_or(dash),
            dbflux_i18n::t!("modals.schema_drift.change.default_changed"),
        ),
        SchemaChange::IndexAdded(snap) => (
            snap.name.clone(),
            dash,
            index_label(snap),
            dbflux_i18n::t!("modals.schema_drift.change.index_added"),
        ),
        SchemaChange::IndexRemoved(snap) => (
            snap.name.clone(),
            index_label(snap),
            dash,
            dbflux_i18n::t!("modals.schema_drift.change.index_removed"),
        ),
    }
}

/// One table's changes in a cut-8 frame (P1Modals): a header row over a
/// 30 px row per change with the column, its cached and current definition
/// and a badge naming the change.
fn render_diff_table(changes: &[SchemaChange], cx: &App) -> Div {
    let theme = cx.theme();
    let strong = ChromeColors::strong(theme);
    let muted = theme.muted_foreground;

    let columns =
        |row: Div, name: AnyElement, cached: AnyElement, now: AnyElement, note: AnyElement| {
            row.flex()
                .items_center()
                .h(ModalMetrics::TABLE_ROW_HEIGHT)
                .px(ModalMetrics::LIST_ROW_PADDING_X)
                .child(div().w(DRIFT_NAME_WIDTH).min_w_0().truncate().child(name))
                .child(div().flex_1().min_w_0().truncate().child(cached))
                .child(div().flex_1().min_w_0().truncate().child(now))
                .child(div().w(DRIFT_NOTE_WIDTH).flex().child(note))
        };

    let header = columns(
        div()
            .border_b_1()
            .border_color(theme.input)
            .text_size(ModalMetrics::TABLE_HEADER_FONT)
            .text_color(muted),
        dbflux_i18n::t!("modals.schema_drift.column_header").into_any_element(),
        dbflux_i18n::t!("modals.schema_drift.local_header").into_any_element(),
        dbflux_i18n::t!("modals.schema_drift.remote_header").into_any_element(),
        div().into_any_element(),
    );

    let rows = changes.iter().map(|change| {
        let (name, cached, now, note) = change_cells(change);

        columns(
            div()
                .border_b_1()
                .border_color(theme.table_row_border)
                .font_family(crate::fonts::editor_family(cx))
                .text_size(ModalMetrics::CODE_FONT),
            div().text_color(strong).child(name).into_any_element(),
            div().text_color(muted).child(cached).into_any_element(),
            div().text_color(strong).child(now).into_any_element(),
            Badge::new(note, change_tone(change)).into_any_element(),
        )
    });

    modal_frame(cx).child(header).children(rows)
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

// ---------------------------------------------------------------------------
// Tests — catalog key resolution
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #[test]
    fn schema_drift_keys_resolve_in_both_locales() {
        let keys = [
            "modals.schema_drift.title",
            "modals.schema_drift.lead",
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
        assert_eq!(en, "Refresh and run again");
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
