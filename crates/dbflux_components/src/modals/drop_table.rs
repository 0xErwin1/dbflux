use crate::controls::{GpuiInput as Input, InputEvent, InputState};
use crate::modals::shell::{ModalFocus, ModalShell, ModalVariant};
use crate::primitives::{Text, surface_raised};
use crate::tokens::{FontSizes, Spacing};
use crate::typography::AppFonts;
use dbflux_core::{LogErr, RelationKind, RelationRef, SqlDialect};
use gpui::prelude::*;
use gpui::{Context, Entity, EventEmitter, Focusable, Subscription, Window, div, px};
use gpui_component::ActiveTheme;
use gpui_component::Disableable;
use gpui_component::button::{Button, ButtonVariants};

/// Outcome emitted when the user resolves the modal.
#[derive(Clone, Debug)]
pub enum DropTableOutcome {
    Confirmed,
    Cancelled,
}

/// Request payload for `pending_modal_open` on the sidebar / workspace.
#[derive(Clone, Debug)]
pub struct DropTableRequest {
    /// Short or qualified table name shown in the body.
    pub table_name: String,
    /// Schema name, when the table lives in one.
    pub schema_name: Option<String>,
    /// Dependent objects — empty if none.
    pub dependents: Vec<RelationRef>,
    /// The table reference as the connection's SQL dialect writes it, e.g.
    /// `"public"."orders"`, `` `shop`.`orders` `` or `[dbo].[orders]`.
    pub qualified_table: String,
}

impl DropTableRequest {
    /// Builds a request whose SQL preview quotes the table the way `dialect`
    /// does, so the preview matches the connection it will run against.
    pub fn new(
        table_name: String,
        schema_name: Option<String>,
        dependents: Vec<RelationRef>,
        dialect: &dyn SqlDialect,
    ) -> Self {
        let qualified_table = dialect.qualified_table(schema_name.as_deref(), &table_name);

        Self {
            table_name,
            schema_name,
            dependents,
            qualified_table,
        }
    }

    /// Build the SQL preview text for this request.
    pub fn sql_preview(&self) -> String {
        let base = format!("DROP TABLE {}", self.qualified_table);

        if self.dependents.is_empty() {
            format!("{};", base)
        } else {
            format!("{}\n  CASCADE;", base)
        }
    }
}

fn relation_kind_label(kind: &RelationKind) -> String {
    match kind {
        RelationKind::View => dbflux_i18n::t!("modals.drop_table.relation_kind.view"),
        RelationKind::MaterializedView => {
            dbflux_i18n::t!("modals.drop_table.relation_kind.materialized_view")
        }
        RelationKind::ForeignKeyChild => {
            dbflux_i18n::t!("modals.drop_table.relation_kind.foreign_key")
        }
        RelationKind::Trigger => dbflux_i18n::t!("modals.drop_table.relation_kind.trigger"),
    }
}

/// Confirmation hint shown while the typed table name does not yet match,
/// with the expected table name interpolated into the translated prompt.
fn confirm_hint(table: &str) -> String {
    dbflux_i18n::t!("modals.drop_table.confirm_prompt", table = table)
}

/// Modal entity for "drop table" with TypeToConfirm gate.
///
/// Uses `ModalShell::Danger` (560 px). The "Drop table" button is disabled
/// until the user types the exact table name in the confirmation input.
/// Listens to `InputEvent` changes on the internal `InputState` directly
/// (no `TypeToConfirm` entity needed — we compare inline to keep this self-contained).
pub struct ModalDropTable {
    request: Option<DropTableRequest>,
    visible: bool,
    confirm_input: Entity<InputState>,
    drop_enabled: bool,
    focus: ModalFocus,
    _subscription: Option<Subscription>,
}

impl ModalDropTable {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let confirm_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("modals.drop_table.confirm_placeholder"))
        });
        Self {
            request: None,
            visible: false,
            confirm_input,
            drop_enabled: false,
            focus: ModalFocus::new(cx),
            _subscription: None,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(&mut self, request: DropTableRequest, window: &mut Window, cx: &mut Context<Self>) {
        // Reset the input when opening.
        self.confirm_input.update(cx, |input, cx| {
            input.set_value(String::new(), window, cx);
        });
        self.drop_enabled = false;

        let expected = request.table_name.clone();
        let input = self.confirm_input.clone();

        let subscription = cx.subscribe_in(
            &input,
            window,
            move |this, input_state, event: &InputEvent, _, cx| {
                if !matches!(event, InputEvent::Change) {
                    return;
                }
                let typed = input_state.read(cx).value().to_string();
                let matches = typed == expected;
                if this.drop_enabled != matches {
                    this.drop_enabled = matches;
                    cx.notify();
                }
            },
        );

        self.request = Some(request);
        self.visible = true;
        self._subscription = Some(subscription);

        let input_focus = self.confirm_input.read(cx).focus_handle(cx);
        self.focus.focus(Some(&input_focus), window, cx);

        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.request = None;
        self.drop_enabled = false;
        self._subscription = None;
        self.focus.restore(cx);
        cx.notify();
    }

    /// Drop the table, as the "Drop table" button does. Does nothing until
    /// the typed name matches.
    pub fn confirm(&mut self, cx: &mut Context<Self>) {
        if !self.drop_enabled {
            return;
        }

        cx.emit(DropTableOutcome::Confirmed);
        self.close(cx);
    }

    /// Dismiss the modal without dropping anything.
    pub fn cancel(&mut self, cx: &mut Context<Self>) {
        cx.emit(DropTableOutcome::Cancelled);
        self.close(cx);
    }
}

impl EventEmitter<DropTableOutcome> for ModalDropTable {}

impl Render for ModalDropTable {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let Some(ref request) = self.request else {
            return div().into_any_element();
        };

        let theme = cx.theme();
        let table_name = request.table_name.clone();
        let dependents = request.dependents.clone();
        let sql = request.sql_preview();
        let has_deps = !dependents.is_empty();
        let drop_enabled = self.drop_enabled;

        // Table name badge.
        let name_badge = surface_raised(cx)
            .w_full()
            .px(Spacing::SM)
            .py(Spacing::XS)
            .child(
                div()
                    .text_size(FontSizes::SM)
                    .font_family(AppFonts::MONO)
                    .text_color(theme.foreground)
                    .child(table_name.clone()),
            );

        // Dependents section.
        let dependents_section = if has_deps {
            let mut dep_list = div().flex().flex_col().gap(Spacing::XS).child(
                div()
                    .text_size(FontSizes::XS)
                    .font_weight(gpui::FontWeight::SEMIBOLD)
                    .text_color(theme.muted_foreground)
                    .child(dbflux_i18n::t!("modals.drop_table.cascade_warning")),
            );

            for dep in &dependents {
                let kind_label = relation_kind_label(&dep.kind);
                let dep_name = dep.qualified_name.clone();
                dep_list = dep_list.child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .child(
                            div()
                                .text_size(FontSizes::XS)
                                .text_color(theme.muted_foreground)
                                .bg(theme.secondary)
                                .px(Spacing::XS)
                                .rounded(px(2.0))
                                .child(kind_label),
                        )
                        .child(
                            div()
                                .text_size(FontSizes::XS)
                                .font_family(AppFonts::MONO)
                                .text_color(theme.foreground)
                                .child(dep_name),
                        ),
                );
            }

            dep_list.into_any_element()
        } else {
            div().into_any_element()
        };

        // SQL preview.
        let sql_block = surface_raised(cx)
            .w_full()
            .px(Spacing::SM)
            .py(Spacing::XS)
            .child(
                div()
                    .text_size(FontSizes::XS)
                    .font_family(AppFonts::MONO)
                    .text_color(theme.foreground)
                    .child(sql),
            );

        // Confirmation input.
        let hint = if !drop_enabled {
            Some(
                div()
                    .text_size(FontSizes::XS)
                    .text_color(theme.muted_foreground)
                    .child(confirm_hint(&table_name))
                    .into_any_element(),
            )
        } else {
            None
        };

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(
                Text::body(dbflux_i18n::t!("modals.drop_table.delete_warning")).into_any_element(),
            )
            .child(name_badge)
            .when(has_deps, |el| el.child(dependents_section))
            .child(sql_block)
            .child(Input::new(&self.confirm_input))
            .when_some(hint, |el, h| el.child(h));

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            this.cancel(cx);
        });

        let on_drop = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            this.confirm(cx);
        });

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Button::new("drop-table-cancel")
                    .label(dbflux_i18n::t!("modals.drop_table.cancel"))
                    .on_click(on_cancel),
            )
            .child(
                Button::new("drop-table-confirm")
                    .label(dbflux_i18n::t!("modals.drop_table.confirm"))
                    .danger()
                    .disabled(!drop_enabled)
                    .on_click(on_drop),
            );

        ModalShell::new(
            dbflux_i18n::t!("modals.drop_table.title"),
            body.into_any_element(),
            footer.into_any_element(),
        )
        .variant(ModalVariant::Danger)
        .width(px(560.0))
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
        .confirm_enabled(drop_enabled)
        .into_any_element()
    }
}

// ---------------------------------------------------------------------------
// Tests — pure SQL preview logic
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{DefaultSqlDialect, PlaceholderStyle};

    fn request(table: &str, schema: Option<&str>, deps: Vec<RelationRef>) -> DropTableRequest {
        DropTableRequest::new(
            table.to_string(),
            schema.map(str::to_string),
            deps,
            &DefaultSqlDialect,
        )
    }

    /// Quotes identifiers with backticks and keeps the schema, as MySQL does.
    struct BacktickDialect;

    impl SqlDialect for BacktickDialect {
        fn quote_identifier(&self, name: &str) -> String {
            format!("`{}`", name.replace('`', "``"))
        }

        fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
            match schema {
                Some(schema) => format!(
                    "{}.{}",
                    self.quote_identifier(schema),
                    self.quote_identifier(table)
                ),
                None => self.quote_identifier(table),
            }
        }

        fn value_to_literal(&self, _value: &dbflux_core::Value) -> String {
            String::new()
        }

        fn escape_string(&self, text: &str) -> String {
            text.to_string()
        }

        fn placeholder_style(&self) -> PlaceholderStyle {
            PlaceholderStyle::QuestionMark
        }
    }

    /// Quotes identifiers with brackets, as SQL Server does.
    struct BracketDialect;

    impl SqlDialect for BracketDialect {
        fn quote_identifier(&self, name: &str) -> String {
            format!("[{}]", name.replace(']', "]]"))
        }

        fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
            match schema {
                Some(schema) => format!(
                    "{}.{}",
                    self.quote_identifier(schema),
                    self.quote_identifier(table)
                ),
                None => self.quote_identifier(table),
            }
        }

        fn value_to_literal(&self, _value: &dbflux_core::Value) -> String {
            String::new()
        }

        fn escape_string(&self, text: &str) -> String {
            text.to_string()
        }

        fn placeholder_style(&self) -> PlaceholderStyle {
            PlaceholderStyle::AtSign
        }
    }

    #[test]
    fn sql_preview_quotes_through_the_connection_dialect() {
        let postgres = DropTableRequest::new(
            "orders".to_string(),
            Some("public".to_string()),
            vec![],
            &DefaultSqlDialect,
        );
        let mysql = DropTableRequest::new(
            "orders".to_string(),
            Some("shop".to_string()),
            vec![],
            &BacktickDialect,
        );
        let sql_server = DropTableRequest::new(
            "orders".to_string(),
            Some("dbo".to_string()),
            vec![view_dep("dbo.order_view")],
            &BracketDialect,
        );

        assert_eq!(postgres.sql_preview(), "DROP TABLE \"public\".\"orders\";");
        assert_eq!(mysql.sql_preview(), "DROP TABLE `shop`.`orders`;");
        assert_eq!(
            sql_server.sql_preview(),
            "DROP TABLE [dbo].[orders]\n  CASCADE;"
        );
    }

    #[test]
    fn sql_preview_escapes_the_quote_character_inside_a_name() {
        let request = DropTableRequest::new("odd`name".to_string(), None, vec![], &BacktickDialect);

        assert_eq!(request.sql_preview(), "DROP TABLE `odd``name`;");
    }

    fn view_dep(name: &str) -> RelationRef {
        RelationRef {
            kind: RelationKind::View,
            qualified_name: name.to_string(),
        }
    }

    #[test]
    fn sql_preview_no_schema_no_deps() {
        let r = request("orders", None, vec![]);
        assert_eq!(r.sql_preview(), "DROP TABLE \"orders\";");
    }

    #[test]
    fn sql_preview_with_schema_no_deps() {
        let r = request("orders", Some("public"), vec![]);
        assert_eq!(r.sql_preview(), "DROP TABLE \"public\".\"orders\";");
    }

    #[test]
    fn sql_preview_with_schema_and_deps() {
        let r = request(
            "orders",
            Some("public"),
            vec![view_dep("public.order_view")],
        );
        assert_eq!(
            r.sql_preview(),
            "DROP TABLE \"public\".\"orders\"\n  CASCADE;"
        );
    }

    #[test]
    fn sql_preview_no_schema_with_deps() {
        let r = request("orders", None, vec![view_dep("public.order_view")]);
        assert_eq!(r.sql_preview(), "DROP TABLE \"orders\"\n  CASCADE;");
    }

    #[test]
    fn drop_table_keys_resolve_in_both_locales() {
        let keys = [
            "modals.drop_table.title",
            "modals.drop_table.confirm",
            "modals.drop_table.cancel",
            "modals.drop_table.confirm_placeholder",
            "modals.drop_table.confirm_prompt",
            "modals.drop_table.cascade_warning",
            "modals.drop_table.delete_warning",
            "modals.drop_table.relation_kind.view",
            "modals.drop_table.relation_kind.materialized_view",
            "modals.drop_table.relation_kind.foreign_key",
            "modals.drop_table.relation_kind.trigger",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(!en.is_empty() && en != key, "en missing for {key}");
            assert!(!es.is_empty() && es != key, "es missing for {key}");
        }
    }

    #[test]
    fn drop_table_title_diverges_between_locales() {
        let en = dbflux_i18n::t!("modals.drop_table.title", locale = "en");
        let es = dbflux_i18n::t!("modals.drop_table.title", locale = "es");
        assert_ne!(en, es);
    }

    #[test]
    fn relation_kind_label_covers_every_kind() {
        assert_eq!(relation_kind_label(&RelationKind::View), "View");
        assert_eq!(
            relation_kind_label(&RelationKind::MaterializedView),
            "MatView"
        );
        assert_eq!(relation_kind_label(&RelationKind::ForeignKeyChild), "FK");
        assert_eq!(relation_kind_label(&RelationKind::Trigger), "Trigger");
    }

    #[test]
    fn relation_kind_label_view_diverges_between_locales() {
        let en = dbflux_i18n::t!("modals.drop_table.relation_kind.view", locale = "en");
        let es = dbflux_i18n::t!("modals.drop_table.relation_kind.view", locale = "es");
        assert_eq!(en, "View");
        assert_eq!(es, "Vista");
        assert_ne!(en, es);
    }

    #[test]
    fn confirm_hint_interpolates_table_name() {
        let hint = confirm_hint("orders");
        assert!(hint.contains("orders"));
        assert_eq!(
            hint,
            dbflux_i18n::t!("modals.drop_table.confirm_prompt", table = "orders")
        );
    }
}

#[cfg(test)]
mod keyboard_tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{DropTableOutcome, DropTableRequest, ModalDropTable};
    use gpui::{
        AppContext as _, Context, Entity, IntoElement, ParentElement as _, Render, Styled as _,
        TestAppContext, VisualTestContext, Window, div,
    };
    use std::cell::RefCell;
    use std::rc::Rc;

    struct Host {
        modal: Entity<ModalDropTable>,
    }

    impl Render for Host {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().size_full().child(self.modal.clone())
        }
    }

    fn open_modal(
        cx: &mut TestAppContext,
    ) -> (
        Entity<ModalDropTable>,
        &mut VisualTestContext,
        Rc<RefCell<Vec<DropTableOutcome>>>,
    ) {
        cx.update(gpui_component::init);

        let (host, window) = cx.add_window_view(|window, cx| Host {
            modal: cx.new(|cx| ModalDropTable::new(window, cx)),
        });
        let modal = window.update(|_, cx| host.read(cx).modal.clone());

        let outcomes: Rc<RefCell<Vec<DropTableOutcome>>> = Rc::default();
        window.update(|window, cx| {
            let sink = outcomes.clone();
            cx.subscribe(&modal, move |_, outcome: &DropTableOutcome, _| {
                sink.borrow_mut().push(outcome.clone());
            })
            .detach();

            modal.update(cx, |modal, cx| {
                modal.open(
                    DropTableRequest::new(
                        "orders".to_string(),
                        None,
                        Vec::new(),
                        &dbflux_core::DefaultSqlDialect,
                    ),
                    window,
                    cx,
                );
            });
        });
        window.run_until_parked();

        (modal, window, outcomes)
    }

    #[gpui::test]
    fn enter_drops_only_once_the_typed_name_matches(cx: &mut TestAppContext) {
        let (modal, window, outcomes) = open_modal(cx);

        window.simulate_input("order");
        window.simulate_keystrokes("enter");
        assert!(outcomes.borrow().is_empty(), "a partial name must not drop");
        assert!(window.update(|_, cx| modal.read(cx).is_visible()));

        window.simulate_input("s");
        window.simulate_keystrokes("enter");
        assert!(matches!(
            outcomes.borrow().as_slice(),
            [DropTableOutcome::Confirmed]
        ));
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
    }

    #[gpui::test]
    fn escape_cancels_from_the_confirmation_input(cx: &mut TestAppContext) {
        let (modal, window, outcomes) = open_modal(cx);

        window.simulate_keystrokes("escape");

        assert!(matches!(
            outcomes.borrow().as_slice(),
            [DropTableOutcome::Cancelled]
        ));
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
    }
}
