use crate::ui::tokens::{FontSizes, Spacing};
use dbflux_components::controls::{GpuiInput as Input, InputEvent, InputState};
use dbflux_components::primitives::{Text, surface_raised};
use dbflux_core::{RelationKind, RelationRef};
use gpui::prelude::*;
use gpui::{Context, Entity, EventEmitter, Subscription, Window, div, px};
use gpui_component::ActiveTheme;
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
    /// Schema name (for `DROP TABLE "schema"."table"`).
    pub schema_name: Option<String>,
    /// Dependent objects — empty if none.
    pub dependents: Vec<RelationRef>,
}

impl DropTableRequest {
    /// Build the SQL preview text for this request.
    pub fn sql_preview(&self) -> String {
        let has_deps = !self.dependents.is_empty();
        match &self.schema_name {
            Some(schema) => {
                let base = format!("DROP TABLE \"{}\".\"{}\"", schema, self.table_name);
                if has_deps {
                    format!("{}\n  CASCADE;", base)
                } else {
                    format!("{};", base)
                }
            }
            None => {
                let base = format!("DROP TABLE \"{}\"", self.table_name);
                if has_deps {
                    format!("{}\n  CASCADE;", base)
                } else {
                    format!("{};", base)
                }
            }
        }
    }
}

fn relation_kind_label(kind: &RelationKind) -> &'static str {
    match kind {
        RelationKind::View => "View",
        RelationKind::MaterializedView => "MatView",
        RelationKind::ForeignKeyChild => "FK",
        RelationKind::Trigger => "Trigger",
    }
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
    _subscription: Option<Subscription>,
}

impl ModalDropTable {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let confirm_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Type table name to confirm"));
        Self {
            request: None,
            visible: false,
            confirm_input,
            drop_enabled: false,
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
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.request = None;
        self.drop_enabled = false;
        self._subscription = None;
        cx.notify();
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
                    .font_family(dbflux_components::typography::AppFonts::MONO)
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
                    .child("Dependent objects will also be dropped (CASCADE):"),
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
                                .font_family(dbflux_components::typography::AppFonts::MONO)
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
                    .font_family(dbflux_components::typography::AppFonts::MONO)
                    .text_color(theme.foreground)
                    .child(sql),
            );

        // Confirmation input.
        let hint = if !drop_enabled {
            Some(
                div()
                    .text_size(FontSizes::XS)
                    .text_color(theme.muted_foreground)
                    .child(format!("Type \"{}\" to confirm", table_name))
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
                Text::body("This will permanently delete the table and any dependent objects.")
                    .into_any_element(),
            )
            .child(name_badge)
            .when(has_deps, |el| el.child(dependents_section))
            .child(sql_block)
            .child(Input::new(&self.confirm_input))
            .when_some(hint, |el, h| el.child(h));

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(DropTableOutcome::Cancelled);
            this.close(cx);
        });

        let on_drop = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(DropTableOutcome::Confirmed);
            this.close(cx);
        });

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Button::new("drop-table-cancel")
                    .label("Cancel")
                    .on_click(on_cancel),
            )
            .child(if drop_enabled {
                Button::new("drop-table-confirm")
                    .label("Drop table")
                    .danger()
                    .on_click(on_drop)
                    .into_any_element()
            } else {
                div()
                    .flex()
                    .items_center()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .rounded(px(4.0))
                    .opacity(0.4)
                    .bg(theme.danger)
                    .cursor(gpui::CursorStyle::default())
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .text_color(theme.background)
                            .child("Drop table"),
                    )
                    .into_any_element()
            });

        use super::shell::{ModalShell, ModalVariant};

        ModalShell::new(
            "Drop table",
            body.into_any_element(),
            footer.into_any_element(),
        )
        .variant(ModalVariant::Danger)
        .width(px(560.0))
        .into_any_element()
    }
}

// ---------------------------------------------------------------------------
// Tests — pure SQL preview logic
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn request(table: &str, schema: Option<&str>, deps: Vec<RelationRef>) -> DropTableRequest {
        DropTableRequest {
            table_name: table.to_string(),
            schema_name: schema.map(str::to_string),
            dependents: deps,
        }
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
}
