use gpui::prelude::*;
use gpui::*;
use gpui_component::Sizable;
use gpui_component::input::InputState;

use crate::ui::components::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use dbflux_core::values::ValueRef;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ValueSourceKind {
    Literal,
    Env,
    Secret,
    Parameter,
    Auth,
}

impl ValueSourceKind {
    fn from_value(value: &str) -> Self {
        match value {
            "env" => Self::Env,
            "secret" => Self::Secret,
            "parameter" => Self::Parameter,
            "auth" => Self::Auth,
            _ => Self::Literal,
        }
    }

    fn dropdown_index(self) -> usize {
        match self {
            Self::Literal => 0,
            Self::Env => 1,
            Self::Secret => 2,
            Self::Parameter => 3,
            Self::Auth => 4,
        }
    }
}

pub struct ValueSourceSelector {
    source_dropdown: Entity<Dropdown>,
    secondary_input: Entity<InputState>,
    _subscriptions: Vec<Subscription>,
}

impl ValueSourceSelector {
    pub fn new(id_prefix: &'static str, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let source_dropdown = cx.new(|_cx| {
            Dropdown::new(id_prefix)
                .placeholder("Literal")
                .items(vec![
                    DropdownItem::with_value("Literal", "literal"),
                    DropdownItem::with_value("Environment Variable", "env"),
                    DropdownItem::with_value("Secret Manager", "secret"),
                    DropdownItem::with_value("Parameter Store", "parameter"),
                    DropdownItem::with_value("Auth Session Field", "auth"),
                ])
                .selected_index(Some(0))
        });

        let secondary_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("JSON key (optional)"));

        let dropdown_subscription = cx.subscribe(
            &source_dropdown,
            |_this, _dropdown, _event: &DropdownSelectionChanged, cx| {
                cx.notify();
            },
        );

        Self {
            source_dropdown,
            secondary_input,
            _subscriptions: vec![dropdown_subscription],
        }
    }

    pub fn set_value_ref(
        &mut self,
        value_ref: Option<&ValueRef>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> String {
        let (source_kind, primary_value, secondary_value) = match value_ref {
            Some(ValueRef::Literal { .. }) | None => {
                (ValueSourceKind::Literal, String::new(), String::new())
            }
            Some(ValueRef::Env { key }) => (ValueSourceKind::Env, key.clone(), String::new()),
            Some(ValueRef::Secret {
                locator, json_key, ..
            }) => (
                ValueSourceKind::Secret,
                locator.clone(),
                json_key.clone().unwrap_or_default(),
            ),
            Some(ValueRef::Parameter { name, json_key, .. }) => (
                ValueSourceKind::Parameter,
                name.clone(),
                json_key.clone().unwrap_or_default(),
            ),
            Some(ValueRef::Auth { field }) => (ValueSourceKind::Auth, field.clone(), String::new()),
        };

        self.source_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(Some(source_kind.dropdown_index()), cx);
        });

        self.secondary_input.update(cx, |input, cx| {
            input.set_value(secondary_value.clone(), window, cx);
        });

        primary_value
    }

    pub fn value_ref(&self, primary_value: &str, cx: &App) -> Option<ValueRef> {
        let source_kind = self.selected_source(cx);

        let primary_value = primary_value.trim().to_string();
        let secondary_value = self.secondary_input.read(cx).value().trim().to_string();

        match source_kind {
            ValueSourceKind::Literal => None,
            ValueSourceKind::Env if !primary_value.is_empty() => Some(ValueRef::env(primary_value)),
            ValueSourceKind::Secret if !primary_value.is_empty() => {
                let (locator, inline_key) = parse_secret_inline_reference(&primary_value);
                let json_key = if secondary_value.is_empty() {
                    inline_key
                } else {
                    Some(secondary_value)
                };

                Some(ValueRef::secret("aws-secrets-manager", locator, json_key))
            }
            ValueSourceKind::Parameter if !primary_value.is_empty() => {
                let (name, inline_key) = parse_secret_inline_reference(&primary_value);
                let json_key = if secondary_value.is_empty() {
                    inline_key
                } else {
                    Some(secondary_value)
                };

                Some(ValueRef::parameter_with_key("aws-ssm", name, json_key))
            }
            ValueSourceKind::Auth if !primary_value.is_empty() => {
                Some(ValueRef::auth(primary_value))
            }
            _ => None,
        }
    }

    pub fn is_literal(&self, cx: &App) -> bool {
        self.selected_source(cx) == ValueSourceKind::Literal
    }

    pub fn is_source_dropdown_open(&self, cx: &App) -> bool {
        self.source_dropdown.read(cx).is_open()
    }

    pub fn open_source_dropdown(&mut self, cx: &mut Context<Self>) {
        self.source_dropdown.update(cx, |dropdown, cx| {
            dropdown.open(cx);
        });
    }

    pub fn close_source_dropdown(&mut self, cx: &mut Context<Self>) {
        self.source_dropdown.update(cx, |dropdown, cx| {
            dropdown.close(cx);
        });
    }

    pub fn source_dropdown_next(&mut self, cx: &mut Context<Self>) {
        self.source_dropdown.update(cx, |dropdown, cx| {
            dropdown.select_next_item(cx);
        });
    }

    pub fn source_dropdown_prev(&mut self, cx: &mut Context<Self>) {
        self.source_dropdown.update(cx, |dropdown, cx| {
            dropdown.select_prev_item(cx);
        });
    }

    pub fn source_dropdown_next_page(&mut self, cx: &mut Context<Self>) {
        self.source_dropdown.update(cx, |dropdown, cx| {
            dropdown.select_next_page(cx);
        });
    }

    pub fn source_dropdown_prev_page(&mut self, cx: &mut Context<Self>) {
        self.source_dropdown.update(cx, |dropdown, cx| {
            dropdown.select_prev_page(cx);
        });
    }

    pub fn source_dropdown_accept(&mut self, cx: &mut Context<Self>) {
        self.source_dropdown.update(cx, |dropdown, cx| {
            dropdown.accept_selection(cx);
        });
    }

    fn selected_source(&self, cx: &App) -> ValueSourceKind {
        let selected_value = self
            .source_dropdown
            .read(cx)
            .selected_value()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "literal".to_string());

        ValueSourceKind::from_value(&selected_value)
    }
}

fn parse_secret_inline_reference(value: &str) -> (String, Option<String>) {
    let trimmed = value.trim();

    if let Some((locator, key)) = trimmed.rsplit_once('#') {
        let locator = locator.trim();
        let key = key.trim();

        if !locator.is_empty() && !key.is_empty() {
            return (locator.to_string(), Some(key.to_string()));
        }
    }

    (trimmed.to_string(), None)
}

impl Render for ValueSourceSelector {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let source_kind = self.selected_source(cx);

        div()
            .flex()
            .items_center()
            .gap_2()
            .child(div().w(px(170.0)).child(self.source_dropdown.clone()))
            .when(
                source_kind == ValueSourceKind::Secret || source_kind == ValueSourceKind::Parameter,
                |el| {
                    el.child(
                        div().w(px(140.0)).child(
                            gpui_component::input::Input::new(&self.secondary_input).small(),
                        ),
                    )
                },
            )
            .into_any_element()
    }
}
