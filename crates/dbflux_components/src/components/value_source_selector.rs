use crate::controls::{Dropdown, DropdownItem, DropdownSelectionChanged, InputState};
use dbflux_core::values::ValueRef;
use gpui::prelude::*;
use gpui::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ValueSourceKind {
    Literal,
    Env,
    Dotenv,
    Secret,
    Parameter,
    Auth,
}

impl ValueSourceKind {
    fn from_value(value: &str) -> Self {
        match value {
            "env" => Self::Env,
            "dotenv" => Self::Dotenv,
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
            Self::Dotenv => 2,
            Self::Secret => 3,
            Self::Parameter => 4,
            Self::Auth => 5,
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
                .placeholder(dbflux_i18n::t!("components.value_source.literal"))
                .items(vec![
                    DropdownItem::with_value(
                        dbflux_i18n::t!("components.value_source.literal"),
                        "literal",
                    ),
                    DropdownItem::with_value(dbflux_i18n::t!("components.value_source.env"), "env"),
                    DropdownItem::with_value(
                        dbflux_i18n::t!("components.value_source.dotenv"),
                        "dotenv",
                    ),
                    DropdownItem::with_value(
                        dbflux_i18n::t!("components.value_source.secret"),
                        "secret",
                    ),
                    DropdownItem::with_value(
                        dbflux_i18n::t!("components.value_source.parameter"),
                        "parameter",
                    ),
                    DropdownItem::with_value(
                        dbflux_i18n::t!("components.value_source.auth"),
                        "auth",
                    ),
                ])
                .selected_index(Some(0))
        });

        let secondary_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "components.value_source.json_key_placeholder"
            ))
        });

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
            Some(ValueRef::Dotenv { path, key }) => (
                ValueSourceKind::Dotenv,
                format_inline_reference(path, Some(key)),
                String::new(),
            ),
            Some(ValueRef::Secret {
                locator, json_key, ..
            }) => (
                ValueSourceKind::Secret,
                format_inline_reference(locator, json_key.as_deref()),
                String::new(),
            ),
            Some(ValueRef::Parameter { name, json_key, .. }) => (
                ValueSourceKind::Parameter,
                format_inline_reference(name, json_key.as_deref()),
                String::new(),
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
        match source_kind {
            ValueSourceKind::Literal => None,
            ValueSourceKind::Env if !primary_value.is_empty() => Some(ValueRef::env(primary_value)),
            ValueSourceKind::Dotenv if !primary_value.is_empty() => {
                let (path, key) = parse_dotenv_inline_reference(&primary_value);

                Some(ValueRef::dotenv(path, key))
            }
            ValueSourceKind::Secret if !primary_value.is_empty() => {
                let (locator, json_key) = parse_secret_inline_reference(&primary_value);

                Some(ValueRef::secret("aws-secrets-manager", locator, json_key))
            }
            ValueSourceKind::Parameter if !primary_value.is_empty() => {
                let (name, json_key) = parse_secret_inline_reference(&primary_value);

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

    /// Example of what the field's input holds for an environment or dotenv
    /// source, or `None` for any other source.
    pub fn source_placeholder(&self, cx: &App) -> Option<String> {
        source_placeholder(self.selected_source(cx))
    }

    /// One line telling the user where an environment or dotenv value is read
    /// from and when, built from the reference typed in the field's input.
    /// `None` for any other source or an empty reference.
    pub fn source_hint(&self, primary_value: &str, cx: &App) -> Option<String> {
        source_hint(self.selected_source(cx), primary_value)
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

fn format_inline_reference(locator: &str, json_key: Option<&str>) -> String {
    match json_key.map(str::trim).filter(|key| !key.is_empty()) {
        Some(json_key) => format!("{}#{}", locator.trim(), json_key),
        None => locator.trim().to_string(),
    }
}

/// Splits `path#KEY` at the last `#`. A reference without one keeps the
/// whole text as the path and an empty key, so the connection fails with a
/// visible "not a valid variable name" error instead of dropping the source.
fn parse_dotenv_inline_reference(value: &str) -> (String, String) {
    match value.trim().rsplit_once('#') {
        Some((path, key)) => (path.trim().to_string(), key.trim().to_string()),
        None => (value.trim().to_string(), String::new()),
    }
}

fn source_placeholder(kind: ValueSourceKind) -> Option<String> {
    let key = match kind {
        ValueSourceKind::Env => "components.value_source.placeholder.env",
        ValueSourceKind::Dotenv => "components.value_source.placeholder.dotenv",
        ValueSourceKind::Literal
        | ValueSourceKind::Secret
        | ValueSourceKind::Parameter
        | ValueSourceKind::Auth => return None,
    };

    Some(dbflux_i18n::t!(key))
}

fn source_hint(kind: ValueSourceKind, primary_value: &str) -> Option<String> {
    let primary_value = primary_value.trim();

    if primary_value.is_empty() {
        return None;
    }

    match kind {
        ValueSourceKind::Env => Some(dbflux_i18n::t!(
            "components.value_source.hint.env",
            name = primary_value
        )),
        ValueSourceKind::Dotenv => {
            let (path, key) = parse_dotenv_inline_reference(primary_value);

            if key.is_empty() {
                return Some(dbflux_i18n::t!(
                    "components.value_source.hint.dotenv_missing_key"
                ));
            }

            Some(dbflux_i18n::t!(
                "components.value_source.hint.dotenv",
                key = key,
                path = path
            ))
        }
        ValueSourceKind::Literal
        | ValueSourceKind::Secret
        | ValueSourceKind::Parameter
        | ValueSourceKind::Auth => None,
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
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .gap_2()
            .child(
                div()
                    .flex_1()
                    .min_w(px(120.0))
                    .child(self.source_dropdown.clone()),
            )
            .into_any_element()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::{
        ValueSourceKind, format_inline_reference, parse_dotenv_inline_reference, source_hint,
        source_placeholder,
    };

    #[test]
    fn dotenv_inline_reference_splits_at_the_last_hash() {
        assert_eq!(
            parse_dotenv_inline_reference(" ~/dir#1/prod.env#DB_PASSWORD "),
            ("~/dir#1/prod.env".to_string(), "DB_PASSWORD".to_string())
        );
        assert_eq!(
            parse_dotenv_inline_reference("~/prod.env"),
            ("~/prod.env".to_string(), String::new())
        );
        assert_eq!(
            format_inline_reference("~/prod.env", Some("DB_PASSWORD")),
            "~/prod.env#DB_PASSWORD"
        );
    }

    #[test]
    fn hints_name_the_source_and_never_apply_to_literals() {
        assert_eq!(source_hint(ValueSourceKind::Literal, "secret"), None);
        assert_eq!(source_hint(ValueSourceKind::Env, "  "), None);
        assert_eq!(source_placeholder(ValueSourceKind::Literal), None);
        assert_eq!(source_hint(ValueSourceKind::Secret, "prod/db"), None);

        let env = source_hint(ValueSourceKind::Env, "DB_PASSWORD").expect("env hint");
        assert!(env.contains("DB_PASSWORD"), "{env}");

        let dotenv =
            source_hint(ValueSourceKind::Dotenv, "~/prod.env#DB_PASSWORD").expect("dotenv hint");
        assert!(
            dotenv.contains("DB_PASSWORD") && dotenv.contains("~/prod.env"),
            "{dotenv}"
        );

        assert_eq!(
            source_hint(ValueSourceKind::Dotenv, "~/prod.env"),
            Some(dbflux_i18n::t!(
                "components.value_source.hint.dotenv_missing_key"
            ))
        );
    }

    #[test]
    fn value_source_keys_resolve_in_both_locales() {
        let keys = [
            "components.value_source.literal",
            "components.value_source.env",
            "components.value_source.dotenv",
            "components.value_source.secret",
            "components.value_source.parameter",
            "components.value_source.auth",
            "components.value_source.json_key_placeholder",
            "components.value_source.placeholder.env",
            "components.value_source.placeholder.dotenv",
            "components.value_source.hint.env",
            "components.value_source.hint.dotenv",
            "components.value_source.hint.dotenv_missing_key",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(!en.is_empty() && en != key, "en missing for {key}");
            assert!(!es.is_empty() && es != key, "es missing for {key}");
        }
    }

    #[test]
    fn value_source_env_differs_between_locales() {
        let en = dbflux_i18n::t!("components.value_source.env", locale = "en");
        let es = dbflux_i18n::t!("components.value_source.env", locale = "es");
        assert_ne!(en, es);
    }
}
