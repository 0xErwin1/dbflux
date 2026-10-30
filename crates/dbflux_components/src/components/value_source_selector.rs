use crate::controls::{Dropdown, DropdownItem, DropdownSelectionChanged, InputState};
use dbflux_core::values::ValueRef;
use gpui::prelude::*;
use gpui::*;

const AWS_SECRETS_MANAGER_PROVIDER: &str = "aws-secrets-manager";
const AWS_SSM_PROVIDER: &str = "aws-ssm";
const SECRET_PROVIDER_VALUE_PREFIX: &str = "value-source-secret-";

#[derive(Clone, Debug, PartialEq, Eq)]
enum ValueSourceKind {
    Literal,
    Env,
    Dotenv,
    Secret,
    Parameter,
    Auth,
    /// A secret provider other than the built-in Secrets Manager option,
    /// identified by its registered provider id.
    SecretProvider(String),
}

impl ValueSourceKind {
    fn from_value(value: &str) -> Self {
        if let Some(provider_id) = value.strip_prefix(SECRET_PROVIDER_VALUE_PREFIX) {
            return Self::SecretProvider(provider_id.to_string());
        }

        match value {
            "env" => Self::Env,
            "dotenv" => Self::Dotenv,
            "secret" => Self::Secret,
            "parameter" => Self::Parameter,
            "auth" => Self::Auth,
            _ => Self::Literal,
        }
    }

    fn for_value_ref(value_ref: Option<&ValueRef>) -> Self {
        match value_ref {
            Some(ValueRef::Literal { .. } | ValueRef::File { .. }) | None => Self::Literal,
            Some(ValueRef::Env { .. }) => Self::Env,
            Some(ValueRef::Dotenv { .. }) => Self::Dotenv,
            Some(ValueRef::Secret { provider, .. }) if provider == AWS_SECRETS_MANAGER_PROVIDER => {
                Self::Secret
            }
            Some(ValueRef::Secret { provider, .. }) => Self::SecretProvider(provider.clone()),
            Some(ValueRef::Parameter { .. }) => Self::Parameter,
            Some(ValueRef::Auth { .. }) => Self::Auth,
        }
    }
}

fn secret_provider_item_value(provider_id: &str) -> String {
    format!("{SECRET_PROVIDER_VALUE_PREFIX}{provider_id}")
}

/// Builds the source options: the built-in sources, then one option per
/// registered external secret provider, then `unregistered_provider` when a
/// loaded reference names a provider that is not registered, so the reference
/// stays selectable and round-trips unchanged.
fn source_items(
    external_providers: &[(String, String)],
    unregistered_provider: Option<&str>,
) -> Vec<DropdownItem> {
    let mut items = vec![
        DropdownItem::with_value(
            dbflux_i18n::t!("components.value_source.literal"),
            "literal",
        ),
        DropdownItem::with_value(dbflux_i18n::t!("components.value_source.env"), "env"),
        DropdownItem::with_value(dbflux_i18n::t!("components.value_source.dotenv"), "dotenv"),
        DropdownItem::with_value(dbflux_i18n::t!("components.value_source.secret"), "secret"),
        DropdownItem::with_value(
            dbflux_i18n::t!("components.value_source.parameter"),
            "parameter",
        ),
        DropdownItem::with_value(dbflux_i18n::t!("components.value_source.auth"), "auth"),
    ];

    items.extend(
        external_providers
            .iter()
            .map(|(provider_id, display_name)| {
                DropdownItem::with_value(
                    display_name.clone(),
                    secret_provider_item_value(provider_id),
                )
            }),
    );

    if let Some(provider_id) = unregistered_provider {
        items.push(DropdownItem::with_value(
            provider_id.to_string(),
            secret_provider_item_value(provider_id),
        ));
    }

    items
}

fn value_ref_for_source(source_kind: &ValueSourceKind, primary_value: &str) -> Option<ValueRef> {
    let primary_value = primary_value.trim();
    if primary_value.is_empty() {
        return None;
    }

    match source_kind {
        ValueSourceKind::Literal => None,
        ValueSourceKind::Env => Some(ValueRef::env(primary_value)),
        ValueSourceKind::Dotenv => {
            let (path, key) = parse_dotenv_inline_reference(primary_value);

            Some(ValueRef::dotenv(path, key))
        }
        ValueSourceKind::Secret => {
            let (locator, json_key) = parse_secret_inline_reference(primary_value);

            Some(ValueRef::secret(
                AWS_SECRETS_MANAGER_PROVIDER,
                locator,
                json_key,
            ))
        }
        ValueSourceKind::SecretProvider(provider_id) => {
            let (locator, json_key) = parse_secret_inline_reference(primary_value);

            Some(ValueRef::secret(provider_id.as_str(), locator, json_key))
        }
        ValueSourceKind::Parameter => {
            let (name, json_key) = parse_secret_inline_reference(primary_value);

            Some(ValueRef::parameter_with_key(
                AWS_SSM_PROVIDER,
                name,
                json_key,
            ))
        }
        ValueSourceKind::Auth => Some(ValueRef::auth(primary_value)),
    }
}

fn primary_value_for_ref(value_ref: Option<&ValueRef>) -> String {
    match value_ref {
        Some(ValueRef::Literal { .. } | ValueRef::File { .. }) | None => String::new(),
        Some(ValueRef::Env { key }) => key.clone(),
        Some(ValueRef::Dotenv { path, key }) => format_inline_reference(path, Some(key)),
        Some(ValueRef::Secret {
            locator, json_key, ..
        }) => format_inline_reference(locator, json_key.as_deref()),
        Some(ValueRef::Parameter { name, json_key, .. }) => {
            format_inline_reference(name, json_key.as_deref())
        }
        Some(ValueRef::Auth { field }) => field.clone(),
    }
}

fn selected_index_for(items: &[DropdownItem], source_kind: &ValueSourceKind) -> Option<usize> {
    let wanted = match source_kind {
        ValueSourceKind::Literal => "literal".to_string(),
        ValueSourceKind::Env => "env".to_string(),
        ValueSourceKind::Dotenv => "dotenv".to_string(),
        ValueSourceKind::Secret => "secret".to_string(),
        ValueSourceKind::Parameter => "parameter".to_string(),
        ValueSourceKind::Auth => "auth".to_string(),
        ValueSourceKind::SecretProvider(provider_id) => secret_provider_item_value(provider_id),
    };

    items.iter().position(|item| item.value.as_ref() == wanted)
}

pub struct ValueSourceSelector {
    source_dropdown: Entity<Dropdown>,
    secondary_input: Entity<InputState>,
    external_providers: Vec<(String, String)>,
    unregistered_provider: Option<String>,
    _subscriptions: Vec<Subscription>,
}

impl ValueSourceSelector {
    pub fn new(id_prefix: &'static str, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let source_dropdown = cx.new(|_cx| {
            Dropdown::new(id_prefix)
                .placeholder(dbflux_i18n::t!("components.value_source.literal"))
                .items(source_items(&[], None))
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
            external_providers: Vec::new(),
            unregistered_provider: None,
            _subscriptions: vec![dropdown_subscription],
        }
    }

    pub fn set_value_ref(
        &mut self,
        value_ref: Option<&ValueRef>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> String {
        let source_kind = ValueSourceKind::for_value_ref(value_ref);
        let primary_value = primary_value_for_ref(value_ref);

        self.unregistered_provider = match &source_kind {
            ValueSourceKind::SecretProvider(provider_id)
                if !self.is_registered_provider(provider_id) =>
            {
                Some(provider_id.clone())
            }
            _ => None,
        };
        self.select_source(&source_kind, cx);

        self.secondary_input.update(cx, |input, cx| {
            input.set_value(String::new(), window, cx);
        });

        primary_value
    }

    pub fn value_ref(&self, primary_value: &str, cx: &App) -> Option<ValueRef> {
        value_ref_for_source(&self.selected_source(cx), primary_value)
    }

    /// Offers each `(provider_id, display_name)` as a secret source option,
    /// keeping the current selection when its provider is still offered.
    pub fn set_external_secret_providers(
        &mut self,
        providers: Vec<(String, String)>,
        cx: &mut Context<Self>,
    ) {
        if self.external_providers == providers {
            return;
        }

        let source_kind = self.selected_source(cx);
        self.external_providers = providers;

        if let ValueSourceKind::SecretProvider(provider_id) = &source_kind {
            self.unregistered_provider =
                (!self.is_registered_provider(provider_id)).then(|| provider_id.clone());
        }

        self.select_source(&source_kind, cx);
    }

    fn is_registered_provider(&self, provider_id: &str) -> bool {
        self.external_providers
            .iter()
            .any(|(registered_id, _)| registered_id == provider_id)
    }

    fn select_source(&mut self, source_kind: &ValueSourceKind, cx: &mut Context<Self>) {
        let items = source_items(
            &self.external_providers,
            self.unregistered_provider.as_deref(),
        );
        let selected_index = selected_index_for(&items, source_kind).or(Some(0));

        self.source_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(items, cx);
            dropdown.set_selected_index(selected_index, cx);
        });
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
        | ValueSourceKind::SecretProvider(_)
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
        | ValueSourceKind::SecretProvider(_)
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
        ValueSourceKind, format_inline_reference, parse_dotenv_inline_reference,
        primary_value_for_ref, selected_index_for, source_hint, source_items, source_placeholder,
        value_ref_for_source,
    };
    use dbflux_core::values::ValueRef;

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

    fn assert_same_ref(actual: Option<ValueRef>, expected: Option<ValueRef>) {
        assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
    }

    fn vault_providers() -> Vec<(String, String)> {
        vec![("vault".to_string(), "HashiCorp Vault".to_string())]
    }

    fn round_trip(value_ref: &ValueRef, providers: &[(String, String)]) -> Option<ValueRef> {
        let source_kind = ValueSourceKind::for_value_ref(Some(value_ref));
        let unregistered = match &source_kind {
            ValueSourceKind::SecretProvider(id) if !providers.iter().any(|(p, _)| p == id) => {
                Some(id.as_str())
            }
            _ => None,
        };
        let items = source_items(providers, unregistered);
        let index = selected_index_for(&items, &source_kind)?;
        let selected = ValueSourceKind::from_value(items.get(index)?.value.as_ref());

        value_ref_for_source(&selected, &primary_value_for_ref(Some(value_ref)))
    }

    #[test]
    fn value_source_external_provider_option_produces_secret_ref() {
        let items = source_items(&vault_providers(), None);
        let vault = items
            .last()
            .map(|item| (item.label.to_string(), item.value.to_string()));

        assert_eq!(
            vault,
            Some((
                "HashiCorp Vault".to_string(),
                "value-source-secret-vault".to_string()
            ))
        );
        assert_same_ref(
            value_ref_for_source(
                &ValueSourceKind::from_value("value-source-secret-vault"),
                " kv/db#password ",
            ),
            Some(ValueRef::secret(
                "vault",
                "kv/db",
                Some("password".to_string()),
            )),
        );
    }

    #[test]
    fn value_source_external_secret_ref_selects_its_provider_option() {
        let value_ref = ValueRef::secret("vault", "kv/db", None);
        let items = source_items(&vault_providers(), None);
        let index = selected_index_for(&items, &ValueSourceKind::for_value_ref(Some(&value_ref)));

        assert_eq!(index, Some(6));
        assert_same_ref(round_trip(&value_ref, &vault_providers()), Some(value_ref));
    }

    #[test]
    fn value_source_unknown_provider_round_trips_and_shows_raw_id() {
        let value_ref = ValueRef::secret("acme.secrets", "app/db", Some("user".to_string()));
        let items = source_items(&vault_providers(), Some("acme.secrets"));

        assert_eq!(
            items.last().map(|item| item.label.to_string()),
            Some("acme.secrets".to_string())
        );
        assert_same_ref(round_trip(&value_ref, &vault_providers()), Some(value_ref));
    }

    #[test]
    fn value_source_aws_refs_keep_builtin_options() {
        let secret = ValueRef::secret("aws-secrets-manager", "prod/db", Some("pw".to_string()));
        let parameter = ValueRef::parameter_with_key("aws-ssm", "/prod/db", None);

        assert_eq!(
            ValueSourceKind::for_value_ref(Some(&secret)),
            ValueSourceKind::Secret
        );
        assert_same_ref(round_trip(&secret, &vault_providers()), Some(secret));
        assert_same_ref(round_trip(&parameter, &[]), Some(parameter));
        assert_eq!(source_items(&[], None).len(), 6);
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
