//! Driver-defined connection form fields.
//!
//! This module provides types for drivers to define their connection form
//! fields dynamically, allowing the UI to render forms without hardcoding
//! driver-specific logic.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Option for a select field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectOption {
    pub value: String,
    pub label: String,
}

impl SelectOption {
    pub fn new(value: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            label: label.into(),
        }
    }
}

/// Controls when a `DynamicSelect` field re-fetches its options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefreshTrigger {
    /// User must explicitly click a refresh button.
    Manual,
    /// Options are re-fetched whenever a field listed in `depends_on` changes.
    OnDependencyChange,
    /// Options are re-fetched when the field gains focus.
    OnFocus,
    /// Options are re-fetched after each successful login.
    OnLoginComplete,
}

/// Type of form field input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FormFieldKind {
    Text,
    Password,
    /// A write-only secret input.
    ///
    /// The field is rendered as a masked, always-empty input. The placeholder
    /// signals write-only semantics (e.g. "Leave blank to keep current"). When
    /// the user saves without entering a value, the provider receives an empty
    /// string; it MUST interpret blank as "preserve the existing on-disk value"
    /// (spec R9.4.2). A non-blank value is written to disk and then discarded
    /// from memory — it is never stored in DBFlux SQLite, keyring, or logs
    /// (spec R9.4.4, R9.6).
    ///
    /// Use this instead of `Password` for credentials that live exclusively on
    /// disk and must never be pre-filled or round-tripped through the UI.
    WriteOnly,
    Number,
    FilePath,
    Checkbox,
    Select {
        options: Vec<SelectOption>,
    },
    /// A dropdown whose options are fetched at runtime via the provider's
    /// `fetch_dynamic_options` method. Older clients that do not recognize this
    /// variant will fail loudly at parse time (no silent fallback).
    DynamicSelect {
        /// Field ids whose current values are forwarded to the provider when
        /// fetching options for this field.
        depends_on: Vec<String>,
        /// When and how the options are refreshed.
        refresh: RefreshTrigger,
        /// When `true`, the field is not fetched until an active session exists.
        #[serde(default)]
        requires_session: bool,
        /// When `true`, the user may type a value not present in the options list.
        #[serde(default)]
        allow_freeform: bool,
    },
    /// A dropdown that references another `AuthProfile`. The UI populates
    /// options based on the `provider_id` filter.
    ///
    /// - `None` — all eligible auth profiles are listed regardless of their
    ///   provider origin (built-in or external/RPC). Used by driver forms.
    /// - `Some(id)` — only AuthProfiles whose `provider_id` equals `id` are
    ///   shown. Used by auth-provider forms that reference a specific provider
    ///   (e.g. an SSO-session reference on an SSO profile).
    ///
    /// When `expand_auth_profile_refs` runs, it follows this reference and
    /// merges the referenced profile's fields into the consumer profile so
    /// downstream code (login, validation, dynamic options) sees a single
    /// flat field map.
    AuthProfileRef {
        /// Optional filter: `None` lists all eligible auth profiles;
        /// `Some(id)` filters to profiles whose `provider_id` equals `id`.
        provider_id: Option<String>,
    },
}

/// Definition of a single form field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormFieldDef {
    pub id: String,
    pub label: String,
    pub kind: FormFieldKind,
    pub placeholder: String,
    /// Whether this field is required for validation.
    /// If `enabled_when_checked` or `enabled_when_unchecked` is set,
    /// the field is only required when it's enabled.
    pub required: bool,
    pub default_value: String,
    /// Field is enabled only when this checkbox field is checked.
    pub enabled_when_checked: Option<String>,
    /// Field is enabled only when this checkbox field is unchecked.
    pub enabled_when_unchecked: Option<String>,
    /// Field is disabled whenever the named field has a non-empty value.
    /// Used for fields whose value is supplied by an `AuthProfileRef`
    /// expansion (e.g. `sso_start_url` disabled when `sso_session_ref` is
    /// set), so the user sees the inherited value cannot be edited inline.
    #[serde(default)]
    pub disabled_when_field_set: Option<String>,
    /// Optional hint displayed below the input (FontSizes::XS, muted_foreground).
    #[serde(default)]
    pub help: Option<String>,
}

/// A section of related form fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormSection {
    pub title: String,
    pub fields: Vec<FormFieldDef>,
}

/// A tab containing form sections.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormTab {
    pub id: String,
    pub label: String,
    pub sections: Vec<FormSection>,
}

/// Complete form definition for a driver.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverFormDef {
    pub tabs: Vec<FormTab>,
}

/// Values collected from a driver form.
pub type FormValues = HashMap<String, String>;

/// How a connection field value travels in an export bundle.
///
/// The default derivation maps `Password`/`WriteOnly` form kinds to `Secret`
/// and `FilePath` to `LocalPath`; everything else defaults to `Include`.
/// Drivers override `DbDriver::export_field_hint` only for values whose
/// semantics cannot be inferred from the field kind alone — for example, a
/// `Text` or `AuthProfileRef` field that names an environment-local AWS
/// profile must be marked `RequiredOnImport` explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFieldHint {
    /// Value travels as-is in the cleartext config section of the bundle.
    Include,
    /// Value is a keyring-managed secret and is placed only in the encrypted
    /// (or force-plaintext) secrets section of the bundle.
    Secret,
    /// Value is a filesystem-local path that may not resolve at the destination.
    /// Included verbatim in the cleartext config with a portability warning.
    LocalPath,
    /// Value is omitted from the bundle entirely and recorded as a required
    /// reference; the importer must supply it before the connection can be used.
    RequiredOnImport,
}

/// Describes a structured per-field transform to apply during export.
///
/// Returned by `DbDriver::export_field_transform`. The default implementation
/// returns `None`, leaving all fields on the existing `export_field_hint` path.
/// URI-bearing drivers override it for the `uri` field when credentials are
/// embedded, splitting the URI into a cleartext skeleton and a recoverable secret.
pub enum FieldExportTransform {
    /// No special transform; fall through to `export_field_hint`.
    None,
    /// The field value was split: the password-stripped skeleton stays in the
    /// cleartext `[connections.fields]` section, while the extracted password
    /// rides in `[secrets]` and is re-merged by the runtime injection path on
    /// connect (e.g. `inject_password_into_pg_uri`).
    SplitSecret {
        /// URI with the password component replaced by an empty placeholder
        /// (e.g. `postgres://alice:@host/db`). Contains NO credential.
        skeleton: String,
        /// The extracted password. Staged into `[secrets]` and written to
        /// `connection_secret_ref` on import so the runtime injection path picks it up.
        secret: ::secrecy::SecretString,
    },
}

// ---------------------------------------------------------------------------
// Builder helpers — keep form definitions concise
// ---------------------------------------------------------------------------

pub fn field(id: &str, label: &str, kind: FormFieldKind, placeholder: &str) -> FormFieldDef {
    FormFieldDef {
        id: id.into(),
        label: label.into(),
        kind,
        placeholder: placeholder.into(),
        required: false,
        default_value: String::new(),
        enabled_when_checked: None,
        enabled_when_unchecked: None,
        disabled_when_field_set: None,
        help: None,
    }
}

pub fn field_required(
    id: &str,
    label: &str,
    kind: FormFieldKind,
    placeholder: &str,
) -> FormFieldDef {
    FormFieldDef {
        required: true,
        ..field(id, label, kind, placeholder)
    }
}

pub fn with_help(mut f: FormFieldDef, help: &str) -> FormFieldDef {
    f.help = Some(help.to_string());
    f
}

pub fn with_default(mut f: FormFieldDef, default: &str) -> FormFieldDef {
    f.default_value = default.into();
    f
}

pub fn when_checked(mut f: FormFieldDef, dep: &str) -> FormFieldDef {
    f.enabled_when_checked = Some(dep.into());
    f
}

pub fn when_unchecked(mut f: FormFieldDef, dep: &str) -> FormFieldDef {
    f.enabled_when_unchecked = Some(dep.into());
    f
}

// ---------------------------------------------------------------------------
// Common field constructors
// ---------------------------------------------------------------------------

pub fn field_password() -> FormFieldDef {
    field("password", "Password", FormFieldKind::Password, "")
}

pub fn field_file_path() -> FormFieldDef {
    field_required(
        "path",
        "File Path",
        FormFieldKind::FilePath,
        "/path/to/database.db",
    )
}

pub fn field_use_uri() -> FormFieldDef {
    field("use_uri", "Use Connection URI", FormFieldKind::Checkbox, "")
}

fn ssh_auth_method_options() -> Vec<SelectOption> {
    vec![
        SelectOption::new("private_key", "Private Key"),
        SelectOption::new("password", "Password"),
    ]
}

pub fn ssh_tab() -> FormTab {
    FormTab {
        id: "ssh".into(),
        label: "SSH".into(),
        sections: vec![FormSection {
            title: "SSH Tunnel".into(),
            fields: vec![
                field(
                    "ssh_enabled",
                    "Enable SSH tunnel",
                    FormFieldKind::Checkbox,
                    "",
                ),
                field(
                    "ssh_host",
                    "SSH Host",
                    FormFieldKind::Text,
                    "bastion.example.com",
                ),
                with_default(
                    field("ssh_port", "SSH Port", FormFieldKind::Number, "22"),
                    "22",
                ),
                field("ssh_user", "SSH User", FormFieldKind::Text, "ec2-user"),
                with_default(
                    field(
                        "ssh_auth_method",
                        "Auth Method",
                        FormFieldKind::Select {
                            options: ssh_auth_method_options(),
                        },
                        "",
                    ),
                    "private_key",
                ),
                field(
                    "ssh_key_path",
                    "Private Key Path",
                    FormFieldKind::FilePath,
                    "~/.ssh/id_rsa",
                ),
                field(
                    "ssh_passphrase",
                    "Key Passphrase",
                    FormFieldKind::Password,
                    "Key passphrase (optional)",
                ),
                field(
                    "ssh_password",
                    "SSH Password",
                    FormFieldKind::Password,
                    "SSH password",
                ),
            ],
        }],
    }
}

// ---------------------------------------------------------------------------
// Impl blocks
// ---------------------------------------------------------------------------

impl DriverFormDef {
    pub fn main_tab(&self) -> Option<&FormTab> {
        self.tabs.first()
    }

    pub fn ssh_tab(&self) -> Option<&FormTab> {
        self.tabs.iter().find(|t| t.id == "ssh")
    }

    pub fn supports_ssh(&self) -> bool {
        self.tabs.iter().any(|t| t.id == "ssh")
    }

    pub fn uses_file_form(&self) -> bool {
        self.tabs
            .iter()
            .flat_map(|t| t.sections.iter())
            .flat_map(|s| s.fields.iter())
            .any(|f| f.id == "path")
    }

    pub fn field(&self, id: &str) -> Option<&FormFieldDef> {
        self.tabs
            .iter()
            .flat_map(|t| t.sections.iter())
            .flat_map(|s| s.fields.iter())
            .find(|f| f.id == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_select_round_trips_via_serde() {
        let kind = FormFieldKind::DynamicSelect {
            depends_on: vec!["region".to_string()],
            refresh: RefreshTrigger::OnDependencyChange,
            requires_session: true,
            allow_freeform: false,
        };

        let serialized = serde_json::to_string(&kind).unwrap();
        let deserialized: FormFieldKind = serde_json::from_str(&serialized).unwrap();

        assert_eq!(kind, deserialized);
    }

    #[test]
    fn dynamic_select_defaults_requires_session_and_allow_freeform_to_false() {
        // JSON that omits the optional bool fields to verify #[serde(default)] behavior.
        let json = r#"{
            "DynamicSelect": {
                "depends_on": [],
                "refresh": "OnFocus"
            }
        }"#;

        let kind: FormFieldKind = serde_json::from_str(json).unwrap();

        let FormFieldKind::DynamicSelect {
            requires_session,
            allow_freeform,
            refresh,
            ..
        } = kind
        else {
            panic!("expected DynamicSelect variant");
        };

        assert!(!requires_session);
        assert!(!allow_freeform);
        assert_eq!(refresh, RefreshTrigger::OnFocus);
    }

    #[test]
    fn unknown_form_field_kind_variant_is_rejected() {
        // A future variant unknown to this binary must NOT silently deserialize.
        let json = r#"{"QuantumField": {"some": "data"}}"#;
        let result = serde_json::from_str::<FormFieldKind>(json);
        assert!(
            result.is_err(),
            "expected deserialization to fail for unknown variant"
        );
    }

    #[test]
    fn refresh_trigger_all_variants_round_trip() {
        for trigger in [
            RefreshTrigger::Manual,
            RefreshTrigger::OnDependencyChange,
            RefreshTrigger::OnFocus,
            RefreshTrigger::OnLoginComplete,
        ] {
            let serialized = serde_json::to_string(&trigger).unwrap();
            let deserialized: RefreshTrigger = serde_json::from_str(&serialized).unwrap();
            assert_eq!(trigger, deserialized);
        }
    }

    #[test]
    fn export_field_hint_variants_are_eq_debug_clone() {
        let variants = [
            ExportFieldHint::Include,
            ExportFieldHint::Secret,
            ExportFieldHint::LocalPath,
            ExportFieldHint::RequiredOnImport,
        ];

        for hint in &variants {
            let cloned = hint.clone();
            assert_eq!(hint, &cloned);
            let debug_str = format!("{:?}", hint);
            assert!(!debug_str.is_empty());
        }

        assert_ne!(ExportFieldHint::Include, ExportFieldHint::Secret);
        assert_ne!(ExportFieldHint::Secret, ExportFieldHint::LocalPath);
        assert_ne!(
            ExportFieldHint::LocalPath,
            ExportFieldHint::RequiredOnImport
        );
    }
}
