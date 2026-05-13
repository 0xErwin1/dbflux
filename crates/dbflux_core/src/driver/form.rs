//! Driver-defined connection form fields.
//!
//! This module provides types for drivers to define their connection form
//! fields dynamically, allowing the UI to render forms without hardcoding
//! driver-specific logic.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::LazyLock;

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

// ---------------------------------------------------------------------------
// Builder helpers — keep form definitions concise
// ---------------------------------------------------------------------------

fn field(id: &str, label: &str, kind: FormFieldKind, placeholder: &str) -> FormFieldDef {
    FormFieldDef {
        id: id.into(),
        label: label.into(),
        kind,
        placeholder: placeholder.into(),
        required: false,
        default_value: String::new(),
        enabled_when_checked: None,
        enabled_when_unchecked: None,
        help: None,
    }
}

fn field_required(id: &str, label: &str, kind: FormFieldKind, placeholder: &str) -> FormFieldDef {
    FormFieldDef {
        required: true,
        ..field(id, label, kind, placeholder)
    }
}

pub fn with_help(mut f: FormFieldDef, help: &str) -> FormFieldDef {
    f.help = Some(help.to_string());
    f
}

fn with_default(mut f: FormFieldDef, default: &str) -> FormFieldDef {
    f.default_value = default.into();
    f
}

fn when_checked(mut f: FormFieldDef, dep: &str) -> FormFieldDef {
    f.enabled_when_checked = Some(dep.into());
    f
}

fn when_unchecked(mut f: FormFieldDef, dep: &str) -> FormFieldDef {
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
// Pre-defined form definitions for common database types
// ---------------------------------------------------------------------------

pub static POSTGRES_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![
        FormTab {
            id: "main".into(),
            label: "Main".into(),
            sections: vec![
                FormSection {
                    title: "Server".into(),
                    fields: vec![
                        field_use_uri(),
                        when_checked(
                            field_required(
                                "uri",
                                "Connection URI",
                                FormFieldKind::Text,
                                "postgresql://user:pass@localhost:5432/db",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field_required("host", "Host", FormFieldKind::Text, "localhost"),
                                "localhost",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field_required("port", "Port", FormFieldKind::Number, "5432"),
                                "5432",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field_required(
                                    "database",
                                    "Database",
                                    FormFieldKind::Text,
                                    "postgres",
                                ),
                                "postgres",
                            ),
                            "use_uri",
                        ),
                    ],
                },
                FormSection {
                    title: "Authentication".into(),
                    fields: vec![
                        when_unchecked(
                            with_default(
                                field_required("user", "User", FormFieldKind::Text, "postgres"),
                                "postgres",
                            ),
                            "use_uri",
                        ),
                        with_help(
                            field_password(),
                            "via Auth Profile · resolved at runtime, never persisted on disk",
                        ),
                    ],
                },
            ],
        },
        ssh_tab(),
    ],
});

pub static MYSQL_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![
        FormTab {
            id: "main".into(),
            label: "Main".into(),
            sections: vec![
                FormSection {
                    title: "Server".into(),
                    fields: vec![
                        field_use_uri(),
                        when_checked(
                            field_required(
                                "uri",
                                "Connection URI",
                                FormFieldKind::Text,
                                "mysql://user:pass@localhost:3306/db",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field_required("host", "Host", FormFieldKind::Text, "localhost"),
                                "localhost",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field_required("port", "Port", FormFieldKind::Number, "3306"),
                                "3306",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            field(
                                "database",
                                "Database",
                                FormFieldKind::Text,
                                "optional - leave empty to browse all",
                            ),
                            "use_uri",
                        ),
                    ],
                },
                FormSection {
                    title: "Authentication".into(),
                    fields: vec![
                        when_unchecked(
                            with_default(
                                field_required("user", "User", FormFieldKind::Text, "root"),
                                "root",
                            ),
                            "use_uri",
                        ),
                        field_password(),
                    ],
                },
            ],
        },
        ssh_tab(),
    ],
});

pub static SQLITE_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![FormTab {
        id: "main".into(),
        label: "Main".into(),
        sections: vec![FormSection {
            title: "Database".into(),
            fields: vec![field_file_path()],
        }],
    }],
});

pub static MONGODB_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![
        FormTab {
            id: "main".into(),
            label: "Main".into(),
            sections: vec![
                FormSection {
                    title: "Server".into(),
                    fields: vec![
                        field_use_uri(),
                        when_checked(
                            field_required(
                                "uri",
                                "Connection URI",
                                FormFieldKind::Text,
                                "mongodb://host:port or mongodb+srv://...",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field_required("host", "Host", FormFieldKind::Text, "localhost"),
                                "localhost",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field_required("port", "Port", FormFieldKind::Number, "27017"),
                                "27017",
                            ),
                            "use_uri",
                        ),
                        field(
                            "database",
                            "Database",
                            FormFieldKind::Text,
                            "optional - leave empty to browse all",
                        ),
                    ],
                },
                FormSection {
                    title: "Authentication".into(),
                    fields: vec![
                        field("user", "User", FormFieldKind::Text, "optional"),
                        field_password(),
                        when_unchecked(
                            field(
                                "auth_database",
                                "Auth Database",
                                FormFieldKind::Text,
                                "admin (default)",
                            ),
                            "use_uri",
                        ),
                    ],
                },
            ],
        },
        ssh_tab(),
    ],
});

pub static REDIS_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![
        FormTab {
            id: "main".into(),
            label: "Main".into(),
            sections: vec![
                FormSection {
                    title: "Server".into(),
                    fields: vec![
                        field_use_uri(),
                        when_checked(
                            field_required(
                                "uri",
                                "Connection URI",
                                FormFieldKind::Text,
                                "redis://localhost:6379/0",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field_required("host", "Host", FormFieldKind::Text, "localhost"),
                                "localhost",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field_required("port", "Port", FormFieldKind::Number, "6379"),
                                "6379",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field("database", "Database Index", FormFieldKind::Number, "0"),
                                "0",
                            ),
                            "use_uri",
                        ),
                    ],
                },
                FormSection {
                    title: "Authentication".into(),
                    fields: vec![
                        when_unchecked(
                            field("user", "User", FormFieldKind::Text, "optional"),
                            "use_uri",
                        ),
                        field_password(),
                    ],
                },
            ],
        },
        ssh_tab(),
    ],
});

pub static DYNAMODB_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![FormTab {
        id: "main".into(),
        label: "Main".into(),
        sections: vec![
            FormSection {
                title: "AWS".into(),
                fields: vec![
                    field_required("region", "Region", FormFieldKind::Text, "us-east-1"),
                    field(
                        "profile",
                        "Profile",
                        FormFieldKind::Text,
                        "optional AWS profile",
                    ),
                ],
            },
            FormSection {
                title: "Target".into(),
                fields: vec![
                    field(
                        "endpoint",
                        "Endpoint Override",
                        FormFieldKind::Text,
                        "http://localhost:8000",
                    ),
                    field("table", "Default Table", FormFieldKind::Text, "optional"),
                ],
            },
        ],
    }],
});

pub static CLOUDWATCH_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![FormTab {
        id: "main".into(),
        label: "Main".into(),
        sections: vec![FormSection {
            title: "AWS".into(),
            fields: vec![
                field_required("region", "Region", FormFieldKind::Text, "us-east-1"),
                field(
                    "profile",
                    "Profile",
                    FormFieldKind::Text,
                    "optional AWS profile",
                ),
                field(
                    "endpoint",
                    "Endpoint Override",
                    FormFieldKind::Text,
                    "http://localhost:4566",
                ),
            ],
        }],
    }],
});

/// InfluxDB connection form.
///
/// Uses a `use_v2` checkbox to toggle between v1 (InfluxQL, user/password) and
/// v2 (Flux/InfluxQL, token-based) modes. The form framework only supports checkbox-based
/// conditional visibility, so a Select field cannot drive field show/hide directly.
pub static INFLUXDB_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![FormTab {
        id: "main".into(),
        label: "Main".into(),
        sections: vec![
            FormSection {
                title: "Version".into(),
                fields: vec![with_help(
                    with_default(
                        field(
                            "use_v2",
                            "Use InfluxDB v2 (token auth / Flux)",
                            FormFieldKind::Checkbox,
                            "",
                        ),
                        "true",
                    ),
                    "Enable for InfluxDB v2+ (token-based auth). Disable for InfluxDB v1 (username/password).",
                )],
            },
            FormSection {
                title: "Connection".into(),
                fields: vec![
                    field_required("url", "URL", FormFieldKind::Text, "http://localhost:8086"),
                    // v2-only: org
                    when_checked(
                        field("org", "Organization", FormFieldKind::Text, "my-org"),
                        "use_v2",
                    ),
                    // v2-only: bucket
                    when_checked(
                        field_required("bucket", "Bucket", FormFieldKind::Text, "my-bucket"),
                        "use_v2",
                    ),
                    // v1-only: database
                    when_unchecked(
                        field_required("database", "Database", FormFieldKind::Text, "mydb"),
                        "use_v2",
                    ),
                    // v1-only: retention policy
                    when_unchecked(
                        field(
                            "retention_policy",
                            "Retention Policy",
                            FormFieldKind::Text,
                            "autogen",
                        ),
                        "use_v2",
                    ),
                ],
            },
            // The actual secret (API token for v2 / password for v1) is collected by the
            // generic password section the connection manager renders for any driver
            // declaring AUTHENTICATION. That section already provides masking, an eye
            // toggle, the "save to keyring" checkbox, and the literal/keyring source
            // selector. We only expose the v1 username here.
            FormSection {
                title: "Authentication".into(),
                fields: vec![when_unchecked(
                    field("user", "User", FormFieldKind::Text, "optional"),
                    "use_v2",
                )],
            },
        ],
    }],
});

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
    fn cloudwatch_form_exposes_aws_region_profile_and_endpoint_fields() {
        let main_tab = CLOUDWATCH_FORM.main_tab().expect("main tab");

        assert!(
            main_tab
                .sections
                .iter()
                .flat_map(|section| section.fields.iter())
                .any(|field| field.id == "region" && field.required)
        );
        assert!(CLOUDWATCH_FORM.field("profile").is_some());
        assert!(CLOUDWATCH_FORM.field("endpoint").is_some());
    }

    // --- INFLUXDB_FORM tests ---

    /// v2 mode: org and bucket are gated on `use_v2` being checked;
    /// database, retention_policy, and user are gated on it being unchecked.
    /// The actual secret (token / password) lives in the generic password section
    /// the connection manager renders for any driver declaring AUTHENTICATION, so
    /// the form itself owns no Password field.
    #[test]
    fn influxdb_form_v2_fields_are_gated_on_use_v2_checkbox() {
        let url_field = INFLUXDB_FORM.field("url").expect("url field must exist");
        assert!(url_field.required, "url must be required");
        assert!(
            url_field.enabled_when_checked.is_none() && url_field.enabled_when_unchecked.is_none(),
            "url must not be version-gated"
        );

        for v2_field_id in &["org", "bucket"] {
            let field = INFLUXDB_FORM
                .field(v2_field_id)
                .unwrap_or_else(|| panic!("field '{}' must exist in INFLUXDB_FORM", v2_field_id));

            assert_eq!(
                field.enabled_when_checked.as_deref(),
                Some("use_v2"),
                "field '{}' must be visible only when use_v2 is checked",
                v2_field_id
            );
        }

        for v1_field_id in &["database", "retention_policy", "user"] {
            let field = INFLUXDB_FORM
                .field(v1_field_id)
                .unwrap_or_else(|| panic!("field '{}' must exist in INFLUXDB_FORM", v1_field_id));

            assert_eq!(
                field.enabled_when_unchecked.as_deref(),
                Some("use_v2"),
                "field '{}' must be visible only when use_v2 is unchecked",
                v1_field_id
            );
        }

        // The form must NOT define its own Password fields — that responsibility is
        // delegated to the generic password section.
        for delegated_id in &["token", "password"] {
            assert!(
                INFLUXDB_FORM.field(delegated_id).is_none(),
                "field '{}' should NOT live in INFLUXDB_FORM (handled by the generic password section)",
                delegated_id
            );
        }
    }

    /// v2 mode: bucket is required (gated but required within its gate);
    /// org and token are optional (convenience).
    #[test]
    fn influxdb_form_bucket_is_required_in_v2_mode() {
        let bucket_field = INFLUXDB_FORM
            .field("bucket")
            .expect("bucket field must exist");
        assert!(
            bucket_field.required,
            "bucket must be required for v2 connections"
        );
    }

    /// v1 mode: database is required; retention_policy and user are optional.
    #[test]
    fn influxdb_form_database_is_required_in_v1_mode() {
        let db_field = INFLUXDB_FORM
            .field("database")
            .expect("database field must exist");
        assert!(
            db_field.required,
            "database must be required for v1 connections"
        );

        let rp_field = INFLUXDB_FORM
            .field("retention_policy")
            .expect("retention_policy field must exist");
        assert!(
            !rp_field.required,
            "retention_policy must be optional (v1 default is 'autogen')"
        );
    }

    /// The form must not have an SSH tab (InfluxDB uses HTTP, no tunnel in Phase A).
    #[test]
    fn influxdb_form_has_no_ssh_tab() {
        assert!(
            !INFLUXDB_FORM.supports_ssh(),
            "INFLUXDB_FORM must not include an SSH tab in Phase A"
        );
    }

    /// The use_v2 checkbox should default to true (V2 is the current standard).
    #[test]
    fn influxdb_form_use_v2_defaults_to_true() {
        let use_v2 = INFLUXDB_FORM
            .field("use_v2")
            .expect("use_v2 checkbox must exist");
        assert_eq!(
            use_v2.default_value, "true",
            "use_v2 must default to true (V2 is default)"
        );
    }
}
