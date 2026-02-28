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

/// Type of form field input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FormFieldKind {
    Text,
    Password,
    Number,
    FilePath,
    Checkbox,
    Select { options: Vec<SelectOption> },
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
    }
}

fn field_required(id: &str, label: &str, kind: FormFieldKind, placeholder: &str) -> FormFieldDef {
    FormFieldDef {
        required: true,
        ..field(id, label, kind, placeholder)
    }
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
                        field_password(),
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
                                "mongodb://localhost:27017",
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
                        when_unchecked(
                            field("tls", "Use TLS", FormFieldKind::Checkbox, ""),
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
