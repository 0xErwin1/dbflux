//! Driver-defined connection form fields.
//!
//! This module provides types for drivers to define their connection form
//! fields dynamically, allowing the UI to render forms without hardcoding
//! driver-specific logic.

use std::collections::HashMap;

/// Option for a select field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectOption {
    pub value: &'static str,
    pub label: &'static str,
}

/// Type of form field input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormFieldKind {
    /// Single-line text input.
    Text,
    /// Password input (masked).
    Password,
    /// Numeric input (port numbers, etc).
    Number,
    /// File path input with browse button.
    FilePath,
    /// Checkbox for boolean values.
    Checkbox,
    /// Select dropdown with options.
    Select { options: &'static [SelectOption] },
}

/// Definition of a single form field.
#[derive(Debug, Clone)]
pub struct FormFieldDef {
    /// Unique identifier for this field (e.g., "host", "port", "database").
    pub id: &'static str,

    /// Display label shown to the user.
    pub label: &'static str,

    /// Type of input widget to render.
    pub kind: FormFieldKind,

    /// Placeholder text shown when field is empty.
    pub placeholder: &'static str,

    /// Whether this field is required for validation.
    pub required: bool,

    /// Default value for new connections.
    pub default_value: &'static str,
}

/// A section of related form fields.
#[derive(Debug, Clone)]
pub struct FormSection {
    /// Section title (e.g., "Server", "Authentication").
    pub title: &'static str,

    /// Fields in this section.
    pub fields: &'static [FormFieldDef],
}

/// A tab containing form sections.
#[derive(Debug, Clone, Copy)]
pub struct FormTab {
    pub id: &'static str,
    pub label: &'static str,
    pub sections: &'static [FormSection],
}

/// Complete form definition for a driver.
#[derive(Debug, Clone)]
pub struct DriverFormDef {
    pub tabs: &'static [FormTab],
}

/// Values collected from a driver form.
pub type FormValues = HashMap<String, String>;

// Common field definitions that drivers can reuse

pub const FIELD_HOST: FormFieldDef = FormFieldDef {
    id: "host",
    label: "Host",
    kind: FormFieldKind::Text,
    placeholder: "localhost",
    required: true,
    default_value: "localhost",
};

pub const FIELD_PORT_POSTGRES: FormFieldDef = FormFieldDef {
    id: "port",
    label: "Port",
    kind: FormFieldKind::Number,
    placeholder: "5432",
    required: true,
    default_value: "5432",
};

pub const FIELD_PORT_MYSQL: FormFieldDef = FormFieldDef {
    id: "port",
    label: "Port",
    kind: FormFieldKind::Number,
    placeholder: "3306",
    required: true,
    default_value: "3306",
};

pub const FIELD_PASSWORD: FormFieldDef = FormFieldDef {
    id: "password",
    label: "Password",
    kind: FormFieldKind::Password,
    placeholder: "",
    required: false,
    default_value: "",
};

pub const FIELD_DATABASE_OPTIONAL: FormFieldDef = FormFieldDef {
    id: "database",
    label: "Database",
    kind: FormFieldKind::Text,
    placeholder: "optional - leave empty to browse all",
    required: false,
    default_value: "",
};

pub const FIELD_FILE_PATH: FormFieldDef = FormFieldDef {
    id: "path",
    label: "File Path",
    kind: FormFieldKind::FilePath,
    placeholder: "/path/to/database.db",
    required: true,
    default_value: "",
};

// SSH tunnel field definitions (shared by Postgres, MySQL, MariaDB)

#[allow(dead_code)]
pub const SSH_SSL_MODE_OPTIONS: &[SelectOption] = &[
    SelectOption {
        value: "disable",
        label: "Disable",
    },
    SelectOption {
        value: "prefer",
        label: "Prefer",
    },
    SelectOption {
        value: "require",
        label: "Require",
    },
];

pub const SSH_AUTH_METHOD_OPTIONS: &[SelectOption] = &[
    SelectOption {
        value: "private_key",
        label: "Private Key",
    },
    SelectOption {
        value: "password",
        label: "Password",
    },
];

pub const FIELD_SSH_ENABLED: FormFieldDef = FormFieldDef {
    id: "ssh_enabled",
    label: "Enable SSH tunnel",
    kind: FormFieldKind::Checkbox,
    placeholder: "",
    required: false,
    default_value: "",
};

pub const FIELD_SSH_HOST: FormFieldDef = FormFieldDef {
    id: "ssh_host",
    label: "SSH Host",
    kind: FormFieldKind::Text,
    placeholder: "bastion.example.com",
    required: false,
    default_value: "",
};

pub const FIELD_SSH_PORT: FormFieldDef = FormFieldDef {
    id: "ssh_port",
    label: "SSH Port",
    kind: FormFieldKind::Number,
    placeholder: "22",
    required: false,
    default_value: "22",
};

pub const FIELD_SSH_USER: FormFieldDef = FormFieldDef {
    id: "ssh_user",
    label: "SSH User",
    kind: FormFieldKind::Text,
    placeholder: "ec2-user",
    required: false,
    default_value: "",
};

pub const FIELD_SSH_AUTH_METHOD: FormFieldDef = FormFieldDef {
    id: "ssh_auth_method",
    label: "Auth Method",
    kind: FormFieldKind::Select {
        options: SSH_AUTH_METHOD_OPTIONS,
    },
    placeholder: "",
    required: false,
    default_value: "private_key",
};

pub const FIELD_SSH_KEY_PATH: FormFieldDef = FormFieldDef {
    id: "ssh_key_path",
    label: "Private Key Path",
    kind: FormFieldKind::FilePath,
    placeholder: "~/.ssh/id_rsa",
    required: false,
    default_value: "",
};

pub const FIELD_SSH_PASSPHRASE: FormFieldDef = FormFieldDef {
    id: "ssh_passphrase",
    label: "Key Passphrase",
    kind: FormFieldKind::Password,
    placeholder: "Key passphrase (optional)",
    required: false,
    default_value: "",
};

pub const FIELD_SSH_PASSWORD: FormFieldDef = FormFieldDef {
    id: "ssh_password",
    label: "SSH Password",
    kind: FormFieldKind::Password,
    placeholder: "SSH password",
    required: false,
    default_value: "",
};

pub static SSH_TAB: FormTab = FormTab {
    id: "ssh",
    label: "SSH",
    sections: &[FormSection {
        title: "SSH Tunnel",
        fields: &[
            FIELD_SSH_ENABLED,
            FIELD_SSH_HOST,
            FIELD_SSH_PORT,
            FIELD_SSH_USER,
            FIELD_SSH_AUTH_METHOD,
            FIELD_SSH_KEY_PATH,
            FIELD_SSH_PASSPHRASE,
            FIELD_SSH_PASSWORD,
        ],
    }],
};

// Pre-defined form definitions for common database types

pub static POSTGRES_FORM: DriverFormDef = DriverFormDef {
    tabs: &[
        FormTab {
            id: "main",
            label: "Main",
            sections: &[
                FormSection {
                    title: "Server",
                    fields: &[
                        FIELD_HOST,
                        FIELD_PORT_POSTGRES,
                        FormFieldDef {
                            id: "database",
                            label: "Database",
                            kind: FormFieldKind::Text,
                            placeholder: "postgres",
                            required: true,
                            default_value: "postgres",
                        },
                    ],
                },
                FormSection {
                    title: "Authentication",
                    fields: &[
                        FormFieldDef {
                            id: "user",
                            label: "User",
                            kind: FormFieldKind::Text,
                            placeholder: "postgres",
                            required: true,
                            default_value: "postgres",
                        },
                        FIELD_PASSWORD,
                    ],
                },
            ],
        },
        SSH_TAB,
    ],
};

pub static MYSQL_FORM: DriverFormDef = DriverFormDef {
    tabs: &[
        FormTab {
            id: "main",
            label: "Main",
            sections: &[
                FormSection {
                    title: "Server",
                    fields: &[FIELD_HOST, FIELD_PORT_MYSQL, FIELD_DATABASE_OPTIONAL],
                },
                FormSection {
                    title: "Authentication",
                    fields: &[
                        FormFieldDef {
                            id: "user",
                            label: "User",
                            kind: FormFieldKind::Text,
                            placeholder: "root",
                            required: true,
                            default_value: "root",
                        },
                        FIELD_PASSWORD,
                    ],
                },
            ],
        },
        SSH_TAB,
    ],
};

pub static SQLITE_FORM: DriverFormDef = DriverFormDef {
    tabs: &[FormTab {
        id: "main",
        label: "Main",
        sections: &[FormSection {
            title: "Database",
            fields: &[FIELD_FILE_PATH],
        }],
    }],
};

pub static MONGODB_FORM: DriverFormDef = DriverFormDef {
    tabs: &[FormTab {
        id: "main",
        label: "Main",
        sections: &[FormSection {
            title: "Connection",
            fields: &[
                FormFieldDef {
                    id: "uri",
                    label: "Connection URI",
                    kind: FormFieldKind::Text,
                    placeholder: "mongodb://localhost:27017",
                    required: true,
                    default_value: "mongodb://localhost:27017",
                },
                FormFieldDef {
                    id: "database",
                    label: "Database",
                    kind: FormFieldKind::Text,
                    placeholder: "optional - leave empty to browse all",
                    required: false,
                    default_value: "",
                },
            ],
        }],
    }],
};

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
