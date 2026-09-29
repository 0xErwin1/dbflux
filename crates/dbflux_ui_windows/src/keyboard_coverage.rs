//! Keyboard coverage registries of the settings and connection manager
//! windows (see `dbflux_ui_base::keyboard_coverage`). The coverage tests
//! beside each window render it and check the frame against its registry,
//! so a new `.id(..).on_click(..)` element fails until it is listed here.

use dbflux_app::keymap::{Command, ContextId};
use dbflux_ui_base::keyboard_coverage::{KeyboardPath, SurfaceRegistry};

/// The settings window. Ctrl+H and Ctrl+L move between the section list and
/// the section; in a section the form ring (J and K, Tab) reaches every
/// control, Enter or Space works it and H and L step a segmented field. The
/// section keys add (N), import (I) and save (Ctrl+S).
pub(crate) const SETTINGS: SurfaceRegistry = SurfaceRegistry {
    name: "settings window",
    contexts: &[ContextId::Settings, ContextId::FormNavigation],
    entries: &[
        ("settings-nav-*", KeyboardPath::Command(Command::SelectNext)),
        (
            "settings-close",
            KeyboardPath::Command(Command::CloseWindow),
        ),
        // Save, new and import of every section.
        ("save-*", KeyboardPath::Command(Command::SaveQuery)),
        ("updates-save", KeyboardPath::Command(Command::SaveQuery)),
        ("new-*", KeyboardPath::Command(Command::AddItem)),
        ("import-*", KeyboardPath::Command(Command::ImportItems)),
        // The MCP sections' master-detail list and forms.
        (
            "master-detail-list-new-action",
            KeyboardPath::Command(Command::AddItem),
        ),
        ("mcp-*-save", KeyboardPath::Command(Command::SaveQuery)),
        (
            "master-detail-row-*",
            KeyboardPath::Command(Command::SelectNext),
        ),
        // Segmented fields: H and L step the value on the ring.
        (
            "segmented-density-*",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "segmented-focus-on-launch-*",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "segmented-theme-*",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "segmented-policy-class-*",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-blocking",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-detached",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-command",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-script",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-lua",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-disconnect",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-ignore",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-warn",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-basic",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-http",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-https",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-none",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-socks5",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-auth_provider",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-driver",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-password",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        (
            "seg-ctl-item-private-key",
            KeyboardPath::Command(Command::ColumnRight),
        ),
        // Keybindings: rows move with J and K, a context header folds with
        // Space, Enter records new keys and C opens the context filter.
        ("keybinding-*-edit", KeyboardPath::Command(Command::Execute)),
        ("keybinding-*", KeyboardPath::Command(Command::SelectNext)),
        ("context-*", KeyboardPath::Command(Command::ExpandCollapse)),
        (
            "keybindings-context-filter.*",
            KeyboardPath::Command(Command::FilterByContext),
        ),
        // Drivers: the driver list moves with J and K.
        (
            "settings-driver-*",
            KeyboardPath::Command(Command::SelectNext),
        ),
        // Stops of the sections' form rings.
        ("confirm-dangerous", KeyboardPath::Command(Command::Execute)),
        (
            "general-language.*",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "general-refresh-policy.*",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "general-vim-leader.*",
            KeyboardPath::Command(Command::Execute),
        ),
        ("pause-on-error", KeyboardPath::Command(Command::Execute)),
        ("refresh-visible", KeyboardPath::Command(Command::Execute)),
        ("reopen-conns", KeyboardPath::Command(Command::Execute)),
        ("requires-preview", KeyboardPath::Command(Command::Execute)),
        ("requires-where", KeyboardPath::Command(Command::Execute)),
        ("restore-session", KeyboardPath::Command(Command::Execute)),
        ("vim-mode", KeyboardPath::Command(Command::Execute)),
        ("audit-enabled", KeyboardPath::Command(Command::Execute)),
        (
            "capture-query-text",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "audit-log-capture-level.*",
            KeyboardPath::Command(Command::Execute),
        ),
        ("purge-on-startup", KeyboardPath::Command(Command::Execute)),
        ("redact-sensitive", KeyboardPath::Command(Command::Execute)),
        (
            "auth-profile-enabled",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "cancel-auth-profile",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "drv-confirm-dangerous.*",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "drv-override-refresh-interval",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "drv-override-refresh-policy",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "drv-requires-preview.*",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "drv-requires-where.*",
            KeyboardPath::Command(Command::Execute),
        ),
        ("hook-enabled", KeyboardPath::Command(Command::Execute)),
        ("hook-inherit-env", KeyboardPath::Command(Command::Execute)),
        ("proxy-enabled", KeyboardPath::Command(Command::Execute)),
        ("add-arg", KeyboardPath::Command(Command::Execute)),
        ("add-env", KeyboardPath::Command(Command::Execute)),
        ("svc-enabled", KeyboardPath::Command(Command::Execute)),
        ("browse-ssh-key", KeyboardPath::Command(Command::Execute)),
        ("ssh-save-secret", KeyboardPath::Command(Command::Execute)),
        ("test-ssh-tunnel", KeyboardPath::Command(Command::Execute)),
        (
            "toggle-ssh-passphrase",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "updates-check-on-startup",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "updates-show-whats-new",
            KeyboardPath::Command(Command::Execute),
        ),
        ("updates-check-now", KeyboardPath::Command(Command::Execute)),
        (
            "updates-open-whats-new",
            KeyboardPath::Command(Command::Execute),
        ),
        ("updates-reset", KeyboardPath::Command(Command::Execute)),
        (
            "updates-show-welcome",
            KeyboardPath::Command(Command::Execute),
        ),
        ("about-link-*", KeyboardPath::Command(Command::Execute)),
        ("mcp-client-active", KeyboardPath::Command(Command::Execute)),
        ("mcp-role-*.*", KeyboardPath::Command(Command::Execute)),
        ("mcp-policy-*", KeyboardPath::Command(Command::Execute)),
        ("mcp-policy-*.*", KeyboardPath::Command(Command::Execute)),
        ("policy-tool-*", KeyboardPath::Command(Command::Execute)),
    ],
};

/// The connection manager: the driver list, the form tabs (Main, Access,
/// Settings, MCP) and their rings. J and K or the arrows move the form
/// ring, Enter or Space works the control under it, H and L step a choice,
/// Ctrl+H and Ctrl+L switch tabs and Ctrl+S saves.
pub(crate) const CONNECTION_MANAGER: SurfaceRegistry = SurfaceRegistry {
    name: "connection manager",
    contexts: &[ContextId::ConnectionManager, ContextId::FormNavigation],
    entries: &[
        // The driver list: arrows, H, J, K, L move over the cards, Enter
        // configures the driver under the cursor, Escape closes the window.
        (
            "cm-driver-card-*",
            KeyboardPath::Command(Command::SelectNext),
        ),
        (
            "cm-driver-configure",
            KeyboardPath::Command(Command::Execute),
        ),
        ("cm-driver-cancel", KeyboardPath::Command(Command::Cancel)),
        (
            "cm-driver-import",
            KeyboardPath::MouseOnly("gap: no key opens Import connections from the driver list"),
        ),
        (
            "cm-driver-import-external",
            KeyboardPath::MouseOnly(
                "gap: no key opens Import from another client from the driver list",
            ),
        ),
        // The form: Ctrl+L and Ctrl+H switch tabs, Escape goes back to the
        // driver list (or closes an edited profile), Ctrl+S saves.
        ("tab-*", KeyboardPath::Command(Command::CycleFocusForward)),
        ("back", KeyboardPath::Command(Command::Cancel)),
        ("footer-cancel", KeyboardPath::Command(Command::Cancel)),
        ("save-connection", KeyboardPath::Command(Command::SaveQuery)),
        // Choices on the ring: Left and Right step them.
        (
            "cm-environment-*",
            KeyboardPath::Command(Command::FocusRight),
        ),
        (
            "segmented-ssl-mode-*",
            KeyboardPath::Command(Command::FocusRight),
        ),
        (
            "segmented-enter-as-*",
            KeyboardPath::Command(Command::FocusRight),
        ),
        // Stops of the form ring: Enter works them.
        ("cm-host.*", KeyboardPath::Command(Command::Execute)),
        ("cm-database.*", KeyboardPath::Command(Command::Execute)),
        ("cm-user.*", KeyboardPath::Command(Command::Execute)),
        ("cm-password.*", KeyboardPath::Command(Command::Execute)),
        ("save-password", KeyboardPath::Command(Command::Execute)),
        ("toggle-password", KeyboardPath::Command(Command::Execute)),
        ("test-connection", KeyboardPath::Command(Command::Execute)),
        (
            "access-method-dropdown.*",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "auth-profile-dropdown.*",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "auth-open-settings",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "auth-refresh-session",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "auth-login-selected",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "ssm-auth-open-settings",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "ssm-auth-refresh-session",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "ssm-auth-login-selected",
            KeyboardPath::Command(Command::Execute),
        ),
        ("ssh-enabled", KeyboardPath::Command(Command::Execute)),
        ("browse-ssh-key", KeyboardPath::Command(Command::Execute)),
        ("clear-ssh-tunnel", KeyboardPath::Command(Command::Execute)),
        (
            "ssh-edit-in-settings",
            KeyboardPath::Command(Command::Execute),
        ),
        ("save-ssh-tunnel", KeyboardPath::Command(Command::Execute)),
        (
            "save-ssh-passphrase",
            KeyboardPath::Command(Command::Execute),
        ),
        ("save-ssh-password", KeyboardPath::Command(Command::Execute)),
        ("test-ssh", KeyboardPath::Command(Command::Execute)),
        ("clear-proxy", KeyboardPath::Command(Command::Execute)),
        (
            "proxy-edit-in-settings",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "conn-override-refresh-policy",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "conn-override-refresh-interval",
            KeyboardPath::Command(Command::Execute),
        ),
        ("conn-mcp-enabled", KeyboardPath::Command(Command::Execute)),
        (
            "conn-mcp-client-allowed",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "cm-mcp-client-filter.*",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "conn-refresh-policy.*",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "conn-confirm-dangerous.*",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "conn-requires-where.*",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "conn-requires-preview.*",
            KeyboardPath::Command(Command::Execute),
        ),
        ("conn-pre-hook.*", KeyboardPath::Command(Command::Execute)),
        ("conn-post-hook.*", KeyboardPath::Command(Command::Execute)),
        (
            "conn-pre-disconnect-hook.*",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "conn-post-disconnect-hook.*",
            KeyboardPath::Command(Command::Execute),
        ),
    ],
};
