//! Shared picker for hosts declared in the user's `~/.ssh/config` (#837).
//!
//! The settings SSH form and the connection manager's Access tab (T5) both
//! embed [`SshHostPicker`]. It loads the pickable hosts of the user's SSH
//! config off the foreground thread (C4), keeps the resolver's diagnostics
//! and exposes the selected alias to the owning form.
//!
//! A missing `config` file is an empty list with no error: the manual fields
//! stay for hosts the config does not describe (S2). An unreadable config or
//! a resolver diagnostic surfaces as a status line near the picker, never a
//! panic. A host the tunnel cannot dial (S5) stays selectable and is marked.

use std::path::{Path, PathBuf};

use dbflux_components::controls::{Dropdown, DropdownItem};
use dbflux_core::LogErr;
use dbflux_ssh::ssh_config::{SshConfigFile, SshConfigHost};
use gpui::{App, AppContext as _, Context, Entity, EventEmitter};

#[cfg(not(test))]
use dirs;

/// Element id of the shared dropdown. Registered in the keyboard coverage
/// registry of every surface that embeds the picker, as
/// `ssh-config-host-picker.*`.
pub const PICKER_ID: &str = "ssh-config-host-picker";

/// Value of the first dropdown entry: no SSH config host, manual fields.
pub const MANUAL_ENTRY_VALUE: &str = "";

/// One load of the user's SSH config, ready to render.
#[derive(Debug, Clone, Default)]
pub struct SshHostSnapshot {
    /// Concrete, pickable hosts in file order (glob patterns stay out, A5).
    pub hosts: Vec<SshConfigHost>,
    /// Lines the resolver could not parse, for the UI to surface.
    pub diagnostics: Vec<String>,
    /// Set when the config file exists but could not be read.
    pub load_error: Option<String>,
}

impl SshHostSnapshot {
    pub fn empty() -> Self {
        Self::default()
    }

    /// The resolved host of `alias`, if the snapshot lists it.
    pub fn host(&self, alias: Option<&str>) -> Option<&SshConfigHost> {
        let alias = alias?;
        self.hosts.iter().find(|host| host.alias == alias)
    }
}

/// Loads the pickable hosts of `config_dir/config`, expanding `~` against
/// `home`. A missing file is an empty snapshot with no error; a read failure
/// is `load_error` and never a panic. Pure and synchronous: the caller runs
/// it off the foreground thread (C4).
pub fn load_ssh_hosts(config_dir: &Path, home: &Path) -> SshHostSnapshot {
    match SshConfigFile::load_with_home(config_dir, home) {
        Ok(file) => SshHostSnapshot {
            hosts: file.hosts(home),
            diagnostics: file.diagnostics().to_vec(),
            load_error: None,
        },
        Err(error) => SshHostSnapshot {
            hosts: Vec::new(),
            diagnostics: Vec::new(),
            load_error: Some(error.to_string()),
        },
    }
}

/// Label of the manual entry: no SSH config host, fields typed by hand.
pub fn manual_entry_label() -> String {
    dbflux_i18n::t!("ssh.config_host.manual_entry")
}

/// The dropdown item standing for "no SSH config host".
pub fn manual_entry_item() -> DropdownItem {
    DropdownItem::with_value(manual_entry_label(), MANUAL_ENTRY_VALUE)
}

/// `user@host:port` of a resolved host; without the user when the config
/// sets none (the local-user fallback applies at connect time).
pub fn resolved_target_label(host: &SshConfigHost) -> String {
    match &host.user {
        Some(user) => format!("{}@{}:{}", user, host.host_name, host.port),
        None => format!("{}:{}", host.host_name, host.port),
    }
}

/// Label of one host entry: the alias, its resolved target and, for a host
/// the tunnel cannot dial, the unsupported directive (S5).
pub fn host_item_label(host: &SshConfigHost) -> String {
    let mut label = format!("{} — {}", host.alias, resolved_target_label(host));
    if let Some(directive) = host.unsupported {
        label.push_str(&format!(
            " ({})",
            dbflux_i18n::t!(
                "ssh.config_host.unsupported",
                directive = directive.as_str()
            )
        ));
    } else if let Some(identity_file) = &host.identity_file {
        label.push_str(&format!(" · {}", identity_file.display()));
    }
    label
}

/// Dropdown items: the manual entry first, then one per host in file order.
pub fn ssh_config_host_items(snapshot: &SshHostSnapshot) -> Vec<DropdownItem> {
    std::iter::once(manual_entry_item())
        .chain(
            snapshot
                .hosts
                .iter()
                .map(|host| DropdownItem::with_value(host_item_label(host), host.alias.clone())),
        )
        .collect()
}

/// A status line shown next to the picker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SshPickerStatus {
    /// The resolved target of the active alias.
    Resolved(String),
    /// A resolver diagnostic, or an alias no longer in the config.
    Warning(String),
    /// The config file exists but could not be read.
    Error(String),
}

/// Emitted once a background load has been applied to the picker.
pub struct SshHostsLoaded;

/// Shared SSH config host picker: a [`Dropdown`] of the user's config hosts
/// plus the selected alias and the loader state around it.
pub struct SshHostPicker {
    dropdown: Entity<Dropdown>,
    snapshot: SshHostSnapshot,
    selected_alias: Option<String>,
}

impl EventEmitter<SshHostsLoaded> for SshHostPicker {}

impl SshHostPicker {
    /// A picker with nothing loaded: the manual entry only.
    pub fn empty(cx: &mut App) -> Entity<Self> {
        cx.new(Self::with_empty_state)
    }

    fn with_empty_state(cx: &mut Context<Self>) -> Self {
        let dropdown = cx.new(|_| {
            Dropdown::new(PICKER_ID)
                .items(vec![manual_entry_item()])
                .selected_index(Some(0))
        });
        Self {
            dropdown,
            snapshot: SshHostSnapshot::empty(),
            selected_alias: None,
        }
    }

    /// Loads `~/.ssh/config` off the foreground thread (C4). Under
    /// `cfg(test)` this stays empty: no test may read the real home
    /// directory (S7); tests inject fixtures through [`load_in`](Self::load_in).
    pub fn load_default(cx: &mut App) -> Entity<Self> {
        #[cfg(test)]
        return Self::empty(cx);

        #[cfg(not(test))]
        {
            let Some(home) = dirs::home_dir() else {
                return Self::empty(cx);
            };
            Self::load_in(home.join(".ssh"), home, cx)
        }
    }

    /// Loads the config directory the caller names, off the foreground
    /// thread (C4), and applies the snapshot when it arrives. Test seam for
    /// fixtures (S7) and the path every surface uses in production.
    pub fn load_in(config_dir: PathBuf, home: PathBuf, cx: &mut App) -> Entity<Self> {
        cx.new(|cx| {
            let picker = Self::with_empty_state(cx);
            let task = cx
                .background_executor()
                .spawn(async move { load_ssh_hosts(&config_dir, &home) });
            cx.spawn(async move |this, cx| {
                let snapshot = task.await;
                this.update(cx, |picker, cx| {
                    picker.apply_snapshot(snapshot, cx);
                    cx.emit(SshHostsLoaded);
                })
                .log_err();
            })
            .detach();
            picker
        })
    }

    pub fn dropdown(&self) -> &Entity<Dropdown> {
        &self.dropdown
    }

    pub fn snapshot(&self) -> &SshHostSnapshot {
        &self.snapshot
    }

    /// The selected config alias; `None` is the manual entry.
    pub fn selected_alias(&self) -> Option<&str> {
        self.selected_alias.as_deref()
    }

    /// The alias at a dropdown index; index 0 is the manual entry and has
    /// no alias.
    pub fn alias_at(&self, index: usize) -> Option<String> {
        self.snapshot
            .hosts
            .get(index.checked_sub(1)?)
            .map(|host| host.alias.clone())
    }

    /// Sets the selection from an alias and syncs the dropdown. An alias
    /// missing from the loaded snapshot stays stored — the connect path
    /// reports it (A7) — with no dropdown row selected.
    pub fn set_selected_alias(&mut self, alias: Option<&str>, cx: &mut Context<Self>) {
        self.selected_alias = alias.map(str::to_string);
        let index = match &self.selected_alias {
            None => Some(0),
            Some(alias) => self
                .snapshot
                .hosts
                .iter()
                .position(|host| host.alias == *alias)
                .map(|position| position + 1),
        };
        self.dropdown
            .update(cx, |dropdown, cx| dropdown.set_selected_index(index, cx));
        cx.notify();
    }

    /// Replaces the loaded hosts and re-syncs the selection.
    pub fn apply_snapshot(&mut self, snapshot: SshHostSnapshot, cx: &mut Context<Self>) {
        let items = ssh_config_host_items(&snapshot);
        self.snapshot = snapshot;
        self.dropdown
            .update(cx, |dropdown, cx| dropdown.set_items(items, cx));
        let alias = self.selected_alias.clone();
        self.set_selected_alias(alias.as_deref(), cx);
    }

    /// Status lines to render next to the picker: the load error, the state
    /// of the active alias, then the resolver diagnostics.
    pub fn statuses(&self) -> Vec<SshPickerStatus> {
        let mut statuses = Vec::new();

        if let Some(error) = &self.snapshot.load_error {
            statuses.push(SshPickerStatus::Error(dbflux_i18n::t!(
                "ssh.config_host.load_error",
                error = error
            )));
        }

        match self
            .selected_alias
            .as_deref()
            .and_then(|alias| self.snapshot.host(Some(alias)))
        {
            Some(host) => statuses.push(SshPickerStatus::Resolved(dbflux_i18n::t!(
                "ssh.config_host.resolved",
                target = resolved_target_label(host)
            ))),
            None if self.selected_alias.is_some() => statuses.push(SshPickerStatus::Warning(
                dbflux_i18n::t!("ssh.config_host.alias_missing"),
            )),
            None => {}
        }

        for diagnostic in &self.snapshot.diagnostics {
            statuses.push(SshPickerStatus::Warning(dbflux_i18n::t!(
                "ssh.config_host.diagnostic",
                diagnostic = diagnostic
            )));
        }

        statuses
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_ssh::ssh_config::UnsupportedDirective;
    use std::fs;

    /// A throwaway fixture directory. Never the real home directory (S7).
    struct FixtureDir(PathBuf);

    impl FixtureDir {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "dbflux-ssh-host-picker-{name}-{}-{}",
                std::process::id(),
                uuid::Uuid::new_v4()
            ));
            fs::create_dir_all(&dir).expect("create fixture dir");
            Self(dir)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        /// A home directory inside the fixture, so `~` expansion never
        /// reaches the real home (S7).
        fn home(&self) -> PathBuf {
            self.0.join("home")
        }

        fn write_config(&self, text: &str) {
            fs::write(self.0.join("config"), text).expect("write fixture config");
        }
    }

    impl Drop for FixtureDir {
        fn drop(&mut self) {
            if let Err(error) = fs::remove_dir_all(&self.0) {
                eprintln!("fixture cleanup failed: {error}");
            }
        }
    }

    #[test]
    fn lists_concrete_hosts_in_order_with_their_resolved_targets() {
        let fixture = FixtureDir::new("order");
        fixture.write_config(
            "Host alpha\n  HostName alpha.example.com\n  User deploy\n  Port 2222\n  IdentityFile ~/.ssh/id_alpha\n\nHost beta\n  HostName 10.0.0.5\n\nHost *\n  Compression yes\n",
        );

        let snapshot = load_ssh_hosts(fixture.path(), &fixture.home());

        assert!(snapshot.load_error.is_none(), "{:?}", snapshot.load_error);
        assert_eq!(
            snapshot
                .hosts
                .iter()
                .map(|host| host.alias.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "beta"],
            "`Host *` is not pickable (A5) and the order follows the file"
        );
        assert_eq!(
            snapshot.hosts[0].identity_file,
            Some(fixture.home().join(".ssh/id_alpha"))
        );

        let items = ssh_config_host_items(&snapshot);
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].value, MANUAL_ENTRY_VALUE);
        assert_eq!(items[0].label, manual_entry_label());
        assert_eq!(items[1].value, "alpha");
        assert!(items[1].label.contains("alpha"), "{}", items[1].label);
        assert!(
            items[1].label.contains("deploy@alpha.example.com:2222"),
            "{}",
            items[1].label
        );
        assert!(items[1].label.contains("id_alpha"), "{}", items[1].label);
        assert_eq!(items[2].value, "beta");
        assert!(items[2].label.contains("10.0.0.5:22"), "{}", items[2].label);
    }

    #[test]
    fn unsupported_host_is_marked_and_its_alias_stays_selectable() {
        let fixture = FixtureDir::new("unsupported");
        fixture.write_config(
            "Host bastion\n  HostName bastion.example.com\n  ProxyJump jump.example.com\n",
        );

        let snapshot = load_ssh_hosts(fixture.path(), &fixture.home());

        assert_eq!(
            snapshot.hosts[0].unsupported,
            Some(UnsupportedDirective::ProxyJump)
        );
        let items = ssh_config_host_items(&snapshot);
        assert!(items[1].label.contains("ProxyJump"), "{}", items[1].label);
        // S5: the host stays selectable — the connect path reports the
        // directive; the picker only marks it.
        assert_eq!(items[1].value, "bastion");
        assert_eq!(
            snapshot
                .host(Some("bastion"))
                .map(|host| host.alias.as_str()),
            Some("bastion")
        );
    }

    #[test]
    fn missing_config_yields_an_empty_list_no_error_and_the_manual_entry() {
        let fixture = FixtureDir::new("missing");

        let snapshot = load_ssh_hosts(fixture.path(), &fixture.home());

        assert!(snapshot.hosts.is_empty());
        assert!(snapshot.load_error.is_none());
        assert!(snapshot.diagnostics.is_empty());
        let items = ssh_config_host_items(&snapshot);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].value, MANUAL_ENTRY_VALUE);
        assert_eq!(items[0].label, manual_entry_label());
    }

    #[test]
    fn unreadable_config_reports_an_error_instead_of_panicking() {
        let fixture = FixtureDir::new("unreadable");
        // A directory named `config` makes the read fail.
        fs::create_dir_all(fixture.path().join("config")).expect("create config dir");

        let snapshot = load_ssh_hosts(fixture.path(), &fixture.home());

        assert!(snapshot.hosts.is_empty());
        assert!(snapshot.load_error.is_some());
    }

    #[gpui::test]
    fn selecting_and_clearing_an_alias_syncs_the_dropdown(cx: &mut gpui::TestAppContext) {
        let fixture = FixtureDir::new("selection");
        fixture.write_config(
            "Host alpha\n  HostName alpha.example.com\n\nHost beta\n  HostName 10.0.0.5\n",
        );
        let home = fixture.home();
        let config_dir = fixture.path().to_path_buf();

        let picker = cx.update(SshHostPicker::empty);
        cx.update(|cx| {
            let snapshot = load_ssh_hosts(&config_dir, &home);
            picker.update(cx, |picker, cx| picker.apply_snapshot(snapshot, cx));
        });

        cx.update(|cx| {
            picker.update(cx, |picker, cx| picker.set_selected_alias(Some("beta"), cx));
        });
        let (alias, dropdown_value, beta_at_index) = cx.update(|cx| {
            let alias = picker.read(cx).selected_alias().map(str::to_string);
            let value = picker
                .read(cx)
                .dropdown()
                .read(cx)
                .selected_value()
                .map(|value| value.to_string());
            let at = picker.read(cx).alias_at(2);
            (alias, value, at)
        });
        assert_eq!(alias.as_deref(), Some("beta"));
        assert_eq!(
            dropdown_value.as_deref(),
            Some("beta"),
            "the dropdown shows the selected alias"
        );
        assert_eq!(
            beta_at_index.as_deref(),
            Some("beta"),
            "index 1 is the manual entry, index 2 is the first host"
        );

        cx.update(|cx| {
            picker.update(cx, |picker, cx| picker.set_selected_alias(None, cx));
        });
        let (alias, dropdown_value) = cx.update(|cx| {
            let alias = picker.read(cx).selected_alias().map(str::to_string);
            let value = picker
                .read(cx)
                .dropdown()
                .read(cx)
                .selected_value()
                .map(|value| value.to_string());
            (alias, value)
        });
        assert_eq!(alias, None);
        assert_eq!(
            dropdown_value.as_deref(),
            Some(MANUAL_ENTRY_VALUE),
            "clearing selects the manual entry again"
        );
    }

    #[gpui::test]
    fn an_alias_missing_after_a_reload_stays_stored_and_is_reported(cx: &mut gpui::TestAppContext) {
        let picker = cx.update(SshHostPicker::empty);

        cx.update(|cx| {
            picker.update(cx, |picker, cx| picker.set_selected_alias(Some("gone"), cx));
        });
        let (alias, statuses) = cx.update(|cx| {
            let alias = picker.read(cx).selected_alias().map(str::to_string);
            let statuses = picker.read(cx).statuses();
            (alias, statuses)
        });

        assert_eq!(alias.as_deref(), Some("gone"));
        assert!(
            statuses
                .iter()
                .any(|status| matches!(status, SshPickerStatus::Warning(_))),
            "the picker reports the alias is not in the config: {statuses:?}"
        );
    }
}
