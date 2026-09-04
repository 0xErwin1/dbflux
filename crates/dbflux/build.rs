//! Embeds the Windows executable resources: the application icon and the
//! `VERSIONINFO` block that Explorer shows under Properties and that the
//! taskbar, the Open With dialog, and installers read the product name from.
//!
//! macOS takes the icon and version from the bundle and Linux from the desktop
//! entry, so on those platforms the script only registers its inputs and exits.
//!
//! The resource text is assembled on every platform so `cargo check` on Linux
//! or macOS still type-checks it; only the resource compiler call is gated.

use std::env;
use std::path::{Path, PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    let version = env_var("CARGO_PKG_VERSION");
    let identity = ChannelIdentity::from_version(&version);

    let manifest_dir = PathBuf::from(env_var("CARGO_MANIFEST_DIR"));
    let icon = manifest_dir
        .join("../../packaging/icons")
        .join(identity.icon_file);
    println!("cargo:rerun-if-changed={}", icon.display());

    let out_dir = PathBuf::from(env_var("OUT_DIR"));
    let script = out_dir.join("dbflux.rc");
    let resource = ResourceScript::new(&icon, &version, identity).render();

    if let Err(error) = std::fs::write(&script, resource) {
        panic!("failed to write {}: {error}", script.display());
    }

    #[cfg(windows)]
    embed(&script, &icon);

    #[cfg(not(windows))]
    drop(script);
}

fn env_var(name: &str) -> String {
    match env::var(name) {
        Ok(value) => value,
        Err(error) => panic!("{name} is not set for the build script: {error}"),
    }
}

/// Compile the resource script and link it into the executable.
///
/// `manifest_required` fails the build when no resource compiler is available
/// or compilation fails. An executable that silently shipped without its icon
/// and version block would be the worse outcome.
#[cfg(windows)]
fn embed(script: &Path, icon: &Path) {
    if !icon.is_file() {
        panic!("application icon not found at {}", icon.display());
    }

    if let Err(result) = embed_resource::compile(script, embed_resource::NONE).manifest_required() {
        panic!("failed to embed the Windows resources: {result}");
    }
}

/// The per-channel identity the executable carries.
///
/// Mirrors `ReleaseChannel::from_version` in `dbflux_core`. The build script
/// cannot depend on that crate without pulling the whole workspace into the
/// build-dependency graph, so the three-line rule is repeated here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ChannelIdentity {
    product_name: &'static str,
    icon_file: &'static str,
    prerelease: bool,
}

impl ChannelIdentity {
    fn from_version(version: &str) -> Self {
        if version.contains("-nightly") {
            Self {
                product_name: "DBFlux Nightly",
                icon_file: "dbflux-nightly.ico",
                prerelease: true,
            }
        } else if version.contains("-rc") {
            Self {
                product_name: "DBFlux",
                icon_file: "dbflux.ico",
                prerelease: true,
            }
        } else {
            Self {
                product_name: "DBFlux",
                icon_file: "dbflux.ico",
                prerelease: false,
            }
        }
    }
}

/// The text of the `.rc` file handed to the resource compiler.
struct ResourceScript<'a> {
    icon: &'a Path,
    version: &'a str,
    identity: ChannelIdentity,
}

impl<'a> ResourceScript<'a> {
    fn new(icon: &'a Path, version: &'a str, identity: ChannelIdentity) -> Self {
        Self {
            icon,
            version,
            identity,
        }
    }

    fn render(&self) -> String {
        let [major, minor, patch] = numeric_version(self.version);
        let file_flags = if self.identity.prerelease {
            "0x2L"
        } else {
            "0x0L"
        };
        let company = authors_without_email(&env_var("CARGO_PKG_AUTHORS"));
        let description = env_var("CARGO_PKG_DESCRIPTION");

        // Resource scripts take C string literals, so a Windows path's
        // backslashes have to be escaped.
        let icon_literal = self.icon.display().to_string().replace('\\', "\\\\");

        let strings = [
            ("CompanyName", company.as_str()),
            ("FileDescription", description.as_str()),
            ("FileVersion", self.version),
            ("InternalName", "dbflux"),
            ("LegalCopyright", "MIT OR Apache-2.0"),
            ("OriginalFilename", "dbflux.exe"),
            ("ProductName", self.identity.product_name),
            ("ProductVersion", self.version),
        ];

        let string_values = strings
            .iter()
            .map(|(key, value)| format!("      VALUE \"{key}\", \"{}\"", escape_rc_string(value)))
            .collect::<Vec<_>>()
            .join("\n");

        // Resource id 1 is what the shell reads as the executable's icon. gpui
        // embeds its manifest under the same id but a different resource type,
        // so the two do not collide.
        format!(
            r#"1 ICON "{icon_literal}"

1 VERSIONINFO
FILEVERSION {major},{minor},{patch},0
PRODUCTVERSION {major},{minor},{patch},0
FILEFLAGSMASK 0x3fL
FILEFLAGS {file_flags}
FILEOS 0x40004L
FILETYPE 0x1L
FILESUBTYPE 0x0L
BEGIN
  BLOCK "StringFileInfo"
  BEGIN
    BLOCK "040904b0"
    BEGIN
{string_values}
    END
  END
  BLOCK "VarFileInfo"
  BEGIN
    VALUE "Translation", 0x409, 1200
  END
END
"#
        )
    }
}

/// `FILEVERSION` takes four 16-bit integers, so the pre-release and build
/// metadata of the semver string are dropped and the fourth component is
/// always zero. The full string survives in `FileVersion` and `ProductVersion`.
fn numeric_version(version: &str) -> [u16; 3] {
    let core = version.split(['-', '+']).next().unwrap_or(version);

    let mut parts = core.split('.').map(|part| part.parse::<u16>().unwrap_or(0));

    [
        parts.next().unwrap_or(0),
        parts.next().unwrap_or(0),
        parts.next().unwrap_or(0),
    ]
}

/// `CARGO_PKG_AUTHORS` is `Name <email>:Name <email>`; Properties shows the
/// names only.
fn authors_without_email(authors: &str) -> String {
    authors
        .split(':')
        .map(|author| author.split('<').next().unwrap_or(author).trim())
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>()
        .join(", ")
}

fn escape_rc_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\"\"")
}
