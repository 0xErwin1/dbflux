#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use dbflux_core::{DbConfig, DbKind, SshAuthMethod};

use super::*;
use crate::external::{ImportSource, importers};

fn string(value: &str) -> String {
    format!("<string>{value}</string>")
}

fn integer(value: i64) -> String {
    format!("<integer>{value}</integer>")
}

/// One connection dictionary carrying the fields TablePlus always writes,
/// with `overrides` replacing or adding entries.
fn connection(overrides: &[(&str, String)]) -> String {
    let mut fields: Vec<(&str, String)> = vec![
        ("ID", string("00000000-0000-0000-0000-000000000001")),
        ("GroupID", "<string />".to_string()),
        ("ConnectionName", string("Demo")),
        ("Enviroment", string("local")),
        ("Driver", string("PostgreSQL")),
        ("DriverVersion", integer(0)),
        ("DatabaseHost", string("db.example.invalid")),
        ("DatabasePort", string("5432")),
        ("DatabaseUser", string("demo")),
        ("DatabaseName", string("app")),
        ("tLSMode", integer(0)),
        ("TlsKeyPaths", "<array />".to_string()),
        ("isOverSSH", integer(0)),
        ("ServerAddress", "<string />".to_string()),
        ("ServerPort", "<string />".to_string()),
        ("ServerUser", "<string />".to_string()),
        ("isUsePrivateKey", integer(0)),
        ("ServerPrivateKeyName", "<string />".to_string()),
        (
            "RecentlySchema",
            "<array>\r\n<string>public.users</string>\r\n</array>".to_string(),
        ),
        (
            "SectionStates",
            "<dict>\r\n<key>public</key>\r\n<string>expanded</string>\r\n</dict>".to_string(),
        ),
        ("Favorites", "<dict />".to_string()),
        ("DatabasePasswordMode", integer(0)),
    ];

    for (key, value) in overrides {
        match fields.iter_mut().find(|(existing, _)| existing == key) {
            Some(field) => field.1 = value.clone(),
            None => fields.push((key, value.clone())),
        }
    }

    let body: String = fields
        .iter()
        .map(|(key, value)| format!("      <key>{key}</key>\r\n      {value}\r\n"))
        .collect();

    format!("    <dict>\r\n{body}    </dict>\r\n")
}

fn document(connections: &[String]) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \
         \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\r\n\
         <plist version=\"1.0\">\r\n  <array>\r\n{}  </array>\r\n</plist>\r\n",
        connections.concat()
    )
}

fn parse(text: &str) -> Result<ExternalImportOutcome, ExternalImportError> {
    let source = ImportSource::Bytes(text.as_bytes().to_vec());
    TablePlusImporter.parse(&ImportInput::new(&source))
}

fn parse_one(overrides: &[(&str, String)]) -> ExternalImportCandidate {
    let outcome = parse(&document(&[connection(overrides)])).expect("parse");
    assert!(
        outcome.skips.is_empty(),
        "unexpected skips: {:?}",
        outcome.skips.iter().map(|s| &s.reason).collect::<Vec<_>>()
    );
    outcome.candidates.into_iter().next().expect("candidate")
}

#[test]
fn registry_includes_tableplus() {
    assert!(importers().iter().any(|i| i.id() == "tableplus"));
}

#[test]
fn postgres_connection_reads_string_port_and_skips_password() {
    let candidate = parse_one(&[("DatabasePort", string("6543"))]);

    assert_eq!(candidate.name, "Demo");
    assert_eq!(candidate.kind, DbKind::Postgres);
    assert!(candidate.secret.is_none());
    assert!(candidate.secret_skip_reason.is_some());
    match &candidate.config {
        DbConfig::Postgres {
            host,
            port,
            user,
            database,
            ssh_tunnel,
            ..
        } => {
            assert_eq!(host, "db.example.invalid");
            assert_eq!(*port, 6543);
            assert_eq!(user, "demo");
            assert_eq!(database, "app");
            assert!(ssh_tunnel.is_none());
        }
        other => panic!("expected Postgres config, got {other:?}"),
    }
}

#[test]
fn empty_port_falls_back_to_driver_default() {
    let candidate = parse_one(&[
        ("Driver", string("MicrosoftSQLServer")),
        ("DatabasePort", "<string />".to_string()),
    ]);

    match &candidate.config {
        DbConfig::SqlServer { port, database, .. } => {
            assert_eq!(*port, 1433);
            assert_eq!(database.as_deref(), Some("app"));
        }
        other => panic!("expected SQL Server config, got {other:?}"),
    }
}

#[test]
fn mariadb_reports_mariadb_kind_on_mysql_config() {
    let candidate = parse_one(&[("Driver", string("MariaDB"))]);

    assert_eq!(candidate.kind, DbKind::MariaDB);
    assert!(matches!(candidate.config, DbConfig::MySQL { .. }));
}

#[test]
fn sqlite_path_comes_from_host_field() {
    let candidate = parse_one(&[
        ("Driver", string("SQLite")),
        ("DatabaseHost", string(r"C:\Users\demo\data\notes.sqlite")),
        ("DatabaseName", string("notes.sqlite")),
        ("DatabaseUser", "<string />".to_string()),
    ]);

    assert_eq!(candidate.kind, DbKind::SQLite);
    assert!(candidate.secret_skip_reason.is_none());
    match &candidate.config {
        DbConfig::SQLite { path, .. } => {
            assert_eq!(path, &PathBuf::from(r"C:\Users\demo\data\notes.sqlite"));
        }
        other => panic!("expected SQLite config, got {other:?}"),
    }
}

#[test]
fn redis_numeric_database_becomes_index() {
    let candidate = parse_one(&[("Driver", string("Redis")), ("DatabaseName", string("3"))]);

    match &candidate.config {
        DbConfig::Redis { database, user, .. } => {
            assert_eq!(*database, Some(3));
            assert_eq!(user.as_deref(), Some("demo"));
        }
        other => panic!("expected Redis config, got {other:?}"),
    }
}

#[test]
fn ssh_with_private_key_builds_key_tunnel_on_default_port() {
    let candidate = parse_one(&[
        ("Driver", string("MySQL")),
        ("isOverSSH", integer(1)),
        ("ServerAddress", string("bastion.example.invalid")),
        ("ServerUser", string("deploy")),
        ("isUsePrivateKey", integer(1)),
        ("ServerPrivateKeyName", string("id_ed25519")),
    ]);

    let DbConfig::MySQL {
        ssh_tunnel: Some(tunnel),
        ..
    } = &candidate.config
    else {
        panic!("expected MySQL config with an SSH tunnel");
    };

    assert_eq!(tunnel.host, "bastion.example.invalid");
    assert_eq!(tunnel.port, 22);
    assert_eq!(tunnel.user, "deploy");
    assert_eq!(
        tunnel.auth_method,
        SshAuthMethod::PrivateKey { key_path: None }
    );
}

#[test]
fn ssh_fields_are_ignored_when_ssh_is_off() {
    let candidate = parse_one(&[
        ("ServerAddress", string("bastion.example.invalid")),
        ("isUsePrivateKey", integer(1)),
    ]);

    assert!(matches!(
        candidate.config,
        DbConfig::Postgres {
            ssh_tunnel: None,
            ..
        }
    ));
}

#[test]
fn ssh_with_password_uses_password_auth_and_server_port() {
    let candidate = parse_one(&[
        ("isOverSSH", integer(1)),
        ("ServerAddress", string("bastion.example.invalid")),
        ("ServerPort", string("2222")),
    ]);

    let DbConfig::Postgres {
        ssh_tunnel: Some(tunnel),
        ..
    } = &candidate.config
    else {
        panic!("expected Postgres config with an SSH tunnel");
    };

    assert_eq!(tunnel.port, 2222);
    assert_eq!(tunnel.auth_method, SshAuthMethod::Password);
}

#[test]
fn escaped_characters_in_names_are_decoded() {
    let candidate = parse_one(&[("ConnectionName", string("Sales &amp; Ops &#8211; prod"))]);

    assert_eq!(candidate.name, "Sales & Ops \u{2013} prod");
}

#[test]
fn bad_entries_are_skipped_without_aborting_the_batch() {
    let outcome = parse(&document(&[
        connection(&[("ConnectionName", string("Good"))]),
        connection(&[
            ("ConnectionName", string("Local SQL Server")),
            ("Driver", string("MicrosoftSQLServer")),
            ("DatabaseHost", "<string />".to_string()),
        ]),
        connection(&[
            ("ConnectionName", string("Warehouse")),
            ("Driver", string("Snowflake")),
        ]),
        connection(&[
            ("ConnectionName", string("Tunnel")),
            ("isOverSSH", integer(1)),
        ]),
    ]))
    .expect("parse");

    assert_eq!(outcome.candidates.len(), 1);
    let reasons: Vec<(&str, &str)> = outcome
        .skips
        .iter()
        .map(|skip| (skip.name.as_str(), skip.reason.as_str()))
        .collect();
    assert_eq!(
        reasons,
        vec![
            ("Local SQL Server", "missing required field 'host'"),
            ("Warehouse", "unsupported driver 'Snowflake'"),
            ("Tunnel", "SSH is enabled but no SSH server address is set"),
        ]
    );
}

#[test]
fn socket_connections_are_skipped_under_either_platform_key() {
    let outcome = parse(&document(&[
        connection(&[
            ("ConnectionName", string("Windows socket")),
            ("UseSock", integer(1)),
        ]),
        connection(&[
            ("ConnectionName", string("macOS socket")),
            ("Driver", string("MySQL")),
            ("DatabaseHost", "<string />".to_string()),
            ("isUseSocket", integer(1)),
        ]),
        connection(&[("UseSock", integer(0)), ("isUseSocket", integer(0))]),
    ]))
    .expect("parse");

    assert_eq!(outcome.candidates.len(), 1);
    let reasons: Vec<(&str, &str)> = outcome
        .skips
        .iter()
        .map(|skip| (skip.name.as_str(), skip.reason.as_str()))
        .collect();
    assert_eq!(
        reasons,
        vec![
            ("Windows socket", "socket connections are not supported"),
            ("macOS socket", "socket connections are not supported"),
        ]
    );
}

#[test]
fn malformed_xml_is_a_plist_error() {
    let result = parse("<plist><array><dict><key>ID</key></array></plist>");

    assert!(matches!(result, Err(ExternalImportError::InvalidPlist(_))));
}

#[test]
fn binary_plist_is_rejected_as_unsupported() {
    let mut bytes = b"bplist00".to_vec();
    bytes.extend_from_slice(&[0xD1, 0x01, 0x02, 0xFF, 0x00]);
    let source = ImportSource::Bytes(bytes);

    let result = TablePlusImporter.parse(&ImportInput::new(&source));

    assert!(matches!(result, Err(ExternalImportError::BinaryPlist)));
}

#[test]
fn non_array_root_is_a_structure_error() {
    let result = parse("<?xml version=\"1.0\"?><plist version=\"1.0\"><dict /></plist>");

    assert!(matches!(result, Err(ExternalImportError::Structure(_))));
}
