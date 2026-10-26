use std::io::Write;
use std::path::Path;

use secrecy::ExposeSecret;

use super::*;

fn write_dotenv(directory: &Path, name: &str, content: &str) -> String {
    let path = directory.join(name);
    let mut file = std::fs::File::create(&path).expect("create dotenv fixture");
    file.write_all(content.as_bytes())
        .expect("write dotenv fixture");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("restrict dotenv fixture");
    }

    path.to_string_lossy().into_owned()
}

fn error_message(result: Result<SecretString, DbError>) -> String {
    match result {
        Ok(_) => panic!("expected the read to fail"),
        Err(DbError::ValueResolutionFailed(message)) => message,
        Err(other) => panic!("unexpected error kind: {other:?}"),
    }
}

fn parse(content: &str, key: &str) -> Option<String> {
    find_value(content, key)
        .expect("content must parse")
        .map(|value| value.expose_secret().to_string())
}

#[test]
fn reads_the_requested_key() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_dotenv(
        directory.path(),
        "app.env",
        "DB_USER=app\nDB_PASSWORD=s3cret\n",
    );

    let value = read_dotenv_value(&path, "DB_PASSWORD").expect("read value");

    assert_eq!(value.expose_secret(), "s3cret");
}

#[test]
fn parses_comments_export_and_quotes() {
    let content = "\
# comment
export PLAIN = value with spaces   # trailing comment
SINGLE='literal \\n $HOME # kept'
DOUBLE=\"line\\nnext \\\"quoted\\\" \\$HOME\" # comment
HASH=abc#def
";

    assert_eq!(
        parse(content, "PLAIN").as_deref(),
        Some("value with spaces")
    );
    assert_eq!(
        parse(content, "SINGLE").as_deref(),
        Some("literal \\n $HOME # kept")
    );
    assert_eq!(
        parse(content, "DOUBLE").as_deref(),
        Some("line\nnext \"quoted\" $HOME")
    );
    assert_eq!(parse(content, "HASH").as_deref(), Some("abc#def"));
}

#[test]
fn last_definition_wins() {
    assert_eq!(
        parse("KEY=first\nKEY=second\n", "KEY").as_deref(),
        Some("second")
    );
}

#[test]
fn malformed_entries_report_the_line_without_its_text() {
    for (content, line) in [
        ("GOOD=1\nnot an entry\n", 2),
        ("1KEY=value\n", 1),
        ("KEY='unterminated\n", 1),
        ("KEY=\"unterminated\n", 1),
        ("KEY=\"value\" trailing\n", 1),
    ] {
        assert_eq!(
            find_value(content, "GOOD").map(|_| ()),
            Err(ParseFailure::MalformedEntry { line }),
            "content: {content:?}"
        );
    }

    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_dotenv(
        directory.path(),
        "broken.env",
        "DB_PASSWORD=hunter2\nleaked-secret-text\n",
    );

    let message = error_message(read_dotenv_value(&path, "DB_PASSWORD"));

    assert_eq!(
        message,
        format!("Dotenv file '{path}' has a malformed entry on line 2")
    );
    assert!(!message.contains("hunter2"));
    assert!(!message.contains("leaked-secret-text"));
}

#[test]
fn missing_and_empty_keys_fail_with_distinct_errors() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_dotenv(directory.path(), "app.env", "EMPTY=\nOTHER=value\n");

    assert_eq!(
        error_message(read_dotenv_value(&path, "ABSENT")),
        format!("Key 'ABSENT' is not defined in dotenv file '{path}'")
    );
    assert_eq!(
        error_message(read_dotenv_value(&path, "EMPTY")),
        format!("Key 'EMPTY' in dotenv file '{path}' is empty")
    );
}

#[test]
fn missing_file_and_bad_paths_fail_visibly() {
    let directory = tempfile::tempdir().expect("tempdir");
    let missing = directory.path().join("absent.env");
    let missing = missing.to_string_lossy();

    assert_eq!(
        error_message(read_dotenv_value(&missing, "KEY")),
        format!("Dotenv file '{missing}' does not exist")
    );
    assert_eq!(
        error_message(read_dotenv_value("relative/app.env", "KEY")),
        "Dotenv file 'relative/app.env' must be an absolute path or start with '~/'"
    );
    assert_eq!(
        error_message(read_dotenv_value("  ", "KEY")),
        "Dotenv file path is empty"
    );
    assert_eq!(
        error_message(read_dotenv_value(&missing, "BAD-KEY")),
        "Dotenv key 'BAD-KEY' is not a valid variable name"
    );

    let directory_path = directory.path().to_string_lossy();
    assert_eq!(
        error_message(read_dotenv_value(&directory_path, "KEY")),
        format!("Dotenv file '{directory_path}' is not a regular file")
    );
}

#[test]
fn oversized_files_and_too_many_entries_are_rejected() {
    let directory = tempfile::tempdir().expect("tempdir");
    let oversized = "KEY=".to_string() + &"x".repeat(MAX_FILE_BYTES as usize);
    let path = write_dotenv(directory.path(), "big.env", &oversized);

    assert_eq!(
        error_message(read_dotenv_value(&path, "KEY")),
        format!("Dotenv file '{path}' is larger than 64 KiB")
    );

    let many: String = (0..=MAX_ENTRIES)
        .map(|index| format!("K{index}=v\n"))
        .collect();
    assert_eq!(
        find_value(&many, "K0").map(|_| ()),
        Err(ParseFailure::TooManyEntries)
    );
}

#[test]
fn invalid_utf8_is_rejected() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("binary.env");
    std::fs::write(&path, [b'K', b'=', 0xff, 0xfe]).expect("write fixture");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("restrict fixture");
    }

    let path = path.to_string_lossy();
    assert_eq!(
        error_message(read_dotenv_value(&path, "K")),
        format!("Dotenv file '{path}' is not valid UTF-8")
    );
}

#[test]
fn rotation_is_picked_up_on_the_next_read() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_dotenv(directory.path(), "app.env", "TOKEN=old\n");

    assert_eq!(
        read_dotenv_value(&path, "TOKEN")
            .expect("first read")
            .expose_secret(),
        "old"
    );

    let replacement = write_dotenv(directory.path(), "app.env.new", "TOKEN=new\n");
    std::fs::rename(&replacement, &path).expect("atomic replace");

    assert_eq!(
        read_dotenv_value(&path, "TOKEN")
            .expect("read after rotation")
            .expose_secret(),
        "new"
    );
}

#[cfg(unix)]
#[test]
fn symlink_swaps_are_followed() {
    let directory = tempfile::tempdir().expect("tempdir");
    let first = write_dotenv(directory.path(), "v1.env", "TOKEN=one\n");
    let second = write_dotenv(directory.path(), "v2.env", "TOKEN=two\n");
    let link = directory.path().join("current.env");
    std::os::unix::fs::symlink(&first, &link).expect("create link");
    let link_text = link.to_string_lossy().into_owned();

    assert_eq!(
        read_dotenv_value(&link_text, "TOKEN")
            .expect("read through link")
            .expose_secret(),
        "one"
    );

    let staged = directory.path().join("current.env.tmp");
    std::os::unix::fs::symlink(&second, &staged).expect("stage link");
    std::fs::rename(&staged, &link).expect("swap link");

    assert_eq!(
        read_dotenv_value(&link_text, "TOKEN")
            .expect("read after swap")
            .expose_secret(),
        "two"
    );
}

#[cfg(unix)]
#[test]
fn files_open_to_other_users_are_rejected() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_dotenv(directory.path(), "app.env", "KEY=value\n");

    for mode in [0o644, 0o640, 0o620, 0o604, 0o602] {
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("set mode");

        assert_eq!(
            error_message(read_dotenv_value(&path, "KEY")),
            format!(
                "Dotenv file '{path}' can be read or changed by other users (mode {mode:o}); restrict it with chmod 600"
            )
        );
    }

    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400)).expect("set mode");
    assert!(read_dotenv_value(&path, "KEY").is_ok());
}

#[cfg(unix)]
#[test]
fn unreadable_files_report_permission_denied() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_dotenv(directory.path(), "locked.env", "KEY=value\n");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).expect("lock file");

    // Root ignores file modes, so the denial cannot be observed there.
    if std::fs::File::open(&path).is_ok() {
        return;
    }

    assert_eq!(
        error_message(read_dotenv_value(&path, "KEY")),
        format!("Dotenv file '{path}' cannot be read: permission denied")
    );
}

#[test]
fn key_validation() {
    assert!(is_valid_key("DB_PASSWORD"));
    assert!(is_valid_key("_private.value"));
    assert!(!is_valid_key(""));
    assert!(!is_valid_key("9LIVES"));
    assert!(!is_valid_key("WITH SPACE"));
    assert!(!is_valid_key("DASH-KEY"));
}
