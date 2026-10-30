use std::io::Write;
use std::path::Path;

use secrecy::ExposeSecret;

use super::*;

const DISTINCTIVE_SECRET: &str = "zx-very-distinctive-secret-91";

fn write_secret(directory: &Path, name: &str, content: &[u8]) -> String {
    let path = directory.join(name);
    let mut file = std::fs::File::create(&path).expect("create secret fixture");
    file.write_all(content).expect("write secret fixture");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("restrict secret fixture");
    }

    path.to_string_lossy().into_owned()
}

fn read(path: &str, json_key: Option<&str>) -> String {
    read_secret_file(path, json_key)
        .expect("read secret file")
        .expose_secret()
        .to_string()
}

fn error_message(result: Result<SecretString, DbError>) -> String {
    match result {
        Ok(_) => panic!("expected the read to fail"),
        Err(DbError::ValueResolutionFailed(message)) => message,
        Err(other) => panic!("unexpected error kind: {other:?}"),
    }
}

#[test]
fn reads_the_whole_file_and_strips_one_trailing_newline() {
    let directory = tempfile::tempdir().expect("tempdir");

    for (content, expected) in [
        ("pw", "pw"),
        ("pw\n", "pw"),
        ("pw\r\n", "pw"),
        ("pw\n\n", "pw\n"),
        ("  pw with spaces \t\n", "  pw with spaces \t"),
        ("line one\nline two\n", "line one\nline two"),
    ] {
        let path = write_secret(directory.path(), "secret", content.as_bytes());
        assert_eq!(read(&path, None), expected, "content {content:?}");
    }
}

#[test]
fn empty_file_is_rejected() {
    let directory = tempfile::tempdir().expect("tempdir");

    for content in ["", "\n", "\r\n"] {
        let path = write_secret(directory.path(), "secret", content.as_bytes());
        assert_eq!(
            error_message(read_secret_file(&path, None)),
            format!("Secret file '{path}' is empty")
        );
    }
}

#[test]
fn json_key_extracts_strings_numbers_and_booleans() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_secret(
        directory.path(),
        "secret.json",
        br#"{"password":"s3cret","port":5432,"ratio":1.5,"tls":true,"nested":{"password":"x"}}"#,
    );

    assert_eq!(read(&path, Some("password")), "s3cret");
    assert_eq!(read(&path, Some("port")), "5432");
    assert_eq!(read(&path, Some("ratio")), "1.5");
    assert_eq!(read(&path, Some("tls")), "true");
}

#[test]
fn json_key_errors_name_the_key_and_file() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_secret(
        directory.path(),
        "secret.json",
        format!(
            r#"{{"empty":"","nothing":null,"list":["{DISTINCTIVE_SECRET}"],"object":{{"a":"{DISTINCTIVE_SECRET}"}}}}"#
        )
        .as_bytes(),
    );

    assert_eq!(
        error_message(read_secret_file(&path, Some("missing"))),
        format!("Key 'missing' is not defined in secret file '{path}'")
    );
    for key in ["nothing", "list", "object"] {
        assert_eq!(
            error_message(read_secret_file(&path, Some(key))),
            format!("Key '{key}' in secret file '{path}' is not a string, number or boolean")
        );
    }
    assert_eq!(
        error_message(read_secret_file(&path, Some("empty"))),
        format!("Key 'empty' in secret file '{path}' is empty")
    );
    assert_eq!(
        error_message(read_secret_file(&path, Some(""))),
        "JSON key in secret file reference is empty"
    );
}

#[test]
fn json_key_requires_a_json_object() {
    let directory = tempfile::tempdir().expect("tempdir");

    for content in [
        format!("{{\"password\": \"{DISTINCTIVE_SECRET}\""),
        format!("[\"{DISTINCTIVE_SECRET}\"]"),
        format!("\"{DISTINCTIVE_SECRET}\""),
        DISTINCTIVE_SECRET.to_string(),
    ] {
        let path = write_secret(directory.path(), "secret.json", content.as_bytes());
        let message = error_message(read_secret_file(&path, Some("password")));

        assert_eq!(
            message,
            format!("Secret file '{path}' is not a JSON object")
        );
        assert!(!message.contains(DISTINCTIVE_SECRET));
    }
}

#[test]
fn missing_file_and_bad_paths_fail_visibly() {
    let directory = tempfile::tempdir().expect("tempdir");
    let missing = directory
        .path()
        .join("absent")
        .to_string_lossy()
        .into_owned();

    assert_eq!(
        error_message(read_secret_file(&missing, None)),
        format!("Secret file '{missing}' does not exist")
    );
    assert_eq!(
        error_message(read_secret_file("relative/secret", None)),
        "Secret file 'relative/secret' must be an absolute path or start with '~/'"
    );
    assert_eq!(
        error_message(read_secret_file("  ", None)),
        "Secret file path is empty"
    );

    let directory_path = directory.path().to_string_lossy().into_owned();
    assert_eq!(
        error_message(read_secret_file(&directory_path, None)),
        format!("Secret file '{directory_path}' is not a regular file")
    );
}

#[cfg(unix)]
#[test]
fn a_fifo_is_rejected_without_blocking() {
    let directory = tempfile::tempdir().expect("tempdir");
    let fifo = directory.path().join("pipe");
    let status = std::process::Command::new("mkfifo")
        .arg("-m")
        .arg("600")
        .arg(&fifo)
        .status()
        .expect("run mkfifo");
    assert!(status.success(), "mkfifo failed: {status}");

    let fifo = fifo.to_string_lossy().into_owned();
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn({
        let fifo = fifo.clone();
        move || {
            if sender.send(read_secret_file(&fifo, None)).is_err() {
                eprintln!("fifo read finished after the test gave up");
            }
        }
    });

    let result = receiver
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("reading a FIFO without a writer must not block");

    assert_eq!(
        error_message(result),
        format!("Secret file '{fifo}' is not a regular file")
    );
}

#[test]
fn oversized_and_non_utf8_files_are_rejected() {
    let directory = tempfile::tempdir().expect("tempdir");
    let oversized = "x".repeat(MAX_FILE_BYTES as usize + 1);
    let path = write_secret(directory.path(), "big", oversized.as_bytes());

    assert_eq!(
        error_message(read_secret_file(&path, None)),
        format!("Secret file '{path}' is larger than 64 KiB")
    );

    let exact = "x".repeat(MAX_FILE_BYTES as usize);
    let path = write_secret(directory.path(), "exact", exact.as_bytes());
    assert_eq!(read(&path, None).len(), MAX_FILE_BYTES as usize);

    let path = write_secret(directory.path(), "binary", &[b'p', 0xff, 0xfe]);
    assert_eq!(
        error_message(read_secret_file(&path, None)),
        format!("Secret file '{path}' is not valid UTF-8")
    );
}

#[test]
fn rotation_by_atomic_rename_is_picked_up_on_the_next_read() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_secret(directory.path(), "secret", b"old\n");
    assert_eq!(read(&path, None), "old");

    let replacement = write_secret(directory.path(), "secret.new", b"new\n");
    std::fs::rename(&replacement, &path).expect("atomic replace");

    assert_eq!(read(&path, None), "new");
}

#[cfg(unix)]
#[test]
fn symlink_swaps_are_followed() {
    let directory = tempfile::tempdir().expect("tempdir");
    let first = write_secret(directory.path(), "v1", b"one\n");
    let second = write_secret(directory.path(), "v2", b"two\n");
    let link = directory.path().join("current");
    std::os::unix::fs::symlink(&first, &link).expect("create link");
    let link_text = link.to_string_lossy().into_owned();

    assert_eq!(read(&link_text, None), "one");

    let staged = directory.path().join("current.tmp");
    std::os::unix::fs::symlink(&second, &staged).expect("stage link");
    std::fs::rename(&staged, &link).expect("swap link");

    assert_eq!(read(&link_text, None), "two");
}

#[cfg(unix)]
#[test]
fn files_open_to_other_users_are_rejected() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_secret(directory.path(), "secret", DISTINCTIVE_SECRET.as_bytes());

    for mode in [0o644, 0o640, 0o620, 0o604, 0o602] {
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("set mode");

        assert_eq!(
            error_message(read_secret_file(&path, None)),
            format!(
                "Secret file '{path}' can be read or changed by other users (mode {mode:o}); restrict it with chmod 600"
            )
        );
    }

    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400)).expect("set mode");
    assert_eq!(read(&path, None), DISTINCTIVE_SECRET);
}

#[cfg(unix)]
#[test]
fn unreadable_files_report_permission_denied() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("tempdir");
    let path = write_secret(directory.path(), "locked", DISTINCTIVE_SECRET.as_bytes());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).expect("lock file");

    // Root ignores file modes, so the denial cannot be observed there.
    if std::fs::File::open(&path).is_ok() {
        return;
    }

    assert_eq!(
        error_message(read_secret_file(&path, None)),
        format!("Secret file '{path}' cannot be read: permission denied")
    );
}

#[test]
fn no_error_message_contains_the_file_content() {
    let directory = tempfile::tempdir().expect("tempdir");
    let json = write_secret(
        directory.path(),
        "secret.json",
        format!(r#"{{"password":["{DISTINCTIVE_SECRET}"]}}"#).as_bytes(),
    );
    let plain = write_secret(
        directory.path(),
        "plain",
        format!("{DISTINCTIVE_SECRET}\n").as_bytes(),
    );

    for (path, key) in [
        (&json, Some("password")),
        (&json, Some("other")),
        (&plain, Some("password")),
    ] {
        let message = error_message(read_secret_file(path, key));
        assert!(!message.contains(DISTINCTIVE_SECRET), "leaked: {message}");
    }
}
