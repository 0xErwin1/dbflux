use std::io::Write;

use secrecy::ExposeSecret;

use super::*;

#[cfg(unix)]
#[test]
fn file_owned_by_the_process_user_is_read() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("owned");
    let mut file = std::fs::File::create(&path).expect("create fixture");
    file.write_all(b"value").expect("write fixture");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
        .expect("restrict fixture");

    let content =
        read_owner_only_file("Secret file", &path.to_string_lossy()).expect("read owned file");

    assert_eq!(content.expose_secret(), "value");
}

#[cfg(unix)]
#[test]
fn file_owned_by_another_user_is_rejected() {
    for label in ["Secret file", "Dotenv file"] {
        let message = match check_owner(label, "/run/secrets/db", 1001, 1000) {
            Err(DbError::ValueResolutionFailed(message)) => message,
            other => panic!("expected a value resolution failure, got {other:?}"),
        };

        assert_eq!(
            message,
            format!(
                "{label} '/run/secrets/db' is owned by another user; DBFlux only reads files owned by the user it runs as"
            )
        );
    }

    assert!(check_owner("Secret file", "/run/secrets/db", 1000, 1000).is_ok());
}
