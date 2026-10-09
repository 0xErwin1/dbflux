use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::OnceLock;

pub const APP_CONTROL_AUTH_TOKEN_ENV: &str = "DBFLUX_IPC_TOKEN";
pub const DRIVER_RPC_AUTH_TOKEN_ENV: &str = "DBFLUX_DRIVER_IPC_TOKEN";
pub const AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV: &str = "DBFLUX_AUTH_PROVIDER_IPC_TOKEN";

const AUTH_TOKEN_FILE: &str = "ipc_auth_token";

/// Process-global IPC auth token store.
///
/// Replaces delivery through the process environment: the token is set once by
/// `init_process_auth_tokens()` and read through `process_auth_token()`, so no
/// environment mutation is needed after threads exist.
static PROCESS_AUTH_TOKEN: OnceLock<String> = OnceLock::new();

pub fn init_process_auth_tokens() -> io::Result<String> {
    let token = uuid::Uuid::new_v4().to_string();

    if PROCESS_AUTH_TOKEN.set(token.clone()).is_err() {
        log::debug!("IPC auth token store already initialized; retaining the first token");
    }

    write_app_control_token(&token)?;
    Ok(token)
}

/// Returns the process-global IPC auth token set by `init_process_auth_tokens()`.
///
/// `None` when the store was never initialized in this process (e.g. the
/// standalone `dbflux mcp` server); readers then fall back to the
/// caller-supplied token environment variables.
pub fn process_auth_token() -> Option<&'static str> {
    PROCESS_AUTH_TOKEN.get().map(String::as_str)
}

pub fn read_app_control_token() -> io::Result<String> {
    let path = app_control_token_path()?;
    let token = fs::read_to_string(path)?;
    Ok(token.trim().to_string())
}

pub fn write_app_control_token(token: &str) -> io::Result<()> {
    let path = app_control_token_path()?;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(&path, token)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(&path)?.permissions();
        permissions.set_mode(0o600);
        fs::set_permissions(&path, permissions)?;
    }

    Ok(())
}

pub fn app_control_token_path() -> io::Result<PathBuf> {
    let dir = dbflux_storage::paths::data_dir().map_err(|e| io::Error::other(e.to_string()))?;
    Ok(dir.join(AUTH_TOKEN_FILE))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_control_token_path_is_under_data_dir() {
        let token_path = app_control_token_path().expect("must resolve token path");
        let data_dir = dbflux_storage::paths::data_dir().expect("must resolve data dir");

        assert_eq!(
            token_path.parent().expect("token path must have a parent"),
            data_dir,
            "ipc_auth_token must be located under the data directory"
        );
        assert_eq!(
            token_path.file_name().and_then(|n| n.to_str()),
            Some("ipc_auth_token"),
            "token file must be named 'ipc_auth_token'"
        );
    }
}
