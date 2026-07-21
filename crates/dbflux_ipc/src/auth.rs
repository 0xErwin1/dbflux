use std::fs;
use std::io;
use std::path::PathBuf;

pub const APP_CONTROL_AUTH_TOKEN_ENV: &str = "DBFLUX_IPC_TOKEN";
pub const DRIVER_RPC_AUTH_TOKEN_ENV: &str = "DBFLUX_DRIVER_IPC_TOKEN";
pub const AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV: &str = "DBFLUX_AUTH_PROVIDER_IPC_TOKEN";

const AUTH_TOKEN_FILE: &str = "ipc_auth_token";

pub fn init_process_auth_tokens() -> io::Result<String> {
    let token = uuid::Uuid::new_v4().to_string();

    unsafe {
        std::env::set_var(APP_CONTROL_AUTH_TOKEN_ENV, &token);
        std::env::set_var(DRIVER_RPC_AUTH_TOKEN_ENV, &token);
        std::env::set_var(AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV, &token);
    }

    write_app_control_token(&token)?;
    Ok(token)
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
