use std::fs;
use std::io;
use std::path::PathBuf;

pub const APP_CONTROL_AUTH_TOKEN_ENV: &str = "DBFLUX_IPC_TOKEN";
pub const DRIVER_RPC_AUTH_TOKEN_ENV: &str = "DBFLUX_DRIVER_IPC_TOKEN";

const AUTH_TOKEN_FILE: &str = "ipc_auth_token";

pub fn init_process_auth_tokens() -> io::Result<String> {
    let token = uuid::Uuid::new_v4().to_string();

    unsafe {
        std::env::set_var(APP_CONTROL_AUTH_TOKEN_ENV, &token);
        std::env::set_var(DRIVER_RPC_AUTH_TOKEN_ENV, &token);
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
    let config_dir = dirs::config_dir().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "failed to resolve user config directory",
        )
    })?;

    Ok(config_dir.join("dbflux").join(AUTH_TOKEN_FILE))
}
