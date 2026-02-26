use std::io;
use std::path::PathBuf;

pub fn socket_path() -> PathBuf {
    let runtime_dir = std::env::var("XDG_RUNTIME_DIR")
        .unwrap_or_else(|_| format!("/tmp/dbflux-{}", unsafe { libc::getuid() }));

    let suffix = if cfg!(debug_assertions) { "-debug" } else { "" };
    PathBuf::from(runtime_dir)
        .join("dbflux")
        .join(format!("dbflux{}.sock", suffix))
}

pub fn ensure_socket_dir() -> io::Result<()> {
    let path = socket_path();
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    Ok(())
}
