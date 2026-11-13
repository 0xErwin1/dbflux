use dbflux_ipc::protocol::IpcMessage;
use dbflux_ipc::{send_app_control_request, send_focus_request};
use std::io;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

const CONNECT_RETRIES: usize = 20;
const RETRY_DELAY_MS: u64 = 50;

pub(crate) fn focus_running_instance() -> bool {
    send_focus_request().is_ok()
}

pub(crate) fn open_or_spawn(path: Option<PathBuf>) -> i32 {
    let msg = match path.as_ref() {
        None => IpcMessage::Focus,
        Some(p) => {
            let Ok(absolute) = p.canonicalize() else {
                eprintln!("Invalid path: {}", p.display());
                return 1;
            };
            IpcMessage::OpenScript { path: absolute }
        },
    };

    if send_app_control_request(msg.clone()).is_ok() {
        return 0;
    }
    if let Err(e) = spawn_gui() {
        eprintln!("Failed to spawn GUI: {}", e);
        return 1;
    }

    match retry_send(&msg) {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Failed to connect after spawn: {}", e);
            1
        }
    }
}

fn retry_send(msg: &IpcMessage) -> io::Result<()> {
    for _ in 0..CONNECT_RETRIES {
        std::thread::sleep(Duration::from_millis(RETRY_DELAY_MS));
        if send_app_control_request(msg.clone()).is_ok() {
            return Ok(());
        }
    }
    Err(io::Error::other("connection timeout"))
}

fn spawn_gui() -> io::Result<()> {
    let exe = std::env::current_exe()?;

    let mut cmd = Command::new(exe);
    cmd.arg("--gui")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    #[cfg(unix)]
    #[expect(
        unsafe_code,
        reason = "`CommandExt::pre_exec` is the documented unsafe API for \
                  post-fork, pre-exec setup; the closure only calls `setsid()` \
                  and maps `errno`, both async-signal-safe"
    )]
    {
        use std::os::unix::process::CommandExt;
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    cmd.spawn()?;
    Ok(())
}
