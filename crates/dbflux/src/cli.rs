use dbflux_ipc::{
    APP_CONTROL_VERSION, framing,
    protocol::{AppControlRequest, AppControlResponse, IpcMessage, IpcResponse},
    socket_name,
};
use interprocess::local_socket::{Stream as IpcStream, prelude::*};
use std::io;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const CONNECT_RETRIES: usize = 20;
const RETRY_DELAY_MS: u64 = 50;
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub fn run(args: &[String]) -> i32 {
    let path = args.get(1).map(PathBuf::from);

    match try_send(path.as_ref()) {
        Ok(_) => 0,
        Err(_) => {
            if let Err(e) = spawn_gui() {
                eprintln!("Failed to spawn GUI: {}", e);
                return 1;
            }

            match retry_send(path.as_ref()) {
                Ok(_) => 0,
                Err(e) => {
                    eprintln!("Failed to connect after spawn: {}", e);
                    1
                }
            }
        }
    }
}

fn try_send(path: Option<&PathBuf>) -> io::Result<()> {
    let name = socket_name()?;
    let mut stream = IpcStream::connect(name)?;

    let msg = match path {
        Some(p) => IpcMessage::OpenScript { path: p.clone() },
        None => IpcMessage::Focus,
    };

    let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let request = AppControlRequest::new(request_id, msg);

    framing::send_msg(&mut stream, &request)?;
    let response: AppControlResponse = framing::recv_msg(&mut stream)?;

    if !response
        .protocol_version
        .is_compatible_with(APP_CONTROL_VERSION)
    {
        return Err(io::Error::other(
            "incompatible app-control protocol version",
        ));
    }

    if response.request_id != request_id {
        return Err(io::Error::other("mismatched app-control response id"));
    }

    match response.body {
        IpcResponse::Error { message } => Err(io::Error::other(message)),
        _ => Ok(()),
    }
}

fn retry_send(path: Option<&PathBuf>) -> io::Result<()> {
    for _ in 0..CONNECT_RETRIES {
        std::thread::sleep(Duration::from_millis(RETRY_DELAY_MS));
        if try_send(path).is_ok() {
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
