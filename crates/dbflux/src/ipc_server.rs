use crate::ui::workspace::Workspace;
use dbflux_ipc::{
    APP_CONTROL_VERSION, framing,
    protocol::{AppControlRequest, AppControlResponse, IpcMessage, IpcResponse},
};
use gpui::*;
use interprocess::local_socket::{
    Listener as IpcListener, Stream as IpcStream, traits::Listener as _,
};
use std::io;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;

pub struct IpcServer;

enum IpcCommand {
    OpenScript { path: PathBuf },
    Focus,
}

impl IpcServer {
    pub fn start_with_listener(listener: IpcListener, workspace: Entity<Workspace>, cx: &mut App) {
        let (cmd_tx, cmd_rx): (Sender<IpcCommand>, Receiver<IpcCommand>) = mpsc::channel();

        thread::spawn(move || {
            accept_loop(listener, cmd_tx);
        });

        cx.spawn(async move |cx| {
            process_commands(cmd_rx, workspace, cx.clone()).await;
        })
        .detach();
    }
}

fn accept_loop(listener: IpcListener, cmd_tx: Sender<IpcCommand>) {
    loop {
        match listener.accept() {
            Ok(stream) => {
                if let Err(e) = handle_connection(stream, &cmd_tx) {
                    log::warn!("IPC connection error: {}", e);
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(e) => {
                log::warn!("IPC accept error: {}", e);
                break;
            }
        }
    }
}

fn handle_connection(mut stream: IpcStream, cmd_tx: &Sender<IpcCommand>) -> io::Result<()> {
    let request: AppControlRequest = framing::recv_msg(&mut stream)?;
    let request_id = request.request_id;

    if !request
        .protocol_version
        .is_compatible_with(APP_CONTROL_VERSION)
    {
        let response = AppControlResponse::ok(
            request_id,
            IpcResponse::Error {
                message: "incompatible app-control protocol version".to_string(),
            },
        );
        framing::send_msg(&mut stream, &response)?;
        return Ok(());
    }

    let response_body = match request.body {
        IpcMessage::Ping => IpcResponse::Pong {
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
        IpcMessage::OpenScript { path } => {
            let path = path.canonicalize().unwrap_or(path);
            if cmd_tx.send(IpcCommand::OpenScript { path }).is_ok() {
                IpcResponse::Ok
            } else {
                IpcResponse::Error {
                    message: "failed to send command".into(),
                }
            }
        }
        IpcMessage::Focus => {
            if cmd_tx.send(IpcCommand::Focus).is_ok() {
                IpcResponse::Ok
            } else {
                IpcResponse::Error {
                    message: "failed to send command".into(),
                }
            }
        }
    };

    let response = AppControlResponse::ok(request_id, response_body);

    framing::send_msg(&mut stream, &response)?;
    Ok(())
}

async fn process_commands(
    cmd_rx: Receiver<IpcCommand>,
    workspace: Entity<Workspace>,
    cx: AsyncApp,
) {
    loop {
        match cmd_rx.try_recv() {
            Ok(cmd) => {
                let _ = cx.update(|cx| {
                    workspace.update(cx, |ws, cx| match cmd {
                        IpcCommand::OpenScript { path } => {
                            ws.open_script_from_path(path, cx);
                        }
                        IpcCommand::Focus => {
                            // TODO: implement window focus
                        }
                    });
                });
            }
            Err(mpsc::TryRecvError::Empty) => {
                cx.background_executor()
                    .timer(Duration::from_millis(10))
                    .await;
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
}
