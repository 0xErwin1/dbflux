use crate::APP_CONTROL_VERSION;
use crate::framing;
use crate::protocol::{AppControlRequest, AppControlResponse, IpcMessage, IpcResponse};
use crate::{read_app_control_token, socket_name};
use interprocess::local_socket::{Stream as IpcStream, prelude::*};
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub fn send_app_control_request(msg: IpcMessage) -> io::Result<()> {
    let name = socket_name()?;
    let mut stream = IpcStream::connect(name)?;

    let auth_token = read_app_control_token()?;
    let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let request = AppControlRequest::new(request_id, Some(auth_token), msg);

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

pub fn send_focus_request<S: Read + Write>(stream: &mut S, request_id: u64) -> io::Result<()> {
    let auth_token = read_app_control_token()?;
    let request = AppControlRequest::new(request_id, Some(auth_token), IpcMessage::Focus);
    framing::send_msg(&mut *stream, &request)?;

    let response: AppControlResponse = framing::recv_msg(&mut *stream)?;

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
