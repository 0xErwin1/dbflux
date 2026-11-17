use crate::APP_CONTROL_VERSION;
use crate::framing;
use crate::protocol::{AppControlRequest, AppControlResponse, IpcMessage, IpcResponse};
use crate::{read_app_control_token, socket_name};
use interprocess::local_socket::{Stream as IpcStream, prelude::*};
use std::io;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

/// Sends `msg` to the running instance and waits for its reply.
///
/// Fails if no instance is listening, the auth token is unreadable, the
/// protocol version is incompatible, or the instance answers with an error.
pub fn send_app_control_request(msg: IpcMessage) -> io::Result<()> {
    let name = socket_name()?;
    let mut stream = IpcStream::connect(name)?;
    let auth_token = read_app_control_token()?;
    let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);

    exchange(&mut stream, request_id, auth_token, msg)
}

/// Asks the running instance to bring its window to the front.
pub fn send_focus_request() -> io::Result<()> {
    send_app_control_request(IpcMessage::Focus)
}

fn exchange<S: Read + Write>(
    stream: &mut S,
    request_id: u64,
    auth_token: String,
    msg: IpcMessage,
) -> io::Result<()> {
    let request = AppControlRequest::new(request_id, Some(auth_token), msg);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::ProtocolVersion;
    use std::io::Cursor;

    /// In-memory peer: replays a canned reply and records what was written.
    struct FakeStream {
        reply: Cursor<Vec<u8>>,
        written: Vec<u8>,
    }

    impl Read for FakeStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.reply.read(buf)
        }
    }

    impl Write for FakeStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.written.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn run(response: AppControlResponse, request_id: u64) -> (io::Result<()>, FakeStream) {
        let mut reply = Vec::new();
        framing::send_msg(&mut reply, &response).expect("encode reply");
        let mut stream = FakeStream {
            reply: Cursor::new(reply),
            written: Vec::new(),
        };
        let result = exchange(&mut stream, request_id, "token".into(), IpcMessage::Focus);
        (result, stream)
    }

    #[test]
    fn sends_request_with_token_and_accepts_ok_reply() {
        let (result, stream) = run(AppControlResponse::ok(7, IpcResponse::Ok), 7);
        result.expect("ok reply succeeds");

        let request: AppControlRequest =
            framing::recv_msg(stream.written.as_slice()).expect("decode request");
        assert_eq!(request.request_id, 7);
        assert_eq!(request.auth_token.as_deref(), Some("token"));
        assert!(matches!(request.body, IpcMessage::Focus));
    }

    #[test]
    fn error_reply_surfaces_message() {
        let reply = AppControlResponse::ok(
            1,
            IpcResponse::Error {
                message: "denied".into(),
            },
        );
        let (result, _) = run(reply, 1);
        assert_eq!(result.expect_err("error reply").to_string(), "denied");
    }

    #[test]
    fn mismatched_request_id_is_rejected() {
        let (result, _) = run(AppControlResponse::ok(2, IpcResponse::Ok), 1);
        assert!(result.is_err());
    }

    #[test]
    fn incompatible_protocol_version_is_rejected() {
        let mut reply = AppControlResponse::ok(1, IpcResponse::Ok);
        reply.protocol_version = ProtocolVersion::new(APP_CONTROL_VERSION.major + 1, 0);
        let (result, _) = run(reply, 1);
        assert!(result.is_err());
    }
}
