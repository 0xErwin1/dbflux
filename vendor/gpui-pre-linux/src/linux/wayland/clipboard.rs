use std::{
    fs::File,
    io::{ErrorKind, Write},
    os::fd::{AsRawFd, BorrowedFd, OwnedFd},
};

use calloop::{LoopHandle, PostAction};
use filedescriptor::Pipe;
use gpui_util::ResultExt as _;
use strum::IntoEnumIterator;
use wayland_client::{Connection, protocol::wl_data_offer::WlDataOffer};
use wayland_protocols::wp::primary_selection::zv1::client::zwp_primary_selection_offer_v1::ZwpPrimarySelectionOfferV1;

use crate::linux::platform::{PIPE_READ_TIMEOUT, read_fd_with_timeout};
use gpui::{ClipboardEntry, ClipboardItem, Image, ImageFormat, hash};

/// Text mime types that we'll offer to other programs.
pub(crate) const TEXT_MIME_TYPES: [&str; 3] =
    ["text/plain;charset=utf-8", "UTF8_STRING", "text/plain"];
pub(crate) const FILE_LIST_MIME_TYPE: &str = "text/uri-list";

/// Text mime types that we'll accept from other programs.
pub(crate) const ALLOWED_TEXT_MIME_TYPES: [&str; 2] = ["text/plain;charset=utf-8", "UTF8_STRING"];

pub(crate) struct Clipboard {
    connection: Connection,
    loop_handle: LoopHandle<'static, ()>,
    self_mime: String,

    // Internal clipboard
    contents: Option<ClipboardItem>,
    primary_contents: Option<ClipboardItem>,

    // External clipboard
    cached_read: Option<ClipboardItem>,
    current_offer: Option<DataOffer<WlDataOffer>>,
    cached_primary_read: Option<ClipboardItem>,
    current_primary_offer: Option<DataOffer<ZwpPrimarySelectionOfferV1>>,
}

pub(crate) trait ReceiveData {
    fn receive_data(&self, mime_type: String, fd: BorrowedFd<'_>);
}

impl ReceiveData for WlDataOffer {
    fn receive_data(&self, mime_type: String, fd: BorrowedFd<'_>) {
        self.receive(mime_type, fd);
    }
}

impl ReceiveData for ZwpPrimarySelectionOfferV1 {
    fn receive_data(&self, mime_type: String, fd: BorrowedFd<'_>) {
        self.receive(mime_type, fd);
    }
}

#[derive(Clone, Debug)]
/// Wrapper for `WlDataOffer` and `ZwpPrimarySelectionOfferV1`, used to help track mime types.
pub(crate) struct DataOffer<T: ReceiveData> {
    pub inner: T,
    mime_types: Vec<String>,
}

impl<T: ReceiveData> DataOffer<T> {
    pub fn new(offer: T) -> Self {
        Self {
            inner: offer,
            mime_types: Vec::new(),
        }
    }

    pub fn add_mime_type(&mut self, mime_type: String) {
        self.mime_types.push(mime_type)
    }

    fn has_mime_type(&self, mime_type: &str) -> bool {
        self.mime_types.iter().any(|t| t == mime_type)
    }

    fn read_bytes(&self, connection: &Connection, mime_type: &str) -> Option<Vec<u8>> {
        let pipe = Pipe::new().unwrap();
        self.inner.receive_data(mime_type.to_string(), unsafe {
            BorrowedFd::borrow_raw(pipe.write.as_raw_fd())
        });
        let fd = pipe.read;
        drop(pipe.write);

        connection.flush().unwrap();

        match read_fd_with_timeout(fd, PIPE_READ_TIMEOUT) {
            Ok(bytes) => Some(bytes),
            Err(err) => {
                log::error!("error reading clipboard pipe: {err:?}");
                None
            }
        }
    }

    fn read_text(&self, connection: &Connection) -> Option<ClipboardItem> {
        let mime_type = self.mime_types.iter().find(|&mime_type| {
            ALLOWED_TEXT_MIME_TYPES
                .iter()
                .any(|&allowed| allowed == mime_type)
        })?;
        let bytes = self.read_bytes(connection, mime_type)?;
        let text_content = match String::from_utf8(bytes) {
            Ok(content) => content,
            Err(e) => {
                log::error!("Failed to convert clipboard content to UTF-8: {}", e);
                return None;
            }
        };

        // Normalize the text to unix line endings, otherwise
        // copying from eg: firefox inserts a lot of blank
        // lines, and that is super annoying.
        let result = text_content.replace("\r\n", "\n");
        Some(ClipboardItem::new_string(result))
    }

    fn read_image(&self, connection: &Connection) -> Option<ClipboardItem> {
        for format in ImageFormat::iter() {
            let mime_type = format.mime_type();
            if !self.has_mime_type(mime_type) {
                continue;
            }

            if let Some(bytes) = self.read_bytes(connection, mime_type) {
                let id = hash(&bytes);
                return Some(ClipboardItem {
                    entries: vec![ClipboardEntry::Image(Image { format, bytes, id })],
                });
            }
        }
        None
    }
}

impl Clipboard {
    pub fn new(connection: Connection, loop_handle: LoopHandle<'static, ()>) -> Self {
        Self {
            connection,
            loop_handle,
            self_mime: format!("pid/{}", std::process::id()),

            contents: None,
            primary_contents: None,

            cached_read: None,
            current_offer: None,
            cached_primary_read: None,
            current_primary_offer: None,
        }
    }

    pub fn set(&mut self, item: ClipboardItem) {
        self.contents = Some(item);
    }

    pub fn set_primary(&mut self, item: ClipboardItem) {
        self.primary_contents = Some(item);
    }

    pub fn set_offer(&mut self, data_offer: Option<DataOffer<WlDataOffer>>) {
        self.cached_read = None;
        self.current_offer = data_offer;
    }

    pub fn set_primary_offer(&mut self, data_offer: Option<DataOffer<ZwpPrimarySelectionOfferV1>>) {
        self.cached_primary_read = None;
        self.current_primary_offer = data_offer;
    }

    pub fn self_mime(&self) -> String {
        self.self_mime.clone()
    }

    pub fn send(&self, _mime_type: String, fd: OwnedFd) {
        if let Some(text) = self.contents.as_ref().and_then(|contents| contents.text()) {
            self.send_bytes(fd, text.as_bytes().to_owned());
        }
    }

    pub fn send_primary(&self, _mime_type: String, fd: OwnedFd) {
        if let Some(text) = self
            .primary_contents
            .as_ref()
            .and_then(|contents| contents.text())
        {
            self.send_bytes(fd, text.as_bytes().to_owned());
        }
    }

    pub fn read(&mut self) -> Option<ClipboardItem> {
        let offer = self.current_offer.as_ref()?;
        if let Some(cached) = self.cached_read.clone() {
            return Some(cached);
        }

        if offer.has_mime_type(&self.self_mime) {
            return self.contents.clone();
        }

        let item = offer
            .read_text(&self.connection)
            .or_else(|| offer.read_image(&self.connection))?;

        self.cached_read = Some(item.clone());
        Some(item)
    }

    pub fn read_primary(&mut self) -> Option<ClipboardItem> {
        let offer = self.current_primary_offer.as_ref()?;
        if let Some(cached) = self.cached_primary_read.clone() {
            return Some(cached);
        }

        if offer.has_mime_type(&self.self_mime) {
            return self.primary_contents.clone();
        }

        let item = offer
            .read_text(&self.connection)
            .or_else(|| offer.read_image(&self.connection))?;

        self.cached_primary_read = Some(item.clone());
        Some(item)
    }

    pub fn send_bytes(&self, fd: OwnedFd, bytes: Vec<u8>) {
        send_bytes_on(&self.loop_handle, fd, bytes);
    }
}

/// Writes `bytes` to `fd` and closes it, which the reader sees as the end of the data.
///
/// The write happens here, not on a later pass of the event loop: a drop target waits for the
/// end of the data before it finishes, so every pass of delay is a pause the user feels. Only
/// what the pipe cannot take yet is left to the loop.
fn send_bytes_on(loop_handle: &LoopHandle<'static, ()>, fd: OwnedFd, bytes: Vec<u8>) {
    let mut file = File::from(fd);
    if let Err(err) = set_nonblocking(&file) {
        log::warn!("failed to make a data pipe non-blocking: {err}");
    }
    let mut written = 0;
    if write_available(&mut file, &bytes, &mut written) != WriteProgress::WouldBlock {
        return;
    }
    loop_handle
        .insert_source(
            calloop::generic::Generic::new(file, calloop::Interest::WRITE, calloop::Mode::Level),
            move |_, file, _| {
                let file = unsafe { file.get_mut() };
                Ok(
                    if write_available(file, &bytes, &mut written) == WriteProgress::WouldBlock {
                        PostAction::Continue
                    } else {
                        PostAction::Remove
                    },
                )
            },
        )
        .log_err();
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteProgress {
    /// Everything was written.
    Complete,
    /// The reader has not made room for the rest yet.
    WouldBlock,
    /// The reader went away, or the pipe broke.
    Failed,
}

fn write_available(writer: &mut impl Write, bytes: &[u8], written: &mut usize) -> WriteProgress {
    while *written < bytes.len() {
        match writer.write(&bytes[*written..]) {
            Ok(0) => return WriteProgress::Failed,
            Ok(count) => *written += count,
            Err(err) if err.kind() == ErrorKind::Interrupted => {}
            Err(err) if err.kind() == ErrorKind::WouldBlock => return WriteProgress::WouldBlock,
            Err(_) => return WriteProgress::Failed,
        }
    }
    WriteProgress::Complete
}

fn set_nonblocking(file: &File) -> std::io::Result<()> {
    let fd = file.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags == -1 {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        io::Read,
        os::fd::{FromRawFd, OwnedFd},
        thread,
        time::{Duration, Instant},
    };

    use calloop::EventLoop;

    use super::*;

    fn pipe() -> (File, OwnedFd) {
        let mut fds = [0; 2];
        assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
        unsafe { (File::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
    }

    fn set_nonblocking(file: &File) {
        unsafe {
            let flags = libc::fcntl(file.as_raw_fd(), libc::F_GETFL);
            assert_ne!(flags, -1);
            assert_eq!(
                libc::fcntl(file.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK),
                0
            );
        }
    }

    #[test]
    fn a_small_payload_is_written_and_the_pipe_closed_before_send_returns() {
        let event_loop = EventLoop::<()>::try_new().expect("event loop");
        let (mut reader, writer) = pipe();
        set_nonblocking(&reader);

        send_bytes_on(&event_loop.handle(), writer, b"file:///tmp/a\r\n".to_vec());

        // No loop iteration ran, so only a synchronous write can have delivered the data, and
        // only a closed write end lets the read reach end of file.
        let mut received = Vec::new();
        reader
            .read_to_end(&mut received)
            .expect("the data and the end of file are already there");
        assert_eq!(received, b"file:///tmp/a\r\n");
    }

    #[test]
    fn a_payload_larger_than_the_pipe_is_written_in_full_while_the_loop_runs() {
        let mut event_loop = EventLoop::<()>::try_new().expect("event loop");
        let (mut reader, writer) = pipe();
        let payload: Vec<u8> = (0..1_000_000u32).map(|n| (n % 251) as u8).collect();

        let reading = thread::spawn(move || {
            let mut received = Vec::new();
            reader.read_to_end(&mut received).expect("read the payload");
            received
        });
        send_bytes_on(&event_loop.handle(), writer, payload.clone());

        let deadline = Instant::now() + Duration::from_secs(10);
        while !reading.is_finished() {
            assert!(Instant::now() < deadline, "the payload was never finished");
            event_loop
                .dispatch(Some(Duration::from_millis(5)), &mut ())
                .expect("dispatch");
        }
        assert_eq!(reading.join().expect("reader"), payload);
    }

    struct ChunkedWriter {
        chunk: usize,
        capacity: usize,
        taken: Vec<u8>,
        interrupt_first: bool,
        error: Option<ErrorKind>,
    }

    impl Write for ChunkedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if std::mem::take(&mut self.interrupt_first) {
                return Err(ErrorKind::Interrupted.into());
            }
            if let Some(kind) = self.error {
                return Err(kind.into());
            }
            if self.taken.len() >= self.capacity {
                return Err(ErrorKind::WouldBlock.into());
            }
            let count = bytes
                .len()
                .min(self.chunk)
                .min(self.capacity - self.taken.len());
            self.taken.extend_from_slice(&bytes[..count]);
            Ok(count)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn writer(chunk: usize, capacity: usize) -> ChunkedWriter {
        ChunkedWriter {
            chunk,
            capacity,
            taken: Vec::new(),
            interrupt_first: false,
            error: None,
        }
    }

    #[test]
    fn writing_in_chunks_completes() {
        let mut sink = writer(3, 100);
        let mut written = 0;
        assert_eq!(
            write_available(&mut sink, b"0123456789", &mut written),
            WriteProgress::Complete
        );
        assert_eq!((sink.taken.as_slice(), written), (&b"0123456789"[..], 10));
    }

    #[test]
    fn a_full_pipe_reports_how_far_it_got_and_resumes() {
        let mut sink = writer(4, 6);
        let mut written = 0;
        assert_eq!(
            write_available(&mut sink, b"0123456789", &mut written),
            WriteProgress::WouldBlock
        );
        assert_eq!(written, 6);
        sink.capacity = 100;
        assert_eq!(
            write_available(&mut sink, b"0123456789", &mut written),
            WriteProgress::Complete
        );
        assert_eq!(sink.taken, b"0123456789");
    }

    #[test]
    fn an_interrupted_write_is_retried() {
        let mut sink = writer(100, 100);
        sink.interrupt_first = true;
        let mut written = 0;
        assert_eq!(
            write_available(&mut sink, b"abc", &mut written),
            WriteProgress::Complete
        );
        assert_eq!(sink.taken, b"abc");
    }

    #[test]
    fn a_broken_pipe_fails() {
        let mut sink = writer(100, 100);
        sink.error = Some(ErrorKind::BrokenPipe);
        let mut written = 0;
        assert_eq!(
            write_available(&mut sink, b"abc", &mut written),
            WriteProgress::Failed
        );
    }

    #[test]
    fn an_empty_payload_completes_without_writing() {
        let mut sink = writer(100, 0);
        let mut written = 0;
        assert_eq!(
            write_available(&mut sink, b"", &mut written),
            WriteProgress::Complete
        );
    }
}
