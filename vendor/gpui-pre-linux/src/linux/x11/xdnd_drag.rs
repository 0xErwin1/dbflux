//! Carries a [`SourceSession`] out on the X connection: grabs, selection ownership, client
//! messages and the event loop's timer.

use calloop::RegistrationToken;
use calloop::timer::{TimeoutAction, Timer};
use gpui::{CursorStyle, ExternalDragPayload, FileDropEvent, PlatformInput};
use gpui_util::ResultExt as _;
use x11rb::{
    connection::Connection as _,
    protocol::{
        Event, xinput,
        xinput::ConnectionExt as _,
        xproto::{
            self, AtomEnum, ClientMessageData, ClientMessageEvent, ConnectionExt as _, EventMask,
            GrabMode, KeyButMask,
        },
    },
    wrapper::ConnectionExt as _,
    xcb_ffi::XCBConnection,
};
use xkbcommon::xkb as xkbc;

use super::{
    X11Client, XcbAtoms, check_reply, get_reply,
    xdnd_source::{
        self, Command, FINISH_TIMEOUT, Finished, Message, MessageKind, SourceSession, Status,
        Target,
    },
};
use crate::linux::drag_uri_list;

/// How deep below the root window to look for a window that takes drops.
const MAX_WINDOW_DEPTH: usize = 32;

/// `XI_ButtonPress`, `XI_ButtonRelease` and `XI_Motion` in an XInput event mask.
const GRAB_EVENT_MASK: u32 = (1 << 4) | (1 << 5) | (1 << 6);

const BUTTON_LEFT: u32 = 1;

/// A file drag this client is the source of.
pub(crate) struct XdndSource {
    session: SourceSession,
    window: xproto::Window,
    uri_list: Vec<u8>,
    pointer_device: xinput::DeviceId,
    pointer_grabbed: bool,
    /// Whether the grab's cursor shows that a drop would be taken.
    cursor_shows_accepted: bool,
    keyboard_grabbed: bool,
    timeout: Option<RegistrationToken>,
}

impl X11Client {
    pub(crate) fn start_external_drag(
        &self,
        x_window: xproto::Window,
        payload: &ExternalDragPayload,
    ) -> bool {
        let ExternalDragPayload::Files(paths) = payload;
        let uri_list = drag_uri_list(paths);
        if uri_list.is_empty() {
            return false;
        }

        let mut state = self.0.borrow_mut();
        if state.xdnd_source.is_some() {
            return false;
        }
        let connection = state.xcb_connection.clone();
        let atoms = state.atoms;
        let pointer_device = state.pointer_device;
        let root = connection.setup().roots[state.x_root_index].root;

        let Some(pointer) = get_reply(
            || "Failed to query the pointer for a drag",
            connection.query_pointer(root),
        )
        .log_err() else {
            return false;
        };
        if !pointer.mask.contains(KeyButMask::BUTTON1) {
            return false;
        }

        if check_reply(
            || "Failed to own XdndSelection",
            connection.set_selection_owner(x_window, atoms.XdndSelection, x11rb::CURRENT_TIME),
        )
        .log_err()
        .is_none()
        {
            return false;
        }
        let owns_selection = get_reply(
            || "Failed to read the XdndSelection owner",
            connection.get_selection_owner(atoms.XdndSelection),
        )
        .log_err()
        .is_some_and(|reply| reply.owner == x_window);
        if !owns_selection {
            return false;
        }

        let refusing_cursor = state
            .get_cursor_icon(CursorStyle::OperationNotAllowed)
            .unwrap_or(x11rb::NONE);
        let pointer_grabbed = grab_pointer(&connection, x_window, pointer_device, refusing_cursor);
        let keyboard_grabbed = grab_keyboard(&connection, x_window);

        let (x, y) = (i32::from(pointer.root_x), i32::from(pointer.root_y));
        let target = find_target(&connection, &atoms, root, x, y);
        let mut session = SourceSession::new(x_window, atoms.TextUriList, atoms.XdndActionCopy);
        let commands = session.pointer_moved(target, x, y, x11rb::CURRENT_TIME);
        state.xdnd_source = Some(XdndSource {
            session,
            window: x_window,
            uri_list: uri_list.into_bytes(),
            pointer_device,
            pointer_grabbed,
            cursor_shows_accepted: false,
            keyboard_grabbed,
            timeout: None,
        });
        drop(state);

        self.run_xdnd_commands(commands);
        true
    }

    /// Consumes the input events of a window while it is dragging files out, and feeds them to
    /// the session. Returns whether `event` was consumed.
    pub(crate) fn xdnd_source_intercepts(&self, event: &Event) -> bool {
        if self.0.borrow().xdnd_source.is_none() {
            return false;
        }
        let commands = match event {
            Event::XinputMotion(motion) => {
                let (x, y) = (motion.root_x >> 16, motion.root_y >> 16);
                let state = self.0.borrow();
                let connection = state.xcb_connection.clone();
                let atoms = state.atoms;
                let root = connection.setup().roots[state.x_root_index].root;
                drop(state);
                let target = find_target(&connection, &atoms, root, x, y);
                self.with_xdnd_session(|session| session.pointer_moved(target, x, y, motion.time))
            }
            Event::XinputButtonRelease(release) => {
                if release.detail == BUTTON_LEFT {
                    self.with_xdnd_session(|session| session.button_released(release.time))
                } else {
                    Vec::new()
                }
            }
            Event::KeyPress(press) => {
                let is_escape = self
                    .0
                    .borrow()
                    .xkb
                    .key_get_one_sym(press.detail.into())
                    .raw()
                    == xkbc::keysyms::KEY_Escape;
                if is_escape {
                    self.with_xdnd_session(SourceSession::cancel)
                } else {
                    Vec::new()
                }
            }
            Event::XinputButtonPress(_) | Event::KeyRelease(_) => Vec::new(),
            Event::FocusIn(focus) | Event::FocusOut(focus)
                if focus.mode != xproto::NotifyMode::NORMAL =>
            {
                Vec::new()
            }
            _ => return false,
        };
        self.run_xdnd_commands(commands);
        true
    }

    pub(crate) fn xdnd_source_status(&self, data: [u32; 5]) {
        let commands =
            self.with_xdnd_session(|session| session.status_received(Status::decode(data)));
        self.run_xdnd_commands(commands);
    }

    pub(crate) fn xdnd_source_finished(&self, data: [u32; 5]) {
        let commands =
            self.with_xdnd_session(|session| session.finished_received(Finished::decode(data)));
        self.run_xdnd_commands(commands);
    }

    fn xdnd_source_timed_out(&self) {
        if let Some(source) = self.0.borrow_mut().xdnd_source.as_mut() {
            source.timeout = None;
        }
        let commands = self.with_xdnd_session(SourceSession::timed_out);
        self.run_xdnd_commands(commands);
    }

    /// Answers a request for the dragged data. Requests for a selection that is not ours get
    /// a refusal.
    pub(crate) fn xdnd_handle_selection_request(&self, request: &xproto::SelectionRequestEvent) {
        let state = self.0.borrow();
        let connection = state.xcb_connection.clone();
        let atoms = state.atoms;
        let uri_list = (request.selection == atoms.XdndSelection)
            .then(|| {
                state
                    .xdnd_source
                    .as_ref()
                    .map(|source| source.uri_list.clone())
            })
            .flatten();
        drop(state);

        // An obsolete client may send no property; the convention is to reuse the target.
        let property = if request.property == x11rb::NONE {
            request.target
        } else {
            request.property
        };
        let answered = match uri_list {
            Some(uri_list) if request.target == atoms.TextUriList => check_reply(
                || "Failed to write the dragged URI list",
                connection.change_property8(
                    xproto::PropMode::REPLACE,
                    request.requestor,
                    property,
                    atoms.TextUriList,
                    &uri_list,
                ),
            ),
            Some(_) if request.target == atoms.TARGETS => check_reply(
                || "Failed to write the drag targets",
                connection.change_property32(
                    xproto::PropMode::REPLACE,
                    request.requestor,
                    property,
                    AtomEnum::ATOM,
                    &[atoms.TARGETS, atoms.TextUriList],
                ),
            ),
            _ => Err(anyhow::anyhow!("unsupported target {}", request.target)),
        }
        .is_ok();

        let notify = xproto::SelectionNotifyEvent {
            response_type: xproto::SELECTION_NOTIFY_EVENT,
            sequence: 0,
            time: request.time,
            requestor: request.requestor,
            selection: request.selection,
            target: request.target,
            property: if answered { property } else { x11rb::NONE },
        };
        check_reply(
            || "Failed to send the selection notification",
            connection.send_event(false, request.requestor, EventMask::NO_EVENT, notify),
        )
        .log_err();
        connection.flush().log_err();
    }

    /// Forgets the drag of a window that is being destroyed. The window cannot be told the
    /// drag ended.
    pub(crate) fn abort_xdnd_source_of(&self, x_window: xproto::Window) {
        let mut state = self.0.borrow_mut();
        if state
            .xdnd_source
            .as_ref()
            .is_some_and(|source| source.window == x_window)
        {
            self.release_xdnd_source(&mut state);
        }
    }

    fn with_xdnd_session(
        &self,
        step: impl FnOnce(&mut SourceSession) -> Vec<Command>,
    ) -> Vec<Command> {
        let mut state = self.0.borrow_mut();
        match state.xdnd_source.as_mut() {
            Some(source) => step(&mut source.session),
            None => Vec::new(),
        }
    }

    fn run_xdnd_commands(&self, commands: Vec<Command>) {
        for command in commands {
            match command {
                Command::Send {
                    destination,
                    target,
                    message,
                } => self.send_xdnd_message(destination, target, message),
                Command::ArmTimeout => self.arm_xdnd_timeout(),
                Command::End => self.end_xdnd_source(),
            }
        }
        self.refresh_xdnd_cursor();
    }

    /// Shows with the pointer's cursor whether a drop here would be taken, since a drag has no
    /// icon of its own on X11. A grab's cursor can only change by grabbing again.
    fn refresh_xdnd_cursor(&self) {
        let mut state = self.0.borrow_mut();
        let Some(source) = state.xdnd_source.as_ref() else {
            return;
        };
        let accepted = source.session.drop_would_be_accepted();
        if !source.pointer_grabbed || source.cursor_shows_accepted == accepted {
            return;
        }
        let (window, device) = (source.window, source.pointer_device);
        let style = if accepted {
            CursorStyle::DragCopy
        } else {
            CursorStyle::OperationNotAllowed
        };
        let cursor = state.get_cursor_icon(style).unwrap_or(x11rb::NONE);
        grab_pointer(&state.xcb_connection, window, device, cursor);
        if let Some(source) = state.xdnd_source.as_mut() {
            source.cursor_shows_accepted = accepted;
        }
    }

    fn send_xdnd_message(
        &self,
        destination: xproto::Window,
        target: xproto::Window,
        message: Message,
    ) {
        let state = self.0.borrow();
        let atoms = &state.atoms;
        let type_ = match message.kind() {
            MessageKind::Enter => atoms.XdndEnter,
            MessageKind::Position => atoms.XdndPosition,
            MessageKind::Leave => atoms.XdndLeave,
            MessageKind::Drop => atoms.XdndDrop,
        };
        let event = ClientMessageEvent {
            format: 32,
            window: target,
            type_,
            data: ClientMessageData::from(message.data()),
            sequence: 0,
            response_type: xproto::CLIENT_MESSAGE_EVENT,
        };
        check_reply(
            || "Failed to send an XDND message",
            state
                .xcb_connection
                .send_event(false, destination, EventMask::NO_EVENT, event),
        )
        .log_err();
        state.xcb_connection.flush().log_err();
    }

    fn arm_xdnd_timeout(&self) {
        let mut state = self.0.borrow_mut();
        let this = state.this.clone();
        let Some(previous) = state
            .xdnd_source
            .as_mut()
            .map(|source| source.timeout.take())
        else {
            return;
        };
        if let Some(previous) = previous {
            state.loop_handle.remove(previous);
        }
        let token = state
            .loop_handle
            .insert_source(Timer::from_duration(FINISH_TIMEOUT), move |_, _, _| {
                if let Some(client) = this.get_client() {
                    client.xdnd_source_timed_out();
                }
                TimeoutAction::Drop
            })
            .log_err();
        if let Some(source) = state.xdnd_source.as_mut() {
            source.timeout = token;
        }
    }

    /// Releases what the drag holds, then tells its window the drag ended.
    fn end_xdnd_source(&self) {
        let mut state = self.0.borrow_mut();
        let Some(window) = state.xdnd_source.as_ref().map(|source| source.window) else {
            return;
        };
        self.release_xdnd_source(&mut state);
        drop(state);
        if let Some(window) = self.get_window(window) {
            window.handle_input(PlatformInput::FileDrop(FileDropEvent::Ended));
        }
    }

    fn release_xdnd_source(&self, state: &mut super::X11ClientState) {
        let Some(source) = state.xdnd_source.take() else {
            return;
        };
        if let Some(token) = source.timeout {
            state.loop_handle.remove(token);
        }
        let connection = &state.xcb_connection;
        if source.pointer_grabbed {
            check_reply(
                || "Failed to release the pointer grab",
                connection.xinput_xi_ungrab_device(x11rb::CURRENT_TIME, source.pointer_device),
            )
            .log_err();
        }
        if source.keyboard_grabbed {
            check_reply(
                || "Failed to release the keyboard grab",
                connection.ungrab_keyboard(x11rb::CURRENT_TIME),
            )
            .log_err();
        }
        let still_owner = get_reply(
            || "Failed to read the XdndSelection owner",
            connection.get_selection_owner(state.atoms.XdndSelection),
        )
        .log_err()
        .is_some_and(|reply| reply.owner == source.window);
        if still_owner {
            check_reply(
                || "Failed to release XdndSelection",
                connection.set_selection_owner(
                    x11rb::NONE,
                    state.atoms.XdndSelection,
                    x11rb::CURRENT_TIME,
                ),
            )
            .log_err();
        }
        connection.flush().log_err();
    }
}

fn grab_pointer(
    connection: &XCBConnection,
    window: xproto::Window,
    device: xinput::DeviceId,
    cursor: xproto::Cursor,
) -> bool {
    let reply = get_reply(
        || "Failed to grab the pointer for a drag",
        connection.xinput_xi_grab_device(
            window,
            x11rb::CURRENT_TIME,
            cursor,
            device,
            GrabMode::ASYNC,
            GrabMode::ASYNC,
            xinput::GrabOwner::NO_OWNER,
            &[GRAB_EVENT_MASK],
        ),
    )
    .log_err();
    reply.is_some_and(|reply| reply.status == xproto::GrabStatus::SUCCESS)
}

fn grab_keyboard(connection: &XCBConnection, window: xproto::Window) -> bool {
    let reply = get_reply(
        || "Failed to grab the keyboard for a drag",
        connection.grab_keyboard(
            false,
            window,
            x11rb::CURRENT_TIME,
            GrabMode::ASYNC,
            GrabMode::ASYNC,
        ),
    )
    .log_err();
    reply.is_some_and(|reply| reply.status == xproto::GrabStatus::SUCCESS)
}

/// The window under `(x, y)` that takes drops: the first window found going down from the root
/// that has `XdndAware`, or its `XdndProxy`.
pub(crate) fn find_target(
    connection: &XCBConnection,
    atoms: &XcbAtoms,
    root: xproto::Window,
    x: i32,
    y: i32,
) -> Option<Target> {
    let x = x.clamp(i32::from(i16::MIN), i32::from(i16::MAX)) as i16;
    let y = y.clamp(i32::from(i16::MIN), i32::from(i16::MAX)) as i16;
    let mut parent = root;
    for _ in 0..MAX_WINDOW_DEPTH {
        let child = connection
            .translate_coordinates(root, parent, x, y)
            .ok()?
            .reply()
            .ok()?
            .child;
        if child == x11rb::NONE {
            return None;
        }
        if let Some(target) = target_of(connection, atoms, child) {
            return Some(target);
        }
        parent = child;
    }
    None
}

fn target_of(
    connection: &XCBConnection,
    atoms: &XcbAtoms,
    window: xproto::Window,
) -> Option<Target> {
    let aware = read_u32_property(connection, window, atoms.XdndAware, AtomEnum::ATOM.into())?;
    let proxy = read_u32_property(connection, window, atoms.XdndProxy, AtomEnum::WINDOW.into())
        .filter(|proxy| *proxy != window)
        .and_then(|proxy| {
            let points_back =
                read_u32_property(connection, proxy, atoms.XdndProxy, AtomEnum::WINDOW.into())
                    == Some(proxy);
            let proxy_aware =
                read_u32_property(connection, proxy, atoms.XdndAware, AtomEnum::ATOM.into())?;
            points_back.then_some((proxy, proxy_aware))
        });
    let (destination, aware) = proxy.unwrap_or((window, aware));
    Some(Target {
        window,
        destination,
        version: xdnd_source::negotiated_version(aware)?,
    })
}

fn read_u32_property(
    connection: &XCBConnection,
    window: xproto::Window,
    property: xproto::Atom,
    type_: xproto::Atom,
) -> Option<u32> {
    connection
        .get_property(false, window, property, type_, 0, 1)
        .ok()?
        .reply()
        .ok()?
        .value32()?
        .next()
}

/// Runs against a real X server, named by `GPUI_X11_TEST_DISPLAY` (for example a scratch Xvfb).
/// The tests skip when it is unset, so they never touch the display of the person running them.
#[cfg(test)]
mod tests {
    use std::ffi::CString;

    use x11rb::protocol::xproto::{CreateWindowAux, WindowClass};

    use super::*;

    struct Server {
        connection: XCBConnection,
        atoms: XcbAtoms,
        root: xproto::Window,
    }

    fn connect() -> Option<Server> {
        let display = std::env::var("GPUI_X11_TEST_DISPLAY").ok()?;
        let display = CString::new(display).expect("display name without NUL");
        let (connection, screen) = XCBConnection::connect(Some(&display))
            .expect("GPUI_X11_TEST_DISPLAY names a display that cannot be reached");
        let root = connection.setup().roots[screen].root;
        let atoms = XcbAtoms::new(&connection)
            .expect("intern atoms")
            .reply()
            .expect("intern atoms");
        Some(Server {
            connection,
            atoms,
            root,
        })
    }

    impl Server {
        fn window(&self, parent: xproto::Window, x: i16, y: i16, size: u16) -> xproto::Window {
            let window = self.connection.generate_id().unwrap();
            self.connection
                .create_window(
                    x11rb::COPY_DEPTH_FROM_PARENT,
                    window,
                    parent,
                    x,
                    y,
                    size,
                    size,
                    0,
                    WindowClass::INPUT_OUTPUT,
                    0,
                    &CreateWindowAux::new(),
                )
                .unwrap();
            self.connection.map_window(window).unwrap();
            window
        }

        fn set(&self, window: xproto::Window, property: xproto::Atom, type_: AtomEnum, value: u32) {
            self.connection
                .change_property32(xproto::PropMode::REPLACE, window, property, type_, &[value])
                .unwrap();
        }

        fn target_at(&self, x: i32, y: i32) -> Option<Target> {
            self.connection.get_input_focus().unwrap().reply().unwrap();
            find_target(&self.connection, &self.atoms, self.root, x, y)
        }
    }

    #[test]
    fn finds_the_aware_window_below_an_unaware_frame() {
        let Some(server) = connect() else {
            return;
        };
        let frame = server.window(server.root, 1000, 100, 100);
        let client = server.window(frame, 0, 0, 100);
        server.set(client, server.atoms.XdndAware, AtomEnum::ATOM, 5);

        assert_eq!(
            server.target_at(1050, 150),
            Some(Target {
                window: client,
                destination: client,
                version: 5,
            })
        );
    }

    #[test]
    fn caps_the_version_at_the_one_this_source_speaks() {
        let Some(server) = connect() else {
            return;
        };
        let window = server.window(server.root, 1000, 300, 100);
        server.set(window, server.atoms.XdndAware, AtomEnum::ATOM, 9);

        assert_eq!(server.target_at(1050, 350).map(|t| t.version), Some(5));
    }

    #[test]
    fn ignores_windows_that_are_not_aware_or_too_old() {
        let Some(server) = connect() else {
            return;
        };
        let plain = server.window(server.root, 1000, 500, 100);
        let old = server.window(server.root, 1200, 500, 100);
        server.set(old, server.atoms.XdndAware, AtomEnum::ATOM, 2);

        assert_eq!(server.target_at(1050, 550), None);
        assert_eq!(server.target_at(1250, 550), None);
        let _ = plain;
    }

    #[test]
    fn sends_to_the_proxy_when_it_points_back_at_itself() {
        let Some(server) = connect() else {
            return;
        };
        let window = server.window(server.root, 1000, 700, 50);
        let proxy = server.window(server.root, 1200, 700, 50);
        server.set(window, server.atoms.XdndAware, AtomEnum::ATOM, 5);
        server.set(window, server.atoms.XdndProxy, AtomEnum::WINDOW, proxy);
        server.set(proxy, server.atoms.XdndProxy, AtomEnum::WINDOW, proxy);
        server.set(proxy, server.atoms.XdndAware, AtomEnum::ATOM, 4);

        assert_eq!(
            server.target_at(1020, 720),
            Some(Target {
                window,
                destination: proxy,
                version: 4,
            })
        );
    }

    #[test]
    fn falls_back_to_the_window_when_the_proxy_does_not_point_back() {
        let Some(server) = connect() else {
            return;
        };
        let window = server.window(server.root, 1000, 900, 50);
        let stray = server.window(server.root, 1200, 900, 50);
        server.set(window, server.atoms.XdndAware, AtomEnum::ATOM, 5);
        server.set(window, server.atoms.XdndProxy, AtomEnum::WINDOW, stray);

        assert_eq!(
            server.target_at(1020, 920),
            Some(Target {
                window,
                destination: window,
                version: 5,
            })
        );
    }
}
