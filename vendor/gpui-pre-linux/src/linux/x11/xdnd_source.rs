//! The source side of the X11 drag-and-drop protocol (XDND version 5), used to hand files
//! to other applications once a drag leaves the window.
//!
//! [`SourceSession`] holds the protocol state and does no I/O: the caller feeds it pointer
//! movements, button releases and the target's replies, and carries out the [`Command`]s it
//! returns. That keeps the protocol rules testable without an X server.

use std::time::Duration;

/// The newest XDND version this source speaks.
pub(crate) const PROTOCOL_VERSION: u32 = 5;
/// The oldest target version this source drags to. Older targets predate the timestamped
/// messages used here.
pub(crate) const MIN_TARGET_VERSION: u32 = 3;
/// How long a dropped session waits for `XdndFinished` before giving up.
pub(crate) const FINISH_TIMEOUT: Duration = Duration::from_secs(10);

/// The version to speak to a target whose `XdndAware` property holds `aware`, or `None` when
/// the target is too old.
pub(crate) fn negotiated_version(aware: u32) -> Option<u32> {
    (aware >= MIN_TARGET_VERSION).then(|| aware.min(PROTOCOL_VERSION))
}

/// A rectangle in root-window coordinates.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Rect {
    pub x: i32,
    pub y: i32,
    pub width: i32,
    pub height: i32,
}

impl Rect {
    fn contains(&self, x: i32, y: i32) -> bool {
        self.width > 0
            && self.height > 0
            && x >= self.x
            && y >= self.y
            && x < self.x + self.width
            && y < self.y + self.height
    }
}

/// A window that can receive the drop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Target {
    /// The window carrying `XdndAware`; the messages name it.
    pub window: u32,
    /// Where the messages are sent: the window itself, or its `XdndProxy`.
    pub destination: u32,
    /// The negotiated protocol version.
    pub version: u32,
}

/// The client messages a source sends to a target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Message {
    Enter {
        source: u32,
        version: u32,
        uri_list: u32,
    },
    Position {
        source: u32,
        x: i32,
        y: i32,
        time: u32,
        action: u32,
    },
    Leave {
        source: u32,
    },
    Drop {
        source: u32,
        time: u32,
    },
}

/// Which `Xdnd*` message type a [`Message`] travels as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MessageKind {
    Enter,
    Position,
    Leave,
    Drop,
}

impl Message {
    pub(crate) fn kind(&self) -> MessageKind {
        match self {
            Message::Enter { .. } => MessageKind::Enter,
            Message::Position { .. } => MessageKind::Position,
            Message::Leave { .. } => MessageKind::Leave,
            Message::Drop { .. } => MessageKind::Drop,
        }
    }

    /// The five 32-bit words of the client message.
    pub(crate) fn data(&self) -> [u32; 5] {
        match *self {
            Message::Enter {
                source,
                version,
                uri_list,
            } => [source, version << 24, uri_list, 0, 0],
            Message::Position {
                source,
                x,
                y,
                time,
                action,
            } => [
                source,
                0,
                (u32::from(x as u16) << 16) | u32::from(y as u16),
                time,
                action,
            ],
            Message::Leave { source } => [source, 0, 0, 0, 0],
            Message::Drop { source, time } => [source, 0, time, 0, 0],
        }
    }
}

/// A target's reply to `XdndPosition`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Status {
    pub target: u32,
    pub accepted: bool,
    /// Whether positions inside [`Status::rect`] must still be sent.
    pub keep_sending_in_rect: bool,
    pub rect: Rect,
    pub action: u32,
}

impl Status {
    pub(crate) fn decode(data: [u32; 5]) -> Self {
        let [target, flags, position, size, action] = data;
        Self {
            target,
            accepted: flags & 1 != 0,
            keep_sending_in_rect: flags & 2 != 0,
            rect: Rect {
                x: i32::from((position >> 16) as u16 as i16),
                y: i32::from(position as u16 as i16),
                width: i32::from((size >> 16) as u16),
                height: i32::from(size as u16),
            },
            action,
        }
    }
}

/// A target's `XdndFinished`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Finished {
    pub target: u32,
    pub accepted: bool,
    pub action: u32,
}

impl Finished {
    pub(crate) fn decode(data: [u32; 5]) -> Self {
        let [target, flags, action, _, _] = data;
        Self {
            target,
            accepted: flags & 1 != 0,
            action,
        }
    }
}

/// Work the caller carries out for a [`SourceSession`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Command {
    /// Send `message` to `destination`; it names `target` as its window.
    Send {
        destination: u32,
        target: u32,
        message: Message,
    },
    /// Start the [`FINISH_TIMEOUT`] clock; call [`SourceSession::timed_out`] when it runs out.
    ArmTimeout,
    /// The session is over: release the grabs and the selection and report the end of the drag.
    /// Returned exactly once.
    End,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Dragging,
    /// The button was released at `time` while the target had not answered the last position.
    ReleasedAwaitingStatus {
        time: u32,
    },
    /// `XdndDrop` was sent; the target is fetching the data.
    Dropped,
    Ended,
}

#[derive(Clone, Copy, Debug)]
struct ActiveTarget {
    target: Target,
    accepted: bool,
    position_in_flight: bool,
    pending_position: Option<(i32, i32, u32)>,
    silent_rect: Option<Rect>,
}

/// The state of one outgoing file drag.
#[derive(Debug)]
pub(crate) struct SourceSession {
    source: u32,
    uri_list: u32,
    copy_action: u32,
    phase: Phase,
    active: Option<ActiveTarget>,
}

impl SourceSession {
    pub(crate) fn new(source: u32, uri_list: u32, copy_action: u32) -> Self {
        Self {
            source,
            uri_list,
            copy_action,
            phase: Phase::Dragging,
            active: None,
        }
    }

    /// Whether releasing the button now would hand the files to a target that took them.
    pub(crate) fn drop_would_be_accepted(&self) -> bool {
        self.phase == Phase::Dragging && self.active.is_some_and(|active| active.accepted)
    }

    #[cfg(test)]
    pub(crate) fn is_ended(&self) -> bool {
        self.phase == Phase::Ended
    }

    /// The pointer is at `(x, y)` in root coordinates over `target`, if any.
    pub(crate) fn pointer_moved(
        &mut self,
        target: Option<Target>,
        x: i32,
        y: i32,
        time: u32,
    ) -> Vec<Command> {
        if self.phase != Phase::Dragging {
            return Vec::new();
        }
        let mut commands = Vec::new();
        let same_target = match (&self.active, &target) {
            (Some(active), Some(target)) => active.target.window == target.window,
            (None, None) => true,
            _ => false,
        };
        if !same_target {
            self.push_leave(&mut commands);
            self.active = target.map(|target| ActiveTarget {
                target,
                accepted: false,
                position_in_flight: false,
                pending_position: None,
                silent_rect: None,
            });
            if let Some(active) = &self.active {
                commands.push(self.send(
                    &active.target,
                    Message::Enter {
                        source: self.source,
                        version: active.target.version,
                        uri_list: self.uri_list,
                    },
                ));
            }
        }
        if let Some(active) = self.active.as_mut() {
            if active.position_in_flight {
                active.pending_position = Some((x, y, time));
            } else if !active.silent_rect.is_some_and(|rect| rect.contains(x, y)) {
                active.position_in_flight = true;
                let active = *active;
                commands.push(self.position_command(&active, x, y, time));
            }
        }
        commands
    }

    /// The drag button was released.
    pub(crate) fn button_released(&mut self, time: u32) -> Vec<Command> {
        if self.phase != Phase::Dragging {
            return Vec::new();
        }
        let Some(active) = self.active else {
            return self.end();
        };
        if active.position_in_flight {
            self.phase = Phase::ReleasedAwaitingStatus { time };
            return vec![Command::ArmTimeout];
        }
        if active.accepted {
            self.drop_on(&active, time)
        } else {
            self.leave_and_end()
        }
    }

    pub(crate) fn status_received(&mut self, status: Status) -> Vec<Command> {
        if !matches!(
            self.phase,
            Phase::Dragging | Phase::ReleasedAwaitingStatus { .. }
        ) {
            return Vec::new();
        }
        let Some(active) = self.active.as_mut() else {
            return Vec::new();
        };
        if active.target.window != status.target {
            return Vec::new();
        }
        active.accepted = status.accepted;
        active.position_in_flight = false;
        active.silent_rect = (!status.keep_sending_in_rect).then_some(status.rect);
        let active = *active;

        if let Phase::ReleasedAwaitingStatus { time } = self.phase {
            return if active.accepted {
                self.drop_on(&active, time)
            } else {
                self.leave_and_end()
            };
        }
        let Some((x, y, time)) = active.pending_position else {
            return Vec::new();
        };
        let Some(active) = self.active.as_mut() else {
            return Vec::new();
        };
        active.pending_position = None;
        if active.silent_rect.is_some_and(|rect| rect.contains(x, y)) {
            return Vec::new();
        }
        active.position_in_flight = true;
        let active = *active;
        vec![self.position_command(&active, x, y, time)]
    }

    pub(crate) fn finished_received(&mut self, finished: Finished) -> Vec<Command> {
        let from_target = self
            .active
            .is_some_and(|active| active.target.window == finished.target);
        if self.phase == Phase::Dropped && from_target {
            self.end()
        } else {
            Vec::new()
        }
    }

    /// The user cancelled the drag.
    pub(crate) fn cancel(&mut self) -> Vec<Command> {
        match self.phase {
            Phase::Ended => Vec::new(),
            Phase::Dropped => self.end(),
            Phase::Dragging | Phase::ReleasedAwaitingStatus { .. } => self.leave_and_end(),
        }
    }

    /// The [`FINISH_TIMEOUT`] clock ran out.
    pub(crate) fn timed_out(&mut self) -> Vec<Command> {
        match self.phase {
            Phase::Dropped => self.end(),
            Phase::ReleasedAwaitingStatus { .. } => self.leave_and_end(),
            Phase::Dragging | Phase::Ended => Vec::new(),
        }
    }

    fn send(&self, target: &Target, message: Message) -> Command {
        Command::Send {
            destination: target.destination,
            target: target.window,
            message,
        }
    }

    fn position_command(&self, active: &ActiveTarget, x: i32, y: i32, time: u32) -> Command {
        self.send(
            &active.target,
            Message::Position {
                source: self.source,
                x,
                y,
                time,
                action: self.copy_action,
            },
        )
    }

    fn push_leave(&self, commands: &mut Vec<Command>) {
        if let Some(active) = &self.active {
            commands.push(self.send(
                &active.target,
                Message::Leave {
                    source: self.source,
                },
            ));
        }
    }

    fn drop_on(&mut self, active: &ActiveTarget, time: u32) -> Vec<Command> {
        let mut commands = vec![self.send(
            &active.target,
            Message::Drop {
                source: self.source,
                time,
            },
        )];
        // Targets older than version 2 never send `XdndFinished`.
        if active.target.version < 2 {
            commands.extend(self.end());
        } else {
            self.phase = Phase::Dropped;
            commands.push(Command::ArmTimeout);
        }
        commands
    }

    fn leave_and_end(&mut self) -> Vec<Command> {
        let mut commands = Vec::new();
        self.push_leave(&mut commands);
        commands.extend(self.end());
        commands
    }

    fn end(&mut self) -> Vec<Command> {
        if self.phase == Phase::Ended {
            return Vec::new();
        }
        self.phase = Phase::Ended;
        vec![Command::End]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOURCE: u32 = 0x100;
    const URI_LIST: u32 = 7;
    const COPY: u32 = 8;

    fn target(window: u32) -> Target {
        Target {
            window,
            destination: window,
            version: 5,
        }
    }

    fn session() -> SourceSession {
        SourceSession::new(SOURCE, URI_LIST, COPY)
    }

    fn send(destination: u32, target: u32, message: Message) -> Command {
        Command::Send {
            destination,
            target,
            message,
        }
    }

    fn enter(target: u32, version: u32) -> Command {
        send(
            target,
            target,
            Message::Enter {
                source: SOURCE,
                version,
                uri_list: URI_LIST,
            },
        )
    }

    fn position(target: u32, x: i32, y: i32, time: u32) -> Command {
        send(
            target,
            target,
            Message::Position {
                source: SOURCE,
                x,
                y,
                time,
                action: COPY,
            },
        )
    }

    fn leave(target: u32) -> Command {
        send(target, target, Message::Leave { source: SOURCE })
    }

    fn drop_message(target: u32, time: u32) -> Command {
        send(
            target,
            target,
            Message::Drop {
                source: SOURCE,
                time,
            },
        )
    }

    fn status(target: u32, accepted: bool) -> Status {
        Status {
            target,
            accepted,
            keep_sending_in_rect: true,
            rect: Rect::default(),
            action: if accepted { COPY } else { 0 },
        }
    }

    fn ends(commands: &[Command]) -> usize {
        commands
            .iter()
            .filter(|command| **command == Command::End)
            .count()
    }

    #[test]
    fn negotiates_the_lower_of_both_versions_and_rejects_old_targets() {
        assert_eq!(negotiated_version(5), Some(5));
        assert_eq!(negotiated_version(4), Some(4));
        assert_eq!(negotiated_version(3), Some(3));
        assert_eq!(negotiated_version(9), Some(5));
        assert_eq!(negotiated_version(2), None);
        assert_eq!(negotiated_version(0), None);
    }

    #[test]
    fn encodes_enter_with_version_in_the_high_byte_and_the_uri_list_type() {
        let data = Message::Enter {
            source: SOURCE,
            version: 5,
            uri_list: URI_LIST,
        }
        .data();
        assert_eq!(data, [SOURCE, 5 << 24, URI_LIST, 0, 0]);
    }

    #[test]
    fn encodes_position_with_packed_root_coordinates() {
        let data = Message::Position {
            source: SOURCE,
            x: 300,
            y: 40,
            time: 99,
            action: COPY,
        }
        .data();
        assert_eq!(data, [SOURCE, 0, (300 << 16) | 40, 99, COPY]);
    }

    #[test]
    fn encodes_negative_coordinates_as_sixteen_bit_halves() {
        let data = Message::Position {
            source: SOURCE,
            x: -1,
            y: -2,
            time: 0,
            action: COPY,
        }
        .data();
        assert_eq!(data[2], 0xFFFF_FFFE);
    }

    #[test]
    fn encodes_leave_and_drop() {
        assert_eq!(
            Message::Leave { source: SOURCE }.data(),
            [SOURCE, 0, 0, 0, 0]
        );
        assert_eq!(
            Message::Drop {
                source: SOURCE,
                time: 12
            }
            .data(),
            [SOURCE, 0, 12, 0, 0]
        );
    }

    #[test]
    fn decodes_status_flags_rectangle_and_action() {
        let decoded = Status::decode([0x200, 0b01, (10 << 16) | 20, (30 << 16) | 40, COPY]);
        assert_eq!(
            decoded,
            Status {
                target: 0x200,
                accepted: true,
                keep_sending_in_rect: false,
                rect: Rect {
                    x: 10,
                    y: 20,
                    width: 30,
                    height: 40
                },
                action: COPY,
            }
        );
        let refused = Status::decode([0x200, 0b10, 0, 0, 0]);
        assert!(!refused.accepted);
        assert!(refused.keep_sending_in_rect);
    }

    #[test]
    fn decodes_finished() {
        assert_eq!(
            Finished::decode([0x200, 1, COPY, 0, 0]),
            Finished {
                target: 0x200,
                accepted: true,
                action: COPY
            }
        );
        assert!(!Finished::decode([0x200, 0, 0, 0, 0]).accepted);
    }

    #[test]
    fn entering_a_target_sends_enter_then_position() {
        let mut session = session();
        assert_eq!(
            session.pointer_moved(Some(target(0x200)), 10, 20, 5),
            vec![enter(0x200, 5), position(0x200, 10, 20, 5)]
        );
    }

    #[test]
    fn enter_uses_the_negotiated_version() {
        let mut session = session();
        let old = Target {
            window: 0x200,
            destination: 0x201,
            version: 3,
        };
        assert_eq!(
            session.pointer_moved(Some(old), 1, 2, 5),
            vec![
                send(
                    0x201,
                    0x200,
                    Message::Enter {
                        source: SOURCE,
                        version: 3,
                        uri_list: URI_LIST
                    }
                ),
                send(
                    0x201,
                    0x200,
                    Message::Position {
                        source: SOURCE,
                        x: 1,
                        y: 2,
                        time: 5,
                        action: COPY
                    }
                ),
            ]
        );
    }

    #[test]
    fn moving_without_a_target_sends_nothing() {
        let mut session = session();
        assert!(session.pointer_moved(None, 10, 20, 5).is_empty());
    }

    #[test]
    fn a_second_position_waits_for_the_status_of_the_first() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        assert!(
            session
                .pointer_moved(Some(target(0x200)), 11, 21, 6)
                .is_empty()
        );
        assert!(
            session
                .pointer_moved(Some(target(0x200)), 12, 22, 7)
                .is_empty()
        );
        assert_eq!(
            session.status_received(status(0x200, true)),
            vec![position(0x200, 12, 22, 7)]
        );
    }

    #[test]
    fn a_drop_is_accepted_only_after_the_target_says_so() {
        let mut session = session();
        assert!(!session.drop_would_be_accepted());
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        assert!(!session.drop_would_be_accepted());
        session.status_received(status(0x200, true));
        assert!(session.drop_would_be_accepted());
        session.pointer_moved(Some(target(0x300)), 400, 20, 6);
        assert!(!session.drop_would_be_accepted());
        session.status_received(status(0x300, true));
        session.status_received(status(0x300, false));
        assert!(!session.drop_would_be_accepted());
    }

    #[test]
    fn a_status_with_nothing_pending_sends_nothing() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        assert!(session.status_received(status(0x200, true)).is_empty());
    }

    #[test]
    fn status_from_another_window_is_ignored() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        session.pointer_moved(Some(target(0x200)), 11, 21, 6);
        assert!(session.status_received(status(0x999, true)).is_empty());
        assert_eq!(
            session.status_received(status(0x200, true)),
            vec![position(0x200, 11, 21, 6)]
        );
    }

    #[test]
    fn positions_inside_the_silent_rectangle_are_not_sent() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        let mut reply = status(0x200, true);
        reply.keep_sending_in_rect = false;
        reply.rect = Rect {
            x: 0,
            y: 0,
            width: 100,
            height: 100,
        };
        assert!(session.status_received(reply).is_empty());
        assert!(
            session
                .pointer_moved(Some(target(0x200)), 50, 50, 6)
                .is_empty()
        );
        assert_eq!(
            session.pointer_moved(Some(target(0x200)), 150, 50, 7),
            vec![position(0x200, 150, 50, 7)]
        );
    }

    #[test]
    fn positions_inside_the_rectangle_are_sent_when_the_target_asks_for_them() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        let mut reply = status(0x200, true);
        reply.rect = Rect {
            x: 0,
            y: 0,
            width: 100,
            height: 100,
        };
        session.status_received(reply);
        assert_eq!(
            session.pointer_moved(Some(target(0x200)), 50, 50, 6),
            vec![position(0x200, 50, 50, 6)]
        );
    }

    #[test]
    fn changing_target_leaves_the_old_one_and_enters_the_new_one() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        assert_eq!(
            session.pointer_moved(Some(target(0x300)), 400, 20, 6),
            vec![leave(0x200), enter(0x300, 5), position(0x300, 400, 20, 6)]
        );
    }

    #[test]
    fn a_status_from_the_old_target_does_not_unblock_the_new_one() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        session.pointer_moved(Some(target(0x300)), 400, 20, 6);
        assert!(
            session
                .pointer_moved(Some(target(0x300)), 401, 20, 7)
                .is_empty()
        );
        assert!(session.status_received(status(0x200, true)).is_empty());
        assert_eq!(
            session.status_received(status(0x300, true)),
            vec![position(0x300, 401, 20, 7)]
        );
    }

    #[test]
    fn leaving_every_target_sends_leave() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        assert_eq!(session.pointer_moved(None, 900, 20, 6), vec![leave(0x200)]);
        assert!(session.pointer_moved(None, 901, 20, 7).is_empty());
    }

    #[test]
    fn release_over_an_accepting_target_drops_and_waits_for_finished() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        session.status_received(status(0x200, true));
        assert_eq!(
            session.button_released(9),
            vec![drop_message(0x200, 9), Command::ArmTimeout]
        );
        assert!(!session.is_ended());
        assert_eq!(
            session.finished_received(Finished {
                target: 0x200,
                accepted: true,
                action: COPY
            }),
            vec![Command::End]
        );
        assert!(session.is_ended());
    }

    #[test]
    fn release_over_a_refusing_target_leaves_and_ends() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        session.status_received(status(0x200, false));
        assert_eq!(session.button_released(9), vec![leave(0x200), Command::End]);
    }

    #[test]
    fn release_over_no_target_ends() {
        let mut session = session();
        assert_eq!(session.button_released(9), vec![Command::End]);
    }

    #[test]
    fn release_before_the_first_status_waits_for_it() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        assert_eq!(session.button_released(9), vec![Command::ArmTimeout]);
        assert_eq!(
            session.status_received(status(0x200, true)),
            vec![drop_message(0x200, 9), Command::ArmTimeout]
        );
    }

    #[test]
    fn release_before_a_refusing_status_leaves_and_ends() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        session.button_released(9);
        assert_eq!(
            session.status_received(status(0x200, false)),
            vec![leave(0x200), Command::End]
        );
    }

    #[test]
    fn release_while_waiting_for_a_status_times_out_with_leave() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        session.button_released(9);
        assert_eq!(session.timed_out(), vec![leave(0x200), Command::End]);
    }

    #[test]
    fn a_target_older_than_version_two_ends_right_after_the_drop() {
        let mut session = session();
        let old = Target {
            window: 0x200,
            destination: 0x200,
            version: 1,
        };
        session.pointer_moved(Some(old), 10, 20, 5);
        session.status_received(status(0x200, true));
        let commands = session.button_released(9);
        assert_eq!(commands.last(), Some(&Command::End));
        assert_eq!(ends(&commands), 1);
    }

    #[test]
    fn timing_out_after_the_drop_ends_once() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        session.status_received(status(0x200, true));
        session.button_released(9);
        assert_eq!(session.timed_out(), vec![Command::End]);
        assert!(session.timed_out().is_empty());
    }

    #[test]
    fn finished_from_another_window_is_ignored() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        session.status_received(status(0x200, true));
        session.button_released(9);
        assert!(
            session
                .finished_received(Finished {
                    target: 0x999,
                    accepted: true,
                    action: COPY
                })
                .is_empty()
        );
        assert!(!session.is_ended());
    }

    #[test]
    fn cancelling_while_dragging_leaves_and_ends() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        assert_eq!(session.cancel(), vec![leave(0x200), Command::End]);
    }

    #[test]
    fn cancelling_without_a_target_just_ends() {
        let mut session = session();
        assert_eq!(session.cancel(), vec![Command::End]);
    }

    #[test]
    fn an_ended_session_ignores_everything() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        session.cancel();
        assert!(session.cancel().is_empty());
        assert!(session.button_released(9).is_empty());
        assert!(session.timed_out().is_empty());
        assert!(
            session
                .pointer_moved(Some(target(0x300)), 1, 1, 1)
                .is_empty()
        );
        assert!(session.status_received(status(0x200, true)).is_empty());
        assert!(
            session
                .finished_received(Finished {
                    target: 0x200,
                    accepted: true,
                    action: COPY
                })
                .is_empty()
        );
    }

    #[test]
    fn pointer_movement_after_the_drop_is_ignored() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        session.status_received(status(0x200, true));
        session.button_released(9);
        assert!(
            session
                .pointer_moved(Some(target(0x300)), 1, 1, 1)
                .is_empty()
        );
        assert!(session.pointer_moved(None, 1, 1, 1).is_empty());
    }

    #[test]
    fn cancelling_after_the_drop_ends_without_leave() {
        let mut session = session();
        session.pointer_moved(Some(target(0x200)), 10, 20, 5);
        session.status_received(status(0x200, true));
        session.button_released(9);
        assert_eq!(session.cancel(), vec![Command::End]);
    }
}
