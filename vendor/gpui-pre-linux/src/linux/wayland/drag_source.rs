//! The state of one outgoing Wayland file drag, kept apart from the protocol objects so the
//! rules for when the drag is over can be tested without a compositor.
//!
//! A drag ends for the app when the compositor reports the drop (or the cancellation), not when
//! the target finishes reading: a target that never sends `finish`, because it speaks an older
//! version of `wl_data_offer` or is slow, must not keep the window in a drag. The data source
//! stays alive after the drop so the target can still read the data, and is released on
//! `dnd_finished`, `cancelled` or after [`FINISH_TIMEOUT`].

use std::time::Duration;

/// How long the data source outlives the drop when the target never reports `dnd_finished`.
pub(crate) const FINISH_TIMEOUT: Duration = Duration::from_secs(10);

/// What the client does after an event of the drag source.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Outcome {
    /// Tell the window the drag is over. Set for exactly one event of a session.
    pub notify_ended: bool,
    /// Destroy the data source and everything attached to it.
    pub release_source: bool,
    /// Start the [`FINISH_TIMEOUT`] clock; call [`DragSession::timed_out`] when it runs out.
    pub arm_timeout: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Dragging,
    /// `dnd_drop_performed` arrived; the target may still read the data.
    Dropped,
    Released,
}

#[derive(Debug)]
pub(crate) struct DragSession {
    phase: Phase,
    ended_notified: bool,
}

impl DragSession {
    pub(crate) fn new() -> Self {
        Self {
            phase: Phase::Dragging,
            ended_notified: false,
        }
    }

    /// Whether the data may still be requested.
    pub(crate) fn can_send(&self) -> bool {
        self.phase != Phase::Released
    }

    /// `dnd_drop_performed`.
    pub(crate) fn drop_performed(&mut self) -> Outcome {
        if self.phase != Phase::Dragging {
            return Outcome::default();
        }
        self.phase = Phase::Dropped;
        Outcome {
            notify_ended: self.take_end_notification(),
            release_source: false,
            arm_timeout: true,
        }
    }

    /// `dnd_finished`.
    pub(crate) fn finished(&mut self) -> Outcome {
        self.release()
    }

    /// `cancelled`.
    pub(crate) fn cancelled(&mut self) -> Outcome {
        self.release()
    }

    /// The [`FINISH_TIMEOUT`] clock ran out.
    pub(crate) fn timed_out(&mut self) -> Outcome {
        match self.phase {
            Phase::Dropped => self.release(),
            Phase::Dragging | Phase::Released => Outcome::default(),
        }
    }

    fn release(&mut self) -> Outcome {
        if self.phase == Phase::Released {
            return Outcome::default();
        }
        self.phase = Phase::Released;
        Outcome {
            notify_ended: self.take_end_notification(),
            release_source: true,
            arm_timeout: false,
        }
    }

    fn take_end_notification(&mut self) -> bool {
        !std::mem::replace(&mut self.ended_notified, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ended_and_released() -> Outcome {
        Outcome {
            notify_ended: true,
            release_source: true,
            arm_timeout: false,
        }
    }

    #[test]
    fn a_new_session_serves_data() {
        assert!(DragSession::new().can_send());
    }

    #[test]
    fn the_drop_ends_the_drag_for_the_app_but_keeps_the_source() {
        let mut session = DragSession::new();
        assert_eq!(
            session.drop_performed(),
            Outcome {
                notify_ended: true,
                release_source: false,
                arm_timeout: true,
            }
        );
        assert!(session.can_send());
    }

    #[test]
    fn finish_after_the_drop_releases_without_a_second_end() {
        let mut session = DragSession::new();
        session.drop_performed();
        assert_eq!(
            session.finished(),
            Outcome {
                notify_ended: false,
                release_source: true,
                arm_timeout: false,
            }
        );
        assert!(!session.can_send());
    }

    #[test]
    fn the_timeout_after_the_drop_releases_without_a_second_end() {
        let mut session = DragSession::new();
        session.drop_performed();
        assert_eq!(
            session.timed_out(),
            Outcome {
                notify_ended: false,
                release_source: true,
                arm_timeout: false,
            }
        );
    }

    #[test]
    fn cancelling_before_the_drop_ends_and_releases() {
        let mut session = DragSession::new();
        assert_eq!(session.cancelled(), ended_and_released());
        assert!(!session.can_send());
    }

    #[test]
    fn cancelling_after_the_drop_releases_without_a_second_end() {
        let mut session = DragSession::new();
        session.drop_performed();
        assert_eq!(
            session.cancelled(),
            Outcome {
                notify_ended: false,
                release_source: true,
                arm_timeout: false,
            }
        );
    }

    #[test]
    fn finishing_without_a_reported_drop_still_ends_the_drag() {
        let mut session = DragSession::new();
        assert_eq!(session.finished(), ended_and_released());
    }

    #[test]
    fn a_timeout_while_still_dragging_does_nothing() {
        let mut session = DragSession::new();
        assert_eq!(session.timed_out(), Outcome::default());
        assert!(session.can_send());
    }

    #[test]
    fn events_after_the_release_do_nothing() {
        let mut session = DragSession::new();
        session.cancelled();
        assert_eq!(session.drop_performed(), Outcome::default());
        assert_eq!(session.finished(), Outcome::default());
        assert_eq!(session.cancelled(), Outcome::default());
        assert_eq!(session.timed_out(), Outcome::default());
    }

    #[test]
    fn a_second_drop_report_ends_nothing_again() {
        let mut session = DragSession::new();
        session.drop_performed();
        assert_eq!(session.drop_performed(), Outcome::default());
    }
}
