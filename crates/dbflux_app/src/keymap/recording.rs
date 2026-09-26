//! State machine behind the "record a new shortcut" interaction.
//!
//! The recorder captures one chord. A bare Escape cancels, so Escape itself
//! cannot be recorded. A chord that collides with other bindings stops in the
//! conflict state until the user cancels or replaces them.

use super::overrides::{BindingSlot, KeymapOverrides};
use super::{KeyChord, KeymapStack, Modifiers};

/// Keys that only report a modifier being pressed; they never finish a chord.
const MODIFIER_KEYS: &[&str] = &[
    "shift", "control", "ctrl", "alt", "platform", "cmd", "super", "function", "fn",
];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RecordingState {
    #[default]
    Idle,
    /// Waiting for the new chord of `slot`.
    Recording { slot: BindingSlot },
    /// `chord` collides with `conflicts`; waiting for cancel or replace.
    Conflict {
        slot: BindingSlot,
        chord: KeyChord,
        conflicts: Vec<BindingSlot>,
    },
}

/// What a key press did to the recorder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordingOutcome {
    /// The key was not part of a recording (idle, modifier only, or a key
    /// other than Escape while a conflict waits for a decision).
    Ignored,
    /// The recording was cancelled; nothing changes.
    Cancelled,
    /// `chord` is free: save it for `slot`.
    Commit { slot: BindingSlot, chord: KeyChord },
    /// The chord collides with other bindings; the recorder now waits in
    /// [`RecordingState::Conflict`].
    ConflictFound,
}

/// A decided replacement: `slot` takes `chord` and every binding in
/// `replaced` loses its shortcut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replacement {
    pub slot: BindingSlot,
    pub chord: KeyChord,
    pub replaced: Vec<BindingSlot>,
}

#[derive(Debug, Clone, Default)]
pub struct KeybindingRecorder {
    state: RecordingState,
}

impl KeybindingRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn state(&self) -> &RecordingState {
        &self.state
    }

    pub fn is_active(&self) -> bool {
        self.state != RecordingState::Idle
    }

    /// The binding being recorded or waiting on a conflict decision.
    pub fn target(&self) -> Option<&BindingSlot> {
        match &self.state {
            RecordingState::Idle => None,
            RecordingState::Recording { slot } | RecordingState::Conflict { slot, .. } => {
                Some(slot)
            }
        }
    }

    /// Starts recording a new chord for `slot`, abandoning any other
    /// recording in progress.
    pub fn start(&mut self, slot: BindingSlot) {
        self.state = RecordingState::Recording { slot };
    }

    pub fn cancel(&mut self) {
        self.state = RecordingState::Idle;
    }

    /// Feeds one key press to the recorder.
    pub fn handle_chord(
        &mut self,
        chord: KeyChord,
        defaults: &KeymapStack,
        overrides: &KeymapOverrides,
    ) -> RecordingOutcome {
        let is_cancel = chord.key == "escape" && chord.modifiers == Modifiers::none();

        match &self.state {
            RecordingState::Idle => RecordingOutcome::Ignored,
            RecordingState::Conflict { .. } => {
                if is_cancel {
                    self.cancel();
                    RecordingOutcome::Cancelled
                } else {
                    RecordingOutcome::Ignored
                }
            }
            RecordingState::Recording { slot } => {
                if is_cancel {
                    self.cancel();
                    return RecordingOutcome::Cancelled;
                }

                if chord.key.is_empty() || MODIFIER_KEYS.contains(&chord.key.as_str()) {
                    return RecordingOutcome::Ignored;
                }

                let slot = slot.clone();
                let conflicts = overrides.conflicts_for(defaults, &slot, &chord);

                if conflicts.is_empty() {
                    self.cancel();
                    RecordingOutcome::Commit { slot, chord }
                } else {
                    self.state = RecordingState::Conflict {
                        slot,
                        chord,
                        conflicts,
                    };
                    RecordingOutcome::ConflictFound
                }
            }
        }
    }

    /// Accepts the pending conflict: returns the replacement to save and
    /// goes back to idle. Returns `None` when no conflict is pending.
    pub fn confirm_replace(&mut self) -> Option<Replacement> {
        match std::mem::take(&mut self.state) {
            RecordingState::Conflict {
                slot,
                chord,
                conflicts,
            } => Some(Replacement {
                slot,
                chord,
                replaced: conflicts,
            }),
            other => {
                self.state = other;
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keymap::{Command, ContextId, KeymapLayer};

    fn defaults() -> KeymapStack {
        let mut global = KeymapLayer::new(ContextId::Global);
        global.bind(KeyChord::new("n", Modifiers::ctrl()), Command::NewQueryTab);
        global.bind(
            KeyChord::new("w", Modifiers::ctrl()),
            Command::CloseCurrentTab,
        );

        let mut stack = KeymapStack::new();
        stack.add_layer(global);
        stack
    }

    fn new_query_tab() -> BindingSlot {
        BindingSlot::new(
            ContextId::Global,
            Command::NewQueryTab,
            KeyChord::new("n", Modifiers::ctrl()),
        )
    }

    fn close_tab() -> BindingSlot {
        BindingSlot::new(
            ContextId::Global,
            Command::CloseCurrentTab,
            KeyChord::new("w", Modifiers::ctrl()),
        )
    }

    fn escape() -> KeyChord {
        KeyChord::new("escape", Modifiers::none())
    }

    #[test]
    fn idle_recorder_ignores_keys() {
        let mut recorder = KeybindingRecorder::new();

        let outcome = recorder.handle_chord(
            KeyChord::new("t", Modifiers::ctrl()),
            &defaults(),
            &KeymapOverrides::new(),
        );

        assert_eq!(outcome, RecordingOutcome::Ignored);
        assert!(!recorder.is_active());
    }

    #[test]
    fn free_chord_commits_and_returns_to_idle() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab());
        assert_eq!(recorder.target(), Some(&new_query_tab()));

        let outcome = recorder.handle_chord(
            KeyChord::new("t", Modifiers::ctrl()),
            &defaults(),
            &KeymapOverrides::new(),
        );

        assert_eq!(
            outcome,
            RecordingOutcome::Commit {
                slot: new_query_tab(),
                chord: KeyChord::new("t", Modifiers::ctrl()),
            }
        );
        assert_eq!(recorder.state(), &RecordingState::Idle);
    }

    #[test]
    fn escape_cancels_and_modifier_keys_wait() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab());

        let modifier_only = recorder.handle_chord(
            KeyChord::new("shift", Modifiers::shift()),
            &defaults(),
            &KeymapOverrides::new(),
        );
        assert_eq!(modifier_only, RecordingOutcome::Ignored);
        assert!(recorder.is_active());

        let cancelled = recorder.handle_chord(escape(), &defaults(), &KeymapOverrides::new());
        assert_eq!(cancelled, RecordingOutcome::Cancelled);
        assert!(!recorder.is_active());
    }

    #[test]
    fn modified_escape_is_recorded() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab());

        let outcome = recorder.handle_chord(
            KeyChord::new("escape", Modifiers::shift()),
            &defaults(),
            &KeymapOverrides::new(),
        );

        assert!(matches!(outcome, RecordingOutcome::Commit { .. }));
    }

    #[test]
    fn taken_chord_waits_for_a_decision_then_replaces() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab());

        let outcome = recorder.handle_chord(
            KeyChord::new("w", Modifiers::ctrl()),
            &defaults(),
            &KeymapOverrides::new(),
        );
        assert_eq!(outcome, RecordingOutcome::ConflictFound);
        assert_eq!(
            recorder.state(),
            &RecordingState::Conflict {
                slot: new_query_tab(),
                chord: KeyChord::new("w", Modifiers::ctrl()),
                conflicts: vec![close_tab()],
            }
        );

        let other_key = recorder.handle_chord(
            KeyChord::new("x", Modifiers::ctrl()),
            &defaults(),
            &KeymapOverrides::new(),
        );
        assert_eq!(
            other_key,
            RecordingOutcome::Ignored,
            "the conflict stays open"
        );

        let replacement = recorder.confirm_replace().expect("pending conflict");
        assert_eq!(replacement.slot, new_query_tab());
        assert_eq!(replacement.replaced, vec![close_tab()]);
        assert!(!recorder.is_active());
        assert_eq!(recorder.confirm_replace(), None);
    }

    #[test]
    fn escape_cancels_a_pending_conflict() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab());
        recorder.handle_chord(
            KeyChord::new("w", Modifiers::ctrl()),
            &defaults(),
            &KeymapOverrides::new(),
        );

        let outcome = recorder.handle_chord(escape(), &defaults(), &KeymapOverrides::new());

        assert_eq!(outcome, RecordingOutcome::Cancelled);
        assert_eq!(recorder.confirm_replace(), None);
    }

    #[test]
    fn confirm_replace_keeps_a_running_recording() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab());

        assert_eq!(recorder.confirm_replace(), None);
        assert_eq!(
            recorder.state(),
            &RecordingState::Recording {
                slot: new_query_tab()
            }
        );
    }
}
