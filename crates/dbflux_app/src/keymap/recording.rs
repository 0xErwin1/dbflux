//! State machine behind the "record a new shortcut" interaction.
//!
//! The recorder captures a key sequence: one chord, or several pressed one
//! after the other. The caller ends the sequence with
//! [`KeybindingRecorder::finish`], after a pause or when the user confirms; a
//! sequence of [`MAX_SEQUENCE_LENGTH`] chords ends on its own. A bare Escape
//! cancels, so Escape itself cannot be recorded. Keys that collide with other
//! bindings stop in the conflict state until the user cancels or replaces
//! them.

use super::chord::MAX_SEQUENCE_LENGTH;
use super::overrides::{BindingSlot, KeymapOverrides, PredicateOverlap};
use super::{KeyChord, KeySequence, KeymapStack, Modifiers};

/// Keys that only report a modifier being pressed; they never finish a chord.
const MODIFIER_KEYS: &[&str] = &[
    "shift", "control", "ctrl", "alt", "platform", "cmd", "super", "function", "fn",
];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RecordingState {
    #[default]
    Idle,
    /// Waiting for the keys of `slot`, which applies under `predicate`.
    /// `captured` holds the chords pressed so far.
    Recording {
        slot: BindingSlot,
        predicate: String,
        captured: Vec<KeyChord>,
    },
    /// `keys` collide with `conflicts`; waiting for cancel or replace.
    Conflict {
        slot: BindingSlot,
        keys: KeySequence,
        conflicts: Vec<BindingSlot>,
    },
}

/// What a key press, or the end of a sequence, did to the recorder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordingOutcome {
    /// The key was not part of a recording (idle, modifier only, or a key
    /// other than Escape while a conflict waits for a decision).
    Ignored,
    /// The chord was added to the sequence; more may follow.
    Captured,
    /// The recording was cancelled; nothing changes.
    Cancelled,
    /// `keys` are free: save them for `slot`.
    Commit {
        slot: BindingSlot,
        keys: KeySequence,
    },
    /// The keys collide with other bindings; the recorder now waits in
    /// [`RecordingState::Conflict`].
    ConflictFound,
}

/// A decided replacement: `slot` takes `keys` and every binding in
/// `replaced` loses its shortcut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replacement {
    pub slot: BindingSlot,
    pub keys: KeySequence,
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
            RecordingState::Recording { slot, .. } | RecordingState::Conflict { slot, .. } => {
                Some(slot)
            }
        }
    }

    /// The chords captured so far while recording.
    pub fn captured(&self) -> &[KeyChord] {
        match &self.state {
            RecordingState::Recording { captured, .. } => captured,
            _ => &[],
        }
    }

    /// Starts recording new keys for `slot`, which will apply under
    /// `predicate`, abandoning any other recording in progress.
    pub fn start(&mut self, slot: BindingSlot, predicate: impl Into<String>) {
        self.state = RecordingState::Recording {
            slot,
            predicate: predicate.into(),
            captured: Vec::new(),
        };
    }

    pub fn cancel(&mut self) {
        self.state = RecordingState::Idle;
    }

    /// Feeds one key press to the recorder.
    ///
    /// A chord that fills the sequence to [`MAX_SEQUENCE_LENGTH`] ends it at
    /// once, as [`KeybindingRecorder::finish`] would.
    pub fn handle_chord(
        &mut self,
        chord: KeyChord,
        defaults: &KeymapStack,
        overrides: &KeymapOverrides,
        overlap: &dyn PredicateOverlap,
    ) -> RecordingOutcome {
        let is_cancel = chord.key == "escape" && chord.modifiers == Modifiers::none();

        match &mut self.state {
            RecordingState::Idle => RecordingOutcome::Ignored,
            RecordingState::Conflict { .. } => {
                if is_cancel {
                    self.cancel();
                    RecordingOutcome::Cancelled
                } else {
                    RecordingOutcome::Ignored
                }
            }
            RecordingState::Recording { captured, .. } => {
                if is_cancel {
                    self.cancel();
                    return RecordingOutcome::Cancelled;
                }

                if chord.key.is_empty() || MODIFIER_KEYS.contains(&chord.key.as_str()) {
                    return RecordingOutcome::Ignored;
                }

                captured.push(chord);

                if captured.len() >= MAX_SEQUENCE_LENGTH {
                    self.finish(defaults, overrides, overlap)
                } else {
                    RecordingOutcome::Captured
                }
            }
        }
    }

    /// Ends the sequence being recorded: commits it when it is free, or
    /// waits in the conflict state when it collides. Does nothing while no
    /// chord has been captured.
    pub fn finish(
        &mut self,
        defaults: &KeymapStack,
        overrides: &KeymapOverrides,
        overlap: &dyn PredicateOverlap,
    ) -> RecordingOutcome {
        let RecordingState::Recording {
            slot,
            predicate,
            captured,
        } = &self.state
        else {
            return RecordingOutcome::Ignored;
        };

        let Some(keys) = KeySequence::new(captured.clone()) else {
            return RecordingOutcome::Ignored;
        };

        let slot = slot.clone();
        let conflicts = overrides.conflicts_for(defaults, &slot, &keys, predicate, overlap);

        if conflicts.is_empty() {
            self.cancel();
            RecordingOutcome::Commit { slot, keys }
        } else {
            self.state = RecordingState::Conflict {
                slot,
                keys,
                conflicts,
            };
            RecordingOutcome::ConflictFound
        }
    }

    /// Accepts the pending conflict: returns the replacement to save and
    /// goes back to idle. Returns `None` when no conflict is pending.
    pub fn confirm_replace(&mut self) -> Option<Replacement> {
        match std::mem::take(&mut self.state) {
            RecordingState::Conflict {
                slot,
                keys,
                conflicts,
            } => Some(Replacement {
                slot,
                keys,
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
    use crate::keymap::{Command, ContextId, ContextTreeOverlap, KeymapLayer};

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

    fn press(recorder: &mut KeybindingRecorder, chord: KeyChord) -> RecordingOutcome {
        recorder.handle_chord(
            chord,
            &defaults(),
            &KeymapOverrides::new(),
            &ContextTreeOverlap,
        )
    }

    fn finish(recorder: &mut KeybindingRecorder) -> RecordingOutcome {
        recorder.finish(&defaults(), &KeymapOverrides::new(), &ContextTreeOverlap)
    }

    #[test]
    fn idle_recorder_ignores_keys() {
        let mut recorder = KeybindingRecorder::new();

        let outcome = press(&mut recorder, KeyChord::new("t", Modifiers::ctrl()));

        assert_eq!(outcome, RecordingOutcome::Ignored);
        assert_eq!(finish(&mut recorder), RecordingOutcome::Ignored);
        assert!(!recorder.is_active());
    }

    #[test]
    fn free_chord_commits_when_the_sequence_ends() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab(), ContextId::Global.default_predicate());
        assert_eq!(recorder.target(), Some(&new_query_tab()));

        let outcome = press(&mut recorder, KeyChord::new("t", Modifiers::ctrl()));
        assert_eq!(outcome, RecordingOutcome::Captured);
        assert_eq!(
            recorder.captured(),
            &[KeyChord::new("t", Modifiers::ctrl())]
        );

        assert_eq!(
            finish(&mut recorder),
            RecordingOutcome::Commit {
                slot: new_query_tab(),
                keys: KeySequence::from(KeyChord::new("t", Modifiers::ctrl())),
            }
        );
        assert_eq!(recorder.state(), &RecordingState::Idle);
    }

    #[test]
    fn several_chords_record_a_sequence() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab(), ContextId::Global.default_predicate());

        press(&mut recorder, KeyChord::new("k", Modifiers::ctrl()));
        press(&mut recorder, KeyChord::new("t", Modifiers::ctrl()));

        assert_eq!(
            finish(&mut recorder),
            RecordingOutcome::Commit {
                slot: new_query_tab(),
                keys: KeySequence::parse("ctrl+k ctrl+t").expect("valid sequence"),
            }
        );
    }

    #[test]
    fn a_full_sequence_ends_on_its_own() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab(), ContextId::Global.default_predicate());

        for _ in 0..MAX_SEQUENCE_LENGTH - 1 {
            assert_eq!(
                press(&mut recorder, KeyChord::new("g", Modifiers::none())),
                RecordingOutcome::Captured
            );
        }

        let outcome = press(&mut recorder, KeyChord::new("g", Modifiers::none()));
        assert!(
            matches!(outcome, RecordingOutcome::Commit { keys, .. } if keys.chord_count() == MAX_SEQUENCE_LENGTH)
        );
    }

    #[test]
    fn escape_cancels_and_modifier_keys_wait() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab(), ContextId::Global.default_predicate());

        let modifier_only = press(&mut recorder, KeyChord::new("shift", Modifiers::shift()));
        assert_eq!(modifier_only, RecordingOutcome::Ignored);
        assert!(recorder.is_active());

        press(&mut recorder, KeyChord::new("k", Modifiers::ctrl()));
        let cancelled = press(&mut recorder, escape());
        assert_eq!(cancelled, RecordingOutcome::Cancelled);
        assert!(!recorder.is_active());
    }

    #[test]
    fn finishing_without_keys_keeps_recording() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab(), ContextId::Global.default_predicate());

        assert_eq!(finish(&mut recorder), RecordingOutcome::Ignored);
        assert!(recorder.is_active());
    }

    #[test]
    fn modified_escape_is_recorded() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab(), ContextId::Global.default_predicate());

        press(&mut recorder, KeyChord::new("escape", Modifiers::shift()));

        assert!(matches!(
            finish(&mut recorder),
            RecordingOutcome::Commit { .. }
        ));
    }

    #[test]
    fn taken_keys_wait_for_a_decision_then_replace() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab(), ContextId::Global.default_predicate());

        press(&mut recorder, KeyChord::new("w", Modifiers::ctrl()));
        assert_eq!(finish(&mut recorder), RecordingOutcome::ConflictFound);
        assert_eq!(
            recorder.state(),
            &RecordingState::Conflict {
                slot: new_query_tab(),
                keys: KeySequence::from(KeyChord::new("w", Modifiers::ctrl())),
                conflicts: vec![close_tab()],
            }
        );

        let other_key = press(&mut recorder, KeyChord::new("x", Modifiers::ctrl()));
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
    fn a_predicate_elsewhere_avoids_the_conflict() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab(), "Editor && vim_mode == normal");

        press(&mut recorder, KeyChord::new("w", Modifiers::ctrl()));

        assert!(matches!(
            finish(&mut recorder),
            RecordingOutcome::Commit { .. }
        ));
    }

    #[test]
    fn escape_cancels_a_pending_conflict() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab(), ContextId::Global.default_predicate());
        press(&mut recorder, KeyChord::new("w", Modifiers::ctrl()));
        finish(&mut recorder);

        let outcome = press(&mut recorder, escape());

        assert_eq!(outcome, RecordingOutcome::Cancelled);
        assert_eq!(recorder.confirm_replace(), None);
    }

    #[test]
    fn confirm_replace_keeps_a_running_recording() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab(), ContextId::Global.default_predicate());

        assert_eq!(recorder.confirm_replace(), None);
        assert!(matches!(
            recorder.state(),
            RecordingState::Recording { slot, .. } if *slot == new_query_tab()
        ));
    }
}
