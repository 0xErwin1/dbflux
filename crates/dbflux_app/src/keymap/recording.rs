//! State machine behind the "record a new shortcut" interaction, over the
//! product's typed slots.
//!
//! The recorder engine lives in `bolt_keys` and holds the engine's
//! id-string slots; this module adapts them to `BindingSlot` and keeps a
//! typed mirror of the state so callers read `ContextId` and `Command`
//! values out of it.

use bolt_keys::{KeyChord, KeySequence};

use super::keymap_layer::KeymapStack;
use super::overrides::PredicateOverlap;
use super::overrides::{BindingSlot, KeymapOverrides};
use super::vocabulary::{command_for, context_for, vocabulary};

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
    engine: bolt_keys::KeybindingRecorder,
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
        let predicate = predicate.into();
        self.engine.start(slot.engine(), predicate.clone());
        self.state = RecordingState::Recording {
            slot,
            predicate,
            captured: Vec::new(),
        };
    }

    pub fn cancel(&mut self) {
        self.engine.cancel();
        self.state = RecordingState::Idle;
    }

    /// Feeds one key press to the recorder.
    ///
    /// A chord that fills the sequence to the engine's maximum length ends
    /// it at once, as [`KeybindingRecorder::finish`] would.
    pub fn handle_chord(
        &mut self,
        chord: KeyChord,
        defaults: &KeymapStack,
        overrides: &KeymapOverrides,
        overlap: &dyn PredicateOverlap,
    ) -> RecordingOutcome {
        let outcome = self.engine.handle_chord(
            chord.clone(),
            vocabulary(),
            defaults.engine(),
            overrides.engine(),
            overlap,
        );

        match outcome {
            bolt_keys::RecordingOutcome::Captured => {
                if let RecordingState::Recording { captured, .. } = &mut self.state {
                    captured.push(chord);
                }
            }
            bolt_keys::RecordingOutcome::Cancelled | bolt_keys::RecordingOutcome::Commit { .. } => {
                self.state = RecordingState::Idle;
            }
            bolt_keys::RecordingOutcome::ConflictFound => self.sync_conflict_state(),
            bolt_keys::RecordingOutcome::Ignored => {}
        }

        self.façade_outcome(outcome)
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
        let outcome =
            self.engine
                .finish(vocabulary(), defaults.engine(), overrides.engine(), overlap);

        match outcome {
            bolt_keys::RecordingOutcome::Commit { .. } => self.state = RecordingState::Idle,
            bolt_keys::RecordingOutcome::ConflictFound => self.sync_conflict_state(),
            bolt_keys::RecordingOutcome::Ignored
            | bolt_keys::RecordingOutcome::Captured
            | bolt_keys::RecordingOutcome::Cancelled => {}
        }

        self.façade_outcome(outcome)
    }

    /// The typed outcome for an engine outcome.
    fn façade_outcome(&self, outcome: bolt_keys::RecordingOutcome) -> RecordingOutcome {
        match outcome {
            bolt_keys::RecordingOutcome::Ignored => RecordingOutcome::Ignored,
            bolt_keys::RecordingOutcome::Captured => RecordingOutcome::Captured,
            bolt_keys::RecordingOutcome::Cancelled => RecordingOutcome::Cancelled,
            bolt_keys::RecordingOutcome::ConflictFound => RecordingOutcome::ConflictFound,
            bolt_keys::RecordingOutcome::Commit { slot, keys } => {
                let Some(slot) = typed_slot(&slot) else {
                    return RecordingOutcome::Ignored;
                };
                RecordingOutcome::Commit { slot, keys }
            }
        }
    }

    /// Accepts the pending conflict: returns the replacement to save and
    /// goes back to idle. Returns `None` when no conflict is pending.
    pub fn confirm_replace(&mut self) -> Option<Replacement> {
        let replacement = self.engine.confirm_replace()?;
        self.state = RecordingState::Idle;

        Some(Replacement {
            slot: typed_slot(&replacement.slot)?,
            keys: replacement.keys,
            replaced: replacement.replaced.iter().filter_map(typed_slot).collect(),
        })
    }

    /// Mirrors the engine's conflict state into the typed state.
    fn sync_conflict_state(&mut self) {
        let typed = match self.engine.state() {
            bolt_keys::RecordingState::Conflict {
                slot,
                keys,
                conflicts,
            } => {
                let conflicts: Vec<BindingSlot> = conflicts.iter().filter_map(typed_slot).collect();
                typed_slot(slot).map(|slot| RecordingState::Conflict {
                    slot,
                    keys: keys.clone(),
                    conflicts,
                })
            }
            _ => None,
        };

        if let Some(state) = typed {
            self.state = state;
        }
    }
}

/// The typed slot for an engine slot, or `None` (logged by the vocabulary
/// adapter) when an id does not name a product context or command.
fn typed_slot(slot: &bolt_keys::BindingSlot) -> Option<BindingSlot> {
    Some(BindingSlot {
        context: context_for(slot.context),
        command: command_for(&slot.command)?,
        default_keys: slot.default_keys.clone(),
        default_predicate: slot.default_predicate,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keymap::ContextTreeOverlap;
    use crate::keymap::{Command, ContextId, KeymapLayer};
    use bolt_keys::Modifiers;

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
    fn a_typed_recording_commits_conflicts_and_replacements() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab(), ContextId::Global.default_predicate());
        assert_eq!(recorder.target(), Some(&new_query_tab()));

        assert_eq!(
            press(&mut recorder, KeyChord::new("w", Modifiers::ctrl())),
            RecordingOutcome::Captured
        );
        assert_eq!(
            recorder.captured(),
            &[KeyChord::new("w", Modifiers::ctrl())]
        );
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
    fn a_free_sequence_commits_and_modifier_keys_wait() {
        let mut recorder = KeybindingRecorder::new();
        recorder.start(new_query_tab(), ContextId::Global.default_predicate());

        let modifier_only = press(&mut recorder, KeyChord::new("shift", Modifiers::shift()));
        assert_eq!(modifier_only, RecordingOutcome::Ignored);
        assert!(recorder.is_active());

        press(&mut recorder, KeyChord::new("k", Modifiers::ctrl()));
        press(&mut recorder, KeyChord::new("t", Modifiers::ctrl()));

        assert_eq!(
            finish(&mut recorder),
            RecordingOutcome::Commit {
                slot: new_query_tab(),
                keys: KeySequence::parse("ctrl+k ctrl+t").expect("valid sequence"),
            }
        );
        assert_eq!(recorder.state(), &RecordingState::Idle);

        recorder.start(new_query_tab(), ContextId::Global.default_predicate());
        let cancelled = press(&mut recorder, escape());
        assert_eq!(cancelled, RecordingOutcome::Cancelled);
        assert!(!recorder.is_active());
    }
}
