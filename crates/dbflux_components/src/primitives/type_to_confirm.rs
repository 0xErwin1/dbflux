//! `TypeToConfirm` — a confirmation input that gates actions on exact text match.
//!
//! The caller provides an `expected` string (e.g. a connection name or "DELETE").
//! The user must type it exactly, including case and whitespace, before the
//! gate is considered open. No trimming is performed.

use gpui::prelude::*;
use gpui::{App, Context, Entity, EventEmitter, SharedString, Subscription, Window, div};
use gpui_component::ActiveTheme;

use crate::controls::{Input, InputEvent, InputState};
use crate::tokens::{FontSizes, Spacing};

// ---------------------------------------------------------------------------
// Pure comparison helper (also enables unit tests without GPUI context)
// ---------------------------------------------------------------------------

/// Return `true` iff `typed` equals `expected` exactly (case-sensitive, no trimming).
pub(crate) fn matches(typed: &str, expected: &str) -> bool {
    typed == expected
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/// Events emitted by [`TypeToConfirm`] when the typed text changes.
pub enum TypeToConfirmEvent {
    /// The typed text now exactly matches the expected string.
    Confirmed,
    /// The typed text no longer matches (was cleared or changed to non-matching).
    Cleared,
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

/// A confirmation input widget.
///
/// Owns an `InputState` internally. Subscribe to [`TypeToConfirmEvent`] to react
/// to confirmation/clearance transitions, or poll [`TypeToConfirm::is_confirmed`]
/// at render time.
pub struct TypeToConfirm {
    expected: SharedString,
    input: Entity<InputState>,
    confirmed: bool,
    _subscription: Subscription,
}

impl TypeToConfirm {
    pub fn new(
        expected: impl Into<SharedString>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let expected: SharedString = expected.into();
        let input = cx.new(|cx| InputState::new(window, cx));

        let subscription = {
            let expected_for_sub = expected.clone();
            cx.subscribe(&input, move |this, _input, event: &InputEvent, cx| {
                if !matches!(event, InputEvent::Change) {
                    return;
                }

                let typed = this.input.read(cx).value().to_string();
                let now_confirmed = matches(typed.as_str(), expected_for_sub.as_ref());

                let was_confirmed = this.confirmed;
                this.confirmed = now_confirmed;

                if now_confirmed && !was_confirmed {
                    cx.emit(TypeToConfirmEvent::Confirmed);
                } else if !now_confirmed && was_confirmed {
                    cx.emit(TypeToConfirmEvent::Cleared);
                }

                cx.notify();
            })
        };

        Self {
            expected,
            input,
            confirmed: false,
            _subscription: subscription,
        }
    }

    /// Return `true` iff the current text exactly equals the expected string.
    pub fn is_confirmed(&self, cx: &App) -> bool {
        let typed = self.input.read(cx).value().to_string();
        matches(typed.as_str(), self.expected.as_ref())
    }

    /// Return the text currently typed in the input.
    pub fn typed_text(&self, cx: &App) -> String {
        self.input.read(cx).value().to_string()
    }
}

impl EventEmitter<TypeToConfirmEvent> for TypeToConfirm {}

impl Render for TypeToConfirm {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let typed = self.input.read(cx).value().to_string();
        let is_empty = typed.is_empty();
        let confirmed = self.confirmed;

        let hint = if confirmed {
            Some(("matches", theme.success))
        } else if !is_empty {
            Some(("does not match", theme.muted_foreground))
        } else {
            None
        };

        let mut container = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Input::new(&self.input));

        if let Some((text, color)) = hint {
            container =
                container.child(div().text_size(FontSizes::XS).text_color(color).child(text));
        }

        container
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::matches;

    #[test]
    fn matches_exact_is_true() {
        assert!(matches("delete-my-db", "delete-my-db"));
    }

    #[test]
    fn matches_mismatch_is_false() {
        assert!(!matches("delete-my-DB", "delete-my-db"));
    }

    #[test]
    fn matches_empty_typed_is_false() {
        assert!(!matches("", "delete-my-db"));
    }

    #[test]
    fn matches_case_sensitive_false_on_case_diff() {
        assert!(!matches("DELETE", "delete"));
        assert!(!matches("delete", "DELETE"));
    }

    #[test]
    fn matches_leading_whitespace_is_false() {
        assert!(!matches(" delete", "delete"));
    }

    #[test]
    fn matches_trailing_whitespace_is_false() {
        assert!(!matches("delete ", "delete"));
    }

    #[test]
    fn matches_both_empty_is_true() {
        assert!(matches("", ""));
    }
}
