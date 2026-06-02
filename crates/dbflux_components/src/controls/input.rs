use gpui::prelude::*;
use gpui::{App, Div, Entity, FontWeight, IntoElement, KeyBinding, Window, div};
use gpui_component::Sizable;

use crate::tokens::FontSizes;
use crate::typography::AppFonts;

pub use gpui_component::input::{
    CompletionProvider, Enter as InputEnter, Escape as InputEscape,
    IndentInline as InputIndentInline, Input as GpuiInput, InputEvent, InputState,
    MoveDown as InputMoveDown, MoveUp as InputMoveUp, OutdentInline as InputOutdentInline,
    Position as InputPosition, Rope,
};

/// Key context for `gpui-component`'s `InputState` element.
const INPUT_CONTEXT: &str = "Input";

/// Register DBFlux-specific keybindings that complement the defaults from
/// `gpui_component::init`. Call this once at app startup, after
/// `gpui_component::init`.
///
/// Adds vim-style `ctrl-j` / `ctrl-k` chords as aliases for `MoveDown` /
/// `MoveUp` inside any focused input. These dispatch the same actions
/// `gpui-component` already routes to the completion popover (via
/// `handle_action_for_context_menu`), so single-line inputs wrapped with
/// [`completion_input_keys_wrapper`] navigate suggestions with the chord too.
pub fn register_input_overrides(cx: &mut App) {
    cx.bind_keys([
        KeyBinding::new("ctrl-j", InputMoveDown, Some(INPUT_CONTEXT)),
        KeyBinding::new("ctrl-k", InputMoveUp, Some(INPUT_CONTEXT)),
    ]);
}

/// Build a focus-region wrapper that routes `MoveUp` / `MoveDown` actions to
/// the input's completion popover.
///
/// Why this exists: `gpui-component` only attaches `MoveUp` / `MoveDown`
/// listeners to `InputState` when `state.mode.is_multi_line()` is true.
/// Single-line inputs that opt into a `CompletionProvider` therefore see
/// arrow keys bubble past the input to whatever parent context owns them
/// (e.g. the data table), so the completion popover never receives them.
///
/// Wrap any single-line input that uses a `CompletionProvider` with this
/// helper to forward arrow keys to `handle_action_for_context_menu`. When
/// the popover is closed the listeners do not call `stop_propagation`, so
/// arrows continue to behave normally for the surrounding UI.
pub fn completion_input_keys_wrapper(state: &Entity<InputState>) -> Div {
    let state_up = state.clone();
    let state_down = state.clone();
    let state_tab = state.clone();
    let state_shift_tab = state.clone();

    div()
        .on_action(move |_: &InputMoveUp, window: &mut Window, cx: &mut App| {
            let handled = state_up.update(cx, |s, cx| {
                s.handle_action_for_context_menu(Box::new(InputMoveUp), window, cx)
            });
            if handled {
                cx.stop_propagation();
            }
        })
        .on_action(
            move |_: &InputMoveDown, window: &mut Window, cx: &mut App| {
                let handled = state_down.update(cx, |s, cx| {
                    s.handle_action_for_context_menu(Box::new(InputMoveDown), window, cx)
                });
                if handled {
                    cx.stop_propagation();
                }
            },
        )
        .on_action(
            move |_: &InputIndentInline, window: &mut Window, cx: &mut App| {
                let handled = state_tab.update(cx, |s, cx| {
                    s.handle_action_for_context_menu(
                        Box::new(InputEnter { secondary: false }),
                        window,
                        cx,
                    )
                });
                if handled {
                    cx.stop_propagation();
                }
            },
        )
        .on_action(
            move |_: &InputOutdentInline, window: &mut Window, cx: &mut App| {
                let handled = state_shift_tab.update(cx, |s, cx| {
                    s.handle_action_for_context_menu(Box::new(InputEscape), window, cx)
                });
                if handled {
                    cx.stop_propagation();
                }
            },
        )
}

/// Thin wrapper around `gpui_component::input::Input` that pre-applies
/// DBFlux design token defaults (height, size).
#[derive(IntoElement)]
pub struct Input {
    state: Entity<InputState>,
    small: bool,
    placeholder: Option<gpui::SharedString>,
    disabled: bool,
    w_full: bool,
    appearance: bool,
    cleanable: bool,
}

impl Input {
    pub fn new(state: &Entity<InputState>) -> Self {
        Self {
            state: state.clone(),
            small: false,
            placeholder: None,
            disabled: false,
            w_full: false,
            appearance: true,
            cleanable: false,
        }
    }

    pub fn small(mut self) -> Self {
        self.small = true;
        self
    }

    pub fn placeholder(mut self, text: impl Into<gpui::SharedString>) -> Self {
        self.placeholder = Some(text.into());
        self
    }

    pub fn disabled(mut self, disabled: bool) -> Self {
        self.disabled = disabled;
        self
    }

    pub fn w_full(mut self) -> Self {
        self.w_full = true;
        self
    }

    pub fn appearance(mut self, appearance: bool) -> Self {
        self.appearance = appearance;
        self
    }

    pub fn cleanable(mut self, cleanable: bool) -> Self {
        self.cleanable = cleanable;
        self
    }
}

impl RenderOnce for Input {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        let mut input = GpuiInput::new(&self.state)
            .appearance(self.appearance)
            .disabled(self.disabled)
            .font_family(AppFonts::BODY)
            .font_weight(FontWeight::MEDIUM)
            .text_size(if self.small {
                FontSizes::SM
            } else {
                FontSizes::BASE
            });

        if self.small {
            input = input.small();
        }

        if self.w_full {
            input = input.w_full();
        }

        if self.cleanable {
            input = input.cleanable(true);
        }

        input
    }
}
