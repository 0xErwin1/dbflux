use gpui::prelude::*;
use gpui::{App, Entity, FontWeight, IntoElement, KeyBinding, Window, actions};
use gpui_component::Sizable;

use crate::tokens::FontSizes;
use crate::typography::AppFonts;

pub use gpui_component::RopeExt;
pub use gpui_component::input::{
    CodeActionProvider, CompletionProvider, Enter as InputEnter, Escape as InputEscape,
    IndentInline as InputIndentInline, Input as GpuiInput, InputEvent, InputState,
    MoveDown as InputMoveDown, MoveUp as InputMoveUp, OutdentInline as InputOutdentInline,
    Position as InputPosition, Rope, Search as InputSearch,
};

actions!(
    dbflux_input,
    [
        /// Manually open the completion popover for the focused single-line
        /// input. Bound to `ctrl-space` by default.
        TriggerCompletion
    ]
);

/// Key context for `gpui-component`'s `InputState` element.
const INPUT_CONTEXT: &str = "Input";

/// Register DBFlux-specific keybindings that complement the defaults from
/// `gpui_component::init`. Call this once at app startup, after
/// `gpui_component::init`.
///
/// Adds vim-style `ctrl-j` / `ctrl-k` chords as aliases for `MoveDown` /
/// `MoveUp` inside any focused input. When a completion menu is open on an
/// `EditorState`, the engine routes these actions to the menu first, so the
/// chords navigate suggestions too.
pub fn register_input_overrides(cx: &mut App) {
    cx.bind_keys([
        KeyBinding::new("ctrl-j", InputMoveDown, Some(INPUT_CONTEXT)),
        KeyBinding::new("ctrl-k", InputMoveUp, Some(INPUT_CONTEXT)),
        KeyBinding::new("ctrl-space", TriggerCompletion, Some(INPUT_CONTEXT)),
    ]);
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
