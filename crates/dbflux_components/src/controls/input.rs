use gpui::prelude::*;
use gpui::{App, Entity, FontWeight, IntoElement, Window};
use gpui_component::Sizable;

use crate::tokens::FontSizes;
use crate::typography::AppFonts;

pub use gpui_component::input::{
    CompletionProvider, Input as GpuiInput, InputEvent, InputState, Position as InputPosition, Rope,
};

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
