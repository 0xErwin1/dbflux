use gpui::prelude::*;
use gpui::{App, Entity, IntoElement, Window};

use crate::controls::{GpuiInput, InputState};

/// Readonly text/code view backed by the shared GPUI input widget.
#[derive(IntoElement)]
pub struct ReadonlyTextView {
    state: Entity<InputState>,
    appearance: bool,
    w_full: bool,
    h_full: bool,
}

impl ReadonlyTextView {
    pub fn new(state: &Entity<InputState>) -> Self {
        Self {
            state: state.clone(),
            appearance: false,
            w_full: false,
            h_full: false,
        }
    }

    pub fn appearance(mut self, appearance: bool) -> Self {
        self.appearance = appearance;
        self
    }

    pub fn w_full(mut self) -> Self {
        self.w_full = true;
        self
    }

    pub fn h_full(mut self) -> Self {
        self.h_full = true;
        self
    }
}

impl RenderOnce for ReadonlyTextView {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        let mut input = GpuiInput::new(&self.state)
            .appearance(self.appearance)
            .disabled(true);

        if self.w_full {
            input = input.w_full();
        }

        if self.h_full {
            input = input.h_full();
        }

        input
    }
}
