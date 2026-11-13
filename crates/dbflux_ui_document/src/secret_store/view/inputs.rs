//! The read-only inputs that draw a retrieved value.

use super::SecretView;
use crate::secret_store::ValueState;
use gpui::{AppContext as _, Context, Window};

impl SecretView {
    /// Builds the inputs that draw a retrieved value, and keeps their masked
    /// flag on the reveal state. Each one is read-only and marked secret, so
    /// the value never reaches the accessibility tree or UI automation,
    /// whether it is masked or revealed on screen.
    pub(super) fn sync_value_inputs(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use dbflux_core::secrecy::ExposeSecret as _;

        let ValueState::Loaded(value) = &self.value else {
            return;
        };

        // The multi-line viewer draws the value itself, so no field input is
        // built for it: an input would hold a second plaintext copy of the
        // value that nothing reads.
        if value.is_multiline() {
            self.value_inputs.clear();
            self.value_input_masks.clear();
            self.value_inputs_view = None;
            return;
        }

        let field_count = value.field_count();

        if self.value_inputs_view != Some(value.view) || self.value_inputs.len() != field_count {
            let texts: Vec<String> = value
                .fields()
                .iter()
                .map(|(_, text)| text.expose_secret().to_string())
                .collect();

            self.value_inputs = texts
                .into_iter()
                .map(|text| cx.new(|cx| masked_value_state(text, window, cx)))
                .collect();
            self.value_input_masks = vec![true; field_count];
            self.value_inputs_view = Some(value.view);
        }

        let masks: Vec<bool> = (0..field_count)
            .map(|index| !value.is_revealed(index))
            .collect();

        for (index, masked) in masks.into_iter().enumerate() {
            if self.value_input_masks.get(index).copied() == Some(masked) {
                continue;
            }

            if let Some(input) = self.value_inputs.get(index) {
                input.update(cx, |input, cx| input.set_masked(masked, window, cx));
            }

            if let Some(mask) = self.value_input_masks.get_mut(index) {
                *mask = masked;
            }
        }
    }
}

/// The state behind one value field: masked until revealed.
fn masked_value_state(
    text: String,
    window: &mut Window,
    cx: &mut Context<dbflux_components::controls::InputState>,
) -> dbflux_components::controls::InputState {
    dbflux_components::controls::InputState::new(window, cx)
        .masked(true)
        .default_value(text)
}
