//! The Raw view: the plan as the engine returned it (JSON re-indented), in the same
//! read-only code styling the app uses for JSON and XML, with line numbers.

use bolt::primitives::{CodeView, FieldWell};
use dbflux_components::tokens::Spacing;
use dbflux_core::PlanFormat;
use gpui::prelude::*;
use gpui::{AnyElement, Context, div};

use super::PlanView;
use super::ids::tagged;

/// The grammar that colors a raw plan of `format`, when there is one.
pub(super) fn raw_language(format: PlanFormat) -> Option<&'static str> {
    match format {
        PlanFormat::Json => Some("json"),
        PlanFormat::Xml => Some("xml"),
        PlanFormat::Text => None,
    }
}

impl PlanView {
    pub(super) fn render_raw(&self, cx: &mut Context<Self>) -> AnyElement {
        let Some(model) = self.model() else {
            return div().into_any_element();
        };

        let mut code = CodeView::new("plan-raw-code", model.raw.clone()).line_numbers(true);
        if let Some(language) = raw_language(model.raw_format) {
            code = code.language(language);
        }

        tagged("plan-raw")
            .font_family(dbflux_components::fonts::editor_family(cx))
            .size_full()
            .overflow_y_scroll()
            .p(Spacing::MD)
            .child(FieldWell::new().id("plan-raw-well").child(code))
            .into_any_element()
    }
}
