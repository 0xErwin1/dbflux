//! `MarkdownView` — a document written in markdown, rendered in the interface
//! typography.
//!
//! The parser and the renderer are gpui-component's `TextView`; this module
//! gives it Bolt's type and color: the interface face for prose, the mono face
//! for a fenced code block, headings that step down from `FontSizes::TITLE`,
//! the document's
//! blocks `MarkdownMetrics::PARAGRAPH_GAP` apart, and links in the link role.
//! Headings, paragraphs, lists, inline code, fenced code blocks, links and
//! tables are drawn by that renderer; Bolt styles them rather than parsing
//! markdown again.
//!
//! ```ignore
//! MarkdownView::new("readme", source)
//!     .well(true)
//!     .aria_label("Request documentation")
//! ```
//!
//! The document is selectable by default, so a reader copies the text it
//! draws (`#` and `**` are gone) rather than the source. Links open with the
//! platform's opener, the renderer's own behavior.

use gpui::prelude::*;
use gpui::{
    App, ElementId, FontWeight, HighlightStyle, Pixels, Rems, Role, SharedString, StyleRefinement,
    Window, div,
};
use gpui_component::ActiveTheme;
use gpui_component::text::{TextView, TextViewStyle};

use crate::colors;
use crate::fonts;
use crate::primitives::Chamfer;
use crate::selector::id_selector;
use crate::tokens::{ChamferCut, FontSizes, MarkdownMetrics, Spacing};

/// The type a [`MarkdownView`] draws with, resolved from the tokens and the
/// active interface size so a product's test can read it without a render.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MarkdownTypography {
    /// Body copy. (`FontSizes::BASE`)
    pub body: Pixels,
    /// Code inside a fenced block. (`MarkdownMetrics::CODE_FONT`)
    pub code: Pixels,
    /// Heading sizes by level: `headings[0]` is an `h1`.
    pub headings: [Pixels; 6],
    /// Space between the document's blocks. (`MarkdownMetrics::PARAGRAPH_GAP`)
    pub paragraph_gap: Rems,
}

impl MarkdownTypography {
    fn resolve(cx: &App) -> Self {
        let size = |value| fonts::ui_px(cx, value);

        Self {
            body: size(FontSizes::BASE),
            code: size(MarkdownMetrics::CODE_FONT),
            headings: [
                size(FontSizes::TITLE),
                size(FontSizes::XL),
                size(FontSizes::LG),
                size(FontSizes::BASE),
                size(FontSizes::XS),
                size(FontSizes::LABEL),
            ],
            paragraph_gap: MarkdownMetrics::PARAGRAPH_GAP,
        }
    }
}

/// A markdown document drawn in the interface typography, on the pane's own
/// ground or inside a well.
///
/// The view is a group under its id, named by `.aria_label(..)` when it has
/// one. The document it draws is the element `<id>-body` (its own debug
/// selector), so a product's test finds the text without depending on the
/// renderer's ids.
#[derive(IntoElement)]
pub struct MarkdownView {
    id: SharedString,
    source: SharedString,
    selectable: bool,
    well: bool,
    aria_label: Option<SharedString>,
}

impl MarkdownView {
    /// The document `source` under `id`.
    pub fn new(id: impl Into<SharedString>, source: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            source: source.into(),
            selectable: true,
            well: false,
            aria_label: None,
        }
    }

    /// Whether the text can be selected with the mouse, `true` by default. A
    /// text still reaches assistive technology when it cannot be selected.
    pub fn selectable(mut self, selectable: bool) -> Self {
        self.selectable = selectable;
        self
    }

    /// Draws the document on the inset ground inside the line border, with
    /// `Spacing::MD` of side padding and `Spacing::SM` above and below, the
    /// way a read-only preview sits in a field. Without it the document sits
    /// on the pane's own ground.
    pub fn well(mut self, well: bool) -> Self {
        self.well = well;
        self
    }

    /// The name the view announces as a whole.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.aria_label = Some(label.into());
        self
    }

    /// The type the view draws with under the active interface size.
    pub fn typography(cx: &App) -> MarkdownTypography {
        MarkdownTypography::resolve(cx)
    }
}

impl RenderOnce for MarkdownView {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme().clone();
        let role = colors(cx);
        let typography = MarkdownTypography::resolve(cx);
        let view_name = self.id;
        let body_name = SharedString::from(format!("{view_name}-body"));
        let text_name = SharedString::from(format!("{view_name}-text"));
        let id = ElementId::Name(view_name);
        let body_id = ElementId::Name(body_name);

        let markdown = TextView::markdown(ElementId::Name(text_name), self.source)
            .selectable(self.selectable)
            .style(text_style(
                &typography,
                &theme,
                role,
                crate::fonts::editor_family(cx),
            ));
        let body = div()
            .debug_selector(id_selector(&body_id))
            .id(body_id)
            .font_family(crate::fonts::ui_family(cx))
            .text_size(typography.body)
            .text_color(role.foreground)
            .child(markdown);

        let frame = div()
            .relative()
            .when(self.well, |frame| {
                frame.px(Spacing::MD).py(Spacing::SM).child(
                    Chamfer::new(ChamferCut::CONTROL)
                        .fill(role.background)
                        .border(role.line),
                )
            })
            .child(body);

        div()
            .debug_selector(id_selector(&id))
            .id(id)
            .w_full()
            .role(Role::Group)
            .when_some(self.aria_label, |view, label| view.aria_label(label))
            .child(frame)
    }
}

/// The renderer's style folded onto Bolt's type and colors: the document's
/// blocks on the interface face, every heading a step of the token scale, a
/// fenced code block in the mono face on the raised fill, and links in the link
/// role.
fn text_style(
    typography: &MarkdownTypography,
    theme: &gpui_component::Theme,
    role: crate::Colors,
    code_family: SharedString,
) -> TextViewStyle {
    let headings = typography.headings;
    let code_block = StyleRefinement::default()
        .font_family(code_family)
        .text_size(typography.code)
        .p(MarkdownMetrics::CODE_BLOCK_PADDING)
        .bg(role.raised);
    // The theme puts a table's head on the inset ground; the header row of a
    // document's table belongs with the block on the raised fill.
    let table_head = StyleRefinement::default()
        .font_weight(FontWeight::SEMIBOLD)
        .bg(role.raised);
    let table_cell = StyleRefinement::default().px(Spacing::SM).py(Spacing::XXS);
    let inline_code = HighlightStyle {
        color: Some(role.strong),
        background_color: Some(role.raised),
        ..HighlightStyle::default()
    };

    let mut style = TextViewStyle::default()
        .paragraph_gap(typography.paragraph_gap)
        // The renderer resolves a heading's size from its level, so the closure
        // answers with the token of that level rather than a multiple of the
        // body size.
        .heading_font_size(move |level, _body| {
            let index = usize::from(level).clamp(1, headings.len()) - 1;

            headings.get(index).copied().unwrap_or(headings[0])
        })
        .code_block(code_block)
        .table_head(table_head)
        .table_cell(table_cell)
        .inline_code(inline_code);
    style.heading_base_font_size = typography.body;
    style.is_dark = theme.is_dark();

    style
}

#[cfg(test)]
mod tests;
