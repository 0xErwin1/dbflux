use dbflux_components::tokens::{Borders, FontSizes};
use gpui::{Entity, FontWeight, Pixels, Styled as _, px};
use gpui_component::input::EditorState;
use lsp_types::{
    CompletionItem, CompletionItemKind, CompletionTextEdit, InsertTextFormat,
    Position as LspPosition, Range as LspRange, TextEdit,
};
use std::cmp::min;
use std::collections::HashSet;

pub(crate) fn byte_offset_to_lsp_position(source: &str, offset: usize) -> LspPosition {
    let before = &source[..offset];
    let line = before.matches('\n').count() as u32;
    let line_start = before.rfind('\n').map(|i| i + 1).unwrap_or(0);
    let character = source[line_start..offset].chars().count() as u32;

    LspPosition { line, character }
}

pub(crate) fn completion_replace_range(
    source: &str,
    prefix_start: usize,
    cursor: usize,
) -> LspRange {
    LspRange {
        start: byte_offset_to_lsp_position(source, prefix_start),
        end: byte_offset_to_lsp_position(source, cursor),
    }
}

pub(crate) fn push_completion_item(
    items: &mut Vec<CompletionItem>,
    seen: &mut HashSet<String>,
    label: &str,
    kind: CompletionItemKind,
    filter_prefix: &str,
    replace_range: LspRange,
) {
    push_completion_item_inner(items, seen, label, kind, filter_prefix, replace_range, None);
}

/// Like [`push_completion_item`], with an explicit rank group: lower groups
/// sort first in the completion menu (`sort_text` = `"<group>_<label>"`).
pub(crate) fn push_completion_item_ranked(
    items: &mut Vec<CompletionItem>,
    seen: &mut HashSet<String>,
    label: &str,
    kind: CompletionItemKind,
    filter_prefix: &str,
    replace_range: LspRange,
    rank_group: u8,
) {
    push_completion_item_inner(
        items,
        seen,
        label,
        kind,
        filter_prefix,
        replace_range,
        Some(rank_group),
    );
}

fn push_completion_item_inner(
    items: &mut Vec<CompletionItem>,
    seen: &mut HashSet<String>,
    label: &str,
    kind: CompletionItemKind,
    filter_prefix: &str,
    replace_range: LspRange,
    rank_group: Option<u8>,
) {
    let key = label.to_uppercase();
    if !seen.insert(key) {
        return;
    }

    items.push(CompletionItem {
        label: label.to_string(),
        kind: Some(kind),
        insert_text_format: Some(InsertTextFormat::PLAIN_TEXT),
        filter_text: Some(filter_prefix.to_string()),
        sort_text: rank_group.map(|group| format!("{}_{}", group, label.to_lowercase())),
        text_edit: Some(CompletionTextEdit::Edit(TextEdit {
            range: replace_range,
            new_text: label.to_string(),
        })),
        ..CompletionItem::default()
    });
}

pub(crate) fn normalize_identifier(value: &str) -> String {
    value.trim_matches('"').to_lowercase()
}

pub(crate) fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$'
}

pub(crate) fn scan_identifier_start(source: &str, end: usize) -> usize {
    let bytes = source.as_bytes();
    let mut start = end;

    while start > 0 {
        let idx = start - 1;
        if !is_identifier_byte(bytes[idx]) {
            break;
        }

        start -= 1;
    }

    start
}

pub(crate) fn extract_identifier_prefix(source: &str, cursor: usize) -> (usize, String) {
    let cursor = min(cursor, source.len());
    let prefix_start = scan_identifier_start(source, cursor);
    (prefix_start, source[prefix_start..cursor].to_string())
}

/// Creates an `EditorState` presented as a visually single-line field.
///
/// gpui-component 0.6.1 hosts the completion engine on `EditorState` only, so
/// inputs that opt into a `CompletionProvider` must use it even when they
/// render as one-row fields. This reproduces the old single-line `InputState`
/// contract: no gutter, no wrap, no editor chrome, and Enter submits instead
/// of inserting a newline (Shift+Enter still does, if the field is ever grown).
pub(crate) fn new_single_line_completion_state(
    window: &mut gpui::Window,
    cx: &mut gpui::Context<'_, EditorState>,
    placeholder: impl Into<gpui::SharedString>,
) -> EditorState {
    EditorState::new(window, cx)
        .line_number(false)
        .soft_wrap(false)
        .folding(false)
        .searchable(false)
        .submit_on_enter(true)
        .placeholder(placeholder)
}

/// Renders a single-line completion input as a one-row `Editor`.
///
/// Pairs with [`new_single_line_completion_state`]: same visual contract as
/// the old `.small()` single-line input (24 px, DBFlux body font) while the
/// underlying state is the `EditorState` the completion engine requires.
pub(crate) fn single_line_completion_editor(
    state: &Entity<EditorState>,
) -> gpui_component::input::Editor {
    // gpui-component builds every `Editor` frame as `Size::Medium` and gives a
    // multi-line code editor `Size::Medium::input_py()` of padding inside it.
    // The frame exposes no size setter, so in a `ROW_COMPACT` row that padding
    // pushes the single line of text past the bottom edge. A negative vertical
    // padding cancels all but the leading the line box needs, plus the frame's
    // border, which offsets the content box as well. That moves the content —
    // text, caret, selection — back to the middle of the row while the frame's
    // border and background stay on the row.
    //
    // The same negative padding grows the editor's hitbox past the visible row,
    // so a click just above or below the field still focuses it. The controls
    // sharing these rows sit beside the field, never over it, so the reachable
    // area stays inside the toolbar.
    let leading = (dbflux_components::tokens::Heights::ROW_COMPACT
        - FontSizes::SM * EDITOR_LINE_HEIGHT)
        / 2.0;

    gpui_component::input::Editor::new(state)
        .h(dbflux_components::tokens::Heights::ROW_COMPACT)
        .py(leading - EDITOR_INPUT_PADDING_Y - Borders::THIN)
        .font_family(dbflux_components::typography::AppFonts::BODY)
        .font_weight(gpui::FontWeight::MEDIUM)
        .text_size(FontSizes::SM)
}

/// The line height gpui-component's `Editor::render` applies, relative to the
/// font size.
const EDITOR_LINE_HEIGHT: f32 = 1.5;

/// The vertical padding gpui-component's `Size::Medium` input frame adds.
const EDITOR_INPUT_PADDING_Y: Pixels = px(8.0); // guardrail-allow: gpui-component's input padding, not a DBFlux token

#[cfg(test)]
mod single_line_editor_geometry_tests {
    use super::*;
    use gpui::prelude::*;
    use gpui::{Context, Render, TestAppContext, Window, div, px};

    struct GeometryHarness {
        state: Entity<EditorState>,
    }

    impl GeometryHarness {
        fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
            Self {
                state: cx.new(|cx| new_single_line_completion_state(window, cx, "e.g. id > 10")),
            }
        }
    }

    impl Render for GeometryHarness {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().size_full().flex().flex_col().child(
                div()
                    .id("completion-row")
                    .debug_selector(|| "completion-row".to_string())
                    .w(px(300.0))
                    .h(dbflux_components::tokens::Heights::ROW_COMPACT)
                    .child(single_line_completion_editor(&self.state)),
            )
        }
    }

    // The code editor's frame padding used to push the placeholder below the
    // row's bottom edge; the caret must sit inside the row and near its middle.
    #[gpui::test]
    fn single_line_editor_centers_its_text_in_a_compact_row(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);

        let state_holder = std::rc::Rc::new(std::cell::RefCell::new(None));
        let (_, window) = cx.add_window_view({
            let state_holder = state_holder.clone();
            move |window, cx| {
                let harness = cx.new(|cx| GeometryHarness::new(window, cx));
                state_holder.replace(Some(harness.read(cx).state.clone()));
                gpui_component::Root::new(harness, window, cx)
            }
        });

        let row = window
            .debug_bounds("completion-row")
            .expect("the row should render");
        let state = state_holder
            .borrow()
            .clone()
            .expect("the harness should build its editor state");

        window.update(|window, cx| {
            state.update(cx, |state, cx| state.focus(window, cx));
        });
        window.update(|_, _| {});

        let (caret, _) = window
            .update(|_, cx| state.read(cx).cursor_layout())
            .expect("a focused editor should lay out its caret");

        assert!(
            caret.origin.y >= row.origin.y,
            "caret {caret:?} starts above the row {row:?}"
        );
        assert!(
            caret.origin.y + caret.size.height <= row.origin.y + row.size.height,
            "caret {caret:?} overflows the row {row:?}"
        );

        let caret_center = caret.origin.y + caret.size.height / 2.0;
        let row_center = row.origin.y + row.size.height / 2.0;
        assert!(
            (f32::from(caret_center) - f32::from(row_center)).abs() <= 1.0,
            "caret center {caret_center:?} is not centered in the row {row:?}"
        );
    }
}
