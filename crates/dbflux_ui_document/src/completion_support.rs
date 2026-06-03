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
    let key = label.to_uppercase();
    if !seen.insert(key) {
        return;
    }

    items.push(CompletionItem {
        label: label.to_string(),
        kind: Some(kind),
        insert_text_format: Some(InsertTextFormat::PLAIN_TEXT),
        filter_text: Some(filter_prefix.to_string()),
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
