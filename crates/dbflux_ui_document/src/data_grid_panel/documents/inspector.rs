//! Document inspector: the side panel a document collection opens where a
//! table opens the row inspector (IslDocTable "Document" panel).
//!
//! The panel draws its whole island content: a header with the document size
//! and the expand and close buttons, then the document as a nested field tree
//! of `key : value` rows with the value colored by type and a short type chip
//! at the right. Objects and arrays expand and collapse in place; top-level
//! fields start expanded, deeper ones collapsed. Fields with a staged,
//! uncommitted grid edit show the staged value, highlighted like the edited
//! grid cell (IslDocTable). The workspace rail hosts it and owns the resize
//! grip.

use std::collections::HashSet;
use std::rc::Rc;

use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Icon;
use dbflux_components::tokens::{
    ChromeColors, CollectionMetrics, DocumentInspectorMetrics, SyntaxColors,
};
use dbflux_components::typography::AppFonts;
use dbflux_core::{Value, set_value_at_path, value_to_document_json};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

use super::columns::type_label;

/// Fields an object element of an array names in its one-line summary.
const SUMMARY_KEYS: usize = 3;

/// Longest scalar value a row shows before it is cut.
const VALUE_PREVIEW_CHARS: usize = 120;

/// A staged, uncommitted grid edit of one field of the shown document.
#[derive(Debug, Clone, PartialEq)]
pub enum PendingFieldEdit {
    /// The field takes this value on commit.
    Set(Value),
    /// The field is removed on commit.
    Unset,
}

/// Staged edits of the shown document, by field path.
pub type PendingFieldEdits = Vec<(Vec<String>, PendingFieldEdit)>;

/// The document the inspector shows, captured from the loaded page.
#[derive(Debug, Clone)]
pub struct DocumentInspectorSnapshot {
    /// Index of the document in the page.
    pub document_index: usize,
    pub document: Value,
    /// Field order of the page, used for the top-level fields.
    pub field_order: Vec<String>,
    /// Edits staged in the grid for this document, not yet committed.
    pub pending: PendingFieldEdits,
}

/// How a row's value is colored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValueTone {
    Text,
    Container,
    Literal,
    Muted,
}

/// One visible row of the field tree.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DocumentRow {
    pub path: Vec<String>,
    pub depth: usize,
    pub key: String,
    pub value: String,
    pub tone: ValueTone,
    pub type_label: &'static str,
    pub expandable: bool,
    pub expanded: bool,
    /// The row shows a staged, uncommitted edit.
    pub pending: bool,
}

/// Requests from the panel's buttons; the owning grid carries them out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocumentInspectorEvent {
    /// The close button: dismiss the panel.
    Close,
    /// The expand button: open the document in the JSON editor.
    Expand,
}

pub struct DocumentInspectorContent {
    snapshot: DocumentInspectorSnapshot,
    /// Paths whose expansion differs from the default (top level expanded,
    /// deeper levels collapsed). Kept across documents, so the same fields
    /// stay open while the selection moves.
    toggled: HashSet<Vec<String>>,
    rows: Rc<Vec<DocumentRow>>,
    size_label: String,
    focus_handle: FocusHandle,
    scroll_handle: UniformListScrollHandle,
}

impl EventEmitter<DocumentInspectorEvent> for DocumentInspectorContent {}

impl DocumentInspectorContent {
    pub fn new(snapshot: DocumentInspectorSnapshot, cx: &mut Context<Self>) -> Self {
        let toggled = HashSet::new();
        let rows = Rc::new(document_rows(
            &snapshot.document,
            &snapshot.field_order,
            &toggled,
            &snapshot.pending,
        ));
        let size_label = document_size_label(&snapshot.document);

        Self {
            snapshot,
            toggled,
            rows,
            size_label,
            focus_handle: cx.focus_handle(),
            scroll_handle: UniformListScrollHandle::new(),
        }
    }

    /// Show another document, keeping the expanded fields.
    pub fn open(&mut self, snapshot: DocumentInspectorSnapshot, cx: &mut Context<Self>) {
        self.size_label = document_size_label(&snapshot.document);
        self.snapshot = snapshot;
        self.rebuild_rows();
        cx.notify();
    }

    pub fn document_index(&self) -> usize {
        self.snapshot.document_index
    }

    /// Replaces the staged edits shown for the current document, when they
    /// changed.
    pub fn set_pending(&mut self, pending: PendingFieldEdits, cx: &mut Context<Self>) {
        if self.snapshot.pending == pending {
            return;
        }

        self.snapshot.pending = pending;
        self.rebuild_rows();
        cx.notify();
    }

    #[cfg(test)]
    pub(crate) fn rows(&self) -> &[DocumentRow] {
        &self.rows
    }

    /// Expand a collapsed object or array row, or collapse an expanded one.
    pub fn toggle(&mut self, path: &[String], cx: &mut Context<Self>) {
        if !self.toggled.remove(path) {
            self.toggled.insert(path.to_vec());
        }

        self.rebuild_rows();
        cx.notify();
    }

    fn rebuild_rows(&mut self) {
        self.rows = Rc::new(document_rows(
            &self.snapshot.document,
            &self.snapshot.field_order,
            &self.toggled,
            &self.snapshot.pending,
        ));
    }

    fn render_header(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(DocumentInspectorMetrics::HEADER_GAP)
            .h(DocumentInspectorMetrics::HEADER_HEIGHT)
            .px(DocumentInspectorMetrics::HEADER_PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .child(
                Icon::new(AppIcon::Braces)
                    .size(DocumentInspectorMetrics::HEADER_ICON)
                    .color(ChromeColors::tint(theme)),
            )
            .child(
                div()
                    .flex_shrink_0()
                    .font_weight(FontWeight::BOLD)
                    .text_color(ChromeColors::strong(theme))
                    .child(dbflux_i18n::t!("document.collection.inspector.title")),
            )
            .child(
                div()
                    .min_w_0()
                    .truncate()
                    .font_family(AppFonts::MONO)
                    .text_size(DocumentInspectorMetrics::SIZE_FONT)
                    .text_color(theme.muted_foreground)
                    .child(self.size_label.clone()),
            )
            .child(div().flex_1())
            .child(
                Button::new(
                    "document-inspector-expand",
                    dbflux_i18n::t!("document.collection.inspector.expand"),
                )
                .icon(AppIcon::Maximize2)
                .icon_only()
                .tab_stop(false)
                .on_click(cx.listener(|_, _, _, cx| {
                    cx.emit(DocumentInspectorEvent::Expand);
                })),
            )
            .child(
                Button::new(
                    "document-inspector-close",
                    dbflux_i18n::t!("document.data.row_inspector.action.close"),
                )
                .icon(AppIcon::CircleX)
                .icon_only()
                .tab_stop(false)
                .on_click(cx.listener(|_, _, _, cx| {
                    cx.emit(DocumentInspectorEvent::Close);
                })),
            )
            .into_any_element()
    }

    fn render_row(&self, row: &DocumentRow, cx: &mut Context<Self>) -> AnyElement {
        let literal = SyntaxColors::for_current(cx).number;
        let theme = cx.theme();
        let muted = theme.muted_foreground;

        let value_color = match row.tone {
            _ if row.pending => theme.warning,
            ValueTone::Text => theme.success,
            ValueTone::Container => theme.info,
            ValueTone::Literal => literal,
            ValueTone::Muted => muted,
        };

        let chevron = if row.expandable {
            Icon::new(if row.expanded {
                AppIcon::ChevronDown
            } else {
                AppIcon::ChevronRight
            })
            .size(DocumentInspectorMetrics::CHEVRON)
            .color(muted)
            .into_any_element()
        } else {
            div()
                .w(DocumentInspectorMetrics::CHEVRON)
                .flex_shrink_0()
                .into_any_element()
        };

        let indent = DocumentInspectorMetrics::ROW_PADDING_X
            + DocumentInspectorMetrics::INDENT * row.depth as f32;
        let path = row.path.clone();
        let hover = theme.secondary;
        let warning = theme.warning;

        div()
            .id(SharedString::from(format!(
                "document-inspector-row-{}",
                row.path.join("\u{1f}")
            )))
            .relative()
            .flex()
            .items_center()
            .h(DocumentInspectorMetrics::ROW_HEIGHT)
            .pl(indent)
            .pr(DocumentInspectorMetrics::ROW_PADDING_X)
            .when(row.pending, |line| {
                line.bg(warning.opacity(CollectionMetrics::EDITED_CELL_ALPHA))
                    .child(
                        div()
                            .absolute()
                            .left_0()
                            .top_0()
                            .bottom_0()
                            .w(DocumentInspectorMetrics::PENDING_EDGE)
                            .bg(warning),
                    )
            })
            .when(row.expandable, |line| {
                line.cursor_pointer()
                    .hover(move |line| line.bg(hover))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.toggle(&path, cx);
                    }))
            })
            .child(
                div()
                    .flex()
                    .flex_1()
                    .min_w_0()
                    .items_center()
                    .gap(DocumentInspectorMetrics::ROW_GAP)
                    .overflow_hidden()
                    .whitespace_nowrap()
                    .child(chevron)
                    .child(
                        div()
                            .flex_shrink_0()
                            .text_color(ChromeColors::strong(theme))
                            .child(row.key.clone()),
                    )
                    .child(div().flex_shrink_0().text_color(muted).child(":"))
                    .child(
                        div()
                            .min_w_0()
                            .truncate()
                            .text_color(value_color)
                            .child(row.value.clone()),
                    ),
            )
            .child(
                div()
                    .w(DocumentInspectorMetrics::TYPE_WIDTH)
                    .flex_shrink_0()
                    .flex()
                    .justify_end()
                    .text_size(DocumentInspectorMetrics::TYPE_FONT)
                    .text_color(muted)
                    .child(row.type_label),
            )
            .into_any_element()
    }
}

impl DocumentInspectorContent {
    /// Scroll the field list by a line, a page, or to either end.
    pub(crate) fn scroll(
        &self,
        step: crate::data_grid_panel::side_island::IslandScroll,
        cx: &mut Context<Self>,
    ) {
        let handle = self.scroll_handle.0.borrow().base_handle.clone();
        crate::data_grid_panel::side_island::scroll_by(&handle, step, cx);
        cx.notify();
    }
}

impl Focusable for DocumentInspectorContent {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for DocumentInspectorContent {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let header = self.render_header(cx);
        let rows = self.rows.clone();
        let entity = cx.entity();

        let list = uniform_list(
            "document-inspector-rows",
            rows.len(),
            move |range, _, cx| {
                entity.update(cx, |this, cx| {
                    range
                        .filter_map(|index| rows.get(index))
                        .map(|row| this.render_row(row, cx))
                        .collect()
                })
            },
        )
        .track_scroll(&self.scroll_handle)
        .flex_1()
        .min_h_0();

        div()
            .id("document-inspector-content")
            .size_full()
            .flex()
            .flex_col()
            .bg(cx.theme().popover)
            .track_focus(&self.focus_handle)
            .child(header)
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .flex()
                    .flex_col()
                    .py(DocumentInspectorMetrics::BODY_PADDING_Y)
                    .font_family(AppFonts::MONO)
                    .text_size(DocumentInspectorMetrics::ROW_FONT)
                    .child(list),
            )
    }
}

/// The visible rows of `document`: its fields, and the contents of every
/// expanded object or array below them. Top-level fields follow
/// `field_order`, nested fields their key order with `_id` first.
pub(crate) fn document_rows(
    document: &Value,
    field_order: &[String],
    toggled: &HashSet<Vec<String>>,
    pending: &[(Vec<String>, PendingFieldEdit)],
) -> Vec<DocumentRow> {
    let mut rows = Vec::new();

    // The staged values replace the loaded ones; an unset field keeps its
    // row, reading "missing", so the pending removal stays visible.
    let staged;
    let document = if pending.is_empty() {
        document
    } else {
        let mut copy = document.clone();
        for (path, edit) in pending {
            let value = match edit {
                PendingFieldEdit::Set(value) => value.clone(),
                PendingFieldEdit::Unset => Value::Null,
            };
            set_value_at_path(&mut copy, path, value);
        }
        staged = copy;
        &staged
    };

    let context = RowContext { toggled, pending };

    if let Value::Document(fields) = document {
        let mut keys: Vec<&String> = field_order
            .iter()
            .filter_map(|key| fields.get_key_value(key).map(|(key, _)| key))
            .collect();
        keys.extend(fields.keys().filter(|key| !field_order.contains(key)));

        for key in keys {
            if let Some(value) = fields.get(key) {
                push_rows(
                    &mut rows,
                    vec![key.clone()],
                    key.clone(),
                    value,
                    false,
                    &context,
                );
            }
        }
    }

    rows
}

/// What every row of one document is built with.
struct RowContext<'a> {
    toggled: &'a HashSet<Vec<String>>,
    pending: &'a [(Vec<String>, PendingFieldEdit)],
}

fn push_rows(
    rows: &mut Vec<DocumentRow>,
    path: Vec<String>,
    key: String,
    value: &Value,
    in_array: bool,
    context: &RowContext<'_>,
) {
    let depth = path.len() - 1;
    let pending_edit = context
        .pending
        .iter()
        .find(|(pending_path, _)| *pending_path == path)
        .map(|(_, edit)| edit);
    let unset = matches!(pending_edit, Some(PendingFieldEdit::Unset));

    let expandable = !unset
        && match value {
            Value::Document(fields) => !fields.is_empty(),
            Value::Array(items) => !items.is_empty(),
            _ => false,
        };
    let expanded_by_default = depth == 0;
    let expanded = expandable && (expanded_by_default != context.toggled.contains(&path));
    let (text, tone) = if unset {
        (
            dbflux_i18n::t!("document.collection.inspector.missing"),
            ValueTone::Muted,
        )
    } else {
        value_preview(value, in_array)
    };

    rows.push(DocumentRow {
        path: path.clone(),
        depth,
        key,
        value: text,
        tone,
        type_label: type_label(value),
        expandable,
        expanded,
        pending: pending_edit.is_some(),
    });

    if !expanded {
        return;
    }

    match value {
        Value::Document(fields) => {
            let mut keys: Vec<&String> = fields.keys().collect();
            if let Some(position) = keys.iter().position(|key| key.as_str() == "_id") {
                let id = keys.remove(position);
                keys.insert(0, id);
            }

            for child_key in keys {
                if let Some(child) = fields.get(child_key) {
                    let mut child_path = path.clone();
                    child_path.push(child_key.clone());
                    push_rows(rows, child_path, child_key.clone(), child, false, context);
                }
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                let mut child_path = path.clone();
                child_path.push(index.to_string());
                push_rows(rows, child_path, index.to_string(), item, true, context);
            }
        }
        _ => {}
    }
}

/// The value text of a row and its color: `{4}` for an object field,
/// `{ sku, qty, price }` for an object inside an array, `[3]` for an array,
/// quoted text, and every other scalar as the grid displays it.
fn value_preview(value: &Value, in_array: bool) -> (String, ValueTone) {
    match value {
        Value::Document(fields) if in_array => {
            let mut keys: Vec<&str> = fields
                .keys()
                .map(String::as_str)
                .take(SUMMARY_KEYS)
                .collect();
            if fields.len() > SUMMARY_KEYS {
                keys.push("\u{2026}");
            }
            (format!("{{ {} }}", keys.join(", ")), ValueTone::Container)
        }
        Value::Document(fields) => (format!("{{{}}}", fields.len()), ValueTone::Container),
        Value::Array(items) => (format!("[{}]", items.len()), ValueTone::Container),
        Value::Text(text) => (
            format!("\"{}\"", truncate_chars(text, VALUE_PREVIEW_CHARS)),
            ValueTone::Text,
        ),
        Value::Null => ("null".to_string(), ValueTone::Muted),
        Value::ObjectId(_) | Value::Bytes(_) | Value::Json(_) | Value::Unsupported(_) => (
            value.as_display_string_truncated(VALUE_PREVIEW_CHARS),
            ValueTone::Muted,
        ),
        _ => (
            value.as_display_string_truncated(VALUE_PREVIEW_CHARS),
            ValueTone::Literal,
        ),
    }
}

fn truncate_chars(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_string();
    }

    let mut cut: String = text.chars().take(limit).collect();
    cut.push('\u{2026}');
    cut
}

/// Size of the document as JSON, the header's "1.4 KiB".
fn document_size_label(document: &Value) -> String {
    let bytes = serde_json::to_vec(&value_to_document_json(document))
        .map(|encoded| encoded.len())
        .unwrap_or(0);

    crate::buckets_table::format_bytes(bytes as u64)
}

#[cfg(test)]
mod tests {
    use super::{PendingFieldEdit, ValueTone, document_rows};
    use dbflux_core::Value;
    use std::collections::{BTreeMap, HashSet};

    fn document(entries: &[(&str, Value)]) -> Value {
        Value::Document(
            entries
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect::<BTreeMap<_, _>>(),
        )
    }

    fn order() -> Vec<String> {
        ["_id", "customer", "items", "total"]
            .into_iter()
            .map(str::to_string)
            .collect()
    }

    fn orders_document() -> Value {
        document(&[
            ("_id", Value::ObjectId("66f0c2a1".to_string())),
            (
                "customer",
                document(&[
                    ("email", Value::Text("ana@northwind.io".to_string())),
                    ("tier", Value::Text("enterprise".to_string())),
                ]),
            ),
            (
                "items",
                Value::Array(vec![document(&[
                    ("sku", Value::Text("A".to_string())),
                    ("qty", Value::Int(2)),
                    ("price", Value::Float(9.5)),
                    ("note", Value::Null),
                ])]),
            ),
            ("total", Value::Float(1284.0)),
        ])
    }

    #[test]
    fn top_level_containers_open_and_nested_ones_stay_closed() {
        let rows = document_rows(&orders_document(), &order(), &HashSet::new(), &[]);
        let keys: Vec<(&str, usize)> = rows
            .iter()
            .map(|row| (row.key.as_str(), row.depth))
            .collect();

        assert_eq!(
            keys,
            [
                ("_id", 0),
                ("customer", 0),
                ("email", 1),
                ("tier", 1),
                ("items", 0),
                ("0", 1),
                ("total", 0),
            ]
        );

        let item = &rows[5];
        assert!(item.expandable && !item.expanded);
        assert_eq!(item.value, "{ note, price, qty, \u{2026} }");
        assert_eq!(item.type_label, "obj");
    }

    #[test]
    fn rows_carry_the_board_summaries_types_and_tones() {
        let rows = document_rows(&orders_document(), &order(), &HashSet::new(), &[]);

        assert_eq!(rows[0].type_label, "oid");
        assert_eq!(rows[0].tone, ValueTone::Muted);
        assert_eq!(rows[1].value, "{2}");
        assert_eq!(rows[1].tone, ValueTone::Container);
        assert_eq!(rows[2].value, "\"ana@northwind.io\"");
        assert_eq!(rows[2].type_label, "str");
        assert_eq!(rows[4].value, "[1]");
        assert_eq!(rows[4].type_label, "arr");
        assert_eq!(rows[6].type_label, "dbl");
        assert_eq!(rows[6].tone, ValueTone::Literal);
    }

    #[test]
    fn toggling_flips_the_default_expansion() {
        let mut toggled = HashSet::new();
        toggled.insert(vec!["customer".to_string()]);
        toggled.insert(vec!["items".to_string(), "0".to_string()]);

        let rows = document_rows(&orders_document(), &order(), &toggled, &[]);
        let keys: Vec<&str> = rows.iter().map(|row| row.key.as_str()).collect();

        assert_eq!(
            keys,
            [
                "_id", "customer", "items", "0", "note", "price", "qty", "sku", "total"
            ]
        );
        assert!(!rows[1].expanded);
        assert!(rows[3].expanded);
    }

    #[test]
    fn staged_edits_show_their_value_and_are_marked_pending() {
        let pending = vec![
            (
                vec!["customer".to_string(), "tier".to_string()],
                PendingFieldEdit::Set(Value::Text("team".to_string())),
            ),
            (vec!["total".to_string()], PendingFieldEdit::Unset),
        ];

        let rows = document_rows(&orders_document(), &order(), &HashSet::new(), &pending);
        let row = |key: &str| {
            rows.iter()
                .find(|row| row.key == key)
                .unwrap_or_else(|| panic!("{key} row"))
        };

        assert!(row("tier").pending);
        assert_eq!(row("tier").value, "\"team\"", "the staged value is shown");
        assert!(row("total").pending, "an unset field keeps a pending row");
        assert!(!row("total").expandable);
        assert!(!row("email").pending);
        assert!(!row("customer").pending, "only the edited field is marked");
    }

    #[gpui::test]
    fn expansion_survives_moving_to_another_document(cx: &mut gpui::TestAppContext) {
        use super::{DocumentInspectorContent, DocumentInspectorSnapshot};
        use gpui::AppContext as _;

        let snapshot = |document_index| DocumentInspectorSnapshot {
            document_index,
            document: orders_document(),
            field_order: order(),
            pending: Vec::new(),
        };

        let content = cx.update(|cx| cx.new(|cx| DocumentInspectorContent::new(snapshot(0), cx)));

        cx.update(|cx| {
            content.update(cx, |content, cx| {
                content.toggle(&["customer".to_string()], cx);
                content.open(snapshot(1), cx);
            });
        });

        cx.update(|cx| {
            let content = content.read(cx);
            assert_eq!(content.document_index(), 1);
            assert!(!content.rows()[1].expanded, "customer stays collapsed");
        });
    }
}
