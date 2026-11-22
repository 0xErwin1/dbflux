use std::{cmp::Reverse, collections::BinaryHeap, rc::Rc};

use fuzzy_matcher::FuzzyMatcher as _;
use fuzzy_matcher::skim::SkimMatcherV2;
use gpui::{
    Action, AnyElement, App, AppContext, AvailableSpace, Context, DismissEvent, Empty, Entity,
    EventEmitter, FontStyle, Half as _, HighlightStyle, InteractiveElement as _, IntoElement,
    ParentElement, Pixels, Render, RenderOnce, SharedString, Styled, StyledText, Subscription,
    WeakEntity, Window, deferred, div, prelude::FluentBuilder, px, relative,
};
use gpui_base::TestSupportExt as _;
use lsp_types::{CompletionItem, CompletionItemKind};
use std::ops::Range;

const MAX_MENU_HEIGHT: Pixels = px(240.);
const POPOVER_GAP: Pixels = px(4.);
const MAX_WIDTH_CANDIDATES: usize = 32;

fn width_measurement_candidates(items: &[Rc<CompletionItem>]) -> Vec<usize> {
    let mut candidates = BinaryHeap::with_capacity(MAX_WIDTH_CANDIDATES + 1);
    for (index, item) in items.iter().enumerate() {
        let character_count = item.label.chars().count()
            + item
                .detail
                .as_deref()
                .map_or(0, |detail| detail.chars().count())
            + item_description(item).map_or(0, |description| description.chars().count());
        candidates.push(Reverse((character_count, index)));
        if candidates.len() > MAX_WIDTH_CANDIDATES {
            candidates.pop();
        }
    }
    let mut indices: Vec<_> = candidates
        .into_iter()
        .map(|Reverse((_, index))| index)
        .collect();
    indices.sort_unstable();
    indices
}

/// The right-aligned text of a row: the `description` of its label details.
fn item_description(item: &CompletionItem) -> Option<&str> {
    item.label_details
        .as_ref()
        .and_then(|details| details.description.as_deref())
        .filter(|description| !description.is_empty())
}

/// The highlight capture whose style colors a label of `kind`, or `None` for a
/// kind that keeps the plain style.
pub fn kind_highlight_capture(kind: Option<CompletionItemKind>) -> Option<&'static str> {
    let kind = kind?;
    if kind == CompletionItemKind::KEYWORD {
        Some("keyword")
    } else if kind == CompletionItemKind::FUNCTION
        || kind == CompletionItemKind::METHOD
        || kind == CompletionItemKind::CONSTRUCTOR
    {
        Some("function")
    } else if kind == CompletionItemKind::STRUCT
        || kind == CompletionItemKind::CLASS
        || kind == CompletionItemKind::INTERFACE
        || kind == CompletionItemKind::ENUM
    {
        Some("type")
    } else if kind == CompletionItemKind::MODULE {
        Some("namespace")
    } else if kind == CompletionItemKind::FIELD || kind == CompletionItemKind::PROPERTY {
        Some("field")
    } else if kind == CompletionItemKind::VARIABLE {
        Some("variable.alias")
    } else if kind == CompletionItemKind::OPERATOR {
        Some("operator")
    } else if kind == CompletionItemKind::CONSTANT || kind == CompletionItemKind::ENUM_MEMBER {
        Some("constant")
    } else {
        None
    }
}

/// The style the active highlight theme gives a label of `kind`; `None` keeps
/// the plain style, for an unmapped kind or a theme without that capture.
pub fn kind_label_style(
    kind: Option<CompletionItemKind>,
    colors: &SyntaxColors,
) -> Option<HighlightStyle> {
    colors.style(kind_highlight_capture(kind)?)
}

use crate::{
    ActiveTheme, IndexPath, Selectable, actions, h_flex,
    highlighter::SyntaxColors,
    input::{
        self, EditorState,
        popovers::{CaretAnchoredPopup, editor_popover, render_markdown},
    },
    label::Label,
    list::{List, ListDelegate, ListEvent, ListState},
};

struct ContextMenuDelegate {
    query: SharedString,
    menu: Entity<CompletionMenu>,
    items: Vec<Rc<CompletionItem>>,
    selected_ix: usize,
}

impl ContextMenuDelegate {
    fn set_items(&mut self, items: Vec<CompletionItem>) {
        self.items = items.into_iter().map(Rc::new).collect();
        self.selected_ix = 0;
    }

    fn selected_item(&self) -> Option<&Rc<CompletionItem>> {
        self.items.get(self.selected_ix)
    }
}

#[derive(IntoElement)]
struct CompletionMenuItem {
    ix: usize,
    item: Rc<CompletionItem>,
    children: Vec<AnyElement>,
    selected: bool,
    highlight_prefix: SharedString,
}

impl CompletionMenuItem {
    fn new(ix: usize, item: Rc<CompletionItem>) -> Self {
        Self {
            ix,
            item,
            children: vec![],
            selected: false,
            highlight_prefix: "".into(),
        }
    }

    fn highlight_prefix(mut self, s: impl Into<SharedString>) -> Self {
        self.highlight_prefix = s.into();
        self
    }
}
impl Selectable for CompletionMenuItem {
    fn selected(mut self, selected: bool) -> Self {
        self.selected = selected;
        self
    }

    fn is_selected(&self) -> bool {
        self.selected
    }
}

impl ParentElement for CompletionMenuItem {
    fn extend(&mut self, elements: impl IntoIterator<Item = AnyElement>) {
        self.children.extend(elements);
    }
}
impl RenderOnce for CompletionMenuItem {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        let item = self.item;

        let deprecated = item.deprecated.unwrap_or(false);
        let query = item
            .filter_text
            .as_deref()
            .unwrap_or(self.highlight_prefix.as_ref());
        let highlight_style = HighlightStyle {
            color: Some(cx.theme().blue),
            ..Default::default()
        };
        let kind_style = kind_label_style(item.kind, &cx.theme().highlight_theme.style.syntax);
        let highlights: Vec<_> = fuzzy_match_ranges(&item.label, query)
            .into_iter()
            .map(|range| (range, highlight_style))
            .collect();

        h_flex()
            .id(self.ix)
            .gap_2()
            .p_1()
            .text_xs()
            .line_height(relative(1.))
            .rounded(cx.theme().radius.half())
            .when(item.deprecated.unwrap_or(false), |this| this.line_through())
            .hover(|this| this.bg(cx.theme().accent.opacity(0.8)))
            .when(self.selected, |this| {
                this.bg(cx.theme().tokens.accent)
                    .text_color(cx.theme().accent_foreground)
            })
            .child(
                div()
                    .when_some(kind_style, |this, style| {
                        this.when(!self.selected, |this| {
                            this.when_some(style.color, |this, color| this.text_color(color))
                        })
                        .when_some(style.font_weight, |this, weight| this.font_weight(weight))
                        .when(style.font_style == Some(FontStyle::Italic), |this| {
                            this.italic()
                        })
                    })
                    .child(StyledText::new(item.label.clone()).with_highlights(highlights)),
            )
            .when(item.detail.is_some(), |this| {
                this.child(
                    Label::new(item.detail.as_deref().unwrap_or("").to_string())
                        .text_color(cx.theme().muted_foreground)
                        .when(deprecated, |this| this.line_through())
                        .italic(),
                )
            })
            .when_some(item_description(&item), |this, description| {
                this.child(
                    div()
                        .ml_auto()
                        .when(!self.selected, |this| {
                            this.text_color(cx.theme().muted_foreground)
                        })
                        .child(description.to_string()),
                )
            })
            .children(self.children)
    }
}

/// Byte ranges of `label` that fuzzy-match `query` (fzf-style), merged into
/// contiguous runs for the match highlight.
fn fuzzy_match_ranges(label: &str, query: &str) -> Vec<Range<usize>> {
    if query.is_empty() {
        return Vec::new();
    }

    let matcher = SkimMatcherV2::default().ignore_case();
    let Some((_, char_indices)) = matcher.fuzzy_indices(label, query) else {
        return Vec::new();
    };

    let mut ranges: Vec<Range<usize>> = Vec::new();
    for (char_index, (byte_index, character)) in label.char_indices().enumerate() {
        if !char_indices.contains(&char_index) {
            continue;
        }

        let byte_end = byte_index + character.len_utf8();
        match ranges.last_mut() {
            Some(last) if last.end == byte_index => last.end = byte_end,
            _ => ranges.push(byte_index..byte_end),
        }
    }

    ranges
}

impl EventEmitter<DismissEvent> for ContextMenuDelegate {}

impl ListDelegate for ContextMenuDelegate {
    type Item = CompletionMenuItem;

    fn items_count(&self, _: usize, _: &gpui::App) -> usize {
        self.items.len()
    }

    fn render_item(
        &mut self,
        ix: crate::IndexPath,
        _: &mut Window,
        _: &mut Context<ListState<Self>>,
    ) -> Option<Self::Item> {
        let item = self.items.get(ix.row)?;
        Some(CompletionMenuItem::new(ix.row, item.clone()).highlight_prefix(self.query.clone()))
    }

    fn set_selected_index(
        &mut self,
        ix: Option<crate::IndexPath>,
        _: &mut Window,
        cx: &mut Context<ListState<Self>>,
    ) {
        self.selected_ix = ix.map(|i| i.row).unwrap_or(0);
        cx.notify();
    }

    fn confirm(&mut self, _: bool, window: &mut Window, cx: &mut Context<ListState<Self>>) {
        let Some(item) = self.selected_item() else {
            return;
        };

        self.menu.update(cx, |this, cx| {
            this.select_item(&item, window, cx);
        });
    }
}

/// A context menu for code completions and code actions.
pub struct CompletionMenu {
    offset: usize,
    editor: WeakEntity<EditorState>,
    list: Entity<ListState<ContextMenuDelegate>>,
    content_width: Pixels,
    open: bool,

    /// The offset of the first character that triggered the completion.
    pub(crate) trigger_start_offset: Option<usize>,
    query: SharedString,
    _subscriptions: Vec<Subscription>,
}

impl CompletionMenu {
    /// Creates a new `CompletionMenu` with the given offset and completion items.
    ///
    /// NOTE: This element should not call from EditorState::new, unless that will stack overflow.
    pub(crate) fn new(
        editor: Entity<EditorState>,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx| {
            let view = cx.entity();
            let menu = ContextMenuDelegate {
                query: SharedString::default(),
                menu: view,
                items: vec![],
                selected_ix: 0,
            };

            let list = cx.new(|cx| ListState::new(menu, window, cx));

            let _subscriptions =
                vec![
                    cx.subscribe(&list, |this: &mut Self, _, ev: &ListEvent, cx| {
                        match ev {
                            ListEvent::Confirm(_) => {
                                this.dismiss(cx);
                            }
                            _ => {}
                        }
                        cx.notify();
                    }),
                ];

            Self {
                offset: 0,
                editor: editor.downgrade(),
                list,
                content_width: px(120.),
                open: false,
                trigger_start_offset: None,
                query: SharedString::default(),
                _subscriptions,
            }
        })
    }

    fn select_item(&mut self, item: &CompletionItem, window: &mut Window, cx: &mut Context<Self>) {
        let item = item.clone();
        let range = self.trigger_start_offset.unwrap_or(self.offset)..self.offset;

        let editor = self.editor.clone();

        cx.spawn_in(window, async move |_, cx| {
            editor.update_in(cx, |editor, window, cx| {
                editor.insert_completion(&item, range, window, cx);
            })
        })
        .detach();

        self.dismiss(cx);
    }

    /// The highlighted item of an open menu.
    pub(crate) fn selected_item(&self, cx: &App) -> Option<CompletionItem> {
        if !self.open {
            return None;
        }
        self.list
            .read(cx)
            .delegate()
            .selected_item()
            .map(|item| CompletionItem::clone(item))
    }

    pub(crate) fn handle_action(
        &mut self,
        action: Box<dyn Action>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if !self.open {
            return false;
        }

        cx.propagate();
        if input::Enter::is_primary(&*action) {
            self.on_action_enter(window, cx);
        } else if action.partial_eq(&input::Escape) {
            self.on_action_escape(window, cx);
        } else if action.partial_eq(&input::MoveUp) {
            self.on_action_up(window, cx);
        } else if action.partial_eq(&input::MoveDown) {
            self.on_action_down(window, cx);
        } else {
            return false;
        }

        true
    }

    fn on_action_enter(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(item) = self.list.read(cx).delegate().selected_item().cloned() else {
            return;
        };
        self.select_item(&item, window, cx);
    }

    fn on_action_escape(&mut self, _: &mut Window, cx: &mut Context<Self>) {
        self.dismiss(cx);
    }

    fn on_action_up(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.list.update(cx, |this, cx| {
            this.on_action_select_prev(&actions::SelectUp, window, cx)
        });
    }

    fn on_action_down(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.list.update(cx, |this, cx| {
            this.on_action_select_next(&actions::SelectDown, window, cx)
        });
    }

    /// Synchronize a hidden editor menu without dismissing a newer response.
    pub(crate) fn hide(&mut self, cx: &mut Context<Self>) {
        self.open = false;
        self.trigger_start_offset = None;
        cx.notify();
    }

    /// Dismiss the menu in response to a user action.
    fn dismiss(&mut self, cx: &mut Context<Self>) {
        self.hide(cx);
        let editor = self.editor.clone();
        cx.spawn(async move |_, cx| {
            let _ = editor.update(cx, |editor, cx| editor.dismiss_completion_overlay(cx));
        })
        .detach();
    }

    /// Sets the trigger start offset if it is not already set.
    pub(crate) fn update_query(&mut self, start_offset: usize, query: impl Into<SharedString>) {
        if self.trigger_start_offset.is_none() {
            self.trigger_start_offset = Some(start_offset);
        }
        self.query = query.into();
    }

    pub(crate) fn show(
        &mut self,
        offset: usize,
        items: impl Into<Vec<CompletionItem>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let items = items.into();
        self.offset = offset;
        self.open = true;
        self.content_width = self.list.update(cx, |this, cx| {
            this.delegate_mut().query = self.query.clone();
            this.delegate_mut().set_items(items);
            // The virtual list measures at its previous viewport width, which can
            // keep a new response as narrow as an earlier, shorter one.
            let mut widest_index = 0;
            let mut content_width = px(120.);
            // Prefix matches can contain an entire schema. Character counts
            // shortlist likely widest rows without laying out thousands of items.
            // Measuring several candidates also handles proportional fonts better
            // than measuring only the single longest label.
            for index in width_measurement_candidates(&this.delegate().items) {
                let item = &this.delegate().items[index];
                let width = editor_popover("completion-menu-measure", cx)
                    .child(CompletionMenuItem::new(index, item.clone()))
                    .into_any_element()
                    .layout_as_root(
                        gpui::size(AvailableSpace::MaxContent, AvailableSpace::MaxContent),
                        window,
                        cx,
                    )
                    .width;
                if width > content_width {
                    content_width = width;
                    widest_index = index;
                }
            }
            this.set_selected_index(Some(IndexPath::new(0)), window, cx);
            this.set_item_to_measure_index(IndexPath::new(widest_index), window, cx);
            content_width
        });

        cx.notify();
    }
}

impl Render for CompletionMenu {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.open {
            return Empty.into_any_element();
        }

        if self.list.read(cx).delegate().items.is_empty() {
            self.open = false;
            return Empty.into_any_element();
        }

        let Some(editor) = self.editor.upgrade() else {
            return Empty.into_any_element();
        };
        // The anchor is resolved in the popup's deferred prepaint from the
        // input's current-frame geometry; the content below is built there.
        let configured_max = editor.read(cx).lsp().completion_menu.max_width;
        let content_width = self.content_width;
        let list = self.list.clone();
        let menu = cx.weak_entity();

        deferred(CaretAnchoredPopup::new(
            self.editor.clone(),
            move |anchor, window, cx| {
                // `anchor` is in window coordinates, from the current frame.
                let max_width = configured_max.min(window.bounds().size.width - anchor.x);
                let vertical_layout =
                    anchor.x + configured_max + POPOVER_GAP + configured_max + POPOVER_GAP
                        > window.bounds().size.width;

                let selected_documentation = list
                    .read(cx)
                    .delegate()
                    .selected_item()
                    .and_then(|item| item.documentation.clone());

                div()
                    .flex()
                    .flex_row()
                    .gap(POPOVER_GAP)
                    .items_start()
                    .when(vertical_layout, |this| this.flex_col())
                    .child(
                        editor_popover("completion-menu", cx)
                            .test_support()
                            .w(content_width.min(max_width))
                            .max_w(max_width)
                            .min_w(px(120.))
                            .child(List::new(&list).max_h(MAX_MENU_HEIGHT)),
                    )
                    .when_some(selected_documentation, |this, documentation| {
                        let mut doc = match documentation {
                            lsp_types::Documentation::String(s) => s.clone(),
                            lsp_types::Documentation::MarkupContent(mc) => mc.value.clone(),
                        };
                        if vertical_layout {
                            doc = doc.split("\n").next().unwrap_or_default().to_string();
                        }

                        this.child(
                            div().child(
                                editor_popover("completion-menu", cx)
                                    .w(configured_max)
                                    .px_2()
                                    .child(render_markdown("doc", doc, window, cx)),
                            ),
                        )
                    })
                    .on_mouse_down_out({
                        let menu = menu.clone();
                        move |_, _, cx: &mut App| {
                            if let Some(menu) = menu.upgrade() {
                                menu.update(cx, |menu, cx| menu.dismiss(cx));
                            }
                        }
                    })
                    .into_any_element()
            },
        ))
        .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn width_measurement_is_bounded_and_keeps_long_candidates_anywhere_in_the_list() {
        let mut items: Vec<_> = (0..10_000)
            .map(|index| {
                Rc::new(CompletionItem {
                    label: format!("table_{index}"),
                    ..Default::default()
                })
            })
            .collect();
        for index in [0, 5_000, 9_999] {
            items[index] = Rc::new(CompletionItem {
                label: "table_with_a_long_name".repeat(10),
                ..Default::default()
            });
        }
        let candidates = width_measurement_candidates(&items);
        assert_eq!(candidates.len(), MAX_WIDTH_CANDIDATES);
        for index in [0, 5_000, 9_999] {
            assert!(candidates.contains(&index));
        }
        assert_eq!(items.len(), 10_000, "all suggestions remain available");
        assert!(width_measurement_candidates(&[]).is_empty());
        assert_eq!(width_measurement_candidates(&items[..3]), vec![0, 1, 2]);
    }

    #[test]
    fn width_candidates_count_characters_and_include_details() {
        let mut items = vec![
            Rc::new(CompletionItem {
                label: "abcdefghij".into(),
                ..Default::default()
            });
            MAX_WIDTH_CANDIDATES
        ];
        items.push(Rc::new(CompletionItem {
            label: "短".repeat(9),
            ..Default::default()
        }));
        items.push(Rc::new(CompletionItem {
            label: "id".into(),
            detail: Some("a long column detail".into()),
            ..Default::default()
        }));
        let candidates = width_measurement_candidates(&items);
        assert!(
            !candidates.contains(&MAX_WIDTH_CANDIDATES),
            "UTF-8 bytes must not inflate character counts"
        );
        assert!(
            candidates.contains(&(MAX_WIDTH_CANDIDATES + 1)),
            "details contribute to the shortlist"
        );
    }

    #[test]
    fn width_candidates_include_the_description() {
        let mut items = vec![
            Rc::new(CompletionItem {
                label: "abcdefghij".into(),
                ..Default::default()
            });
            MAX_WIDTH_CANDIDATES
        ];
        items.push(Rc::new(CompletionItem {
            label: "id".into(),
            label_details: Some(lsp_types::CompletionItemLabelDetails {
                detail: None,
                description: Some("a very long kind word".into()),
            }),
            ..Default::default()
        }));
        assert!(
            width_measurement_candidates(&items).contains(&MAX_WIDTH_CANDIDATES),
            "the description contributes to the shortlist"
        );
    }

    #[test]
    fn every_mapped_kind_names_its_highlight_capture() {
        use lsp_types::CompletionItemKind as Kind;
        let expected = [
            (Kind::KEYWORD, "keyword"),
            (Kind::FUNCTION, "function"),
            (Kind::METHOD, "function"),
            (Kind::CONSTRUCTOR, "function"),
            (Kind::STRUCT, "type"),
            (Kind::CLASS, "type"),
            (Kind::INTERFACE, "type"),
            (Kind::ENUM, "type"),
            (Kind::MODULE, "namespace"),
            (Kind::FIELD, "field"),
            (Kind::PROPERTY, "field"),
            (Kind::VARIABLE, "variable.alias"),
            (Kind::OPERATOR, "operator"),
            (Kind::CONSTANT, "constant"),
            (Kind::ENUM_MEMBER, "constant"),
        ];
        for (kind, capture) in expected {
            assert_eq!(kind_highlight_capture(Some(kind)), Some(capture));
        }
    }

    #[test]
    fn unmapped_kinds_keep_the_plain_style() {
        use lsp_types::CompletionItemKind as Kind;
        assert_eq!(kind_highlight_capture(Some(Kind::VALUE)), None);
        assert_eq!(kind_highlight_capture(Some(Kind::TEXT)), None);
        assert_eq!(kind_highlight_capture(None), None);

        let colors = SyntaxColors::default();
        assert_eq!(kind_label_style(Some(Kind::VALUE), &colors), None);
        assert_eq!(kind_label_style(Some(Kind::KEYWORD), &colors), None);
    }

    #[test]
    fn a_mapped_kind_takes_the_theme_style_of_its_capture() {
        use lsp_types::CompletionItemKind as Kind;
        let colors: SyntaxColors = serde_json::from_str(
            r##"{"keyword":{"color":"#ff0000","font_weight":700},"type":{"color":"#00ff00"}}"##,
        )
        .expect("syntax colors");
        let keyword = kind_label_style(Some(Kind::KEYWORD), &colors).expect("keyword style");
        assert_eq!(keyword.font_weight, Some(gpui::FontWeight::BOLD));
        assert!(keyword.color.is_some());
        let table = kind_label_style(Some(Kind::STRUCT), &colors).expect("type style");
        assert_ne!(keyword.color, table.color);
        assert_eq!(kind_label_style(Some(Kind::FIELD), &colors), None);
    }

    #[gpui::test]
    fn hiding_an_old_menu_does_not_dismiss_a_new_completion_response(
        cx: &mut gpui::TestAppContext,
    ) {
        cx.update(crate::init);
        let window = cx.add_empty_window();
        let (editor, menu) = window.update(|window, cx| {
            let editor = cx.new(|cx| EditorState::new(window, cx).default_value("co"));
            let menu = CompletionMenu::new(editor.clone(), window, cx);
            let old_items = vec![CompletionItem {
                label: "const".into(),
                ..Default::default()
            }];
            editor.update(cx, |editor, cx| {
                editor.present_completion_items(0, "c", old_items.clone(), cx);
            });
            menu.update(cx, |menu, cx| {
                menu.update_query(0, "c");
                menu.show(1, old_items, window, cx);
                // The overlay observes the old request being hidden while a
                // replacement request is pending. Its deferred dismissal must
                // not act on the replacement response that arrives next.
                menu.hide(cx);
            });
            editor.update(cx, |editor, cx| {
                editor.present_completion_items(
                    0,
                    "co",
                    vec![CompletionItem {
                        label: "const".into(),
                        ..Default::default()
                    }],
                    cx,
                );
            });
            (editor, menu)
        });

        cx.run_until_parked();

        cx.read(|cx| {
            let completion = editor.read(cx).completion_menu_state();
            assert!(completion.open, "an old menu dismissed the new response");
            assert_eq!(completion.query, "co");
            assert_eq!(completion.items.len(), 1);
            assert_eq!(completion.items[0].label, "const");
        });
        drop(menu);
    }
}
