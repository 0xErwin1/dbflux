use super::*;
use crate::input::EditorMode;
use std::ops::Range;

use lsp_types::{CompletionItem, Hover};

/// Undo group id of a Tab completion cycle. Host groups use small counters.
const COMPLETION_PREVIEW_GROUP: u64 = u64::MAX;

#[derive(Clone, Debug, Default)]
pub struct CompletionMenuState {
    pub open: bool,
    pub trigger_start_offset: Option<usize>,
    pub query: String,
    pub items: Vec<CompletionItem>,
    revision: u64,
}

impl CompletionMenuState {
    /// Bumped whenever the content changes.
    ///
    /// A renderer that mirrors this menu compares revisions to decide whether
    /// to rebuild, so it never has to compare the item list itself.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub(super) fn bump(&mut self) {
        self.revision = self.revision.wrapping_add(1);
    }
}

#[derive(Clone, Debug, Default)]
pub struct CodeActionMenuState {
    pub open: bool,
    pub items: Vec<CodeActionItem>,
    revision: u64,
}

impl CodeActionMenuState {
    /// Bumped whenever the content changes. See [`CompletionMenuState::revision`].
    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub(super) fn bump(&mut self) {
        self.revision = self.revision.wrapping_add(1);
    }
}

#[derive(Clone, Debug)]
pub struct HoverPopoverState {
    pub symbol_range: Range<usize>,
    pub hover: Hover,
}

pub(crate) type OverlayActionHandler = Rc<
    dyn Fn(
        InputOverlayKind,
        Box<dyn gpui::Action>,
        &mut Window,
        &mut Context<InputBaseState<EditorMode>>,
    ) -> bool,
>;

/// Reads the item the completion menu currently highlights.
pub(crate) type CompletionSelectionReader = Rc<dyn Fn(&App) -> Option<CompletionItem>>;

/// An item written into the editor while Tab cycles the completion menu.
#[derive(Clone, Debug)]
pub(crate) struct CompletionPreview {
    /// The inserted span, in current document offsets.
    pub(crate) range: Range<usize>,
    /// The text the inserted span replaced.
    pub(crate) original_text: String,
    /// The word being completed before the first preview.
    pub(crate) fallback_range: Range<usize>,
    /// Whether the cycle opened the undo group it records into.
    pub(crate) owns_group: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ContextMenuContent {
    pub(crate) completion: CompletionMenuState,
    pub(crate) code_action: CodeActionMenuState,
    pub(crate) completion_preview: Option<CompletionPreview>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputOverlayKind {
    Completion,
    CodeAction,
}

impl InputBaseState<EditorMode> {
    pub fn present_completion_items(
        &mut self,
        trigger_start_offset: usize,
        query: impl Into<String>,
        items: Vec<CompletionItem>,
        cx: &mut Context<Self>,
    ) {
        self.extras
            .context_menu_content
            .completion
            .trigger_start_offset = Some(trigger_start_offset);
        self.extras.context_menu_content.completion.query = query.into();
        self.extras.context_menu_content.completion.items = items;
        self.extras.context_menu_content.completion.open =
            !self.extras.context_menu_content.completion.items.is_empty();
        self.extras.context_menu_content.completion.bump();
        cx.notify();
    }

    pub fn present_code_actions(&mut self, items: Vec<CodeActionItem>, cx: &mut Context<Self>) {
        self.extras.context_menu_content.code_action.items = items;
        self.extras.context_menu_content.code_action.open = !self
            .extras
            .context_menu_content
            .code_action
            .items
            .is_empty();
        self.extras.context_menu_content.code_action.bump();
        cx.notify();
    }

    pub fn present_hover(
        &mut self,
        symbol_range: Range<usize>,
        hover: Hover,
        cx: &mut Context<Self>,
    ) {
        self.extras.hover_popover = Some(HoverPopoverState {
            symbol_range,
            hover,
        });
        cx.notify();
    }

    pub fn present_diagnostic(
        &mut self,
        diagnostic: crate::input::DiagnosticEntry,
        cx: &mut Context<Self>,
    ) {
        self.diagnostic_popover = Some(Rc::new(diagnostic));
        cx.notify();
    }

    pub fn clear_diagnostic_popover(&mut self, cx: &mut Context<Self>) {
        if self.diagnostic_popover.take().is_some() {
            cx.notify();
        }
    }

    pub fn route_overlay_action(
        &mut self,
        action: Box<dyn gpui::Action>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        self.handle_action_for_context_menu(action, window, cx)
    }

    pub fn set_overlay_action_handler(
        &mut self,
        handler: impl Fn(
            InputOverlayKind,
            Box<dyn gpui::Action>,
            &mut Window,
            &mut Context<InputBaseState<EditorMode>>,
        ) -> bool
        + 'static,
    ) {
        self.overlay_action_handler = Some(Rc::new(handler));
    }

    /// Lets the editor read the highlighted completion item synchronously,
    /// which Tab cycling writes into the document.
    pub fn set_completion_selection_reader(
        &mut self,
        reader: impl Fn(&App) -> Option<CompletionItem> + 'static,
    ) {
        self.extras.completion_selection_reader = Some(Rc::new(reader));
    }

    pub fn has_overlay_action_handler(&self) -> bool {
        self.overlay_action_handler.is_some()
    }

    pub fn dismiss_completion_overlay(&mut self, cx: &mut Context<Self>) {
        if !self.completion_inserting {
            self.commit_completion_preview();
        }
        if self.extras.context_menu_content.completion.open {
            self.extras.context_menu_content.completion.open = false;
            cx.notify();
        }
    }

    pub fn dismiss_code_action_overlay(&mut self, cx: &mut Context<Self>) {
        if self.extras.context_menu_content.code_action.open {
            self.extras.context_menu_content.code_action.open = false;
            cx.notify();
        }
    }

    pub fn insert_completion(
        &mut self,
        item: &CompletionItem,
        fallback_range: Range<usize>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.completion_inserting = true;
        // The item's ranges describe the document the menu was opened for.
        self.restore_completion_preview_text(window, cx);
        let (range, new_text) = self.resolve_completion_edit(item, fallback_range);
        let range = self.range_to_utf16(&range);
        self.replace_text_in_range_silent(Some(range), &new_text, window, cx);
        self.completion_inserting = false;
        self.commit_completion_preview();
        self.focus(window, cx);
    }

    /// The document range an item replaces and the text it writes there.
    fn resolve_completion_edit(
        &self,
        item: &CompletionItem,
        fallback_range: Range<usize>,
    ) -> (Range<usize>, String) {
        if let Some(edit) = item.text_edit.as_ref() {
            match edit {
                lsp_types::CompletionTextEdit::Edit(edit) => (
                    self.text.position_to_offset(&edit.range.start)
                        ..self.text.position_to_offset(&edit.range.end),
                    edit.new_text.clone(),
                ),
                lsp_types::CompletionTextEdit::InsertAndReplace(edit) => (
                    self.text.position_to_offset(&edit.replace.start)
                        ..self.text.position_to_offset(&edit.replace.end),
                    edit.new_text.clone(),
                ),
            }
        } else if let Some(insert_text) = item.insert_text.as_ref() {
            (fallback_range.end..fallback_range.end, insert_text.clone())
        } else {
            (fallback_range, item.label.clone())
        }
    }

    pub(crate) fn has_completion_preview(&self) -> bool {
        self.extras
            .context_menu_content
            .completion_preview
            .is_some()
    }

    /// Write the highlighted menu item into the document in place of the
    /// word being completed, replacing the previously previewed item.
    ///
    /// Returns false when no menu reports a highlighted item.
    pub(crate) fn preview_highlighted_completion(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(item) = self
            .extras
            .completion_selection_reader
            .clone()
            .and_then(|reader| reader(cx))
        else {
            return false;
        };

        self.completion_inserting = true;
        let previous = self.restore_completion_preview_text(window, cx);
        let (fallback_range, owns_group) = match previous {
            Some(preview) => (preview.fallback_range, preview.owns_group),
            None => {
                let cursor = self.cursor();
                let start = self
                    .extras
                    .context_menu_content
                    .completion
                    .trigger_start_offset
                    .filter(|&start| start <= cursor)
                    .unwrap_or(cursor);
                // When another group is active, the cycle joins it instead.
                (
                    start..cursor,
                    self.begin_edit_group(COMPLETION_PREVIEW_GROUP),
                )
            }
        };

        let (range, new_text) = self.resolve_completion_edit(&item, fallback_range.clone());
        let original_text = self.text.slice(range.clone()).to_string();
        let range_utf16 = self.range_to_utf16(&range);
        self.replace_text_in_range_silent(Some(range_utf16), &new_text, window, cx);
        self.completion_inserting = false;

        // Normalization can change the inserted length, so the caret, not
        // the item text, marks where the inserted span ends.
        let end = self.cursor().max(range.start);
        self.extras.context_menu_content.completion_preview = Some(CompletionPreview {
            range: range.start..end,
            original_text,
            fallback_range,
            owns_group,
        });
        // The edit hid the menu. Reopening without a revision bump keeps the
        // rendered menu and its highlight as they are.
        self.extras.context_menu_content.completion.open = true;
        cx.notify();
        true
    }

    /// Put back the text the active preview replaced and return the preview.
    ///
    /// The caller sets `completion_inserting`, so the edit does not end the cycle.
    fn restore_completion_preview_text(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<CompletionPreview> {
        let preview = self.extras.context_menu_content.completion_preview.take()?;
        let range_utf16 = self.range_to_utf16(&preview.range);
        self.replace_text_in_range_silent(Some(range_utf16), &preview.original_text, window, cx);
        Some(preview)
    }

    /// Undo the whole Tab cycle, leaving no history entry behind.
    pub(crate) fn revert_completion_preview(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.completion_inserting = true;
        let preview = self.restore_completion_preview_text(window, cx);
        self.completion_inserting = false;
        if preview.is_some_and(|preview| preview.owns_group)
            && !self.discard_edit_group(COMPLETION_PREVIEW_GROUP)
        {
            self.request_end_edit_group(COMPLETION_PREVIEW_GROUP);
        }
    }

    /// Keep the previewed item and end the Tab cycle's undo group.
    pub(crate) fn commit_completion_preview(&mut self) {
        let Some(preview) = self.extras.context_menu_content.completion_preview.take() else {
            return;
        };
        if preview.owns_group {
            self.request_end_edit_group(COMPLETION_PREVIEW_GROUP);
        }
    }

    #[doc(hidden)]
    pub fn completion_menu_state(&self) -> &CompletionMenuState {
        &self.extras.context_menu_content.completion
    }

    #[doc(hidden)]
    pub fn code_action_menu_state(&self) -> &CodeActionMenuState {
        &self.extras.context_menu_content.code_action
    }

    pub fn hover_popover(&self) -> Option<&HoverPopoverState> {
        self.extras.hover_popover.as_ref()
    }

    pub fn dismiss_lsp_overlays(&mut self, cx: &mut Context<Self>) {
        self.hide_context_menu(cx);
        self.clear_hover_state(cx);
    }
}
