//! Snippet sessions in the query code editor.
//!
//! Inserting a snippet expands its body and starts a session: every tabstop
//! range is tracked with the editor's edit anchors, Tab and Shift+Tab walk the
//! fields, typing in a field updates the mirrors of the same index, and the
//! final stop (or Escape, a caret move out of the field, an edit elsewhere, an
//! undo of the insertion, or leaving Vim insert mode) ends it.
//!
//! The session lives in an `Rc<RefCell<..>>` shared with the editor's key
//! interceptor, which runs while the editor state is being updated and so
//! cannot reach the document entity.

use super::*;
use dbflux_core::ParsedSnippet;
use gpui_base::input::{EditAnchor, EditAnchorAffinity, EditorKey, RangeDecoration};
use gpui_component::input::RangeDecorationCollection;
use std::cell::RefCell;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_EDIT_GROUP: AtomicU64 = AtomicU64::new(0x534e_4950_0000_0000);

struct FieldRange {
    start: EditAnchor,
    end: EditAnchor,
}

struct Field {
    ranges: Vec<FieldRange>,
}

struct SnippetSession {
    /// Navigation order; the last field is the final stop.
    fields: Vec<Field>,
    current: usize,
    /// Undo group covering the insertion and every edit made in its fields.
    group: Option<u64>,
    /// Where each field's first range started, to tell an undone insertion
    /// (every field collapsed onto one offset) from a live one.
    initial_starts: Vec<usize>,
    /// The document and the current field as they were before the edit the
    /// next `Change` reports.
    before: Option<Before>,
}

/// The state the next edit is judged against.
struct Before {
    text: String,
    field: Vec<Range<usize>>,
}

/// The session slot and the frames drawn around the fields that are not
/// current. Cloning shares both.
#[derive(Clone)]
pub(crate) struct SnippetHooks {
    session: Rc<RefCell<Option<SnippetSession>>>,
    highlights: RangeDecorationCollection,
}

impl SnippetHooks {
    pub(super) fn install(
        state: &mut GpuiEditorState,
        offers: SnippetOffers,
        cx: &mut Context<GpuiEditorState>,
    ) -> Self {
        let hooks = Self {
            session: Rc::new(RefCell::new(None)),
            highlights: state.create_range_decorations_collection(Vec::new(), cx),
        };
        let for_keys = hooks.clone();
        state.set_key_interceptor(move |key, state, _window, cx| {
            for_keys.handle_key(key, state, cx)
        });
        let for_commits = hooks.clone();
        state.set_completion_commit_handler(move |item, range, state, window, cx| {
            let text = state.text().to_string();
            if let Some(parsed) = offers.take_committed(item, &text, range.clone()) {
                for_commits.start(state, &parsed, range, window, cx);
            }
        });
        hooks
    }

    pub(super) fn is_active(&self) -> bool {
        self.session.borrow().is_some()
    }

    /// Replaces `range` with the expanded snippet and starts the session.
    pub(super) fn start(
        &self,
        state: &mut GpuiEditorState,
        parsed: &ParsedSnippet,
        range: Range<usize>,
        window: &mut Window,
        cx: &mut Context<GpuiEditorState>,
    ) {
        self.end(state, cx);
        if !state.is_editable() {
            return;
        }

        let group_id = NEXT_EDIT_GROUP.fetch_add(1, Ordering::Relaxed);
        let group = state.begin_edit_group(group_id).then_some(group_id);
        let base = range.start;
        state.replace_ranges(&[(range, parsed.text().to_string())], window, cx);

        let session = SnippetSession::track(state, parsed, base, group);
        match session {
            Some(mut session) if session.fields.len() > 1 => {
                session.select_current(state, cx);
                session.remember(state);
                *self.session.borrow_mut() = Some(session);
                self.refresh_highlights(state, cx);
            }
            Some(session) => {
                session.place_final(state, cx);
                session.release(state);
            }
            None => {
                if let Some(group) = group {
                    state.request_end_edit_group(group);
                }
            }
        }
    }

    fn handle_key(
        &self,
        key: EditorKey,
        state: &mut GpuiEditorState,
        cx: &mut Context<GpuiEditorState>,
    ) -> bool {
        let Some(mut session) = self.session.borrow_mut().take() else {
            return false;
        };
        let inside = session.cursor_in_current(state);
        if !inside || key == EditorKey::Escape {
            session.release(state);
            self.refresh_highlights(state, cx);
            return false;
        }

        let finished = match key {
            EditorKey::Tab => session.advance(state, cx),
            _ => {
                session.current = session.current.saturating_sub(1);
                session.select_current(state, cx);
                false
            }
        };
        if finished {
            session.release(state);
        } else {
            session.remember(state);
            *self.session.borrow_mut() = Some(session);
        }
        self.refresh_highlights(state, cx);
        true
    }

    /// Runs after every `InputEvent::Change`: mirrors the edited field, or
    /// ends the session when the edit left it.
    pub(super) fn after_change(
        &self,
        state: &mut GpuiEditorState,
        window: &mut Window,
        cx: &mut Context<GpuiEditorState>,
    ) {
        let Some(mut session) = self.session.borrow_mut().take() else {
            return;
        };
        if !session.cursor_in_current(state)
            || !session.edit_stayed_in_field(state)
            || !session.is_coherent(state)
        {
            session.release(state);
            self.refresh_highlights(state, cx);
            return;
        }

        session.mirror_current(state, window, cx);
        session.remember(state);
        *self.session.borrow_mut() = Some(session);
        self.refresh_highlights(state, cx);
    }

    /// Runs after a click placed the caret.
    pub(super) fn after_caret_placed(
        &self,
        state: &mut GpuiEditorState,
        cx: &mut Context<GpuiEditorState>,
    ) {
        let outside = self
            .session
            .borrow()
            .as_ref()
            .is_some_and(|session| !session.cursor_in_current(state));
        if outside {
            self.end(state, cx);
        }
    }

    pub(super) fn end(&self, state: &mut GpuiEditorState, cx: &mut Context<GpuiEditorState>) {
        let session = self.session.borrow_mut().take();
        if let Some(session) = session {
            session.release(state);
            self.refresh_highlights(state, cx);
        }
    }

    /// Frames every field except the current one. Applied after the running
    /// editor update, because the collection updates the editor itself.
    fn refresh_highlights(&self, state: &GpuiEditorState, cx: &mut Context<GpuiEditorState>) {
        let color = cx.theme().muted_foreground;
        let frames: Vec<RangeDecoration> = self
            .session
            .borrow()
            .as_ref()
            .map(|session| session.inactive_ranges(state))
            .unwrap_or_default()
            .into_iter()
            .map(|range| RangeDecoration::new(range).with_color(color))
            .collect();

        let collection = self.highlights.clone();
        cx.defer(move |cx| collection.set(frames, cx));
    }
}

impl SnippetSession {
    fn track(
        state: &mut GpuiEditorState,
        parsed: &ParsedSnippet,
        base: usize,
        group: Option<u64>,
    ) -> Option<Self> {
        let mut session = Self {
            fields: Vec::new(),
            current: 0,
            group,
            initial_starts: Vec::new(),
            before: None,
        };

        for tabstop in parsed.tabstops() {
            let start_affinity = if tabstop.index == 0 {
                EditAnchorAffinity::Right
            } else {
                EditAnchorAffinity::Left
            };
            let mut field = Field { ranges: Vec::new() };
            for range in &tabstop.ranges {
                let start = state.create_edit_anchor(base + range.start, start_affinity);
                let end = state.create_edit_anchor(base + range.end, EditAnchorAffinity::Right);
                match (start, end) {
                    (Some(start), Some(end)) => field.ranges.push(FieldRange { start, end }),
                    (start, end) => {
                        for anchor in start.into_iter().chain(end) {
                            state.remove_edit_anchor(anchor);
                        }
                        session.fields.push(field);
                        session.remove_anchors(state);
                        return None;
                    }
                }
            }
            session
                .initial_starts
                .push(base + tabstop.ranges.first().map_or(0, |range| range.start));
            session.fields.push(field);
        }
        Some(session)
    }

    fn resolve(state: &GpuiEditorState, range: &FieldRange) -> Option<Range<usize>> {
        let start = state.resolve_edit_anchor(range.start)?;
        let end = state.resolve_edit_anchor(range.end)?;
        (start <= end).then_some(start..end)
    }

    fn resolved(&self, state: &GpuiEditorState, field: usize) -> Option<Vec<Range<usize>>> {
        self.fields
            .get(field)?
            .ranges
            .iter()
            .map(|range| Self::resolve(state, range))
            .collect()
    }

    fn cursor_in_current(&self, state: &GpuiEditorState) -> bool {
        let selection = state.selected_range();
        self.resolved(state, self.current).is_some_and(|ranges| {
            ranges
                .iter()
                .any(|range| range.start <= selection.start && selection.end <= range.end)
        })
    }

    /// Records the document and the current field, the baseline the next
    /// edit is judged against.
    fn remember(&mut self, state: &GpuiEditorState) {
        self.before = Some(Before {
            text: state.text().to_string(),
            field: self.resolved(state, self.current).unwrap_or_default(),
        });
    }

    /// Whether the edit that just happened touched nothing but one range of
    /// the current field, judged by the text outside that range. The anchors
    /// have already moved by now, so a selection that reaches into the field
    /// (`Lx` replaced by `z` over `L${1:x}`) would otherwise look like an edit
    /// of the field, and so would an undo that removes the inserted text.
    fn edit_stayed_in_field(&self, state: &GpuiEditorState) -> bool {
        let Some(before) = &self.before else {
            return true;
        };
        let now = state.text().to_string();
        now == before.text
            || before
                .field
                .iter()
                .any(|range| only_inside_changed(&before.text, &now, range))
    }

    /// False once an undo has folded every field onto one offset.
    fn is_coherent(&self, state: &GpuiEditorState) -> bool {
        let mut starts = Vec::with_capacity(self.fields.len());
        for field in 0..self.fields.len() {
            match self
                .resolved(state, field)
                .and_then(|ranges| ranges.first().cloned())
            {
                Some(range) => starts.push(range.start),
                None => return false,
            }
        }
        let spread =
            |offsets: &[usize]| offsets.iter().any(|offset| Some(offset) != offsets.first());
        let initially_spread = spread(&self.initial_starts);
        let collapsed = !spread(&starts);
        !(initially_spread && collapsed)
    }

    fn select_current(&self, state: &mut GpuiEditorState, cx: &mut Context<GpuiEditorState>) {
        if let Some(range) = self
            .resolved(state, self.current)
            .and_then(|ranges| ranges.into_iter().next())
        {
            state.set_selected_range(range, cx);
        }
    }

    fn place_final(&self, state: &mut GpuiEditorState, cx: &mut Context<GpuiEditorState>) {
        let last = self.fields.len().saturating_sub(1);
        if let Some(range) = self
            .resolved(state, last)
            .and_then(|ranges| ranges.into_iter().next())
        {
            state.set_selected_range(range.start..range.start, cx);
        }
    }

    /// Moves to the next field. Returns true when that was the final stop.
    fn advance(&mut self, state: &mut GpuiEditorState, cx: &mut Context<GpuiEditorState>) -> bool {
        if self.current + 2 >= self.fields.len() {
            self.place_final(state, cx);
            return true;
        }
        self.current += 1;
        self.select_current(state, cx);
        false
    }

    /// Copies the text of the range being edited into the other ranges of
    /// the current field, keeping the caret where it was in the source.
    fn mirror_current(
        &self,
        state: &mut GpuiEditorState,
        window: &mut Window,
        cx: &mut Context<GpuiEditorState>,
    ) {
        let typed = self.typed_text(state);
        let Some(ranges) = self.resolved(state, self.current).filter(|r| r.len() > 1) else {
            return;
        };
        let cursor = state.cursor();
        let source = ranges
            .iter()
            .position(|range| range.start <= cursor && cursor <= range.end)
            .unwrap_or(0);
        let Some(source_range) = ranges.get(source) else {
            return;
        };
        let text = state.text().slice(source_range.clone()).to_string();

        let edits: Vec<(Range<usize>, String)> = ranges
            .iter()
            .enumerate()
            .filter(|(index, range)| {
                *index != source
                    && state.text().slice((*range).clone()).to_string().as_str() != text.as_str()
            })
            .map(|(_, range)| (range.clone(), text.clone()))
            .collect();
        if edits.is_empty() {
            return;
        }

        let relative = cursor.saturating_sub(source_range.start);
        state.replace_ranges(&edits, window, cx);

        let moved = self
            .fields
            .get(self.current)
            .and_then(|field| field.ranges.get(source))
            .and_then(|range| state.resolve_edit_anchor(range.start));
        if let Some(start) = moved {
            state.set_selected_range(start + relative..start + relative, cx);
        }

        // Rewriting the mirrors dropped the completion request the keystroke
        // started; ask again for the buffer as it is now.
        if let Some(typed) = typed {
            state.retrigger_completion(&typed, window, cx);
        }
    }

    /// The text the last edit entered, ending at the caret: what the editor
    /// handed its completion provider.
    fn typed_text(&self, state: &GpuiEditorState) -> Option<String> {
        let before = self.before.as_ref()?;
        let now = state.text().to_string();
        let caret = state.cursor();
        let tail = now.len().checked_sub(caret)?;
        let old_end = before.text.len().checked_sub(tail)?;
        let common = before
            .text
            .bytes()
            .zip(now.bytes())
            .take_while(|(old, new)| old == new)
            .count();
        let start = common.min(old_end);
        now.get(start..caret).map(str::to_string)
    }

    fn inactive_ranges(&self, state: &GpuiEditorState) -> Vec<Range<usize>> {
        let mut frames = Vec::new();
        for field in 0..self.fields.len() {
            if field == self.current {
                continue;
            }
            if let Some(ranges) = self.resolved(state, field) {
                frames.extend(ranges.into_iter().filter(|range| !range.is_empty()));
            }
        }
        frames
    }

    fn remove_anchors(&self, state: &mut GpuiEditorState) {
        for range in self.fields.iter().flat_map(|field| &field.ranges) {
            state.remove_edit_anchor(range.start);
            state.remove_edit_anchor(range.end);
        }
    }

    /// Ends the session: drops its anchors and closes its undo group.
    fn release(self, state: &mut GpuiEditorState) {
        self.remove_anchors(state);
        if let Some(group) = self.group {
            state.request_end_edit_group(group);
        }
    }
}

/// Whether `new` differs from `old` only where `field` (a range of `old`)
/// was: the text before and after it is unchanged.
fn only_inside_changed(old: &str, new: &str, field: &Range<usize>) -> bool {
    let (Some(head), Some(tail)) = (old.get(..field.start), old.get(field.end..)) else {
        return false;
    };
    new.len() >= head.len() + tail.len() && new.starts_with(head) && new.ends_with(tail)
}

impl CodeDocument {
    /// Replaces `replace_range` with the expanded snippet and starts a
    /// tabstop session: the first field is selected, Tab and Shift+Tab move
    /// between fields, and the session ends on the final stop. Does nothing on
    /// a read-only document.
    pub fn insert_snippet(
        &mut self,
        parsed: &ParsedSnippet,
        replace_range: Range<usize>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.read_only {
            return;
        }
        let hooks = self.editor.snippet.clone();
        self.editor.input_state.update(cx, |state, cx| {
            let range = replace_range.start.min(state.text().len())
                ..replace_range.end.min(state.text().len());
            hooks.start(state, parsed, range, window, cx);
        });
    }

    /// Whether a snippet session is waiting for Tab.
    pub fn snippet_session_active(&self) -> bool {
        self.editor.snippet.is_active()
    }

    pub(super) fn snippet_after_change(&self, window: &mut Window, cx: &mut Context<Self>) {
        let hooks = self.editor.snippet.clone();
        self.editor
            .input_state
            .update(cx, |state, cx| hooks.after_change(state, window, cx));
    }

    pub(super) fn snippet_after_caret_placed(&self, cx: &mut Context<Self>) {
        let hooks = self.editor.snippet.clone();
        self.editor
            .input_state
            .update(cx, |state, cx| hooks.after_caret_placed(state, cx));
    }

    /// Leaving Vim insert mode ends the session: Tab in Normal mode must not
    /// walk fields.
    pub(super) fn snippet_follow_vim_mode(&self, cx: &mut Context<Self>) {
        let left_insert = self.vim_mode().is_some_and(|mode| mode != VimMode::Insert);
        if left_insert && self.editor.snippet.is_active() {
            let hooks = self.editor.snippet.clone();
            self.editor
                .input_state
                .update(cx, |state, cx| hooks.end(state, cx));
        }
    }
}

#[cfg(test)]
mod tests;
