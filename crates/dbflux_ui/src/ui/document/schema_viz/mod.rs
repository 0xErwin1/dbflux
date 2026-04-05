use gpui::prelude::*;
use gpui::*;

use crate::keymap::ContextId;
use crate::ui::document::types::{DocumentId, DocumentKind, DocumentState};

pub struct SchemaVizDocument {
    pub id: DocumentId,
    pub state: DocumentState,
    pub focus_handle: FocusHandle,
}

impl SchemaVizDocument {
    pub fn new(_window: &mut Window, cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();
        Self {
            id: DocumentId::new(),
            state: DocumentState::Loading,
            focus_handle,
        }
    }

    pub fn state(&self) -> DocumentState {
        self.state
    }

    pub fn focus(&mut self, window: &mut Window, _cx: &mut Context<Self>) {
        self.focus_handle.focus(window);
    }

    pub fn active_context(&self) -> ContextId {
        // TODO: SchemaVizDocument should have its own context in Batch C
        ContextId::Audit
    }
}

impl Render for SchemaVizDocument {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().child("Schema Diagram — loading...")
    }
}
