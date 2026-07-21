use super::*;

impl Workspace {
    /// Opens (or focuses) the schema-diff & apply document for a connection or
    /// database node. Deduplicated by `(profile_id, database)`.
    ///
    /// The relational gate lives in the sidebar (it only emits
    /// `RequestSchemaDiff` for relational connections); this handler just
    /// resolves an existing tab or creates a new one.
    pub(in crate::ui::views::workspace) fn open_schema_diff(
        &mut self,
        profile_id: uuid::Uuid,
        database: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::document::DocumentKey;
        use dbflux_ui_document::schema_diff::SchemaDiffDocument;

        let key = DocumentKey::SchemaDiff {
            profile_id,
            database: database.clone(),
        };

        if let Some(id) = self.tab_manager.read(cx).find_by_key(&key, cx) {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(id, cx);
            });
            self.set_focus(crate::keymap::FocusTarget::Document, window, cx);
            return;
        }

        let app_state = self.app_state.clone();
        let doc = cx.new(|cx| SchemaDiffDocument::new(profile_id, database, app_state, window, cx));
        let pane = SchemaDiffDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.set_focus(crate::keymap::FocusTarget::Document, window, cx);
        Toast::info("Opened schema diff")
            .meta_right(now_hms())
            .push(cx);
    }
}
