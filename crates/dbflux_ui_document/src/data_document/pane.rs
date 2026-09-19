//! `PaneHandle` constructor for `DataDocument`.
//!
//! `DataDocument::into_pane` converts a typed `Entity<DataDocument>` into the
//! type-erased `PaneHandle` shell. All closures capture the entity by clone;
//! `Window` and `App` are always passed as per-call parameters.

use super::DataDocument;
use crate::dedup::DocumentKey;
use crate::handle::DocumentEvent;
use crate::pane::{BoxedDocEventCallback, PaneHandle};
use crate::types::{DataSourceKind, DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use gpui::{App, Entity, IntoElement};

impl DataDocument {
    /// Wrap a typed `Entity<DataDocument>` in a `PaneHandle`.
    ///
    /// Reads the document ID synchronously from `cx` then seals all operations
    /// behind `Box<dyn Fn>` closures capturing `entity` by clone.
    ///
    /// The `matches_dedup_key` closure handles both `Table` and `Collection`
    /// keys, mirroring the former `is_table_with_database` and `is_collection`
    /// predicates in `DocumentHandle`.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();

        let mut handle = PaneHandle::new_chart(
            id,
            DocumentKind::Data,
            // render
            {
                let e = entity.clone();
                Box::new(move |_w, _cx| e.clone().into_any_element())
            },
            // focus
            {
                let e = entity.clone();
                Box::new(move |w, cx| e.update(cx, |d, cx| d.focus(w, cx)))
            },
            // dispatch_command
            {
                let e = entity.clone();
                Box::new(move |cmd, w, cx| e.update(cx, |d, cx| d.dispatch_command(cmd, w, cx)))
            },
            // meta_snapshot
            {
                let e = entity.clone();
                Box::new(move |cx| {
                    let d = e.read(cx);
                    let icon = match d.source_kind() {
                        DataSourceKind::Table => DocumentIcon::Table,
                        DataSourceKind::Collection => DocumentIcon::Collection,
                        DataSourceKind::QueryResult => DocumentIcon::Table,
                    };
                    DocumentMetaSnapshot {
                        id,
                        kind: DocumentKind::Data,
                        title: d.title(),
                        icon,
                        state: d.state(),
                        closable: true,
                        connection_id: d.connection_id(cx),
                    }
                })
            },
            // tab_title
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).title())
            },
            // can_close
            {
                let e = entity.clone();
                Box::new(move |_cx| e.read(_cx).can_close())
            },
            // connection_id
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).connection_id(cx))
            },
            // active_context
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).active_context(cx))
            },
            // change_summary
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).change_summary(cx))
            },
            // refresh_policy
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).refresh_policy(cx))
            },
            // flush_auto_save — DataDocument has no auto-save
            Box::new(|_cx| {}),
            // set_active_tab
            {
                let e = entity.clone();
                Box::new(move |active, cx| e.update(cx, |d, cx| d.set_active_tab(active, cx)))
            },
            // set_refresh_policy
            {
                let e = entity.clone();
                Box::new(move |policy, cx| e.update(cx, |d, cx| d.set_refresh_policy(policy, cx)))
            },
            // matches_dedup_key — handles Table and Collection variants
            {
                let e = entity.clone();
                Box::new(move |key, cx| {
                    let d = e.read(cx);
                    match key {
                        DocumentKey::Table {
                            profile_id,
                            database,
                            table,
                        } => {
                            d.connection_id(cx) == Some(*profile_id)
                                && d.table_ref(cx).as_ref() == Some(table)
                                && (database.is_none()
                                    || d.database(cx).as_deref() == database.as_deref())
                        }
                        DocumentKey::Collection {
                            profile_id,
                            collection,
                        } => {
                            d.connection_id(cx) == Some(*profile_id)
                                && d.collection_ref(cx).as_ref() == Some(collection)
                        }
                        _ => false,
                    }
                })
            },
            // subscribe — DataDocument emits DocumentEvent directly
            {
                let e = entity.clone();
                Box::new(move |cx, cb: BoxedDocEventCallback| {
                    cx.subscribe(&e, move |_, ev: &DocumentEvent, cx| cb(ev, cx))
                })
            },
        );

        handle.mark_inspector_closed = Some({
            let e = entity.clone();
            Box::new(move |cx| {
                e.update(cx, |d, cx| d.mark_inspector_closed(cx));
            })
        });

        handle.row_inspector_is_tracking = Some({
            let e = entity.clone();
            Box::new(move |cx| e.read(cx).row_inspector_is_tracking(cx))
        });

        handle.set_row_inspector_tracking = Some({
            let e = entity.clone();
            Box::new(move |tracking, cx| {
                e.update(cx, |d, cx| d.set_row_inspector_tracking(tracking, cx));
            })
        });

        handle.value_panel_is_open = Some({
            let e = entity.clone();
            Box::new(move |cx| e.read(cx).value_panel_is_open(cx))
        });

        handle.set_value_panel_open = Some({
            let e = entity.clone();
            Box::new(move |open, cx| {
                e.update(cx, |d, cx| d.set_value_panel_open(open, cx));
            })
        });

        // Populate optional helper: applying the grid's staged edits for an
        // interrupted close. The tab closes only once every staged edit landed,
        // which the grid reports back as `DocumentEvent::RequestClose`.
        handle.apply_for_close = Some({
            let grid = entity.read(cx).data_grid.clone();
            Box::new(move |_w, cx| grid.update(cx, |grid, cx| grid.apply_for_close(cx)))
        });

        handle
    }
}

#[cfg(test)]
mod tests {
    // Explicit imports rather than a glob: combining one with `#[gpui::test]`
    // sends the macro expansion into unbounded recursion.
    use super::*;
    use crate::code::CodeDocument;
    use crate::data_document::DataDocument;
    use dbflux_components::theme;
    use dbflux_core::{Pagination, QueryLanguage, TableRef};
    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_ui_base::AppStateEntity;
    use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
    use gpui::{AppContext, TestAppContext};
    use gpui_component::Root;
    use std::cell::RefCell;
    use std::rc::Rc;
    use uuid::Uuid;

    fn init_test_runtime(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);
        cx.update(theme::init);
        cx.update(|cx| {
            let host = cx.new(|_cx| ToastHost::new());
            cx.set_global(ToastGlobal { host });
        });
    }

    fn isolated_test_app_state(cx: &mut TestAppContext) -> gpui::Entity<AppStateEntity> {
        cx.update(|cx| {
            cx.new(|_| {
                let storage_runtime =
                    StorageRuntime::in_memory().expect("isolated storage runtime");
                AppStateEntity::new_with_storage_runtime(storage_runtime)
                    .expect("test storage setup")
            })
        })
    }

    /// A table tab's pending edits are applied to the database, so the dialog
    /// that asks about them says so — and, with nothing staged, the pane reports
    /// that there is nothing to wait on.
    #[gpui::test]
    fn a_table_pane_applies_its_pending_edits(cx: &mut TestAppContext) {
        init_test_runtime(cx);

        let app_state = isolated_test_app_state(cx);
        let holder: Rc<RefCell<Option<PaneHandle>>> = Rc::new(RefCell::new(None));
        let handle = holder.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let document = cx.new(|cx| {
                DataDocument::new_for_table(
                    Uuid::nil(),
                    TableRef::with_schema("public", "orders"),
                    Some("app".to_string()),
                    app_state.clone(),
                    window,
                    cx,
                )
            });

            handle.replace(Some(DataDocument::into_pane(document.clone(), cx)));
            Root::new(document, window, cx)
        });

        let pane = holder.borrow_mut().take().expect("the pane is built");

        assert_eq!(
            pane.close_action(),
            dbflux_components::modals::CloseAction::Apply,
            "a grid's pending edits are applied, not saved to a file"
        );
        assert!(
            !window.update(|window, cx| pane.apply_for_close(window, cx)),
            "a grid with nothing staged has no apply to wait on"
        );
    }

    /// A code document's pending edits are written to its own file, and the
    /// dialog keeps saying so.
    #[gpui::test]
    fn a_code_pane_saves_its_pending_edits(cx: &mut TestAppContext) {
        init_test_runtime(cx);

        let app_state = isolated_test_app_state(cx);
        let holder: Rc<RefCell<Option<PaneHandle>>> = Rc::new(RefCell::new(None));
        let handle = holder.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let document = cx.new(|cx| {
                CodeDocument::new_with_language(
                    app_state.clone(),
                    None,
                    QueryLanguage::Sql,
                    window,
                    cx,
                )
            });

            handle.replace(Some(CodeDocument::into_pane(document.clone(), cx)));
            Root::new(document, window, cx)
        });

        let pane = holder.borrow_mut().take().expect("the pane is built");

        assert_eq!(
            pane.close_action(),
            dbflux_components::modals::CloseAction::Save,
            "a script writes its own file, so its verb is save"
        );
        assert!(
            !window.update(|window, cx| pane.apply_for_close(window, cx)),
            "a script has no staged edits to apply"
        );
    }
}
