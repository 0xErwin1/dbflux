//! Layout regression tests for modals rendered in `ModalShell`.
//!
//! A body that fits in the window must get the height it needs: the footer is
//! laid out after the body and never covers any of its content. A body taller
//! than the window scrolls while the footer stays whole and on screen.

use dbflux_components::modals::delete_connection::DELETE_CONNECTION_NAME_SELECTOR;
use dbflux_components::modals::shell::{MODAL_SHELL_BODY_SELECTOR, MODAL_SHELL_FOOTER_SELECTOR};
use dbflux_components::modals::{
    ActiveQueryRequest, ActiveQueryTrigger, CloseAction, DeleteConnectionRequest,
    DirtySummaryEntry, DropTableRequest, ModalActiveQuery, ModalDeleteConnection, ModalDropTable,
    ModalMutationConfirmHard, ModalShell, ModalUnsavedChanges, MutationConfirmHardRequest,
    UnsavedChangesRequest,
};
use dbflux_components::theme;
use dbflux_core::document_id::DocumentId;
use dbflux_core::{DefaultSqlDialect, RelationKind, RelationRef};
use gpui::prelude::*;
use gpui::{AnyView, App, Bounds, Context, Pixels, TestAppContext, Window, div, px};

struct Host {
    modal: AnyView,
}

impl Render for Host {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().size_full().child(self.modal.clone())
    }
}

/// Opens the modal built by `open` in a fresh window and returns the bounds of
/// each selector in `selectors`, in order.
fn render_modal(
    cx: &mut TestAppContext,
    open: impl FnOnce(&mut Window, &mut App) -> AnyView + 'static,
    selectors: &[&'static str],
) -> Vec<Bounds<Pixels>> {
    cx.update(theme::init);

    let (_, window) = cx.add_window_view(move |window, cx| Host {
        modal: open(window, cx),
    });
    window.run_until_parked();

    selectors
        .iter()
        .map(|selector| {
            window
                .debug_bounds(selector)
                .unwrap_or_else(|| panic!("{selector} should render"))
        })
        .collect()
}

fn assert_above_footer(name: &str, content: Bounds<Pixels>, footer: Bounds<Pixels>) {
    assert!(
        content.bottom() <= footer.top(),
        "{name} must end above the footer: {name} bottom {:?}, footer top {:?}",
        content.bottom(),
        footer.top()
    );
}

#[gpui::test]
fn delete_connection_name_box_is_not_covered_by_the_footer(cx: &mut TestAppContext) {
    let bounds = render_modal(
        cx,
        |_, cx| {
            let modal = cx.new(ModalDeleteConnection::new);
            modal.update(cx, |modal, cx| {
                modal.open(
                    DeleteConnectionRequest {
                        connection_name: "production-postgres".to_string(),
                        has_open_documents: true,
                    },
                    cx,
                );
            });
            modal.into()
        },
        &[
            DELETE_CONNECTION_NAME_SELECTOR,
            MODAL_SHELL_BODY_SELECTOR,
            MODAL_SHELL_FOOTER_SELECTOR,
        ],
    );

    assert_above_footer("the connection name box", bounds[0], bounds[2]);
    assert_above_footer("the body", bounds[1], bounds[2]);
}

#[gpui::test]
fn drop_table_body_is_not_covered_by_the_footer(cx: &mut TestAppContext) {
    let bounds = render_modal(
        cx,
        |window, cx| {
            let modal = cx.new(|cx| ModalDropTable::new(window, cx));
            modal.update(cx, |modal, cx| {
                modal.open(
                    DropTableRequest::new(
                        "orders".to_string(),
                        Some("public".to_string()),
                        vec![RelationRef {
                            kind: RelationKind::View,
                            qualified_name: "public.order_summary".to_string(),
                        }],
                        &DefaultSqlDialect,
                    ),
                    window,
                    cx,
                );
            });
            modal.into()
        },
        &[MODAL_SHELL_BODY_SELECTOR, MODAL_SHELL_FOOTER_SELECTOR],
    );

    assert_above_footer("the body", bounds[0], bounds[1]);
}

#[gpui::test]
fn unsaved_changes_body_is_not_covered_by_the_footer(cx: &mut TestAppContext) {
    let bounds = render_modal(
        cx,
        |_, cx| {
            let modal = cx.new(ModalUnsavedChanges::new);
            modal.update(cx, |modal, cx| {
                modal.open(
                    UnsavedChangesRequest {
                        entries: vec![
                            DirtySummaryEntry {
                                id: DocumentId::new(),
                                name: "report.sql".to_string(),
                                summary: "+3/-1 lines".to_string(),
                                action: CloseAction::Save,
                            },
                            DirtySummaryEntry {
                                id: DocumentId::new(),
                                name: "orders".to_string(),
                                summary: "2 staged edits".to_string(),
                                action: CloseAction::Apply,
                            },
                        ],
                    },
                    cx,
                );
            });
            modal.into()
        },
        &[MODAL_SHELL_BODY_SELECTOR, MODAL_SHELL_FOOTER_SELECTOR],
    );

    assert_above_footer("the body", bounds[0], bounds[1]);
}

#[gpui::test]
fn mutation_confirm_hard_body_is_not_covered_by_the_footer(cx: &mut TestAppContext) {
    let bounds = render_modal(
        cx,
        |window, cx| {
            let modal = cx.new(|cx| ModalMutationConfirmHard::new(window, cx));
            modal.update(cx, |modal, cx| {
                modal.open(
                    MutationConfirmHardRequest {
                        summary: "Delete every row in orders".to_string(),
                        type_to_confirm: "orders".to_string(),
                        sql_preview: "DELETE FROM \"orders\";".to_string(),
                        sample_rows: Some(vec![
                            vec!["1".to_string(), "pending".to_string()],
                            vec!["2".to_string(), "shipped".to_string()],
                        ]),
                        sample_columns: vec!["id".to_string(), "status".to_string()],
                        require_opt_in: true,
                    },
                    window,
                    cx,
                );
            });
            modal.into()
        },
        &[MODAL_SHELL_BODY_SELECTOR, MODAL_SHELL_FOOTER_SELECTOR],
    );

    assert_above_footer("the body", bounds[0], bounds[1]);
}

#[gpui::test]
fn active_query_preview_and_elapsed_line_are_not_covered_by_the_footer(cx: &mut TestAppContext) {
    let sql = (1..=12)
        .map(|line| format!("SELECT {line} AS step, pg_sleep(10) FROM generate_series(1, 1000);"))
        .collect::<Vec<_>>()
        .join("\n");

    let bounds = render_modal(
        cx,
        move |_, cx| {
            let modal = cx.new(ModalActiveQuery::new);
            modal.update(cx, |modal, cx| {
                modal.open(
                    ActiveQueryRequest {
                        sql,
                        trigger: ActiveQueryTrigger::Disconnect,
                    },
                    cx,
                );
            });
            modal.into()
        },
        &[MODAL_SHELL_BODY_SELECTOR, MODAL_SHELL_FOOTER_SELECTOR],
    );

    // The query preview and the "Running for" line are in-flow children of
    // the body, the line last, so both end above the footer when the body does.
    assert_above_footer("the body", bounds[0], bounds[1]);
}

struct TallBody;

impl Render for TallBody {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        let body = div()
            .flex()
            .flex_col()
            .children((0..40).map(|_| div().h(px(50.0)).flex_shrink_0().into_any_element()));
        let footer = div().h(px(28.0)).child("Confirm");

        ModalShell::new("Tall", body.into_any_element(), footer.into_any_element())
    }
}

#[gpui::test]
fn a_body_taller_than_the_window_scrolls_above_a_whole_footer(cx: &mut TestAppContext) {
    let bounds = render_modal(
        cx,
        |_, cx| cx.new(|_| TallBody).into(),
        &[MODAL_SHELL_BODY_SELECTOR, MODAL_SHELL_FOOTER_SELECTOR],
    );
    let (body, footer) = (bounds[0], bounds[1]);

    assert!(
        body.bottom() > footer.top(),
        "a 2000 px body must overflow the card and scroll: body bottom {:?}, footer top {:?}",
        body.bottom(),
        footer.top()
    );
    assert!(
        footer.size.height >= px(28.0),
        "the footer must keep its full height: {:?}",
        footer.size.height
    );
    assert!(
        footer.bottom() <= px(1080.0),
        "the footer must stay inside the window: footer bottom {:?}",
        footer.bottom()
    );
}
