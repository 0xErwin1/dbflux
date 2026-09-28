//! Keyboard tests of the instance inspector (X6): the table keys, its
//! context menu with the row actions, the confirmation and the refresh.

use super::InspectorPanel;
use crate::keyboard_test_support::{KeymapHost, host_document, init_keyboard_runtime};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_core::{ColumnKind, ColumnMeta, InspectorRowAction, QueryResult, Value};
use dbflux_ui_base::AppStateEntity;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// An inspector showing two sessions, with a Kill row action, under the app
/// keymap with the keyboard on its table.
fn inspector_with_keyboard(
    cx: &mut TestAppContext,
) -> (
    Entity<KeymapHost<InspectorPanel>>,
    Entity<InspectorPanel>,
    &mut VisualTestContext,
) {
    init_keyboard_runtime(cx);

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let storage_runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                .expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage_runtime).expect("test storage setup")
        })
    });

    let (host, window) = host_document(
        cx,
        move |_window, cx| {
            cx.new(|cx| {
                let mut panel =
                    InspectorPanel::new(Uuid::new_v4(), "sessions".to_string(), app_state, cx);
                panel.show_result_for_test(
                    QueryResult::table(
                        vec![ColumnMeta {
                            name: "pid".to_string(),
                            type_name: "int4".to_string(),
                            kind: ColumnKind::Integer,
                            nullable: false,
                            is_primary_key: false,
                        }],
                        vec![vec![Value::Int(101)], vec![Value::Int(102)]],
                        None,
                        Duration::ZERO,
                    ),
                    cx,
                );
                panel
            })
        },
        |panel, cx| panel.active_context(cx),
        InspectorPanel::dispatch_command,
    );
    let panel = window.update(|_, cx| host.read(cx).document.clone());

    window.update(|window, cx| {
        let grid = panel
            .read(cx)
            .data_grid
            .clone()
            .expect("the result builds a table");
        grid.update(cx, |grid, _| {
            grid.set_row_action_provider(Arc::new(|_| {
                vec![InspectorRowAction {
                    id: "kill".to_string(),
                    label: "Kill session".to_string(),
                    description: None,
                    is_destructive: true,
                }]
            }));
        });
        panel.update(cx, |panel, cx| panel.focus(window, cx));
    });
    window.run_until_parked();

    (host, panel, window)
}

fn kill_confirm_open(panel: &Entity<InspectorPanel>, window: &mut VisualTestContext) -> bool {
    window.update(|_, cx| panel.read(cx).pending_kill_confirm.is_some())
}

/// `m` opens the table's context menu, whose row actions the keyboard
/// reaches: choosing Kill session asks for confirmation, and Escape
/// cancels it.
#[gpui::test]
fn m_reaches_the_row_actions_and_escape_cancels_the_confirmation(cx: &mut TestAppContext) {
    let (host, panel, window) = inspector_with_keyboard(cx);

    assert_eq!(
        window.update(|_, cx| panel.read(cx).active_context(cx)),
        ContextId::Results,
        "the inspector takes the table keys"
    );

    // Up from the first row wraps to the Toolbar trigger, then to the last
    // row action above it.
    for keys in ["j", "m", "k", "k", "enter"] {
        window.simulate_keystrokes(keys);
        window.run_until_parked();
    }
    assert!(
        kill_confirm_open(&panel, window),
        "the row action asks for confirmation: {:?}",
        window.update(|_, cx| host.read(cx).commands.clone())
    );

    window.simulate_keystrokes("escape");
    window.run_until_parked();
    assert!(!kill_confirm_open(&panel, window));
}

/// F5 fetches a fresh snapshot; without a connection the panel reports it.
#[gpui::test]
fn f5_refreshes_the_snapshot(cx: &mut TestAppContext) {
    let (host, panel, window) = inspector_with_keyboard(cx);

    window.simulate_keystrokes("f5");

    assert_eq!(
        window.update(|_, cx| host.read(cx).commands.clone()),
        vec![Command::RefreshSchema]
    );
    assert_eq!(
        window.update(|_, cx| panel.read(cx).state()),
        crate::types::DocumentState::Error,
        "the fetch ran and found no connection"
    );
}
