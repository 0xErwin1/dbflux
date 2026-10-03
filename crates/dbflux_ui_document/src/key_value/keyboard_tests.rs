//! Keyboard access to the key-value document: its dialogs keep Tab, its
//! `m` menu lists the toolbar and value panel buttons, and Alt+L / Alt+H
//! step the type filter and the expiry editor's mode.

use super::KeyValueDocument;
use super::context_menu::KvMenuAction;
use crate::keyboard_test_support::{KeymapHost, host_document, init_keyboard_runtime};
use crate::new_key_modal::ModalFocus;
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::components::form_navigation::FormNavigation as _;
use dbflux_core::{KeyType, KeyValueFeatures};
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};

/// A key-value document with no connection, hosted as the workspace hosts
/// it, with the keyboard on the document.
fn open(
    cx: &mut TestAppContext,
) -> (
    Entity<KeymapHost<KeyValueDocument>>,
    Entity<KeyValueDocument>,
    &mut VisualTestContext,
) {
    init_keyboard_runtime(cx);
    let app_state: Entity<dbflux_ui_base::AppStateEntity> = cx.update(|cx| {
        cx.new(|_| {
            let runtime =
                dbflux_storage::bootstrap::StorageRuntime::in_memory().expect("in-memory storage");
            dbflux_ui_base::AppStateEntity::new_with_storage_runtime(runtime)
                .expect("test storage setup")
        })
    });

    let (host, window) = host_document(
        cx,
        move |window, cx| {
            cx.new(|cx| {
                KeyValueDocument::new(uuid::Uuid::nil(), "0".to_string(), app_state, window, cx)
            })
        },
        |document, cx| document.active_context(cx),
        KeyValueDocument::dispatch_command,
    );
    let document = window.update(|_, cx| host.read(cx).document.clone());

    window.update(|window, cx| document.update(cx, |doc, cx| doc.focus(window, cx)));
    window.run_until_parked();

    (host, document, window)
}

fn keys(window: &mut VisualTestContext, keystrokes: &str) {
    for keystroke in keystrokes.split(' ') {
        window.simulate_keystrokes(keystroke);
        window.run_until_parked();
    }
}

/// Tab and Shift+Tab move through the New key dialog's fields and stay in
/// it: the document answers them instead of the workspace pane cycle.
#[gpui::test]
fn tab_moves_through_the_new_key_dialog(cx: &mut TestAppContext) {
    let (host, document, window) = open(cx);

    keys(window, "o");
    let modal = window.update(|_, cx| document.read(cx).new_key_modal.clone());
    assert!(window.update(|_, cx| modal.read(cx).is_visible()));
    assert_eq!(
        window.update(|_, cx| document.read(cx).active_context(cx)),
        ContextId::FormNavigation
    );
    assert_eq!(
        window.update(|_, cx| modal.read(cx).form_focus()),
        ModalFocus::KeyName
    );

    keys(window, "tab");
    assert_eq!(
        window.update(|_, cx| modal.read(cx).form_focus()),
        ModalFocus::Value,
        "Tab moves to the next field"
    );

    keys(window, "shift-tab");
    assert_eq!(
        window.update(|_, cx| modal.read(cx).form_focus()),
        ModalFocus::KeyName,
        "Shift+Tab moves back"
    );

    let handled_by_document = window.update(|window, cx| {
        document.update(cx, |doc, cx| {
            doc.dispatch_command(Command::CycleFocusForward, window, cx)
        })
    });
    assert!(
        handled_by_document,
        "the document keeps Tab while the dialog is open"
    );
    assert!(window.update(|_, cx| modal.read(cx).is_visible()));
    assert!(
        window
            .update(|_, cx| host.read(cx).commands.clone())
            .contains(&Command::CycleFocusForward),
        "Tab resolves to the form's cycle command"
    );
}

/// The key menu ends with the toolbar's layout switch and auto-refresh
/// interval, and running the layout entry switches the key list.
#[gpui::test]
fn the_key_menu_lists_the_toolbar_actions(cx: &mut TestAppContext) {
    let (_host, document, window) = open(cx);

    let actions: Vec<KvMenuAction> = window.update(|_, cx| {
        document
            .read(cx)
            .build_key_menu_items(cx)
            .into_iter()
            .map(|item| item.action)
            .collect()
    });
    assert!(actions.contains(&KvMenuAction::ToggleListLayout));
    assert!(actions.contains(&KvMenuAction::AutoRefresh));
    assert!(
        !actions.contains(&KvMenuAction::BulkDelete),
        "bulk delete needs a connection that supports it"
    );

    let layout_before = window.update(|_, cx| document.read(cx).list_layout);
    let index = actions
        .iter()
        .position(|action| *action == KvMenuAction::ToggleListLayout)
        .expect("layout entry");

    keys(window, "m");
    for _ in 0..index {
        keys(window, "j");
    }
    keys(window, "enter");

    let layout_after = window.update(|_, cx| document.read(cx).list_layout);
    assert_ne!(layout_after, layout_before, "the entry switches the layout");
}

/// Alt+L and Alt+H step the key type filter, All first, wrapping.
#[gpui::test]
fn alt_keys_step_the_type_filter(cx: &mut TestAppContext) {
    let (_host, document, window) = open(cx);
    window.update(|_, cx| {
        document.update(cx, |doc, _| {
            doc.key_features = KeyValueFeatures::SCAN_TYPE_FILTER;
        })
    });

    keys(window, "alt-l");
    assert_eq!(
        window.update(|_, cx| document.read(cx).type_filter),
        Some(KeyType::String),
        "Alt+L moves from All to the first type"
    );

    keys(window, "alt-h alt-h");
    assert_ne!(
        window.update(|_, cx| document.read(cx).type_filter),
        Some(KeyType::String),
        "Alt+H moves back past All to the last type"
    );
    assert!(window.update(|_, cx| document.read(cx).type_filter.is_some()));
}

/// The key list with keys, a selected string key and its value panel.
/// `the_key_menu_lists_the_toolbar_actions` proves `m` opens the menu.
#[gpui::test]
fn the_key_value_browser_is_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::KEY_VALUE;
    use dbflux_core::{KeyEntry, KeyGetResult, KeyLoadState, ValueRepr};
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};

    let (_host, document, window) = open(cx);
    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            let mut entry = KeyEntry::new("user:1");
            entry.key_type = Some(KeyType::String);
            document.keys = vec![
                entry.clone(),
                KeyEntry::new("user:2"),
                KeyEntry::new("jobs"),
            ];
            document.rebuild_key_rows();
            document.selected_index = Some(0);
            document.selected_value = Some(KeyGetResult {
                entry,
                value: b"hello".to_vec(),
                repr: ValueRepr::Text,
                load_state: KeyLoadState::Loaded,
            });
            cx.notify();
        })
    });
    window.run_until_parked();

    let menu: Vec<String> = window.update(|_, cx| {
        let document = document.read(cx);
        document
            .build_key_menu_items(cx)
            .into_iter()
            .chain(document.build_value_menu_items(cx))
            .map(|item| format!("{:?}", item.action))
            .collect()
    });

    let capture = FrameCapture::observe(window);
    let checked = Coverage::new(KEY_VALUE)
        .with_menu_entries(menu)
        .assert_covered(&capture.frame(window));
    assert!(
        checked.iter().any(|id| id == "kv-reload-value"),
        "{checked:?}"
    );
}

/// The value menu (M in the value panel) of a string value lists the View
/// as choices not shown and the decompression list: the first shows the
/// value as hex, the second opens the list with the keyboard in it.
#[gpui::test]
fn the_value_menu_switches_view_as_and_opens_the_decompression(cx: &mut TestAppContext) {
    use super::KeyValueFocusMode;
    use super::decode::ViewAs;
    use dbflux_core::{KeyEntry, KeyGetResult, KeyLoadState, ValueRepr};

    let (_host, document, window) = open(cx);
    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            let mut entry = KeyEntry::new("user:1");
            entry.key_type = Some(KeyType::String);
            document.keys = vec![entry.clone()];
            document.rebuild_key_rows();
            document.selected_index = Some(0);
            document.selected_value = Some(KeyGetResult {
                entry,
                value: b"hello".to_vec(),
                repr: ValueRepr::Text,
                load_state: KeyLoadState::Loaded,
            });
            document.focus_mode = KeyValueFocusMode::ValuePanel;
            cx.notify();
        })
    });
    window.run_until_parked();

    let run_entry = |action: KvMenuAction, window: &mut VisualTestContext| {
        let index = window
            .update(|_, cx| document.read(cx).build_value_menu_items(cx))
            .iter()
            .position(|item| item.action == action)
            .unwrap_or_else(|| panic!("the value menu lists {action:?}"));

        keys(window, "m");
        for _ in 0..index {
            keys(window, "j");
        }
        keys(window, "enter");
    };

    run_entry(KvMenuAction::ViewAs(ViewAs::Hex), window);
    assert_eq!(
        window.update(|_, cx| document.read(cx).value_view_as),
        ViewAs::Hex
    );
    assert!(
        !window
            .update(|_, cx| document.read(cx).build_value_menu_items(cx))
            .iter()
            .any(|item| item.action == KvMenuAction::ViewAs(ViewAs::Hex)),
        "the view shown is not listed"
    );

    run_entry(KvMenuAction::Decompression, window);
    assert!(
        window.update(|_, cx| document.read(cx).compression_dropdown.read(cx).is_open()),
        "the entry opens the decompression list"
    );
}

/// Asserts that every row named in `rows` starts where the header named
/// `header` starts and is as wide as it, so the row's cells sit under the
/// header's columns whatever the length of the row's text.
fn assert_rows_span_the_header(
    window: &mut VisualTestContext,
    header: &'static str,
    rows: &[&'static str],
) {
    let header_bounds = window
        .debug_bounds(header)
        .unwrap_or_else(|| panic!("{header} should render"));

    for row in rows {
        let row_bounds = window
            .debug_bounds(row)
            .unwrap_or_else(|| panic!("{row} should render"));

        assert_eq!(
            row_bounds.origin.x, header_bounds.origin.x,
            "{row} {row_bounds:?} starts where {header} {header_bounds:?} starts"
        );
        assert_eq!(
            row_bounds.size.width, header_bounds.size.width,
            "{row} {row_bounds:?} is as wide as {header} {header_bounds:?}"
        );
    }
}

/// Selects `entry` with `value` as its loaded value.
fn show_value(
    document: &mut KeyValueDocument,
    entry: dbflux_core::KeyEntry,
    value: Vec<u8>,
    repr: dbflux_core::ValueRepr,
) {
    document.keys = vec![entry.clone()];
    document.rebuild_key_rows();
    document.selected_index = Some(0);
    document.selected_value = Some(dbflux_core::KeyGetResult {
        entry,
        value,
        repr,
        load_state: dbflux_core::KeyLoadState::Loaded,
    });
}

/// Every sorted-set row spans the full width of the list, so its score and
/// relative bar line up with the header columns whatever the member name's
/// length.
#[gpui::test]
fn sorted_set_rows_span_the_header_width(cx: &mut TestAppContext) {
    use super::collection_panes::ZSetPane;
    use dbflux_core::{KeyEntry, RangeOrder, ValueRepr, ZSetMember};

    let (_host, document, window) = open(cx);
    let members = [
        "Studio Headphones",
        "Desk",
        "Mechanical Keyboard with Wrist Rest",
    ];

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            let mut entry = KeyEntry::new("leaderboard");
            entry.key_type = Some(KeyType::SortedSet);
            show_value(document, entry, Vec::new(), ValueRepr::Text);

            document.zset_pane = Some(ZSetPane {
                order: RangeOrder::Descending,
                members: members
                    .iter()
                    .enumerate()
                    .map(|(index, member)| ZSetMember {
                        member: member.to_string(),
                        score: 18_420.0 - index as f64 * 1_000.0,
                    })
                    .collect(),
                total: members.len() as u64,
                loading: false,
            });
            document.rebuild_cached_members(cx);
            cx.notify();
        })
    });
    window.run_until_parked();

    assert_rows_span_the_header(
        window,
        "kv-zset-header",
        &["kv-zset-row-0", "kv-zset-row-1", "kv-zset-row-2"],
    );
}

/// Every list element row spans the full width of the list, so its format
/// badge and delete button line up with the header columns.
#[gpui::test]
fn list_rows_span_the_header_width(cx: &mut TestAppContext) {
    use dbflux_core::{KeyEntry, ValueRepr};

    let (_host, document, window) = open(cx);

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            let mut entry = KeyEntry::new("queue");
            entry.key_type = Some(KeyType::List);
            show_value(
                document,
                entry,
                br#"["a","a much longer list element value"]"#.to_vec(),
                ValueRepr::Structured,
            );
            document.rebuild_cached_members(cx);
            cx.notify();
        })
    });
    window.run_until_parked();

    assert_rows_span_the_header(
        window,
        "kv-member-header",
        &["kv-member-row-0", "kv-member-row-1"],
    );
}

/// Every stream entry row spans the full width of the list, so its field
/// cells line up with the header columns.
#[gpui::test]
fn stream_rows_span_the_header_width(cx: &mut TestAppContext) {
    use dbflux_core::{KeyEntry, StreamEntry, ValueRepr};

    let (_host, document, window) = open(cx);

    window.update(|window, cx| {
        document.update(cx, |document, cx| {
            let mut entry = KeyEntry::new("events");
            entry.key_type = Some(KeyType::Stream);
            show_value(document, entry, Vec::new(), ValueRepr::Stream);

            document.load_ranged_key("events".to_string(), KeyType::Stream, Some(window), cx);
            let pane = document
                .stream_pane
                .as_mut()
                .expect("opening a stream key creates its pane");
            pane.entries = vec![
                StreamEntry {
                    id: "1700000000000-0".to_string(),
                    fields: vec![("event".to_string(), "x".to_string())],
                },
                StreamEntry {
                    id: "1700000000001-0".to_string(),
                    fields: vec![("event".to_string(), "a much longer field value".to_string())],
                },
            ];
            pane.total = 2;

            document.rebuild_cached_members(cx);
            cx.notify();
        })
    });
    window.run_until_parked();

    assert_rows_span_the_header(
        window,
        "kv-stream-header",
        &["kv-stream-row-0", "kv-stream-row-1"],
    );
}
