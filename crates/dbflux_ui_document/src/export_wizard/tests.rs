use gpui::{Pixels, px};

use super::SEGMENT_SIZE_INPUT_WIDTH;

fn rem_size(size: f32) -> Pixels {
    px(size) // guardrail-allow: a rem size under test, not layout spacing
}

#[test]
fn text_input_width_follows_the_ui_font_size() {
    assert_eq!(
        SEGMENT_SIZE_INPUT_WIDTH.to_pixels(rem_size(16.0)),
        rem_size(120.0)
    );
    assert_eq!(
        SEGMENT_SIZE_INPUT_WIDTH.to_pixels(rem_size(32.0)),
        rem_size(240.0)
    );
}

mod states {
    //! The Format, Tables and Run steps in the states the approved boards
    //! draw: each unavailable, blocked or failed state says why.

    use std::cell::RefCell;
    use std::path::PathBuf;
    use std::rc::Rc;

    use dbflux_core::TableRef;
    use dbflux_transfer::FileFormat;
    use dbflux_ui_base::app_state_entity::AppStateEntity;
    use gpui::{AppContext, Entity, TestAppContext, VisualTestContext, px};
    use uuid::Uuid;

    use super::super::notes::{ResultKind, ResultRow};
    use super::super::phases::{ExportPhase, RunState};
    use super::super::steps::ContinueBlock;
    use super::super::{EXPORT_WIZARD_WIDTH, ExportWizard};
    use crate::keyboard_test_support::init_keyboard_runtime;

    fn open_wizard(
        cx: &mut TestAppContext,
        tables: Vec<TableRef>,
    ) -> (Entity<ExportWizard>, &mut VisualTestContext) {
        init_keyboard_runtime(cx);
        let app_state = cx.update(|cx| {
            cx.new(|_| {
                let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("isolated storage runtime");
                AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
            })
        });

        let slot: Rc<RefCell<Option<Entity<ExportWizard>>>> = Rc::default();
        let capture = Rc::clone(&slot);
        let (_root, window) = cx.add_window_view(move |window, cx| {
            let wizard = cx.new(|cx| ExportWizard::new(app_state, window, cx));
            wizard.update(cx, |this, cx| {
                this.open(Uuid::new_v4(), None, tables, window, cx);
            });
            *capture.borrow_mut() = Some(wizard.clone());
            gpui_component::Root::new(wizard, window, cx)
        });
        window.run_until_parked();

        let wizard = slot.borrow().clone().expect("wizard");
        (wizard, window)
    }

    fn three_tables() -> Vec<TableRef> {
        ["customers", "orders", "order_items"]
            .into_iter()
            .map(TableRef::new)
            .collect()
    }

    fn update(
        wizard: &Entity<ExportWizard>,
        window: &mut VisualTestContext,
        change: impl FnOnce(&mut ExportWizard),
    ) {
        wizard.update(window, |this, cx| {
            change(this);
            cx.notify();
        });
        window.run_until_parked();
    }

    fn format_step(wizard: &Entity<ExportWizard>, window: &mut VisualTestContext) {
        update(wizard, window, |this| {
            this.phase = ExportPhase::FormatOptions;
            this.output_dir = Some(PathBuf::from("/tmp/exports"));
        });
    }

    fn present(window: &mut VisualTestContext, selector: &'static str) -> bool {
        window.debug_bounds(selector).is_some()
    }

    #[test]
    fn the_dialog_is_wide_enough_for_the_seven_format_picker() {
        assert_eq!(EXPORT_WIZARD_WIDTH, px(760.0));
    }

    /// The picker's box is as wide as its column, so the segments are what
    /// would spill past it: the last one must end inside the picker.
    #[gpui::test]
    fn the_seven_format_picker_fits_inside_the_dialog(cx: &mut TestAppContext) {
        let (wizard, window) = open_wizard(cx, three_tables());
        format_step(&wizard, window);
        update(&wizard, window, |this| this.sql_export_available = true);

        let picker = window
            .debug_bounds("export-wizard-format-picker")
            .expect("the format picker");
        let last_segment = window
            .debug_bounds("seg-ctl-item-sql")
            .expect("the seventh format segment");
        let body = window.debug_bounds("modal-body").expect("the dialog body");

        assert!(
            last_segment.right() <= picker.right(),
            "the last segment {last_segment:?} must end inside the picker {picker:?}"
        );
        assert!(
            picker.right() <= body.right() - px(18.0),
            "the picker {picker:?} must stay inside the body {body:?}"
        );
    }

    #[gpui::test]
    fn sql_is_not_offered_with_a_line_that_says_why(cx: &mut TestAppContext) {
        let (wizard, window) = open_wizard(cx, three_tables());
        format_step(&wizard, window);

        assert!(present(window, "export-wizard-sql-unavailable"));

        update(&wizard, window, |this| this.sql_export_available = true);
        assert!(!present(window, "export-wizard-sql-unavailable"));
    }

    #[gpui::test]
    fn an_invalid_batch_size_marks_the_field_and_names_it_in_the_footer(cx: &mut TestAppContext) {
        let (wizard, window) = open_wizard(cx, three_tables());
        format_step(&wizard, window);
        update(&wizard, window, |this| {
            this.sql_export_available = true;
            this.selected_format = FileFormat::Sql;
        });
        assert!(!present(window, "export-wizard-batch-size-hint"));
        assert!(!present(window, "export-wizard-footer-reason"));

        update(&wizard, window, |this| this.sql_batch_size_invalid = true);

        assert!(present(window, "export-wizard-batch-size-hint"));
        assert!(present(window, "export-wizard-footer-reason"));
        window.update(|_, cx| {
            let wizard = wizard.read(cx);
            assert_eq!(
                wizard.continue_block_reason(),
                Some(ContinueBlock::BatchSize)
            );
            assert!(!wizard.continue_enabled());
        });
    }

    #[gpui::test]
    fn an_invalid_chunk_size_marks_the_field_and_names_it_in_the_footer(cx: &mut TestAppContext) {
        let (wizard, window) = open_wizard(cx, three_tables());
        format_step(&wizard, window);

        update(&wizard, window, |this| this.segment_size_invalid = true);

        assert!(present(window, "export-wizard-segment-size-hint"));
        assert!(present(window, "export-wizard-footer-reason"));
        window.update(|_, cx| {
            assert_eq!(
                wizard.read(cx).continue_block_reason(),
                Some(ContinueBlock::SegmentSize)
            );
        });
    }

    #[gpui::test]
    fn no_folder_chosen_says_what_is_missing_in_the_field_and_the_footer(cx: &mut TestAppContext) {
        let (wizard, window) = open_wizard(cx, three_tables());
        format_step(&wizard, window);
        update(&wizard, window, |this| this.output_dir = None);

        assert!(present(window, "export-wizard-folder-required"));
        assert!(present(window, "export-wizard-footer-reason"));
        window.update(|_, cx| {
            assert_eq!(
                wizard.read(cx).continue_block_reason(),
                Some(ContinueBlock::Folder)
            );
        });

        update(&wizard, window, |this| {
            this.output_dir = Some(PathBuf::from("/tmp/exports"));
        });
        assert!(!present(window, "export-wizard-folder-required"));
        assert!(!present(window, "export-wizard-footer-reason"));
    }

    #[gpui::test]
    fn an_empty_selection_says_why_continue_is_off(cx: &mut TestAppContext) {
        let (_empty, window) = open_wizard(cx, Vec::new());

        assert!(present(window, "export-wizard-tables-required"));
        assert!(present(window, "export-wizard-footer-reason"));

        let (_wizard, window) = open_wizard(cx, three_tables());
        assert!(!present(window, "export-wizard-tables-required"));
        assert!(!present(window, "export-wizard-footer-reason"));
    }

    fn finished_run(rows: Vec<ResultRow>) -> impl FnOnce(&mut ExportWizard) {
        move |this| {
            this.phase = ExportPhase::Run;
            this.run_state = RunState::Done;
            this.result_summary = Some("Exported".to_string());
            this.result_rows = rows;
        }
    }

    #[gpui::test]
    fn a_finished_run_lists_one_row_per_table_and_a_failed_run_says_what_to_do(
        cx: &mut TestAppContext,
    ) {
        let (wizard, window) = open_wizard(cx, three_tables());
        update(
            &wizard,
            window,
            finished_run(vec![
                ResultRow {
                    kind: ResultKind::Completed,
                    text: "customers: completed (3 row(s))".to_string(),
                },
                ResultRow {
                    kind: ResultKind::Failed,
                    text: "orders: FAILED — sink error".to_string(),
                },
                ResultRow {
                    kind: ResultKind::NotAttempted,
                    text: "order_items: not attempted: the export stopped at the failed table"
                        .to_string(),
                },
            ]),
        );

        assert!(present(window, "export-wizard-result-0-completed"));
        assert!(present(window, "export-wizard-result-1-failed"));
        assert!(present(window, "export-wizard-result-2-not-attempted"));
        assert!(present(window, "export-wizard-closing-line"));

        update(
            &wizard,
            window,
            finished_run(vec![ResultRow {
                kind: ResultKind::Completed,
                text: "customers: completed (3 row(s))".to_string(),
            }]),
        );
        assert!(present(window, "export-wizard-result-0-completed"));
        assert!(
            !present(window, "export-wizard-closing-line"),
            "a clean run has nothing to fix"
        );
    }
}
