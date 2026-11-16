//! Completions of work started before the wizard was reopened, or before a
//! newer request, must not touch the wizard's current state.

use dbflux_core::CancelToken;

use super::super::messages::{DoneTone, DoneView};
use super::support::{FakeKind, SCRIPT, Scratch, ScriptFake, open_wizard, pick, read};
use crate::import_wizard::{FolderRead, WizardStep};

#[gpui::test]
fn a_folder_read_from_an_earlier_request_leaves_the_script_alone(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("stale_folder");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);
    let stale = read(&mut opened, |wizard| wizard.file_read_request);
    pick(&mut opened, scratch.write("shop.sql", SCRIPT.as_bytes()));
    opened
        .wizard
        .update(opened.window, |this, _| this.loading = true);

    opened.wizard.update(opened.window, |this, cx| {
        let read = FolderRead::Read {
            dir: std::env::temp_dir(),
            configs: Vec::new(),
            supports_truncate: false,
        };
        this.finish_folder_read(stale, read, cx);
        this.finish_folder_read(
            stale,
            FolderRead::Failed(crate::import_wizard::errors::ErrorBlock::no_dialog()),
            cx,
        );
    });

    read(&mut opened, |wizard| {
        assert!(wizard.script.is_some(), "the newer script survives");
        assert!(wizard.manifest_dir.is_none());
        assert!(wizard.manifest_error.is_none());
        assert!(wizard.loading, "the newer request owns the loading flag");
        assert!(matches!(wizard.step, WizardStep::Configure));
    });
}

#[gpui::test]
fn the_latest_folder_read_is_applied(cx: &mut gpui::TestAppContext) {
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);
    let current = opened
        .wizard
        .update(opened.window, |this, _| this.next_file_read_request());

    opened.wizard.update(opened.window, |this, cx| {
        this.finish_folder_read(
            current,
            FolderRead::Read {
                dir: std::env::temp_dir(),
                configs: Vec::new(),
                supports_truncate: true,
            },
            cx,
        );
    });

    read(&mut opened, |wizard| {
        assert!(wizard.manifest_dir.is_some());
        assert!(matches!(wizard.step, WizardStep::Configure));
    });
}

#[gpui::test]
fn a_run_that_is_no_longer_active_does_not_touch_the_wizard(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("stale_run");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);
    pick(&mut opened, scratch.write("shop.sql", SCRIPT.as_bytes()));
    let live = opened.wizard.update(opened.window, |this, cx| {
        let task = this.app_state.update(cx, |state, _| {
            state.start_task_for_target(dbflux_core::TaskKind::Import, "run".to_string(), None)
        });
        this.running = true;
        this.step = WizardStep::Running;
        this.active_task_id = Some(task.0);
        this.cancel_token = Some(CancelToken::new());
        task.0
    });
    let other = opened.wizard.update(opened.window, |this, cx| {
        this.app_state
            .update(cx, |state, _| {
                state.start_task_for_target(dbflux_core::TaskKind::Import, "old".to_string(), None)
            })
            .0
    });
    let view = DoneView {
        headline: "old run".to_string(),
        tone: DoneTone::Success,
        block: None,
        failed_statement: None,
        failures: Vec::new(),
        error_text: None,
        outcome: None,
        next_step: None,
    };

    opened.wizard.update(opened.window, |this, cx| {
        this.finish_script_run(other, view.clone(), cx);
    });
    read(&mut opened, |wizard| {
        assert!(wizard.running, "the live run is still running");
        assert!(wizard.cancel_token.is_some(), "and keeps its cancel token");
        assert!(matches!(wizard.step, WizardStep::Running));
    });

    opened.wizard.update(opened.window, |this, cx| {
        this.finish_script_run(live, view.clone(), cx);
    });
    read(&mut opened, |wizard| {
        assert!(!wizard.running);
        assert!(matches!(wizard.step, WizardStep::Done));
    });
}
