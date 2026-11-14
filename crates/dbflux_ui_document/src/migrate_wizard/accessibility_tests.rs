//! Accessible names of the migration wizard's checkboxes (issue #807, S4/S7).
//!
//! Render naming test: the real phase entities draw inside a test window and
//! the rendered accessibility frame is read, the way
//! `schema_viz::accessibility_tests` and `sql_preview_modal`'s
//! `mod accessibility` do. Explicit imports rather than `use super::*`.

use crate::migrate_wizard::column_mapping::TableMigrationConfig;
use crate::migrate_wizard::confirm_run::{ConfirmRunInputs, ConfirmRunPhase, OrderDecision};
use crate::migrate_wizard::options::OptionsPhase;
use crate::migrate_wizard::source_target::shared_coordinator_tests::{
    PickerFakeConnection, test_app_state,
};
use dbflux_components::accessibility::{FrameCapture, accessible_name};
use dbflux_core::{SchemaLoadingStrategy, TableRef, TransferColumn};
use dbflux_transfer::TableMappingMode;
use gpui::{
    AppContext as _, Context, Entity, IntoElement, ParentElement as _, Render, Styled as _,
    TestAppContext, VisualTestContext, Window, div,
};
use uuid::Uuid;

struct PhaseHarness<E: Render> {
    phase: Entity<E>,
}

impl<E: Render> Render for PhaseHarness<E> {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().size_full().child(self.phase.clone())
    }
}

fn init_window_theme(cx: &mut TestAppContext) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
}

/// Renders the Options phase with a target that advertises
/// `DISABLE_FK_CHECKS`, so the disable-referential-integrity checkbox is
/// drawn (R7: an unsupported target never sees the control).
fn options_phase_window(cx: &mut TestAppContext) -> &mut VisualTestContext {
    init_window_theme(cx);

    let (_, window) = cx.add_window_view(|window, cx| {
        let options = cx.new(|cx| OptionsPhase::new(true, window, cx));
        let harness = cx.new(|_| PhaseHarness { phase: options });
        gpui_component::Root::new(harness, window, cx)
    });

    window
}

fn transfer_column(name: &str) -> TransferColumn {
    TransferColumn {
        name: name.to_string(),
        type_name: Some("text".to_string()),
        nullable: true,
        is_primary_key: false,
    }
}

/// A destructive plan is what draws the acknowledgement checkbox: the Confirm
/// phase only renders it when the summary has a destructive change
/// (`Recreate` here), so the test builds that summary instead of skipping the
/// assertion.
fn destructive_config() -> TableMigrationConfig {
    let mut config = TableMigrationConfig::new(
        TableRef::new("users"),
        vec![transfer_column("id")],
        true,
        vec![transfer_column("id")],
    );
    config.mapping_mode = TableMappingMode::Recreate;
    config
}

fn confirm_phase_window(cx: &mut TestAppContext) -> &mut VisualTestContext {
    init_window_theme(cx);
    let state = test_app_state(cx);

    let (_, window) = cx.add_window_view(move |window, cx| {
        let inputs = ConfirmRunInputs {
            app_state: state,
            source_connection: PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase),
            target_connection: PickerFakeConnection::new(SchemaLoadingStrategy::LazyPerDatabase),
            source_database: "app".to_string(),
            target_database: "warehouse".to_string(),
            source_profile_id: Uuid::new_v4(),
            target_profile_id: Uuid::new_v4(),
            source_container_label: "app".to_string(),
            target_container_label: "warehouse".to_string(),
            segment_size: 500,
            disable_referential_integrity: false,
            order: OrderDecision::Ready(vec![TableRef::new("users")]),
            configs: vec![destructive_config()],
            pre_run_warnings: Vec::new(),
        };
        let confirm = cx.new(|cx| ConfirmRunPhase::new(inputs, cx));
        let harness = cx.new(|_| PhaseHarness { phase: confirm });
        gpui_component::Root::new(harness, window, cx)
    });

    window
}

#[gpui::test]
fn the_disable_referential_integrity_checkbox_is_named_by_its_label(cx: &mut TestAppContext) {
    let window = options_phase_window(cx);
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);
    let expected = dbflux_i18n::t!("document.migrate_wizard.options.disable_referential_integrity")
        .to_string();

    assert_eq!(
        accessible_name(&frame, "migrate-options-disable-ri"),
        Some(expected),
        "the disable-referential-integrity checkbox is named by the text beside it"
    );
}

#[gpui::test]
fn the_destructive_acknowledgement_checkbox_is_named_by_its_label(cx: &mut TestAppContext) {
    let window = confirm_phase_window(cx);
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);
    let expected = dbflux_i18n::t!("document.migrate_wizard.confirm.destructive_ack").to_string();

    assert_eq!(
        accessible_name(&frame, "migrate-confirm-destructive-ack"),
        Some(expected),
        "the destructive-plan acknowledgement is named by the text beside it"
    );
}
