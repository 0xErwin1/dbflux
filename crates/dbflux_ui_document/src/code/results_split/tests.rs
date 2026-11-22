use super::{ResultsSplit, editor_share_of, parse_share};
use crate::code::ResultsPosition;
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::AppStateEntity;
use gpui::px;

fn app_state() -> AppStateEntity {
    AppStateEntity::new_with_storage_runtime(
        StorageRuntime::in_memory().expect("in-memory storage"),
    )
    .expect("app state")
}

#[test]
fn the_editor_share_is_its_part_of_both_panels() {
    assert_eq!(editor_share_of(&[px(300.0), px(700.0)]), Some(0.3));
    assert_eq!(editor_share_of(&[px(0.0), px(0.0)]), None);
    assert_eq!(editor_share_of(&[px(300.0)]), None);
}

#[test]
fn a_split_opens_at_half_when_nothing_is_saved() {
    let split = ResultsSplit::load(&app_state());

    assert_eq!(split.editor_share(ResultsPosition::Bottom), 0.5);
    assert_eq!(split.editor_share(ResultsPosition::Right), 0.5);
}

#[test]
fn each_position_remembers_its_own_split_across_loads() {
    let app_state = app_state();
    let mut split = ResultsSplit::load(&app_state);

    split.remember(ResultsPosition::Bottom, &[px(250.0), px(750.0)], &app_state);
    split.remember(ResultsPosition::Right, &[px(600.0), px(400.0)], &app_state);

    assert_eq!(split.editor_share(ResultsPosition::Bottom), 0.25);
    let reloaded = ResultsSplit::load(&app_state);
    assert_eq!(reloaded.editor_share(ResultsPosition::Bottom), 0.25);
    assert_eq!(reloaded.editor_share(ResultsPosition::Right), 0.6);
}

#[test]
fn a_side_under_a_fifteenth_of_the_other_is_not_restored() {
    let app_state = app_state();
    let mut split = ResultsSplit::load(&app_state);

    split.remember(ResultsPosition::Bottom, &[px(50.0), px(950.0)], &app_state);

    assert_eq!(
        ResultsSplit::load(&app_state).editor_share(ResultsPosition::Bottom),
        0.5
    );
    assert_eq!(parse_share("0.97"), 0.5);
    assert_eq!(parse_share("0.07"), 0.07);
}

#[test]
fn an_unreadable_saved_split_opens_at_half() {
    assert_eq!(parse_share("wide"), 0.5);
    assert_eq!(parse_share("NaN"), 0.5);
    assert_eq!(parse_share("1.5"), 0.5);
}

#[test]
fn the_first_frame_weights_follow_the_share() {
    let app_state = app_state();
    let mut split = ResultsSplit::load(&app_state);
    split.remember(ResultsPosition::Right, &[px(200.0), px(600.0)], &app_state);

    assert_eq!(
        split.panel_weights(ResultsPosition::Right),
        (px(250.0), px(750.0))
    );
}
