//! The editor/results split the user last dragged to, one per results
//! position, remembered across tabs and restarts.

use super::ResultsPosition;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::{App, Entity, Global, Pixels};

/// A side smaller than this fraction of the other is not restored, so a
/// divider dragged almost to an edge does not reopen the pane collapsed.
const MIN_SIDE_RATIO: f32 = 1.0 / 15.0;

/// The editor's share of the split when nothing usable is saved.
const DEFAULT_EDITOR_SHARE: f32 = 0.5;

/// Weights for the two panels of a split's first frame. The resizable group
/// shares the container in proportion to them, so only their ratio matters.
const SPLIT_WEIGHT: f32 = 1000.0;

/// The editor's share of each split, as the documents open them.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct ResultsSplit {
    bottom: f32,
    right: f32,
}

impl Default for ResultsSplit {
    fn default() -> Self {
        Self {
            bottom: DEFAULT_EDITOR_SHARE,
            right: DEFAULT_EDITOR_SHARE,
        }
    }
}

impl ResultsSplit {
    pub(super) fn load(app_state: &AppStateEntity) -> Self {
        Self {
            bottom: load_share(app_state, ResultsPosition::Bottom),
            right: load_share(app_state, ResultsPosition::Right),
        }
    }

    pub(super) fn editor_share(&self, position: ResultsPosition) -> f32 {
        match position {
            ResultsPosition::Bottom => self.bottom,
            ResultsPosition::Right => self.right,
        }
    }

    /// The first-frame sizes of the editor and results panels.
    pub(super) fn panel_weights(&self, position: ResultsPosition) -> (Pixels, Pixels) {
        let share = self.editor_share(position);
        (
            gpui::px(share * SPLIT_WEIGHT),
            gpui::px((1.0 - share) * SPLIT_WEIGHT),
        )
    }

    /// Records the split a drag ended on, and saves it in the UI state so
    /// the next start opens with it. The in-memory split is updated even when
    /// saving fails, and the error is returned for the caller to report. A
    /// split that cannot be restored stays on screen through the resizable
    /// state itself.
    pub(super) fn remember(
        &mut self,
        position: ResultsPosition,
        sizes: &[Pixels],
        app_state: &AppStateEntity,
    ) -> Result<(), String> {
        let Some(share) = editor_share_of(sizes) else {
            return Ok(());
        };

        match position {
            ResultsPosition::Bottom => self.bottom = restorable(share),
            ResultsPosition::Right => self.right = restorable(share),
        }

        app_state
            .storage_runtime()
            .ui_state()
            .set(storage_key(position), &share.to_string())
            .map_err(|error| error.to_string())
    }
}

/// The split every code document opens with, shared so a drag in one tab is
/// what the next split to open in any tab uses.
struct SharedResultsSplit(ResultsSplit);

impl Global for SharedResultsSplit {}

/// The shared split, read from the UI state the first time it is needed.
pub(super) fn current(app_state: &Entity<AppStateEntity>, cx: &mut App) -> ResultsSplit {
    if let Some(shared) = cx.try_global::<SharedResultsSplit>() {
        return shared.0;
    }

    let split = ResultsSplit::load(app_state.read(cx));
    cx.set_global(SharedResultsSplit(split));
    split
}

/// Records the split a drag ended on for every tab and the next start,
/// reporting a failed save.
pub(super) fn remember(
    app_state: &Entity<AppStateEntity>,
    position: ResultsPosition,
    sizes: &[Pixels],
    cx: &mut App,
) {
    let mut split = current(app_state, cx);
    let saved = split.remember(position, sizes, app_state.read(cx));
    cx.set_global(SharedResultsSplit(split));

    if let Err(error) = saved {
        report_error(
            UserFacingError::new(
                ErrorKind::Storage,
                crate::labels::results_split_save_failed_error(&error),
            ),
            cx,
        );
    }
}

fn storage_key(position: ResultsPosition) -> &'static str {
    match position {
        ResultsPosition::Bottom => "query_results_split_bottom",
        ResultsPosition::Right => "query_results_split_right",
    }
}

fn load_share(app_state: &AppStateEntity, position: ResultsPosition) -> f32 {
    match app_state
        .storage_runtime()
        .ui_state()
        .get(storage_key(position))
    {
        Ok(Some(value)) => parse_share(&value),
        Ok(None) => DEFAULT_EDITOR_SHARE,
        Err(error) => {
            log::warn!("Could not read the saved results split: {error}");
            DEFAULT_EDITOR_SHARE
        }
    }
}

/// The saved share, or the default when it is unreadable or would reopen a
/// side under a fifteenth of the other.
pub(super) fn parse_share(value: &str) -> f32 {
    value
        .trim()
        .parse::<f32>()
        .map_or(DEFAULT_EDITOR_SHARE, restorable)
}

fn restorable(share: f32) -> f32 {
    if !share.is_finite() || share <= 0.0 || share >= 1.0 {
        return DEFAULT_EDITOR_SHARE;
    }

    let (smaller, larger) = if share < 0.5 {
        (share, 1.0 - share)
    } else {
        (1.0 - share, share)
    };

    if smaller < larger * MIN_SIDE_RATIO {
        DEFAULT_EDITOR_SHARE
    } else {
        share
    }
}

/// The editor's share of a two-panel split, or `None` when the sizes do not
/// describe one.
pub(super) fn editor_share_of(sizes: &[Pixels]) -> Option<f32> {
    let [editor, results] = sizes else {
        return None;
    };

    let editor = editor.as_f32();
    let total = editor + results.as_f32();

    (total.is_finite() && total > 0.0).then(|| editor / total)
}

impl super::CodeDocument {
    /// The callback a split runs when a drag ends: it remembers the split
    /// for this position.
    pub(super) fn remember_split_on_resize(
        &self,
        position: ResultsPosition,
    ) -> impl Fn(
        &gpui::Entity<gpui_component::resizable::ResizableState>,
        &mut gpui::Window,
        &mut gpui::App,
    ) + 'static {
        let app_state = self.app_state.clone();
        move |state, _window, cx| {
            let sizes = state.read(cx).sizes().clone();
            remember(&app_state, position, &sizes, cx);
        }
    }
}

#[cfg(test)]
mod tests;
