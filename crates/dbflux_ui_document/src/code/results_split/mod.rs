//! The editor/results split the user last dragged to, one per results
//! position, remembered across tabs and restarts.

use super::ResultsPosition;
use dbflux_ui_base::AppStateEntity;
use gpui::Pixels;

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

    /// Records the split a drag ended on, in this document and in the UI
    /// state, so new tabs and the next start open with it. A split that
    /// cannot be restored is kept for this document only while it stays
    /// open, through the resizable state itself.
    pub(super) fn remember(
        &mut self,
        position: ResultsPosition,
        sizes: &[Pixels],
        app_state: &AppStateEntity,
    ) {
        let Some(share) = editor_share_of(sizes) else {
            return;
        };

        match position {
            ResultsPosition::Bottom => self.bottom = restorable(share),
            ResultsPosition::Right => self.right = restorable(share),
        }

        if let Err(error) = app_state
            .storage_runtime()
            .ui_state()
            .set(storage_key(position), &share.to_string())
        {
            log::error!("Could not save the results split: {error}");
        }
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
        cx: &mut gpui::Context<Self>,
    ) -> impl Fn(
        &gpui::Entity<gpui_component::resizable::ResizableState>,
        &mut gpui::Window,
        &mut gpui::App,
    ) + 'static {
        let document = cx.entity().downgrade();
        move |state, _window, cx| {
            let sizes = state.read(cx).sizes().clone();
            let Some(document) = document.upgrade() else {
                return;
            };
            document.update(cx, |document, cx| {
                let app_state = document.app_state.clone();
                document
                    .results_split
                    .remember(position, &sizes, app_state.read(cx));
            });
        }
    }
}

#[cfg(test)]
mod tests;
