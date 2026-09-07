//! Type-to-confirm modal for the recursive prefix/bucket delete (DEC-19).
//!
//! The probe, the confirm-state model and the execution path live in
//! `delete_prefix.rs`; this module owns the trigger (`Del` on a prefix row),
//! the modal chrome, and the gate that keeps the Delete button locked until
//! the user has typed the target back.

use super::delete_prefix::{DeletePrefixConfirmState, DeletePrefixProbeState, PrefixDeleteProbe};
use super::tree::ObjectTreeNodeId;
use super::{ObjectBrowserDocument, ObjectBrowserFocusMode};
use crate::buckets_table::BucketDetailsState;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text, TypeToConfirm};
use dbflux_components::tokens::{Heights, Radii, Spacing};
use dbflux_ui_base::modal_frame::ModalFrame;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// What the probe has established so far, as one line of modal copy.
pub(super) fn probe_summary_line(probe: &PrefixDeleteProbe) -> String {
    let totals = crate::labels::delete_prefix_probe_totals(probe.object_count, probe.total_bytes);

    match &probe.state {
        DeletePrefixProbeState::Idle => {
            dbflux_i18n::t!("document.object_browser.delete_prefix_modal.counting")
        }
        DeletePrefixProbeState::Running => dbflux_i18n::t!(
            "document.object_browser.delete_prefix_modal.counting_progress",
            totals = totals,
            pages = probe.pages_walked
        ),
        DeletePrefixProbeState::Done => totals,
        DeletePrefixProbeState::Cancelled => dbflux_i18n::t!(
            "document.object_browser.delete_prefix_modal.cancelled",
            totals = totals
        ),
        DeletePrefixProbeState::Capped => dbflux_i18n::t!(
            "document.object_browser.delete_prefix_modal.capped",
            totals = totals,
            pages = probe.pages_walked
        ),
        DeletePrefixProbeState::Error(message) => dbflux_i18n::t!(
            "document.object_browser.delete_prefix_modal.error",
            error = message.as_str()
        ),
    }
}

/// Trailing "… N more" line under the affected-keys preview, or `None` when
/// the preview already lists every counted key.
pub(super) fn remaining_keys_line(probe: &PrefixDeleteProbe) -> Option<String> {
    let previewed = probe.first_keys.len() as u64;

    if probe.object_count <= previewed {
        return None;
    }

    Some(dbflux_i18n::t!(
        "document.object_browser.delete_prefix_modal.remaining_keys",
        count = probe.object_count - previewed
    ))
}

/// Label of the danger button. The count is omitted while the probe is still
/// walking, so the button never advertises a total that is about to change.
pub(super) fn delete_button_label(probe: &PrefixDeleteProbe) -> String {
    match probe.state {
        DeletePrefixProbeState::Done | DeletePrefixProbeState::Cancelled => {
            crate::labels::delete_prefix_delete_button_label(Some(probe.object_count))
        }
        _ => crate::labels::delete_prefix_delete_button_label(None),
    }
}

impl ObjectBrowserDocument {
    /// `Del` on a prefix row: opens the recursive-delete modal and starts the
    /// bounded probe that fills in its counts.
    pub(super) fn request_delete_prefix(&mut self, target: String, cx: &mut Context<Self>) {
        let versioning = match &self.bucket_details {
            BucketDetailsState::Loaded(details) => Some(details.versioning),
            _ => None,
        };

        // The widget's expected phrase is fixed at construction, so a new
        // target always needs a new one.
        self.delete_prefix_input = None;
        self.start_delete_prefix_probe(target, versioning, cx);
    }

    /// Runs the delete once the typed phrase matches. Called from the modal's
    /// button and from `Command::Execute` while the modal owns the keyboard.
    pub(super) fn confirm_delete_prefix(&mut self, cx: &mut Context<Self>) {
        if !self.delete_prefix_confirmation_matches(cx) {
            return;
        }

        let Some(target) = self
            .delete_prefix_confirm
            .as_ref()
            .map(|confirm| confirm.target.clone())
        else {
            return;
        };

        self.delete_prefix_input = None;
        self.execute_delete_prefix(target, cx);
    }

    fn delete_prefix_confirmation_matches(&self, cx: &Context<Self>) -> bool {
        let Some(confirm) = self.delete_prefix_confirm.as_ref() else {
            return false;
        };

        self.delete_prefix_input
            .as_ref()
            .is_some_and(|input| confirm.confirmation_matches(&input.read(cx).typed_text(cx)))
    }

    /// Builds the type-to-confirm widget on the first render after the modal
    /// opens (`InputState` needs a `Window`) and focuses it, so the modal is
    /// typeable without a click.
    pub(super) fn ensure_delete_prefix_input(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(confirm) = self.delete_prefix_confirm.as_ref() else {
            self.delete_prefix_input = None;
            return;
        };

        if self.delete_prefix_input.is_some() {
            return;
        }

        let expected = confirm.expected_phrase.clone();
        let input = cx.new(|cx| TypeToConfirm::new(expected, window, cx));

        input.update(cx, |confirm, cx| confirm.focus(window, cx));
        self.focus_mode = ObjectBrowserFocusMode::Listing;
        self.delete_prefix_input = Some(input);
    }

    pub(super) fn render_delete_prefix_confirm(
        &self,
        confirm: &DeletePrefixConfirmState,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();
        let entity = cx.entity().downgrade();

        let close = {
            let entity = entity.clone();
            move |_window: &mut Window, cx: &mut App| {
                entity
                    .update(cx, |this, cx| this.close_delete_prefix_confirm(cx))
                    .ok();
            }
        };

        let scope = if confirm.target.is_empty() {
            format!("s3://{}", self.bucket)
        } else {
            format!("s3://{}/{}", self.bucket, confirm.target)
        };

        let probe_running = confirm.probe.state == DeletePrefixProbeState::Running;
        let can_delete = self.delete_prefix_confirmation_matches(cx);

        let mut body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::LG)
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(Text::body(dbflux_i18n::t!(
                        "document.object_browser.delete_prefix_modal.body_intro"
                    )))
                    .child(Text::code(scope).primary())
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .when(probe_running, |this| {
                                this.child(Icon::new(AppIcon::Loader).small().muted())
                            })
                            .child(
                                Text::body(probe_summary_line(&confirm.probe))
                                    .font_weight(FontWeight::BOLD),
                            ),
                    ),
            );

        if probe_running {
            body = body.child(
                div()
                    .id("object-browser-delete-prefix-cancel-probe")
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .cursor_pointer()
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.cancel_delete_prefix_probe(cx);
                    }))
                    .child(Icon::new(AppIcon::X).small().muted())
                    .child(Text::caption(dbflux_i18n::t!(
                        "document.object_browser.delete_prefix_modal.cancel_probe"
                    ))),
            );
        }

        if !confirm.probe.first_keys.is_empty() {
            let mut keys = div()
                .flex()
                .flex_col()
                .gap(Spacing::XXS)
                .p(Spacing::SM)
                .rounded(Radii::SM)
                .border_l_2()
                .border_color(theme.border)
                .bg(theme.secondary)
                .child(
                    Text::caption(dbflux_i18n::t!(
                        "document.object_browser.delete_prefix_modal.first_keys_label"
                    ))
                    .muted_foreground(),
                )
                .children(
                    confirm
                        .probe
                        .first_keys
                        .iter()
                        .map(|key| Text::code(key.clone()).muted_foreground()),
                );

            if let Some(more) = remaining_keys_line(&confirm.probe) {
                keys = keys.child(Text::caption(more).muted_foreground());
            }

            body = body.child(keys);
        }

        if let Some(note) = confirm.versioning_note.as_ref() {
            body = body.child(
                div()
                    .flex()
                    .items_start()
                    .gap(Spacing::SM)
                    .child(Icon::new(AppIcon::History).small().warning())
                    .child(Text::caption(note.clone()).warning()),
            );
        }

        let mut confirmation = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::caption(dbflux_i18n::t!(
                "document.object_browser.delete_prefix_modal.confirm_hint",
                phrase = confirm.expected_phrase.as_str()
            )));

        if let Some(input) = self.delete_prefix_input.as_ref() {
            confirmation = confirmation.child(input.clone());
        }

        body = body.child(confirmation).child(
            div()
                .flex()
                .items_center()
                .justify_between()
                .gap(Spacing::MD)
                .child(
                    Text::caption(dbflux_i18n::t!(
                        "document.object_browser.delete_prefix_modal.batched_caption"
                    ))
                    .muted_foreground(),
                )
                .child(
                    div()
                        .flex()
                        .justify_end()
                        .gap(Spacing::SM)
                        .child(
                            div()
                                .id("object-browser-delete-prefix-cancel")
                                .flex()
                                .items_center()
                                .h(Heights::CONTROL)
                                .px(Spacing::SM)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .bg(theme.secondary)
                                .hover(|d| d.bg(theme.muted))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.close_delete_prefix_confirm(cx);
                                }))
                                .child(Text::caption(dbflux_i18n::t!(
                                    "document.object_browser.delete_prefix_modal.cancel"
                                ))),
                        )
                        .child(
                            div()
                                .id("object-browser-delete-prefix-confirm")
                                .flex()
                                .items_center()
                                .gap(Spacing::XS)
                                .h(Heights::CONTROL)
                                .px(Spacing::SM)
                                .rounded(Radii::SM)
                                .bg(theme.danger)
                                .when(!can_delete, |d| d.opacity(0.5))
                                .when(can_delete, |d| d.cursor_pointer().hover(|d| d.opacity(0.9)))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.confirm_delete_prefix(cx);
                                }))
                                .child(
                                    Icon::new(AppIcon::Delete)
                                        .size(Heights::ICON_SM)
                                        .color(theme.background),
                                )
                                .child(
                                    Text::caption(delete_button_label(&confirm.probe))
                                        .color(theme.background),
                                ),
                        ),
                ),
        );

        ModalFrame::new(
            "object-browser-delete-prefix-modal",
            &self.focus_handle,
            close,
        )
        .title(dbflux_i18n::t!(
            "document.object_browser.delete_prefix_modal.title"
        ))
        .icon(AppIcon::TriangleAlert)
        .width(px(560.0))
        .max_height(px(560.0))
        .center_vertically()
        .child(body.into_any_element())
        .render(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::super::delete_prefix::{DeletePrefixProbeState, PrefixDeleteProbe};
    use super::{delete_button_label, probe_summary_line, remaining_keys_line};
    use dbflux_core::{ObjectListingPage, ObjectSummary};

    fn probe_with(objects: &[(&str, u64)]) -> PrefixDeleteProbe {
        let mut probe = PrefixDeleteProbe::default();
        let generation = probe.start("logs/".to_string());

        probe.apply_page(
            generation,
            ObjectListingPage {
                objects: objects
                    .iter()
                    .map(|(key, size)| ObjectSummary {
                        key: key.to_string(),
                        size_bytes: *size,
                        storage_class: None,
                        last_modified: None,
                    })
                    .collect(),
                common_prefixes: Vec::new(),
                next_continuation_token: None,
            },
        );

        probe
    }

    /// T36: while the walk is in flight the modal advertises the running
    /// totals as provisional, and only reports a final figure once done.
    #[test]
    fn probe_summary_distinguishes_running_from_final_totals() {
        let mut probe = probe_with(&[("logs/a.txt", 1024), ("logs/b.txt", 1024)]);
        assert!(probe_summary_line(&probe).starts_with("Counting…"));

        probe.state = DeletePrefixProbeState::Done;
        assert_eq!(probe_summary_line(&probe), "2 objects · 2.0 KiB");
    }

    /// T36: a capped walk never claims an exact total.
    #[test]
    fn probe_summary_marks_a_capped_walk_as_a_lower_bound() {
        let mut probe = probe_with(&[("logs/a.txt", 10)]);
        probe.state = DeletePrefixProbeState::Capped;

        assert!(probe_summary_line(&probe).starts_with("at least 1 object"));
    }

    /// T36: the preview list only advertises a remainder when the probe
    /// counted more keys than it kept.
    #[test]
    fn remaining_keys_line_appears_only_beyond_the_preview_cap() {
        let probe = probe_with(&[("logs/a.txt", 1)]);
        assert_eq!(remaining_keys_line(&probe), None);

        let mut probe = probe_with(&[("logs/a.txt", 1)]);
        probe.object_count = 9;
        assert_eq!(remaining_keys_line(&probe), Some("… 8 more".to_string()));
    }

    /// T36: the danger button only carries a count once the count is settled.
    #[test]
    fn delete_button_label_waits_for_a_settled_count() {
        let mut probe = probe_with(&[("logs/a.txt", 1), ("logs/b.txt", 1)]);
        assert_eq!(delete_button_label(&probe), "Delete objects");

        probe.state = DeletePrefixProbeState::Done;
        assert_eq!(delete_button_label(&probe), "Delete 2 objects");
    }
}
