//! The export wizard's Format step: the format picker, the options the
//! chosen format uses, the folder and the chunk size.

use dbflux_components::controls::{Button, Input};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::{modal_hint, modal_value_field};
use dbflux_components::primitives::{SegmentedControl, SegmentedItem};
use dbflux_components::tokens::ModalMetrics;
use gpui::*;

use super::format_options::{
    self, available_formats, date_format_label, date_format_segment_id, delimiter_label,
    delimiter_segment_id, encoding_label, encoding_segment_id, visible_options,
};
use super::notes::{alert_hint, form_row, info_line, with_danger_edge};
use super::{ExportWizard, SEGMENT_SIZE_INPUT_WIDTH, format_icon, format_segment_id};

impl ExportWizard {
    /// The per-format option rows, only those the selected format uses.
    fn render_format_option_rows(
        &self,
        visible: format_options::VisibleOptions,
        cx: &mut Context<Self>,
    ) -> Vec<AnyElement> {
        let mut rows = Vec::new();

        if visible.delimiter_and_encoding {
            let entity = cx.entity().downgrade();
            let items: Vec<SegmentedItem> = dbflux_export::CsvDelimiter::ALL
                .into_iter()
                .map(|delimiter| {
                    SegmentedItem::new(delimiter_segment_id(delimiter), delimiter_label(delimiter))
                })
                .collect();
            let picker = SegmentedControl::new(
                items,
                delimiter_segment_id(self.format_options.delimiter),
                move |segment_id, _window, cx| {
                    entity
                        .update(cx, |this, cx| this.select_delimiter(segment_id, cx))
                        .ok();
                },
            )
            .group("export-format-delimiter");
            rows.push(
                form_row(
                    dbflux_i18n::t!("document.export_wizard.format_options.delimiter_label"),
                    div().flex().child(picker),
                    None,
                    cx,
                )
                .into_any_element(),
            );

            let entity = cx.entity().downgrade();
            let items: Vec<SegmentedItem> = dbflux_export::TextEncoding::ALL
                .into_iter()
                .map(|encoding| {
                    SegmentedItem::new(encoding_segment_id(encoding), encoding_label(encoding))
                })
                .collect();
            let picker = SegmentedControl::new(
                items,
                encoding_segment_id(self.format_options.encoding),
                move |segment_id, _window, cx| {
                    entity
                        .update(cx, |this, cx| this.select_encoding(segment_id, cx))
                        .ok();
                },
            )
            .group("export-format-encoding");
            rows.push(
                form_row(
                    dbflux_i18n::t!("document.export_wizard.format_options.encoding_label"),
                    div().flex().child(picker),
                    None,
                    cx,
                )
                .into_any_element(),
            );
        }

        if visible.null_text {
            let hint = modal_hint(
                dbflux_i18n::t!("document.export_wizard.format_options.null_text_hint"),
                cx,
            );
            rows.push(
                form_row(
                    dbflux_i18n::t!("document.export_wizard.format_options.null_text_label"),
                    div()
                        .w(SEGMENT_SIZE_INPUT_WIDTH)
                        .font_family(dbflux_components::fonts::editor_family(cx))
                        .child(
                            Input::new(&self.null_text_input)
                                .id("export-wizard-null-text")
                                .w_full()
                                .aria_label(dbflux_i18n::t!(
                                    "document.export_wizard.format_options.null_text_label"
                                )),
                        ),
                    Some(hint),
                    cx,
                )
                .into_any_element(),
            );
        }

        if visible.sql_batch_size {
            let hint = if self.sql_batch_size_invalid {
                alert_hint(
                    dbflux_i18n::t!("document.export_wizard.format_options.sql_batch_size_invalid"),
                    "export-wizard-batch-size-hint",
                    cx,
                )
            } else {
                modal_hint(
                    dbflux_i18n::t!("document.export_wizard.format_options.sql_batch_size_hint"),
                    cx,
                )
            };
            rows.push(
                form_row(
                    dbflux_i18n::t!("document.export_wizard.format_options.sql_batch_size_label"),
                    with_danger_edge(
                        div()
                            .w(SEGMENT_SIZE_INPUT_WIDTH)
                            .font_family(dbflux_components::fonts::editor_family(cx))
                            .child(
                                Input::new(&self.sql_batch_size_input)
                                    .id("export-wizard-sql-batch-size")
                                    .w_full()
                                    .aria_label(dbflux_i18n::t!(
                                        "document.export_wizard.format_options.sql_batch_size_label"
                                    )),
                            ),
                        self.sql_batch_size_invalid,
                        cx,
                    )
                    .w(SEGMENT_SIZE_INPUT_WIDTH)
                    .debug_selector(|| "export-wizard-batch-size-edge".to_string()),
                    Some(hint),
                    cx,
                )
                .into_any_element(),
            );
        }

        if visible.date_format {
            let entity = cx.entity().downgrade();
            let items: Vec<SegmentedItem> = dbflux_export::DateFormat::ALL
                .into_iter()
                .map(|date_format| {
                    SegmentedItem::new(
                        date_format_segment_id(date_format),
                        date_format_label(date_format),
                    )
                })
                .collect();
            let picker = SegmentedControl::new(
                items,
                date_format_segment_id(self.format_options.date_format),
                move |segment_id, _window, cx| {
                    entity
                        .update(cx, |this, cx| this.select_date_format(segment_id, cx))
                        .ok();
                },
            )
            .group("export-format-date");
            rows.push(
                form_row(
                    dbflux_i18n::t!("document.export_wizard.format_options.date_format_label"),
                    div().flex().child(picker),
                    None,
                    cx,
                )
                .into_any_element(),
            );
        }

        rows
    }

    pub(super) fn render_format_options(&self, cx: &mut Context<Self>) -> AnyElement {
        let format_items: Vec<SegmentedItem> = available_formats(self.sql_export_available)
            .into_iter()
            .map(|format| {
                SegmentedItem::new(format_segment_id(format), format.label())
                    .icon(format_icon(format))
            })
            .collect();

        let entity = cx.entity().downgrade();
        let format_picker = div()
            .debug_selector(|| "export-wizard-format-picker".to_string())
            .flex()
            .child(SegmentedControl::new(
                format_items,
                format_segment_id(self.selected_format),
                move |segment_id, _window, cx| {
                    entity
                        .update(cx, |this, cx| this.select_format(segment_id, cx))
                        .ok();
                },
            ));
        let format_hint = (!self.sql_export_available).then(|| {
            info_line(
                dbflux_i18n::t!("document.export_wizard.format_options.sql_unavailable"),
                "export-wizard-sql-unavailable",
                cx,
            )
        });

        let folder_value = self
            .output_dir
            .as_ref()
            .map(|dir| SharedString::from(dir.display().to_string()));

        let folder_control = div()
            .flex()
            .gap(ModalMetrics::FOOTER_GAP)
            .child(div().flex_1().min_w_0().child(with_danger_edge(
                modal_value_field(
                    folder_value,
                    dbflux_i18n::t!("document.export_wizard.format_options.no_folder_chosen"),
                    None,
                    cx,
                ),
                self.output_dir.is_none(),
                cx,
            )))
            .child(
                Button::new(
                    "export-wizard-choose-folder",
                    if self.choosing_folder {
                        dbflux_i18n::t!("document.export_wizard.format_options.choosing")
                    } else {
                        dbflux_i18n::t!("document.export_wizard.format_options.choose_folder")
                    },
                )
                .icon(AppIcon::Folder)
                .disabled(self.choosing_folder)
                .on_click(cx.listener(|this, _event, _window, cx| this.choose_folder(cx))),
            );

        let folder_hint = match (&self.folder_error, &self.output_dir) {
            (Some(error), _) => Some(alert_hint(error.clone(), "export-wizard-folder-error", cx)),
            (None, None) => Some(alert_hint(
                dbflux_i18n::t!("document.export_wizard.format_options.folder_required"),
                "export-wizard-folder-required",
                cx,
            )),
            (None, Some(_)) => None,
        };

        let segment_hint = if self.segment_size_invalid {
            alert_hint(
                dbflux_i18n::t!("document.export_wizard.format_options.segment_size_invalid"),
                "export-wizard-segment-size-hint",
                cx,
            )
        } else {
            modal_hint(
                dbflux_i18n::t!("document.export_wizard.format_options.segment_size_hint"),
                cx,
            )
        };

        let visible = visible_options(self.selected_format);
        let option_rows = self.render_format_option_rows(visible, cx);

        div()
            .flex()
            .flex_col()
            .child(form_row(
                dbflux_i18n::t!("document.export_wizard.format_options.format_label"),
                format_picker,
                format_hint,
                cx,
            ))
            .children(option_rows)
            .child(form_row(
                dbflux_i18n::t!("document.export_wizard.format_options.output_folder_label"),
                folder_control,
                folder_hint,
                cx,
            ))
            .child(form_row(
                dbflux_i18n::t!("document.export_wizard.format_options.segment_size_label"),
                with_danger_edge(
                    div()
                        .w(SEGMENT_SIZE_INPUT_WIDTH)
                        .font_family(dbflux_components::fonts::editor_family(cx))
                        .child(
                            Input::new(&self.segment_size_input)
                                .id("export-wizard-segment-size")
                                .w_full()
                                .aria_label(dbflux_i18n::t!(
                                    "document.export_wizard.format_options.segment_size_label"
                                )),
                        ),
                    self.segment_size_invalid,
                    cx,
                )
                .w(SEGMENT_SIZE_INPUT_WIDTH)
                .debug_selector(|| "export-wizard-segment-size-edge".to_string()),
                Some(segment_hint),
                cx,
            ))
            .into_any_element()
    }
}
