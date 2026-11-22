//! The Settings > Appearance page: theme, density, language, fonts and
//! scrollbar visibility.
//!
//! It is a view of the same stored `GeneralSettings` the General page edits, so
//! it reuses [`GeneralSection`]'s rows, controls and save path and only picks
//! the fields it owns.

use super::general_section::{FontFamilyField, GeneralFormRow, GeneralSection};
use super::layout;
use gpui::*;

impl GeneralSection {
    /// Rows of the Appearance page, in display order.
    pub(super) fn appearance_form_rows() -> Vec<GeneralFormRow> {
        vec![
            GeneralFormRow::Theme,
            GeneralFormRow::Style,
            GeneralFormRow::Language,
            GeneralFormRow::UiFontFamily,
            GeneralFormRow::UiFontSize,
            GeneralFormRow::EditorFontFamily,
            GeneralFormRow::EditorFontSize,
            GeneralFormRow::GridFontFamily,
            GeneralFormRow::GridFontSize,
            GeneralFormRow::Scrollbars,
            GeneralFormRow::SaveButton,
        ]
    }

    /// Validates the three font sizes and saves the Appearance fields. A
    /// rejected size is reported under its field and nothing is saved.
    pub(super) fn save_appearance_settings(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(ui_font_size) = self.read_font_size(
            &self.input_ui_font_size.clone(),
            GeneralFormRow::UiFontSize,
            "settings.general.ui_font_size.error",
            window,
            cx,
        ) else {
            return;
        };
        let Some(editor_font_size) = self.read_font_size(
            &self.input_editor_font_size.clone(),
            GeneralFormRow::EditorFontSize,
            "settings.general.editor_font_size.error",
            window,
            cx,
        ) else {
            return;
        };
        let Some(grid_font_size) = self.read_font_size(
            &self.input_grid_font_size.clone(),
            GeneralFormRow::GridFontSize,
            "settings.general.grid_font_size.error",
            window,
            cx,
        ) else {
            return;
        };

        self.gen_settings.ui_font_size = ui_font_size;
        self.gen_settings.editor_font_size = editor_font_size;
        self.gen_settings.grid_font_size = grid_font_size;

        self.commit_general_settings(window, cx);
    }

    pub(super) fn render_appearance_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme_index = Self::theme_index(self.gen_settings.theme);
        let style_index = Self::style_index(self.gen_settings.style);
        let scrollbars_index =
            Self::scrollbar_visibility_index(self.gen_settings.scrollbar_visibility);

        layout::scrolled_form_section_shell(
            dbflux_components::composites::page_header(
                dbflux_i18n::t!("settings.nav.appearance"),
                dbflux_i18n::t!("settings.general.appearance.subtitle"),
                cx,
            ),
            div()
                .flex()
                .flex_col()
                .child(self.render_gen_segmented(
                    dbflux_i18n::t!("settings.general.theme.label"),
                    Some(dbflux_i18n::t!("settings.general.theme.help")),
                    Self::theme_items(),
                    theme_index,
                    GeneralFormRow::Theme,
                    "theme",
                    |this, index| this.gen_settings.theme = Self::theme_for_index(index),
                    cx,
                ))
                .child(self.render_gen_segmented(
                    dbflux_i18n::t!("settings.general.style.label"),
                    Some(dbflux_i18n::t!("settings.general.style.help")),
                    Self::style_items(),
                    style_index,
                    GeneralFormRow::Style,
                    "density",
                    |this, index| this.gen_settings.style = Self::style_for_index(index),
                    cx,
                ))
                .child(self.render_gen_dropdown(
                    dbflux_i18n::t!("settings.general.language.label"),
                    Some(dbflux_i18n::t!("settings.general.language.notice")),
                    self.dropdown_language.clone(),
                    GeneralFormRow::Language,
                    cx,
                ))
                .child(self.render_gen_font_family(
                    FontFamilyField::Ui,
                    &self.select_ui_font_family,
                    GeneralFormRow::UiFontFamily,
                    cx,
                ))
                .child(self.render_gen_font_size(
                    dbflux_i18n::t!("settings.general.ui_font_size.label"),
                    dbflux_i18n::t!("settings.general.ui_font_size.help"),
                    &self.input_ui_font_size,
                    GeneralFormRow::UiFontSize,
                    cx,
                ))
                .child(self.render_gen_font_family(
                    FontFamilyField::Editor,
                    &self.select_editor_font_family,
                    GeneralFormRow::EditorFontFamily,
                    cx,
                ))
                .child(self.render_gen_font_size(
                    dbflux_i18n::t!("settings.general.editor_font_size.label"),
                    dbflux_i18n::t!("settings.general.editor_font_size.help"),
                    &self.input_editor_font_size,
                    GeneralFormRow::EditorFontSize,
                    cx,
                ))
                .child(self.render_gen_font_family(
                    FontFamilyField::Grid,
                    &self.select_grid_font_family,
                    GeneralFormRow::GridFontFamily,
                    cx,
                ))
                .child(self.render_gen_font_size(
                    dbflux_i18n::t!("settings.general.grid_font_size.label"),
                    dbflux_i18n::t!("settings.general.grid_font_size.help"),
                    &self.input_grid_font_size,
                    GeneralFormRow::GridFontSize,
                    cx,
                ))
                .child(self.render_gen_segmented(
                    dbflux_i18n::t!("settings.general.scrollbars.label"),
                    Some(dbflux_i18n::t!("settings.general.scrollbars.help")),
                    Self::scrollbar_visibility_items(),
                    scrollbars_index,
                    GeneralFormRow::Scrollbars,
                    "scrollbars",
                    |this, index| {
                        this.gen_settings.scrollbar_visibility =
                            Self::scrollbar_visibility_for_index(index)
                    },
                    cx,
                )),
            &self.form_scroll,
            self.form_viewport.clone(),
        )
    }
}
