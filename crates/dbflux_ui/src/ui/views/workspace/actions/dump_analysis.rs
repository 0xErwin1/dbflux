use super::*;
use crate::ui::labels::{
    dump_analysis_dialog_title, dump_analysis_filter_all_files_label,
    dump_analysis_filter_dumps_label, dump_analysis_no_dialog_message,
    dump_analysis_unsupported_extension_message, select_dump_analyzer,
};
use dbflux_core::DumpAnalyzer;
use std::sync::Arc;

impl Workspace {
    /// Opens a file dialog to pick a driver's native dump/export file and
    /// opens it in a `DumpAnalysisDocument` tab.
    ///
    /// Every registered driver is asked for `dump_analyzer()`; whichever
    /// analyzer's extensions match the picked file drives the analysis. The
    /// workspace never branches on driver id or name.
    pub(in crate::ui::views::workspace) fn analyze_dump_file(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let analyzers: Vec<Arc<dyn DumpAnalyzer>> = {
            let mut analyzers: Vec<Arc<dyn DumpAnalyzer>> = self
                .app_state
                .read(cx)
                .drivers()
                .values()
                .filter_map(|driver| driver.dump_analyzer())
                .collect();
            analyzers.sort_by_key(|analyzer| analyzer.display_name());
            analyzers
        };

        if analyzers.is_empty() {
            return;
        }

        let dialog_available = dbflux_ui_base::file_dialog::is_native_file_dialog_available();

        if !dialog_available {
            report_error(
                UserFacingError::new(ErrorKind::Config, dump_analysis_no_dialog_message()),
                cx,
            );
            return;
        }

        let all_extensions: Vec<&'static str> = {
            let mut extensions: Vec<&'static str> = analyzers
                .iter()
                .flat_map(|analyzer| analyzer.file_extensions().iter().copied())
                .collect();
            extensions.sort_unstable();
            extensions.dedup();
            extensions
        };

        let tab_manager = self.tab_manager.clone();
        let app_state = self.app_state.clone();

        cx.spawn(async move |this, cx| {
            let dialog_title = dump_analysis_dialog_title();
            let dumps_filter_label = dump_analysis_filter_dumps_label();
            let all_files_filter_label = dump_analysis_filter_all_files_label();

            let file_handle = rfd::AsyncFileDialog::new()
                .set_title(&dialog_title)
                .add_filter(&dumps_filter_label, &all_extensions)
                .add_filter(&all_files_filter_label, &["*"])
                .pick_file()
                .await;

            let Some(handle) = file_handle else {
                return;
            };

            let path = handle.path().to_path_buf();

            let extension = path
                .extension()
                .map(|ext| ext.to_string_lossy().into_owned())
                .unwrap_or_default();

            let analyzer_candidates: Vec<(&str, &[&str])> = analyzers
                .iter()
                .map(|analyzer| (analyzer.display_name(), analyzer.file_extensions()))
                .collect();

            let Some((selected_index, multiple_matched)) =
                select_dump_analyzer(&analyzer_candidates, &extension)
            else {
                report_error_async(
                    UserFacingError::new(
                        ErrorKind::User,
                        dump_analysis_unsupported_extension_message(&extension),
                    ),
                    cx,
                );
                return;
            };

            let analyzer = analyzers[selected_index].clone();

            let already_open = match cx.update(|cx| {
                tab_manager.read(cx).find_by_key(
                    &crate::ui::document::DocumentKey::DumpAnalysis { path: path.clone() },
                    cx,
                )
            }) {
                Ok(value) => value,
                Err(error) => {
                    log::warn!(
                        "Failed to inspect open tabs while opening dump analysis: {:?}",
                        error
                    );
                    None
                }
            };

            if let Some(id) = already_open {
                if let Err(error) = cx.update(|cx| {
                    tab_manager.update(cx, |mgr, cx| {
                        mgr.activate(id, cx);
                    });
                }) {
                    log::warn!(
                        "Failed to activate already-open dump analysis tab: {:?}",
                        error
                    );
                }
                return;
            }

            if let Err(error) = cx.update(|cx| {
                this.update(cx, |ws, cx| {
                    let doc = cx.new(|cx| {
                        crate::ui::document::DumpAnalysisDocument::new(
                            analyzer,
                            path,
                            multiple_matched,
                            app_state.clone(),
                            cx,
                        )
                    });
                    let pane = crate::ui::document::DumpAnalysisDocument::into_pane(doc, cx);

                    ws.tab_manager.update(cx, |mgr, cx| {
                        mgr.open(Tab::Pane(Box::new(pane)), cx);
                    });

                    ws.pending_focus = Some(FocusTarget::Document);
                    cx.notify();
                })
                .unwrap_or_else(|inner_error| {
                    log::warn!(
                        "Failed to open dump analysis document in workspace: {:?}",
                        inner_error
                    );
                });
            }) {
                log::warn!("Failed to apply picked dump file to workspace: {:?}", error);
            }
        })
        .detach();
    }
}
