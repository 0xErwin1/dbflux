use super::*;

const DIAGNOSTIC_DEBOUNCE_MS: u64 = 200;

impl SqlQueryDocument {
    /// Debounced diagnostic refresh. Bumps request id so stale runs are discarded.
    pub(super) fn schedule_diagnostic_refresh(&mut self, cx: &mut Context<Self>) {
        self.diagnostic_request_id += 1;
        let request_id = self.diagnostic_request_id;

        let entity = cx.entity().clone();
        self._diagnostic_debounce = Some(cx.spawn(async move |_this, cx| {
            cx.background_executor()
                .timer(std::time::Duration::from_millis(DIAGNOSTIC_DEBOUNCE_MS))
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    if this.diagnostic_request_id != request_id {
                        return;
                    }

                    this.run_diagnostics(cx);
                });
            })
            .ok();
        }));
    }

    /// Run diagnostics immediately, bypassing the debounce timer.
    pub(super) fn refresh_editor_diagnostics(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.diagnostic_request_id += 1;
        self.run_diagnostics(cx);
    }

    fn run_diagnostics(&mut self, cx: &mut Context<Self>) {
        let query_text = self.input_state.read(cx).value().to_string();

        let diagnostics = if let Some(conn_id) = self.connection_id {
            if let Some(connected) = self.app_state.read(cx).connections().get(&conn_id) {
                connected
                    .connection
                    .language_service()
                    .editor_diagnostics(&query_text)
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        self.input_state.update(cx, |state, cx| {
            let text = state.text().clone();

            let Some(diagnostic_set) = state.diagnostics_mut() else {
                return;
            };

            diagnostic_set.reset(&text);

            for diagnostic in diagnostics {
                diagnostic_set.push(Self::to_input_diagnostic(diagnostic));
            }

            cx.notify();
        });
    }

    fn to_input_diagnostic(diagnostic: CoreEditorDiagnostic) -> InputDiagnostic {
        let severity = match diagnostic.severity {
            CoreDiagnosticSeverity::Error => InputDiagnosticSeverity::Error,
            CoreDiagnosticSeverity::Warning => InputDiagnosticSeverity::Warning,
            CoreDiagnosticSeverity::Info => InputDiagnosticSeverity::Info,
            CoreDiagnosticSeverity::Hint => InputDiagnosticSeverity::Hint,
        };

        let start = InputPosition::new(diagnostic.range.start.line, diagnostic.range.start.column);
        let mut end = InputPosition::new(diagnostic.range.end.line, diagnostic.range.end.column);

        if end.line == start.line && end.character <= start.character {
            end.character = start.character.saturating_add(1);
        }

        InputDiagnostic::new(start..end, diagnostic.message).with_severity(severity)
    }
}
