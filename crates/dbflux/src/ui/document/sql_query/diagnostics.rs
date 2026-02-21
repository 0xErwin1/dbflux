use super::*;

impl SqlQueryDocument {
    pub(super) fn refresh_editor_diagnostics(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
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
