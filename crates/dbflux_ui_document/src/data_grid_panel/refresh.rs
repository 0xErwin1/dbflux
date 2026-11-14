use super::*;

impl DataGridPanel {
    pub fn refresh_policy(&self) -> RefreshPolicy {
        self.refresh.refresh_policy
    }

    pub fn set_refresh_policy(&mut self, policy: RefreshPolicy, cx: &mut Context<Self>) {
        if self.refresh.refresh_policy == policy {
            return;
        }

        self.refresh.refresh_policy = policy;
        self.update_refresh_timer(cx);
        cx.notify();
    }

    pub(super) fn update_refresh_timer(&mut self, cx: &mut Context<Self>) {
        self.refresh._refresh_timer = None;

        if !self.supports_auto_refresh() {
            return;
        }

        let Some(duration) = self.refresh.refresh_policy.duration() else {
            return;
        };

        self.refresh._refresh_timer = Some(cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor().timer(duration).await;

                cx.update(|cx| {
                    let Some(entity) = this.upgrade() else {
                        return;
                    };

                    entity.update(cx, |panel, cx| {
                        // A tick with unsaved edits is skipped rather than
                        // reported: the reload would drop them, and a warning
                        // on every interval would be noise.
                        if !panel.refresh.refresh_policy.is_auto()
                            || !panel.supports_auto_refresh()
                            || panel.runner.is_primary_active()
                            || panel.has_pending_edits(cx)
                        {
                            return;
                        }

                        let settings = panel.app_state.read(cx).general_settings();

                        if settings.auto_refresh_pause_on_error
                            && panel.refresh.state == GridState::Error
                        {
                            return;
                        }

                        if settings.auto_refresh_only_if_visible && !panel.is_active_tab {
                            return;
                        }

                        if matches!(panel.source, DataSource::QueryResult { .. }) {
                            if let Some(trp) = panel.chart.chart_source_time_range_panel.clone() {
                                trp.update(cx, |p, cx| p.emit_initial(cx));
                            }
                        } else {
                            panel.pending.refresh = true;
                            cx.notify();
                        }
                    });
                });
            }
        }));
    }
}
