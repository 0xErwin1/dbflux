use super::*;
use std::collections::HashSet;
use uuid::Uuid;

impl Workspace {
    /// Reconnects to the profiles connected at the last quit and to the ones
    /// the restored session documents use.
    pub(in crate::ui::views::workspace) fn reopen_last_connections(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        let restored: Vec<Uuid> = self
            .tab_manager
            .read(cx)
            .documents()
            .iter()
            .filter_map(|doc| doc.meta_snapshot(cx).connection_id)
            .collect();

        let app_state = self.app_state.read(cx);
        let saved = app_state.last_connected_profiles();
        let existing: HashSet<Uuid> = app_state
            .profiles()
            .iter()
            .map(|profile| profile.id)
            .collect();
        let connected: HashSet<Uuid> = app_state.connections().keys().copied().collect();

        let profile_ids = profiles_to_reopen(&saved, &restored, &existing, &connected);
        let sidebar = self.sidebar.clone();

        for profile_id in profile_ids {
            sidebar.update(cx, |sidebar, cx| {
                sidebar.connect_to_profile(profile_id, cx);
            });
        }
    }
}

/// The profiles to reconnect at startup: the saved set followed by the
/// restored tabs' connections, each once, without deleted or already
/// connected profiles.
pub(super) fn profiles_to_reopen(
    saved: &[Uuid],
    restored: &[Uuid],
    existing: &HashSet<Uuid>,
    connected: &HashSet<Uuid>,
) -> Vec<Uuid> {
    let mut seen = HashSet::new();

    saved
        .iter()
        .chain(restored)
        .copied()
        .filter(|id| existing.contains(id) && !connected.contains(id))
        .filter(|id| seen.insert(*id))
        .collect()
}

#[cfg(test)]
mod tests;
