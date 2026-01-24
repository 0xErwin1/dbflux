use super::ContextId;

/// Tracks which UI area currently has keyboard focus.
///
/// This determines which context-specific keybindings are active and
/// where navigation commands (like SelectNext) are routed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum FocusTarget {
    /// SQL editor area (default focus).
    #[default]
    Editor,

    /// Schema tree in the sidebar.
    Sidebar,

    /// Background tasks panel.
    BackgroundTasks,

    /// Results table area.
    Results,
}

impl FocusTarget {
    /// Converts this focus target to its corresponding context ID.
    pub fn to_context(self) -> ContextId {
        match self {
            FocusTarget::Editor => ContextId::Editor,
            FocusTarget::Sidebar => ContextId::Sidebar,
            FocusTarget::BackgroundTasks => ContextId::BackgroundTasks,
            FocusTarget::Results => ContextId::Results,
        }
    }

    /// Returns the next focus target in the Tab cycle order.
    /// Order: Editor -> Sidebar -> BackgroundTasks -> Results -> Editor
    pub fn next(&self) -> FocusTarget {
        match self {
            FocusTarget::Editor => FocusTarget::Sidebar,
            FocusTarget::Sidebar => FocusTarget::BackgroundTasks,
            FocusTarget::BackgroundTasks => FocusTarget::Results,
            FocusTarget::Results => FocusTarget::Editor,
        }
    }

    /// Returns the previous focus target in the Tab cycle order.
    /// Order: Editor -> Results -> BackgroundTasks -> Sidebar -> Editor
    pub fn prev(&self) -> FocusTarget {
        match self {
            FocusTarget::Editor => FocusTarget::Results,
            FocusTarget::Results => FocusTarget::BackgroundTasks,
            FocusTarget::BackgroundTasks => FocusTarget::Sidebar,
            FocusTarget::Sidebar => FocusTarget::Editor,
        }
    }

    /// Returns a human-readable name for this focus target.
    #[allow(dead_code)]
    pub fn display_name(&self) -> &'static str {
        match self {
            FocusTarget::Editor => "Editor",
            FocusTarget::Sidebar => "Sidebar",
            FocusTarget::BackgroundTasks => "Background Tasks",
            FocusTarget::Results => "Results",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FocusTarget;

    #[test]
    fn focus_cycle_skips_history() {
        assert_eq!(FocusTarget::Editor.next(), FocusTarget::Sidebar);
        assert_eq!(FocusTarget::Sidebar.next(), FocusTarget::BackgroundTasks);
        assert_eq!(FocusTarget::BackgroundTasks.next(), FocusTarget::Results);
        assert_eq!(FocusTarget::Results.next(), FocusTarget::Editor);
    }
}
