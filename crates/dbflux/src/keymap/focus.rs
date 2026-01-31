use super::ContextId;

/// Tracks which UI area currently has keyboard focus.
///
/// This determines which context-specific keybindings are active and
/// where navigation commands (like SelectNext) are routed.
///
/// Note: The actual context (Editor vs Results) for the Document area
/// is determined by the active document's internal focus state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum FocusTarget {
    /// Active document tab area (editor + results combined).
    /// The document itself determines if editor or results has internal focus.
    #[default]
    Document,

    /// Schema tree in the sidebar.
    Sidebar,

    /// Background tasks panel.
    BackgroundTasks,
}

impl FocusTarget {
    /// Returns the base context for this focus target.
    /// For Document, the actual context is determined by the document's internal state.
    pub fn to_context(self) -> ContextId {
        match self {
            FocusTarget::Document => ContextId::Editor, // Default, document overrides
            FocusTarget::Sidebar => ContextId::Sidebar,
            FocusTarget::BackgroundTasks => ContextId::BackgroundTasks,
        }
    }

    /// Returns the next focus target in the Tab cycle order.
    pub fn next(&self) -> FocusTarget {
        match self {
            FocusTarget::Document => FocusTarget::Sidebar,
            FocusTarget::Sidebar => FocusTarget::BackgroundTasks,
            FocusTarget::BackgroundTasks => FocusTarget::Document,
        }
    }

    /// Returns the previous focus target in the Tab cycle order.
    pub fn prev(&self) -> FocusTarget {
        match self {
            FocusTarget::Document => FocusTarget::BackgroundTasks,
            FocusTarget::BackgroundTasks => FocusTarget::Sidebar,
            FocusTarget::Sidebar => FocusTarget::Document,
        }
    }

    /// Returns a human-readable name for this focus target.
    #[allow(dead_code)]
    pub fn display_name(&self) -> &'static str {
        match self {
            FocusTarget::Document => "Document",
            FocusTarget::Sidebar => "Sidebar",
            FocusTarget::BackgroundTasks => "Background Tasks",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FocusTarget;

    #[test]
    fn focus_cycle() {
        assert_eq!(FocusTarget::Document.next(), FocusTarget::Sidebar);
        assert_eq!(FocusTarget::Sidebar.next(), FocusTarget::BackgroundTasks);
        assert_eq!(FocusTarget::BackgroundTasks.next(), FocusTarget::Document);
    }
}
