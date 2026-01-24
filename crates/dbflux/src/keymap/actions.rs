use gpui::actions;

actions!(
    dbflux,
    [
        ToggleCommandPalette,
        NewQueryTab,
        CloseCurrentTab,
        NextTab,
        PrevTab,
        SwitchToTab1,
        SwitchToTab2,
        SwitchToTab3,
        SwitchToTab4,
        SwitchToTab5,
        SwitchToTab6,
        SwitchToTab7,
        SwitchToTab8,
        SwitchToTab9,
        FocusSidebar,
        FocusEditor,
        FocusResults,
        FocusBackgroundTasks,
        CycleFocusForward,
        CycleFocusBackward,
        RunQuery,
        Cancel,
        ExportResults,
        OpenConnectionManager,
        Disconnect,
        RefreshSchema,
        ToggleEditor,
        ToggleResults,
        ToggleTasks,
        // List navigation
        SelectNext,
        SelectPrev,
        SelectFirst,
        SelectLast,
        Execute,
        ExpandCollapse,
        // Column navigation (Results)
        ColumnLeft,
        ColumnRight,
        // Directional panel navigation
        FocusLeft,
        FocusRight,
        FocusUp,
        FocusDown,
        // Saved queries / history actions
        Delete,
        ToggleFavorite,
        Rename,
        FocusSearch,
        SaveQuery,
        // Settings
        OpenSettings,
    ]
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actions_module_compiles() {
        let _ = ToggleCommandPalette;
    }
}
