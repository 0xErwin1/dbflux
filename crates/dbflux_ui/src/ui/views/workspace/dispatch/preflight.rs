use crate::keymap::Command;

pub(super) fn sidebar_tree_command_is_blocked_by_search_focus(cmd: Command) -> bool {
    matches!(
        cmd,
        Command::SelectNext
            | Command::SelectPrev
            | Command::SelectFirst
            | Command::SelectLast
            | Command::Execute
            | Command::ExpandCollapse
            | Command::ColumnLeft
            | Command::ColumnRight
            | Command::Cancel
            | Command::Rename
            | Command::Delete
            | Command::CreateFolder
            | Command::SidebarNextTab
            | Command::OpenItemMenu
            | Command::ExtendSelectNext
            | Command::ExtendSelectPrev
            | Command::ToggleSelection
            | Command::MoveSelectedUp
            | Command::MoveSelectedDown
    )
}

#[cfg(test)]
mod tests {
    use super::sidebar_tree_command_is_blocked_by_search_focus;
    use crate::keymap::Command;

    #[test]
    fn sidebar_search_focus_blocks_tree_navigation_commands() {
        assert!(sidebar_tree_command_is_blocked_by_search_focus(
            Command::SelectNext
        ));
        assert!(sidebar_tree_command_is_blocked_by_search_focus(
            Command::Execute
        ));
        assert!(sidebar_tree_command_is_blocked_by_search_focus(
            Command::Delete
        ));
        assert!(sidebar_tree_command_is_blocked_by_search_focus(
            Command::MoveSelectedDown
        ));
    }

    #[test]
    fn sidebar_search_focus_leaves_unrelated_commands_available() {
        assert!(!sidebar_tree_command_is_blocked_by_search_focus(
            Command::RunQuery
        ));
        assert!(!sidebar_tree_command_is_blocked_by_search_focus(
            Command::ToggleSidebar
        ));
        assert!(!sidebar_tree_command_is_blocked_by_search_focus(
            Command::OpenSettings
        ));
    }
}
