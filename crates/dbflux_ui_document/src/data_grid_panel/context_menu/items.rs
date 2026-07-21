use super::ContextMenuItem;
use dbflux_components::components::data_table::ContextMenuAction;
use dbflux_components::icons::AppIcon;

pub(super) fn build_context_menu_items(
    is_editable: bool,
    is_document_view: bool,
    has_row_target: bool,
    can_chart: bool,
    inspect_row_enabled: bool,
) -> Vec<ContextMenuItem> {
    if is_document_view {
        let mut items = Vec::new();

        if has_row_target {
            items.extend([
                ContextMenuItem {
                    label: "Copy",
                    action: Some(ContextMenuAction::Copy),
                    icon: Some(AppIcon::Layers),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "View Document",
                    action: Some(ContextMenuAction::EditInModal),
                    icon: Some(AppIcon::Maximize2),
                    is_separator: false,
                    is_danger: false,
                },
            ]);
        }

        if is_editable {
            if !items.is_empty() {
                items.push(ContextMenuItem {
                    label: "",
                    action: None,
                    icon: None,
                    is_separator: true,
                    is_danger: false,
                });
            }

            items.push(ContextMenuItem {
                label: "Add Document",
                action: Some(ContextMenuAction::AddRow),
                icon: Some(AppIcon::Plus),
                is_separator: false,
                is_danger: false,
            });

            if has_row_target {
                items.extend([
                    ContextMenuItem {
                        label: "Duplicate Document",
                        action: Some(ContextMenuAction::DuplicateRow),
                        icon: Some(AppIcon::Layers),
                        is_separator: false,
                        is_danger: false,
                    },
                    ContextMenuItem {
                        label: "Delete Document",
                        action: Some(ContextMenuAction::DeleteRow),
                        icon: Some(AppIcon::Delete),
                        is_separator: false,
                        is_danger: true,
                    },
                ]);
            }
        }

        return items;
    }

    let mut items = vec![ContextMenuItem {
        label: "Copy",
        action: Some(ContextMenuAction::Copy),
        icon: Some(AppIcon::Layers),
        is_separator: false,
        is_danger: false,
    }];

    if is_editable {
        if has_row_target {
            items.extend([
                ContextMenuItem {
                    label: "Paste",
                    action: Some(ContextMenuAction::Paste),
                    icon: Some(AppIcon::Download),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Edit",
                    action: Some(ContextMenuAction::Edit),
                    icon: Some(AppIcon::Pencil),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Edit in Modal",
                    action: Some(ContextMenuAction::EditInModal),
                    icon: Some(AppIcon::Maximize2),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "",
                    action: None,
                    icon: None,
                    is_separator: true,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Set to Default",
                    action: Some(ContextMenuAction::SetDefault),
                    icon: Some(AppIcon::RotateCcw),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Set to NULL",
                    action: Some(ContextMenuAction::SetNull),
                    icon: Some(AppIcon::X),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "",
                    action: None,
                    icon: None,
                    is_separator: true,
                    is_danger: false,
                },
            ]);
        }

        items.push(ContextMenuItem {
            label: "Add Row",
            action: Some(ContextMenuAction::AddRow),
            icon: Some(AppIcon::Plus),
            is_separator: false,
            is_danger: false,
        });

        if has_row_target {
            if inspect_row_enabled {
                items.push(ContextMenuItem {
                    label: "Inspect Row",
                    action: Some(ContextMenuAction::InspectRow),
                    icon: Some(AppIcon::Info),
                    is_separator: false,
                    is_danger: false,
                });
            }

            items.extend([
                ContextMenuItem {
                    label: "Duplicate Row",
                    action: Some(ContextMenuAction::DuplicateRow),
                    icon: Some(AppIcon::Layers),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Delete Row",
                    action: Some(ContextMenuAction::DeleteRow),
                    icon: Some(AppIcon::Delete),
                    is_separator: false,
                    is_danger: true,
                },
            ]);
        }
    }

    if can_chart {
        items.push(ContextMenuItem {
            label: "",
            action: None,
            icon: None,
            is_separator: true,
            is_danger: false,
        });
        items.push(ContextMenuItem {
            label: "Chart this query",
            action: Some(ContextMenuAction::ChartThisQuery),
            icon: Some(AppIcon::ChartSpline),
            is_separator: false,
            is_danger: false,
        });
    }

    items
}
