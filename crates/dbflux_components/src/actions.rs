use gpui::actions;

actions!(
    dbflux,
    [
        Cancel,
        // List navigation
        SelectNext,
        SelectPrev,
        Execute,
        // CRUD / item actions
        Delete,
        ToggleFavorite,
        Rename,
        // Search / saved queries
        FocusSearch,
        SaveQuery,
        // Save the value of a modal editor (cell editor, document preview)
        SaveEdit,
    ]
);
