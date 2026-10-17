use super::*;

/// Window content for the rendered tests: the builder rail, as the
/// workspace hosts it, or the grid itself.
struct RailHost {
    grid: gpui::Entity<DataGridPanel>,
    show_builder: bool,
}

impl gpui::Render for RailHost {
    fn render(
        &mut self,
        _window: &mut gpui::Window,
        cx: &mut gpui::Context<Self>,
    ) -> impl gpui::IntoElement {
        use gpui::{IntoElement, ParentElement, Styled};

        let builder = self
            .grid
            .read(cx)
            .collection
            .builder
            .panel
            .clone()
            .filter(|_| self.show_builder);

        let content = match builder {
            Some(builder) => builder.into_any_element(),
            None => self.grid.clone().into_any_element(),
        };

        gpui::div().size_full().child(content)
    }
}

fn center(bounds: gpui::Bounds<gpui::Pixels>) -> gpui::Point<gpui::Pixels> {
    gpui::point(
        bounds.origin.x + bounds.size.width / 2.0,
        bounds.origin.y + bounds.size.height / 2.0,
    )
}

/// Opens the builder of a grid rendered, like the workspace rail, as
/// the content of a window `width` wide. The filter slot holds `filter`.
fn rendered_rail<'a>(
    cx: &'a mut TestAppContext,
    filter: &str,
) -> (
    gpui::Entity<DataGridPanel>,
    gpui::Entity<RailHost>,
    &'a mut VisualTestContext,
) {
    rendered_rail_with(cx, filter, builder_features())
}

fn rendered_rail_with<'a>(
    cx: &'a mut TestAppContext,
    filter: &str,
    features: DocumentFeatures,
) -> (
    gpui::Entity<DataGridPanel>,
    gpui::Entity<RailHost>,
    &'a mut VisualTestContext,
) {
    let (app_state, profile_id) = register_stub_connection(
        cx,
        Arc::new(StubDocumentConnection::new(
            DatabaseCategory::Document,
            features,
            true,
        )),
    );

    let grid_slot: Rc<RefCell<Option<gpui::Entity<DataGridPanel>>>> = Rc::new(RefCell::new(None));
    let grid_handle = grid_slot.clone();
    let (host, window) = cx.add_window_view(|window, cx| {
        let grid = cx.new(|cx| {
            let source = DataSource::Collection {
                profile_id,
                collection: CollectionRef::new("shop", "orders"),
                pagination: Pagination::default(),
                total_docs: None,
            };
            DataGridPanel::new_internal(source, app_state, vec![], window, cx)
        });
        grid_handle.replace(Some(grid.clone()));
        RailHost {
            grid,
            show_builder: false,
        }
    });
    let grid = grid_slot.borrow().clone().expect("grid");

    set_slot_texts(&grid, window, filter, "");
    open_builder(&grid, window);
    host.update(window, |host, cx| {
        host.show_builder = true;
        cx.notify();
    });
    window.run_until_parked();

    (grid, host, window)
}

fn bounds_of(
    window: &mut VisualTestContext,
    selector: String,
) -> Option<gpui::Bounds<gpui::Pixels>> {
    window.debug_bounds(selector.leak())
}

#[gpui::test]
fn clicking_an_operator_option_changes_the_condition_and_its_slot(cx: &mut TestAppContext) {
    use gpui::Modifiers;

    let (grid, _host, window) = rendered_rail(cx, r#"{"age": {"$gt": 30}}"#);
    let (id, _, operator) = first_condition(&grid, window);
    assert_eq!(operator, DocumentOperator::Gt);

    let trigger = bounds_of(window, format!("doc-builder-operator-{id}-trigger"))
        .expect("the operator select has its own trigger id");
    window.simulate_click(center(trigger), Modifiers::none());
    window.run_until_parked();

    // With no sample the field offers every operator; the list holds
    // them all, the last one included.
    assert!(bounds_of(window, format!("doc-builder-operator-{id}-option-all")).is_some());

    let option = bounds_of(window, format!("doc-builder-operator-{id}-option-eq"))
        .expect("every operator is an addressable option");
    window.simulate_click(center(option), Modifiers::none());
    window.run_until_parked();

    let (_, _, operator) = first_condition(&grid, window);
    assert_eq!(operator, DocumentOperator::Eq);
    assert!(
        bounds_of(window, format!("doc-builder-operator-{id}-option-eq")).is_none(),
        "choosing an option closes the list"
    );
    assert_eq!(slot_texts(&grid, window).0, r#"{"age": 30}"#);
}

#[gpui::test]
fn the_operator_list_answers_the_keyboard(cx: &mut TestAppContext) {
    use gpui::Modifiers;

    let (grid, _host, window) = rendered_rail(cx, r#"{"age": {"$gt": 30}}"#);
    let (id, _, _) = first_condition(&grid, window);

    let trigger =
        bounds_of(window, format!("doc-builder-operator-{id}-trigger")).expect("operator trigger");
    window.simulate_click(center(trigger), Modifiers::none());
    window.run_until_parked();

    window.simulate_keystrokes("down enter");
    window.run_until_parked();

    let (_, _, operator) = first_condition(&grid, window);
    assert_eq!(operator, DocumentOperator::Gte, "down from $gt is $gte");
    assert_eq!(slot_texts(&grid, window).0, r#"{"age": {"$gte": 30}}"#);

    window.simulate_click(center(trigger), Modifiers::none());
    window.run_until_parked();
    window.simulate_keystrokes("escape");
    window.run_until_parked();
    assert!(bounds_of(window, format!("doc-builder-operator-{id}-option-eq")).is_none());
    assert_eq!(first_condition(&grid, window).2, DocumentOperator::Gte);
}

#[gpui::test]
fn aggregate_mode_shows_the_pipeline_summary_and_the_read_only_banner(cx: &mut TestAppContext) {
    let (grid, host, window) =
        rendered_rail_with(cx, r#"{"status": "failed"}"#, aggregate_features());
    let rail = builder(&grid, window);

    window.update(|_, cx| rail.update(cx, |rail, cx| rail.add_group_stage(cx)));
    window.run_until_parked();

    assert!(bounds_of(window, "doc-builder-group-stage".to_string()).is_some());
    assert!(
        bounds_of(window, "doc-builder-match-summary".to_string()).is_some(),
        "the filter collapses to its $match summary"
    );

    host.update(window, |host, cx| {
        host.show_builder = false;
        cx.notify();
    });
    window.run_until_parked();

    assert!(bounds_of(window, "collection-slot-pipeline".to_string()).is_some());
    assert!(
        bounds_of(window, "collection-slot-filter".to_string()).is_none(),
        "the find slots give way to the pipeline summary"
    );

    window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_run(cx)));
    window.run_until_parked();

    assert!(bounds_of(window, "aggregate-builder-read-only".to_string()).is_some());
    assert!(
        bounds_of(window, "collection-slot-pipeline".to_string()).is_some(),
        "the Aggregate view shows the builder's pipeline in place of its editor"
    );

    window.update(|_, cx| rail.update(cx, |rail, cx| rail.remove_group_stage(cx)));
    window.run_until_parked();

    assert!(bounds_of(window, "collection-slot-pipeline".to_string()).is_none());
    assert!(
        bounds_of(window, "aggregate-builder-read-only".to_string()).is_some(),
        "the banner stays with the results it explains"
    );
}

/// Asserts the preview sits between the scrolling cards and the footer,
/// inside the window.
fn assert_preview_is_pinned(window: &mut VisualTestContext) -> gpui::Bounds<gpui::Pixels> {
    let sections = bounds_of(window, "doc-builder-body".to_string()).expect("scrolling body");
    let preview = bounds_of(window, "doc-builder-preview".to_string()).expect("preview");
    let footer = bounds_of(window, "doc-builder-footer".to_string()).expect("footer");

    assert!(
        preview.origin.y >= sections.bottom(),
        "the preview ({preview:?}) is outside the scrolling cards ({sections:?})"
    );
    assert!(
        preview.bottom() <= footer.origin.y,
        "the preview ({preview:?}) sits above the footer ({footer:?})"
    );

    preview
}

#[gpui::test]
fn the_preview_stays_pinned_above_the_footer_in_both_modes(cx: &mut TestAppContext) {
    let (grid, _host, window) =
        rendered_rail_with(cx, r#"{"status": "failed"}"#, aggregate_features());
    let rail = builder(&grid, window);

    let before = assert_preview_is_pinned(window);

    window.update(|_, cx| {
        rail.update(cx, |rail, cx| {
            let root = rail.draft().filter.id;
            for _ in 0..12 {
                rail.add_condition(root, cx);
            }
        });
    });
    window.run_until_parked();

    // The empty conditions add the "not run yet" line under the text, so
    // the pane grows upwards; it stays anchored to the footer.
    let after = assert_preview_is_pinned(window);
    assert_eq!(
        before.bottom(),
        after.bottom(),
        "more cards scroll behind the preview instead of moving it"
    );

    window.update(|_, cx| rail.update(cx, |rail, cx| rail.add_group_stage(cx)));
    window.run_until_parked();
    assert_eq!(
        window.update(|_, cx| rail.read(cx).mode()),
        DocumentQueryMode::Aggregate
    );
    assert_preview_is_pinned(window);
}

#[gpui::test]
fn a_saved_find_reopens_from_the_list_and_rewrites_the_slots(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
    );
    set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "");
    open_builder(&panel, window);
    let rail = builder(&panel, window);

    assert!(
        !window.update(|_, cx| rail.read(cx).can_save(cx)),
        "a query needs a name before it can be saved"
    );
    set_query_name(&rail, window, "  adults ");
    window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_save(cx)));
    window.run_until_parked();

    let saved = stored_queries(&panel, window);
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].name, "adults");
    assert_eq!(saved[0].mode, DocumentQueryMode::Find);
    assert_eq!(
        window.update(|_, cx| rail.read(cx).loaded_id().map(str::to_string)),
        Some(saved[0].id.clone())
    );

    set_slot_texts(&panel, window, r#"{ "status": "failed" }"#, "");
    sync_slots(&panel, window);
    set_query_name(&rail, window, "");
    assert_eq!(first_condition(&panel, window).1, "status");

    window.update(|_, cx| rail.update(cx, |rail, cx| rail.toggle_saved_menu(cx)));
    window.run_until_parked();
    let listed = window.update(|_, cx| rail.read(cx).saved_queries().to_vec());
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "adults");

    window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_open_saved(&listed[0].id, cx)));
    window.run_until_parked();

    let (_, path, operator) = first_condition(&panel, window);
    assert_eq!((path.as_str(), operator), ("age", DocumentOperator::Gt));
    assert_eq!(slot_texts(&panel, window).0, r#"{"age": {"$gt": 30}}"#);
    window.update(|_, cx| {
        let rail = rail.read(cx);
        assert_eq!(rail.mode(), DocumentQueryMode::Find);
        assert_eq!(rail.query_name(cx), "adults");
        assert!(!rail.is_conflicted());
    });
}

#[gpui::test]
fn a_saved_aggregation_reopens_in_aggregate_mode_and_leaves_the_slots(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
    );
    set_slot_texts(&panel, window, r#"{ "status": "failed" }"#, "");
    open_builder(&panel, window);
    let rail = builder(&panel, window);

    window.update(|_, cx| rail.update(cx, |rail, cx| rail.add_group_stage(cx)));
    set_query_name(&rail, window, "failures by status");
    window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_save(cx)));
    window.run_until_parked();

    let saved = stored_queries(&panel, window);
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].mode, DocumentQueryMode::Aggregate);

    window.update(|_, cx| rail.update(cx, |rail, cx| rail.remove_group_stage(cx)));
    window.run_until_parked();
    set_slot_texts(&panel, window, r#"{"other": 1}"#, "");
    sync_slots(&panel, window);

    window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_open_saved(&saved[0].id, cx)));
    window.run_until_parked();

    assert_eq!(
        window.update(|_, cx| rail.read(cx).mode()),
        DocumentQueryMode::Aggregate
    );
    assert_eq!(first_condition(&panel, window).1, "status");
    assert_eq!(
        slot_texts(&panel, window).0,
        r#"{"other": 1}"#,
        "opening an aggregation writes no slot"
    );
    let stages = window
        .update(|_, cx| panel.read(cx).document_builder_pipeline_stages(cx))
        .expect("the query bar shows the pipeline");
    assert!(stages.contains(&"$group".to_string()), "{stages:?}");
}

#[gpui::test]
fn an_aggregation_does_not_open_where_aggregations_cannot_run(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
    );
    set_slot_texts(&panel, window, r#"{ "status": "failed" }"#, "");
    open_builder(&panel, window);
    let rail = builder(&panel, window);

    let id = window.update(|_, cx| {
        let grid = panel.read(cx);
        let scope = grid.document_query_scope().expect("scope");
        let spec = dbflux_core::DocumentQuerySpec {
            mode: DocumentQueryMode::Aggregate,
            group: Some(dbflux_core::DocumentGroupStage::default()),
            ..dbflux_core::DocumentQuerySpec::default()
        };
        grid.app_state.clone().update(cx, |app, _| {
            app.saved_document_queries
                .save(&scope, "grouped", &spec)
                .expect("save")
                .id
        })
    });

    window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_open_saved(&id, cx)));
    window.run_until_parked();

    window.update(|_, cx| {
        let rail = rail.read(cx);
        assert_eq!(rail.mode(), DocumentQueryMode::Find);
        assert_eq!(rail.loaded_id(), None);
    });
    assert_eq!(first_condition(&panel, window).1, "status");
}

#[gpui::test]
fn deleting_a_saved_query_removes_it_from_the_list(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
    );
    set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "");
    open_builder(&panel, window);
    let rail = builder(&panel, window);

    set_query_name(&rail, window, "adults");
    window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_save(cx)));
    window.update(|_, cx| rail.update(cx, |rail, cx| rail.toggle_saved_menu(cx)));
    window.run_until_parked();

    let id = window.update(|_, cx| rail.read(cx).saved_queries()[0].id.clone());
    window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_delete_saved(&id, cx)));
    window.run_until_parked();

    assert!(stored_queries(&panel, window).is_empty());
    window.update(|_, cx| {
        let rail = rail.read(cx);
        assert!(rail.saved_queries().is_empty());
        assert_eq!(
            rail.loaded_id(),
            None,
            "the deleted query is no longer the loaded one"
        );
    });
}

#[gpui::test]
fn clicking_a_saved_query_in_the_list_opens_it(cx: &mut TestAppContext) {
    use gpui::Modifiers;

    let (grid, _host, window) = rendered_rail(cx, r#"{"age": {"$gt": 30}}"#);
    let rail = builder(&grid, window);

    set_query_name(&rail, window, "adults");
    window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_save(cx)));
    set_slot_texts(&grid, window, r#"{"status": "failed"}"#, "");
    sync_slots(&grid, window);
    window.update(|_, cx| rail.update(cx, |rail, cx| rail.toggle_saved_menu(cx)));
    window.run_until_parked();

    assert!(bounds_of(window, "doc-builder-saved-menu".to_string()).is_some());
    let id = stored_queries(&grid, window)[0].id.clone();
    let row = bounds_of(window, format!("doc-builder-saved-{id}")).expect("a row per query");
    window.simulate_click(center(row), Modifiers::none());
    window.run_until_parked();

    assert!(
        bounds_of(window, "doc-builder-saved-menu".to_string()).is_none(),
        "opening a query closes the list"
    );
    assert_eq!(first_condition(&grid, window).1, "age");
    assert_eq!(slot_texts(&grid, window).0, r#"{"age": {"$gt": 30}}"#);
}

/// A schema sample with a string `tier` and a numeric `price`.
fn give_rail_a_sample(
    rail: &gpui::Entity<crate::document_builder::DocumentBuilderPanel>,
    window: &mut VisualTestContext,
) {
    let field = |path: &str, type_name: &str| dbflux_core::FieldSchemaStats {
        path: path.to_string(),
        presence: 10,
        types: vec![dbflux_core::FieldTypeShare {
            type_name: type_name.to_string(),
            count: 10,
        }],
        summary: dbflux_core::FieldValueSummary::Empty,
    };
    let sample = dbflux_core::CollectionSchemaSample {
        sampled_documents: 10,
        total_documents: Some(10),
        fields: (0..20)
            .map(|index| field(&format!("attribute{index:02}"), "String"))
            .chain([field("tier", "String"), field("price", "Double")])
            .collect(),
    };

    window.update(|_, cx| rail.update(cx, |rail, cx| rail.set_schema(&sample, cx)));
    window.run_until_parked();
}

fn click(window: &mut VisualTestContext, selector: String) {
    let bounds =
        bounds_of(window, selector.clone()).unwrap_or_else(|| panic!("{selector} is rendered"));
    window.simulate_click(center(bounds), gpui::Modifiers::none());
    window.run_until_parked();
}

#[gpui::test]
fn picking_a_field_adds_the_group_key(cx: &mut TestAppContext) {
    let (grid, _host, window) = rendered_rail_with(cx, "", aggregate_features());
    window.simulate_resize(gpui::size(gpui::px(540.0), gpui::px(760.0)));
    let rail = builder(&grid, window);
    give_rail_a_sample(&rail, window);
    window.update(|_, cx| rail.update(cx, |rail, cx| rail.add_group_stage(cx)));
    window.run_until_parked();

    click(window, "doc-builder-group-key-add".to_string());

    // Twenty fields sort before `tier`, so its row starts below the
    // list's visible height until the search narrows the list.
    let list = bounds_of(window, "doc-builder-picker-list".to_string()).expect("list");
    let row = bounds_of(window, "doc-builder-picker-field-tier".to_string()).expect("row");
    assert!(
        row.origin.y >= list.bottom(),
        "{row:?} is scrolled out of {list:?}"
    );

    window.simulate_input("tier");
    window.run_until_parked();
    click(window, "doc-builder-picker-field-tier".to_string());

    window.update(|_, cx| {
        let rail = rail.read(cx);
        assert_eq!(
            rail.draft().group.as_ref().map(|stage| stage.keys.clone()),
            Some(vec!["tier".to_string()])
        );
    });
    let pipeline = window.update(|_, cx| rail.read(cx).pipeline_text());
    assert!(
        pipeline
            .as_deref()
            .is_some_and(|text| text.contains(r#""_id": "$tier""#)),
        "{pipeline:?}"
    );
}

#[gpui::test]
fn picking_a_field_sets_the_accumulator_field(cx: &mut TestAppContext) {
    let (grid, _host, window) = rendered_rail_with(cx, "", aggregate_features());
    let rail = builder(&grid, window);
    give_rail_a_sample(&rail, window);

    let id = window.update(|_, cx| {
        rail.update(cx, |rail, cx| {
            rail.add_group_stage(cx);
            let id = rail.draft().group.as_ref().unwrap().accumulators[0].id;
            rail.set_accumulator_op(id, AccumulatorOp::Sum, cx);
            id
        })
    });
    window.run_until_parked();

    click(window, format!("doc-builder-acc-field-{id}"));
    click(window, "doc-builder-picker-field-price".to_string());

    let path = window.update(|_, cx| {
        rail.read(cx)
            .draft()
            .accumulator(id)
            .map(|accumulator| accumulator.path.clone())
    });
    assert_eq!(path.as_deref(), Some("price"));
}

#[gpui::test]
fn the_field_picker_opens_inside_the_rail(cx: &mut TestAppContext) {
    let (grid, _host, window) = rendered_rail_with(cx, "", aggregate_features());
    let rail_width = crate::document_builder::RAIL_WIDTH;
    window.simulate_resize(gpui::size(rail_width, gpui::px(760.0)));
    let rail = builder(&grid, window);
    give_rail_a_sample(&rail, window);
    window.update(|_, cx| rail.update(cx, |rail, cx| rail.add_group_stage(cx)));
    window.run_until_parked();

    // Group keys push "+ field" to the right, until a picker opened at
    // its left edge would no longer fit in the rail.
    let mut picker = None;
    for index in 0..10 {
        click(window, "doc-builder-group-key-add".to_string());
        let add = bounds_of(window, "doc-builder-group-key-add".to_string()).expect("add");
        let open = bounds_of(window, "doc-builder-picker".to_string()).expect("picker");
        if add.origin.x + open.size.width > rail_width {
            picker = Some(open);
            break;
        }

        window.update(|_, cx| {
            rail.update(cx, |rail, cx| {
                rail.pick(&format!("attribute{index:02}"), cx)
            })
        });
        window.run_until_parked();
    }

    let picker = picker.expect("the group keys moved the add button far enough right");
    assert!(
        picker.origin.x >= gpui::px(0.0) && picker.origin.x + picker.size.width <= rail_width,
        "the picker ({picker:?}) stays inside the {rail_width:?} rail"
    );
}

#[test]
fn saved_query_audit_events_pass_config_validation() {
    let scope = dbflux_storage::DocumentQueryScope::new("profile-1", "shop", "orders");

    for action in [
        dbflux_core::observability::actions::CONFIG_CREATE,
        dbflux_core::observability::actions::CONFIG_UPDATE,
        dbflux_core::observability::actions::CONFIG_DELETE,
    ] {
        let event = super::super::saved_query_audit_event(
            action,
            "Saved \"adults\"",
            "id-1",
            &scope,
            Some("mongodb"),
        );

        dbflux_audit::AuditService::validate_event(&event).expect("a valid config event");
        assert_eq!(event.object_type.as_deref(), Some("saved_document_query"));
        assert_eq!(event.object_id.as_deref(), Some("id-1"));
        assert_eq!(event.driver_id.as_deref(), Some("mongodb"));
    }

    let event = super::super::saved_query_audit_event(
        dbflux_core::observability::actions::CONFIG_DELETE,
        "Deleted a saved document query",
        "id-1",
        &scope,
        None,
    );
    assert_eq!(event.driver_id, None, "an unknown driver is left unset");
    assert_eq!(event.connection_id.as_deref(), Some("profile-1"));
    assert_eq!(event.database_name.as_deref(), Some("shop"));
}

#[gpui::test]
fn the_filter_slot_keeps_a_usable_width_beside_the_rail(cx: &mut TestAppContext) {
    use gpui::{px, size};

    let (_grid, host, window) = rendered_rail(cx, "");
    host.update(window, |host, cx| {
        host.show_builder = false;
        cx.notify();
    });
    window.simulate_resize(size(px(520.0), px(700.0)));
    window.run_until_parked();

    let filter =
        bounds_of(window, "collection-slot-filter".to_string()).expect("filter slot rendered");
    assert!(
        filter.size.width >= px(240.0),
        "the filter slot is {:?} wide in a 520 px grid",
        filter.size.width
    );

    window.simulate_resize(size(px(1400.0), px(700.0)));
    window.run_until_parked();

    let filter = bounds_of(window, "collection-slot-filter".to_string()).expect("filter");
    let limit = bounds_of(window, "collection-slot-limit".to_string()).expect("limit");
    assert_eq!(
        filter.origin.y, limit.origin.y,
        "a wide grid keeps the slots on one line"
    );
}

/// A collection grid with its builder open beside it, hosted as the
/// workspace hosts it, under the app keymap.
fn keyboard_rail(
    cx: &mut TestAppContext,
) -> (
    gpui::Entity<DataGridPanel>,
    gpui::Entity<crate::document_builder::DocumentBuilderPanel>,
    &mut VisualTestContext,
) {
    use crate::data_grid_panel::tests::rail_keys::host_in_rail;

    let (app_state, profile_id) = register_stub_connection(
        cx,
        Arc::new(StubDocumentConnection::new(
            DatabaseCategory::Document,
            aggregate_features(),
            true,
        )),
    );
    cx.update(dbflux_ui_base::keymap::init_keymap);

    let (panel, window) = host_in_rail(cx, move |window, cx| {
        cx.new(|cx| {
            let source = DataSource::Collection {
                profile_id,
                collection: CollectionRef::new("shop", "orders"),
                pagination: Pagination::default(),
                total_docs: None,
            };
            DataGridPanel::new_internal(source, app_state, vec![], window, cx)
        })
    });
    open_builder(&panel, window);
    let builder = builder(&panel, window);

    (panel, builder, window)
}

/// A collection with the Documents, Schema and Aggregate views, hosted
/// as the workspace hosts it, with the keyboard on the grid.
fn keyboard_collection(
    cx: &mut TestAppContext,
) -> (gpui::Entity<DataGridPanel>, &mut VisualTestContext) {
    use crate::data_grid_panel::tests::rail_keys::host_in_rail;

    let (app_state, profile_id) = register_stub_connection(
        cx,
        Arc::new(StubDocumentConnection::new(
            DatabaseCategory::Document,
            aggregate_features(),
            true,
        )),
    );
    cx.update(dbflux_ui_base::keymap::init_keymap);

    host_in_rail(cx, move |window, cx| {
        cx.new(|cx| {
            let source = DataSource::Collection {
                profile_id,
                collection: CollectionRef::new("shop", "orders"),
                pagination: Pagination::default(),
                total_docs: None,
            };
            DataGridPanel::new_internal(source, app_state, vec![], window, cx)
        })
    })
}

fn run_toolbar(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
    action: crate::data_grid_panel::context_menu::toolbar::ToolbarAction,
) {
    window.update(|window, cx| {
        panel.update(cx, |grid, cx| grid.run_toolbar_action(action, window, cx))
    });
    window.run_until_parked();
}

/// The Toolbar submenu lists the collection's Builder toggle: its entry
/// opens the builder rail, and closes it again while it is open.
#[gpui::test]
fn the_toolbar_entry_opens_and_closes_the_document_builder(cx: &mut TestAppContext) {
    use crate::data_grid_panel::context_menu::toolbar::ToolbarAction;

    let (panel, window) = keyboard_collection(cx);
    let is_open = |window: &mut VisualTestContext| {
        window.update(|_, cx| panel.read(cx).document_builder_is_open())
    };

    let actions = window.update(|_, cx| panel.read(cx).toolbar_actions(cx));
    assert!(
        actions.contains(&ToolbarAction::OpenBuilder),
        "the Toolbar lists the Builder toggle: {actions:?}"
    );
    assert!(!is_open(window));

    run_toolbar(&panel, window, ToolbarAction::OpenBuilder);
    assert!(is_open(window), "the entry opens the builder");

    let actions = window.update(|_, cx| panel.read(cx).toolbar_actions(cx));
    assert!(actions.contains(&ToolbarAction::OpenBuilder));

    run_toolbar(&panel, window, ToolbarAction::OpenBuilder);
    assert!(!is_open(window), "the entry closes the open builder");
}

/// The query bar's Find, history and clear filter and the view tabs are
/// reachable from the keyboard: the Toolbar entries run Find and open
/// the history menu, whose menu keys rerun a query; Shift+F empties the
/// filter slot and finds; Alt+L / Alt+H step through the views.
#[gpui::test]
fn the_query_bar_and_views_are_driven_by_keys(cx: &mut TestAppContext) {
    use crate::data_grid_panel::context_menu::toolbar::ToolbarAction;
    use crate::data_grid_panel::tests::rail_keys::{context, keys};
    use dbflux_app::keymap::ContextId;

    let (panel, window) = keyboard_collection(cx);

    set_slot_texts(&panel, window, "{ status: 1 }", "");
    run_toolbar(&panel, window, ToolbarAction::Find);
    assert_eq!(
        window.update(|_, cx| panel.read(cx).collection.history.len()),
        1,
        "the Find entry runs the query and records it"
    );

    let actions = window.update(|_, cx| panel.read(cx).toolbar_actions(cx));
    for expected in [
        ToolbarAction::ClearFilter,
        ToolbarAction::Find,
        ToolbarAction::QueryHistory,
        ToolbarAction::CollectionView(CollectionTab::Schema),
        ToolbarAction::CollectionView(CollectionTab::Aggregate),
    ] {
        assert!(
            actions.contains(&expected),
            "the Toolbar lists {expected:?}: {actions:?}"
        );
    }

    keys(window, "shift-f");
    assert_eq!(
        slot_texts(&panel, window).0,
        "",
        "Shift+F empties the filter slot"
    );

    run_toolbar(&panel, window, ToolbarAction::QueryHistory);
    assert_eq!(context(&panel, window), ContextId::ContextMenu);

    // The cleared query ran last, so it leads the history.
    keys(window, "j enter");
    assert_eq!(
        slot_texts(&panel, window).0,
        "{ status: 1 }",
        "J then Enter reruns the older query"
    );
    assert!(!window.update(|_, cx| panel.read(cx).collection.history_open));

    keys(window, "alt-l");
    assert_eq!(
        window.update(|_, cx| panel.read(cx).collection.tab),
        CollectionTab::Schema,
        "Alt+L shows the next view"
    );

    keys(window, "alt-h");
    assert_eq!(
        window.update(|_, cx| panel.read(cx).collection.tab),
        CollectionTab::Documents,
        "Alt+H shows the previous view"
    );
}

/// Ctrl+L enters the document builder; A adds a condition, whose field
/// is picked by typing its path, whose operator comes from the operator
/// list driven by J and Enter, and whose value is typed; Ctrl+Enter
/// finds; X removes the condition; Alt+L switches to Aggregate; M opens
/// the rail's menu.
/// The document builder rail with a condition, and its rail menu.
/// `the_document_builder_is_driven_by_keys` proves Ctrl+L reaches it and
/// `m` opens its menu.
#[gpui::test]
fn the_document_builder_rail_is_covered(cx: &mut TestAppContext) {
    use crate::data_grid_panel::tests::rail_keys::keys;
    use crate::keyboard_coverage::{DATA_GRID, DOCUMENT_BUILDER};
    use dbflux_components::composites::RailOwner as _;
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};

    let (panel, builder, window) = keyboard_rail(cx);
    keys(window, "ctrl-l j a");

    let menu: Vec<String> = window.update(|_, cx| {
        let rail = builder
            .read(cx)
            .rail_actions(cx)
            .into_iter()
            .map(|entry| entry.id.to_string());
        let toolbar = panel
            .read(cx)
            .toolbar_actions(cx)
            .into_iter()
            .map(|action| action.id().to_string());
        rail.chain(toolbar).collect()
    });

    let capture = FrameCapture::observe(window);
    let coverage = || {
        Coverage::new(DOCUMENT_BUILDER)
            .with_surface(DATA_GRID)
            .with_menu_entries(menu.iter().cloned())
    };

    let checked = coverage().assert_covered(&capture.frame(window));
    assert!(
        checked.iter().any(|id| id.starts_with("doc-builder-")),
        "{checked:?}"
    );

    keys(window, "m");
    let checked = coverage().assert_covered(&capture.frame(window));
    assert!(
        checked.iter().any(|id| id == "rail-action-close"),
        "{checked:?}"
    );
}

#[gpui::test]
fn the_document_builder_is_driven_by_keys(cx: &mut TestAppContext) {
    use crate::data_grid_panel::tests::rail_keys::{context, keys};
    use dbflux_app::keymap::ContextId;

    let (panel, builder, window) = keyboard_rail(cx);

    keys(window, "ctrl-l");
    assert_eq!(context(&panel, window), ContextId::DocumentBuilder);

    keys(window, "j a");
    let (id, path, operator) = first_condition(&panel, window);
    assert!(path.is_empty(), "A adds an empty condition");
    assert_eq!(
        window.update(|_, cx| builder.read(cx).rail_cursor_for_test()),
        Some(format!("condition-{id}")),
        "the cursor moves onto it"
    );

    keys(window, "enter");
    window.simulate_input("age");
    window.run_until_parked();
    keys(window, "enter");
    assert_eq!(
        first_condition(&panel, window).1,
        "age",
        "Enter picks the typed path"
    );
    assert_eq!(
        context(&panel, window),
        ContextId::DocumentBuilder,
        "the keyboard is back on the rail"
    );

    keys(window, "l enter j enter");
    assert_ne!(
        first_condition(&panel, window).2,
        operator,
        "J and Enter pick another operator"
    );
    assert_eq!(context(&panel, window), ContextId::DocumentBuilder);

    keys(window, "l enter");
    window.simulate_input("30");
    window.run_until_parked();
    keys(window, "escape");
    assert_eq!(context(&panel, window), ContextId::DocumentBuilder);
    assert!(
        slot_texts(&panel, window).0.contains("age"),
        "the condition reached the filter slot: {:?}",
        slot_texts(&panel, window)
    );

    keys(window, "ctrl-enter");
    assert_eq!(
        window.update(|_, cx| panel.read(cx).collection.tab),
        CollectionTab::Documents,
        "Ctrl+Enter finds, as the Find button does"
    );
    assert_eq!(
        context(&panel, window),
        ContextId::DocumentBuilder,
        "a Find from the rail keeps the keyboard there"
    );

    keys(window, "x");
    assert!(
        window.update(|_, cx| builder.read(cx).draft().conditions().is_empty()),
        "X removes the condition"
    );

    keys(window, "alt-l");
    assert_eq!(
        window.update(|_, cx| builder.read(cx).mode()),
        DocumentQueryMode::Aggregate,
        "Alt+L switches to Aggregate"
    );

    keys(window, "m");
    assert_eq!(context(&panel, window), ContextId::ContextMenu);
    keys(window, "escape escape");
    assert_eq!(context(&panel, window), ContextId::Results);
}

/// Shift+J and Shift+K move a sort key, the way dragging it does.
#[gpui::test]
fn shift_j_moves_a_sort_key_down(cx: &mut TestAppContext) {
    use crate::data_grid_panel::tests::rail_keys::keys;

    let (panel, builder, window) = keyboard_rail(cx);
    window.update(|window, cx| {
        panel.update(cx, |grid, cx| {
            grid.collection.sort_input.update(cx, |input, cx| {
                input.set_value("{ a: 1, b: -1 }", window, cx)
            });
            grid.sync_slots_into_document_builder(cx);
        });
    });
    window.run_until_parked();

    let sort_paths = |window: &mut VisualTestContext| {
        window.update(|_, cx| {
            builder
                .read(cx)
                .draft()
                .sort
                .iter()
                .map(|key| key.path.clone())
                .collect::<Vec<_>>()
        })
    };
    assert_eq!(sort_paths(window), vec!["a", "b"]);

    keys(window, "ctrl-l");
    let first_sort = window.update(|_, cx| {
        builder
            .read(cx)
            .rail_rows_for_test(cx)
            .iter()
            .position(|id| id == "sort-0")
            .expect("the sort keys are rows")
    });
    for _ in 0..first_sort {
        keys(window, "j");
    }

    keys(window, "shift-j");
    assert_eq!(sort_paths(window), vec!["b", "a"]);
    assert_eq!(
        window.update(|_, cx| builder.read(cx).rail_cursor_for_test()),
        Some("sort-1".to_string()),
        "the cursor stays on the moved key"
    );
}
