use super::*;

#[gpui::test]
fn the_builder_is_offered_only_with_the_feature_and_a_codec(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
    );
    let support = window.update(|_, cx| panel.read(cx).document_builder_support(cx));
    assert_eq!(support, BuilderSupport::Available);

    let mut cx = TestAppContext::single();
    let (panel, window) = collection_panel(
        &mut cx,
        StubDocumentConnection::new(
            DatabaseCategory::Document,
            DocumentFeatures::QUERY_SLOTS,
            false,
        ),
    );
    let support = window.update(|_, cx| panel.read(cx).document_builder_support(cx));
    assert_eq!(support, BuilderSupport::Unsupported);

    let mut cx = TestAppContext::single();
    let (panel, window) = collection_panel(
        &mut cx,
        StubDocumentConnection::new(DatabaseCategory::KeyValue, builder_features(), true),
    );
    let support = window.update(|_, cx| panel.read(cx).document_builder_support(cx));
    assert_eq!(support, BuilderSupport::Hidden);
}

#[gpui::test]
fn an_unsupported_driver_never_opens_an_empty_rail(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(
            DatabaseCategory::Document,
            DocumentFeatures::QUERY_SLOTS | DocumentFeatures::VISUAL_BUILDER,
            false,
        ),
    );
    let events = rail_events(&panel, window);

    open_builder(&panel, window);

    window.update(|_, cx| {
        let grid = panel.read(cx);
        assert!(!grid.document_builder_is_open());
        assert!(grid.collection.builder.panel.is_none());
    });
    assert!(events.borrow().is_empty());
}

#[gpui::test]
fn opening_reads_the_slots_and_mounts_the_rail(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
    );
    set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "");
    let events = rail_events(&panel, window);

    open_builder(&panel, window);

    assert_eq!(*events.borrow(), vec!["open"]);
    assert!(window.update(|_, cx| panel.read(cx).document_builder_is_open()));
    assert_eq!(
        first_condition(&panel, window),
        (
            first_condition(&panel, window).0,
            "age".to_string(),
            DocumentOperator::Gt
        )
    );
}

#[gpui::test]
fn a_builder_edit_rewrites_only_the_slot_it_changed(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
    );
    set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "{ name: 1 }");
    open_builder(&panel, window);

    let (id, _, _) = first_condition(&panel, window);
    let builder = builder(&panel, window);
    window.update(|_, cx| {
        builder.update(cx, |builder, cx| {
            builder.set_operator(id, DocumentOperator::Gte, cx)
        });
    });
    window.run_until_parked();

    assert_eq!(
        slot_texts(&panel, window),
        (
            r#"{"age": {"$gte": 30}}"#.to_string(),
            "{ name: 1 }".to_string()
        )
    );
}

#[gpui::test]
fn a_slot_edit_reads_back_into_the_builder(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
    );
    open_builder(&panel, window);

    set_slot_texts(&panel, window, r#"{ "status": "failed" }"#, "");
    window.update(|_, cx| {
        panel.update(cx, |grid, cx| grid.sync_slots_into_document_builder(cx));
    });

    let (_, path, operator) = first_condition(&panel, window);
    assert_eq!(path, "status");
    assert_eq!(operator, DocumentOperator::Eq);
}

#[gpui::test]
fn a_slot_the_builder_cannot_read_waits_for_a_rewrite(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
    );
    let original = r#"{"$expr": {"$gt": ["$total", "$limit"]}, "c": 1}"#;
    set_slot_texts(&panel, window, original, "");
    open_builder(&panel, window);

    let builder = builder(&panel, window);
    assert!(window.update(|_, cx| builder.read(cx).is_conflicted()));

    let (id, path, _) = first_condition(&panel, window);
    assert_eq!(path, "c");
    window.update(|_, cx| {
        builder.update(cx, |builder, cx| {
            builder.set_operator(id, DocumentOperator::Ne, cx)
        });
    });
    window.run_until_parked();

    assert_eq!(slot_texts(&panel, window).0, original);
    assert!(!window.update(|_, cx| builder.read(cx).can_find()));

    window.update(|_, cx| {
        builder.update(cx, |builder, cx| builder.rewrite_from_builder(cx));
    });
    window.run_until_parked();

    assert_eq!(slot_texts(&panel, window).0, r#"{"c": {"$ne": 1}}"#);
    window.update(|_, cx| {
        assert!(!builder.read(cx).is_conflicted());
        assert!(builder.read(cx).can_find());
    });
}

#[gpui::test]
fn find_starts_at_the_builder_skip(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
    );
    open_builder(&panel, window);

    let builder = builder(&panel, window);
    window.update(|window, cx| {
        builder.update(cx, |builder, cx| builder.set_skip(Some(40), cx));
        panel.update(cx, |grid, cx| {
            grid.filter_bar
                .limit_input
                .update(cx, |input, cx| input.set_value("20", window, cx));
        });
    });

    let (start, skip) = window.update(|_, cx| {
        let grid = panel.read(cx);
        (
            grid.document_find_pagination(cx),
            grid.document_builder_skip(cx),
        )
    });
    assert_eq!(
        start,
        Pagination::Offset {
            limit: 20,
            offset: 0,
        },
        "pages count from the skip"
    );
    assert_eq!(skip, 40);

    window.update(|_, cx| {
        panel.update(cx, |grid, _| grid.mark_document_builder_closed());
    });
    let skip = window.update(|_, cx| panel.read(cx).document_builder_skip(cx));
    assert_eq!(skip, 0, "a closed builder does not skip");
}

#[gpui::test]
fn closing_the_rail_keeps_the_draft(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
    );
    set_slot_texts(&panel, window, r#"{"a": 1}"#, "");
    open_builder(&panel, window);
    let events = rail_events(&panel, window);

    window.update(|window, cx| {
        panel.update(cx, |grid, cx| {
            grid.handle_document_builder_event(&DocumentBuilderEvent::CloseRequested, window, cx);
        });
    });

    assert_eq!(*events.borrow(), vec!["close"]);
    window.update(|_, cx| {
        let grid = panel.read(cx);
        assert!(!grid.document_builder_is_open());
        assert!(grid.collection.builder.panel.is_some());
    });

    open_builder(&panel, window);
    window.update(|_, cx| {
        panel.update(cx, |grid, cx| grid.clear_inspector_state(cx));
    });
    assert!(!window.update(|_, cx| panel.read(cx).document_builder_is_open()));
}

#[gpui::test]
fn aggregate_mode_needs_the_aggregate_feature(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
    );
    open_builder(&panel, window);
    let rail = builder(&panel, window);

    window.update(|_, cx| {
        rail.update(cx, |rail, cx| {
            assert!(!rail.aggregate_available());
            rail.add_group_stage(cx);
            rail.set_mode(DocumentQueryMode::Aggregate, cx);
            assert_eq!(rail.mode(), DocumentQueryMode::Find);
            assert!(rail.draft().group.is_none());
        });
    });

    let mut cx = TestAppContext::single();
    let (panel, window) = collection_panel(
        &mut cx,
        StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
    );
    open_builder(&panel, window);
    let rail = builder(&panel, window);

    window.update(|_, cx| {
        rail.update(cx, |rail, cx| {
            assert!(rail.aggregate_available());
            rail.add_group_stage(cx);
            assert_eq!(rail.mode(), DocumentQueryMode::Aggregate);
        });
    });
}

#[gpui::test]
fn aggregate_mode_leaves_the_slots_alone_until_find_returns(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
    );
    set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "{ name: 1 }");
    open_builder(&panel, window);
    let rail = builder(&panel, window);
    let (id, _, _) = first_condition(&panel, window);

    assert_eq!(
        window.update(|_, cx| panel.read(cx).document_builder_pipeline_stages(cx)),
        None,
        "find mode shows the slots"
    );

    window.update(|_, cx| {
        rail.update(cx, |rail, cx| {
            rail.add_group_stage(cx);
            rail.set_operator(id, DocumentOperator::Gte, cx);
        });
    });
    window.run_until_parked();

    assert_eq!(
        slot_texts(&panel, window),
        (
            r#"{ age: { $gt: 30 } }"#.to_string(),
            "{ name: 1 }".to_string()
        ),
        "no slot is written in aggregate mode"
    );
    assert_eq!(
        window.update(|_, cx| panel.read(cx).document_builder_pipeline_stages(cx)),
        Some(vec![
            "$match".to_string(),
            "$group".to_string(),
            "$limit".to_string()
        ]),
        "the limit slot's page size carries over as the pipeline's limit"
    );

    set_slot_texts(&panel, window, r#"{ "status": "failed" }"#, "{ name: 1 }");
    window.update(|_, cx| {
        panel.update(cx, |grid, cx| grid.sync_slots_into_document_builder(cx));
    });
    assert_eq!(
        first_condition(&panel, window).1,
        "age",
        "a slot edit does not replace the pipeline's filter"
    );

    set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "{ name: 1 }");
    window.update(|_, cx| {
        rail.update(cx, |rail, cx| rail.set_mode(DocumentQueryMode::Find, cx));
    });
    window.run_until_parked();

    assert_eq!(
        slot_texts(&panel, window),
        (
            r#"{"age": {"$gte": 30}}"#.to_string(),
            "{ name: 1 }".to_string()
        ),
        "back in find mode the builder writes the part it changed"
    );
    assert_eq!(
        window.update(|_, cx| panel.read(cx).document_builder_pipeline_stages(cx)),
        None
    );
}

#[gpui::test]
fn a_slot_typed_while_the_rail_was_away_stays_locked_when_find_returns(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
    );
    set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "");
    open_builder(&panel, window);
    let rail = builder(&panel, window);
    let (id, _, _) = first_condition(&panel, window);

    window.update(|_, cx| {
        rail.update(cx, |rail, cx| {
            rail.add_group_stage(cx);
            rail.set_operator(id, DocumentOperator::Gte, cx);
        });
    });
    window.update(|_, cx| {
        panel.update(cx, |grid, _| grid.mark_document_builder_closed());
    });

    let unreadable = r#"{"$expr": {"$gt": ["$total", "$limit"]}}"#;
    set_slot_texts(&panel, window, unreadable, "");
    sync_slots(&panel, window);

    open_builder(&panel, window);
    window.update(|_, cx| rail.update(cx, |rail, cx| rail.remove_group_stage(cx)));
    window.run_until_parked();

    assert_eq!(
        slot_texts(&panel, window).0,
        unreadable,
        "the builder never overwrites a slot it cannot read"
    );
    window.update(|_, cx| {
        let rail = rail.read(cx);
        assert_eq!(rail.mode(), DocumentQueryMode::Find);
        assert!(rail.is_conflicted());
        assert!(!rail.can_find(), "the edit is held until the user decides");
    });
}

#[gpui::test]
fn a_slot_edited_during_an_aggregation_reaches_the_builder_when_find_returns(
    cx: &mut TestAppContext,
) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
    );
    set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "");
    open_builder(&panel, window);
    let rail = builder(&panel, window);

    window.update(|_, cx| rail.update(cx, |rail, cx| rail.add_group_stage(cx)));
    set_slot_texts(&panel, window, r#"{ "status": "failed" }"#, "");
    sync_slots(&panel, window);

    window.update(|_, cx| {
        rail.update(cx, |rail, cx| rail.set_mode(DocumentQueryMode::Find, cx));
    });
    window.run_until_parked();

    assert_eq!(first_condition(&panel, window).1, "status");
    assert_eq!(
        slot_texts(&panel, window).0,
        r#"{ "status": "failed" }"#,
        "a part the builder did not edit takes the slot's text"
    );
    assert!(!window.update(|_, cx| rail.read(cx).is_conflicted()));
}

#[gpui::test]
fn a_history_entry_reloads_the_builder_and_drops_its_skip(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
    );
    set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "");
    open_builder(&panel, window);
    let rail = builder(&panel, window);
    window.update(|_, cx| rail.update(cx, |rail, cx| rail.set_skip(Some(40), cx)));

    window.update(|window, cx| {
        panel.update(cx, |grid, cx| {
            grid.collection.history = vec![super::super::super::QueryHistoryEntry {
                filter: r#"{ "status": "failed" }"#.to_string(),
                projection: String::new(),
                sort: String::new(),
                limit: String::new(),
            }];
            grid.run_history_entry(0, window, cx);
        });
    });
    window.run_until_parked();

    assert_eq!(first_condition(&panel, window).1, "status");
    window.update(|_, cx| {
        let rail = rail.read(cx);
        assert!(!rail.is_conflicted());
        assert_eq!(rail.skip(), None, "a history entry has no skip");
    });
}

#[gpui::test]
fn opening_a_saved_find_writes_every_slot_it_replaces(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
    );
    let saved_filter = r#"{"age": {"$gt": 30}}"#;
    set_slot_texts(&panel, window, saved_filter, "");
    open_builder(&panel, window);
    let rail = builder(&panel, window);

    set_query_name(&rail, window, "adults");
    window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_save(cx)));
    window.run_until_parked();
    let saved_id = stored_queries(&panel, window)[0].id.clone();

    window.update(|_, cx| rail.update(cx, |rail, cx| rail.add_group_stage(cx)));
    set_slot_texts(&panel, window, r#"{ "status": "failed" }"#, "");
    sync_slots(&panel, window);

    window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_open_saved(&saved_id, cx)));
    window.run_until_parked();

    assert_eq!(slot_texts(&panel, window).0, saved_filter);
    assert_eq!(
        window.update(|_, cx| rail.read(cx).mode()),
        DocumentQueryMode::Find
    );
}

#[gpui::test]
fn run_pipeline_hands_the_pipeline_to_the_aggregate_view(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
    );
    set_slot_texts(&panel, window, r#"{ "status": "failed" }"#, "");
    open_builder(&panel, window);
    let rail = builder(&panel, window);

    let expected = window.update(|_, cx| {
        rail.update(cx, |rail, cx| {
            rail.add_group_stage(cx);
            assert!(rail.can_run());
            let spec = rail.draft().to_spec().expect("a valid pipeline");
            MongoDocumentCodec
                .render_pipeline(&spec)
                .expect("renderable")
        })
    });

    window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_run(cx)));
    window.run_until_parked();

    window.update(|_, cx| {
        let grid = panel.read(cx);
        assert_eq!(grid.collection.tab, CollectionTab::Aggregate);

        let aggregate = &grid.collection.aggregate;
        assert_eq!(
            aggregate.pipeline_editor.read(cx).value().to_string(),
            expected
        );
        assert_eq!(aggregate.result_count(), Some(1));
        assert!(aggregate.results_from_builder);
    });
    assert_eq!(
        slot_texts(&panel, window).0,
        r#"{ "status": "failed" }"#,
        "running the pipeline writes no slot"
    );

    window.update(|window, cx| {
        panel.update(cx, |grid, cx| grid.run_aggregate(window, cx));
    });
    window.run_until_parked();
    assert!(
        !window.update(|_, cx| panel.read(cx).collection.aggregate.results_from_builder),
        "a pipeline run from the editor is not the builder's"
    );
}

#[gpui::test]
fn run_pipeline_leaves_a_busy_aggregate_view_alone(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
    );
    open_builder(&panel, window);
    let rail = builder(&panel, window);
    let typed = r#"[{"$match": {"status": "failed"}}]"#;

    window.update(|window, cx| {
        panel.update(cx, |grid, cx| {
            grid.collection
                .aggregate
                .pipeline_editor
                .update(cx, |editor, cx| editor.set_value(typed, window, cx));
            grid.collection.aggregate.running = true;
        });
        rail.update(cx, |rail, cx| {
            rail.add_group_stage(cx);
            rail.request_run(cx);
        });
    });
    window.run_until_parked();

    window.update(|_, cx| {
        let aggregate = &panel.read(cx).collection.aggregate;
        assert_eq!(
            aggregate.pipeline_editor.read(cx).value().to_string(),
            typed,
            "the pipeline being run keeps its text"
        );
        assert!(!aggregate.next_run_from_builder);
        assert!(aggregate.results.is_none());
    });
}

#[gpui::test]
fn paging_a_builder_find_never_goes_below_its_skip(cx: &mut TestAppContext) {
    let offsets = Arc::new(Mutex::new(Vec::new()));
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true)
            .logging_browse_offsets(offsets.clone()),
    );
    open_builder(&panel, window);
    let rail = builder(&panel, window);
    offsets.lock().unwrap().clear();

    window.update(|window, cx| {
        rail.update(cx, |rail, cx| rail.set_skip(Some(40), cx));
        panel.update(cx, |grid, cx| {
            grid.filter_bar
                .limit_input
                .update(cx, |input, cx| input.set_value("20", window, cx));
            grid.find_documents(window, cx);
        });
    });
    window.run_until_parked();

    window.update(|_, cx| {
        let grid = panel.read(cx);
        assert!(!grid.can_go_prev(), "the first page starts at the skip");
        assert_eq!(
            grid.source.pagination().map(|page| page.current_page()),
            Some(1)
        );
    });

    window.update(|window, cx| {
        panel.update(cx, |grid, cx| grid.go_to_next_page(window, cx));
    });
    window.run_until_parked();
    window.update(|window, cx| {
        panel.update(cx, |grid, cx| grid.go_to_prev_page(window, cx));
    });
    window.run_until_parked();
    window.update(|window, cx| {
        panel.update(cx, |grid, cx| grid.go_to_prev_page(window, cx));
    });
    window.run_until_parked();

    window.update(|window, cx| {
        panel.update(cx, |grid, cx| {
            grid.go_to_next_page(window, cx);
        });
    });
    window.run_until_parked();
    window.update(|window, cx| {
        panel.update(cx, |grid, cx| {
            grid.filter_bar
                .limit_input
                .update(cx, |input, cx| input.set_value("10", window, cx));
            grid.refresh(window, cx);
        });
    });
    window.run_until_parked();

    assert_eq!(
        *offsets.lock().unwrap(),
        vec![40, 60, 40, 60, 40],
        "find, next, previous (twice, the second stays), next, then a new \
             page size starts over at the skip"
    );
}

#[gpui::test]
fn the_page_count_and_match_count_leave_out_the_skipped_documents(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
    );
    open_builder(&panel, window);
    let rail = builder(&panel, window);

    window.update(|window, cx| {
        rail.update(cx, |rail, cx| rail.set_skip(Some(700), cx));
        panel.update(cx, |grid, cx| {
            grid.filter_bar
                .limit_input
                .update(cx, |input, cx| input.set_value("50", window, cx));
            grid.find_documents(window, cx);
        });
    });
    window.run_until_parked();
    window.update(|window, cx| {
        panel.update(cx, |grid, cx| grid.process_pending_actions(window, cx));
    });

    window.update(|_, cx| {
        let grid = panel.read(cx);
        assert_eq!(grid.total_pages(), Some(2), "800 matches, 700 skipped");
        assert!(grid.can_go_next());
        assert_eq!(
            grid.document_count_footer(),
            crate::labels::collection_matching_estimated(0, 100)
        );
    });

    window.update(|window, cx| {
        panel.update(cx, |grid, cx| grid.go_to_next_page(window, cx));
    });
    window.run_until_parked();

    assert!(
        !window.update(|_, cx| panel.read(cx).can_go_next()),
        "the second page is the last"
    );
}

#[gpui::test]
fn an_invalid_group_stage_cannot_run(cx: &mut TestAppContext) {
    let (panel, window) = collection_panel(
        cx,
        StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
    );
    open_builder(&panel, window);
    let rail = builder(&panel, window);

    window.update(|_, cx| {
        rail.update(cx, |rail, cx| {
            rail.add_group_stage(cx);
            let id = rail.draft().group.as_ref().unwrap().accumulators[0].id;
            rail.set_accumulator_op(id, AccumulatorOp::Sum, cx);
            assert!(!rail.can_run(), "a sum needs a field");
            rail.request_run(cx);
        });
    });
    window.run_until_parked();

    window.update(|_, cx| {
        let grid = panel.read(cx);
        assert_eq!(grid.collection.tab, CollectionTab::Documents);
        assert!(grid.collection.aggregate.results.is_none());
    });
}
