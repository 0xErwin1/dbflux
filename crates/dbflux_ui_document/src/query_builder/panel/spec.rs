use super::*;

impl QueryBuilderPanel {
    /// Rebuilds `current_spec` from the panel's mutable row data without
    /// interacting with the GPUI context. Called from both the notify path
    /// and from unit tests.
    pub(crate) fn rebuild_spec_pure(&mut self) {
        let projection = match self.projection_mode {
            ProjectionMode::All => Projection::All,
            ProjectionMode::Explicit => Projection::Explicit(
                self.projection_rows
                    .iter()
                    .map(|r| r.to_projected_column())
                    .collect(),
            ),
        };

        let joins: Vec<JoinStep> = self.join_rows.iter().map(|r| r.to_join_step()).collect();

        let sort: Vec<SortEntry> = self
            .sort_rows
            .iter()
            .filter(|r| !r.column.is_empty())
            .map(|r| r.to_sort_entry())
            .collect();

        let group_by: Vec<GroupByEntry> = self
            .group_by_rows
            .iter()
            .filter(|r| !r.column.is_empty())
            .map(|r| r.to_group_by_entry())
            .collect();

        let aggregates: Vec<VisualAggregateSpec> = self
            .aggregate_rows
            .iter()
            .filter(|r| !r.alias.is_empty())
            .filter(|r| r.function == AggFn::CountStar || !r.column.is_empty())
            .map(|r| r.to_aggregate_spec())
            .collect();

        let limit = match self.limit_text.parse::<u64>() {
            Ok(0) | Err(_) => None,
            Ok(n) => Some(n),
        };

        let offset = self.offset_text.parse::<u64>().unwrap_or(0);

        self.current_spec.projection = projection;
        self.current_spec.joins = joins;
        self.current_spec.sort = sort;
        self.current_spec.group_by = group_by;
        self.current_spec.aggregates = aggregates;
        self.current_spec.limit = limit;
        self.current_spec.offset = offset;

        let incomplete_count = self
            .aggregate_rows
            .iter()
            .filter(|r| {
                r.alias.is_empty() || (r.function != AggFn::CountStar && r.column.is_empty())
            })
            .count();
        self.incomplete_aggregate_row_count = incomplete_count;

        self.refresh_mutation_preview_pure();
    }

    /// Rebuilds `current_spec` from the panel's mutable row data, then updates
    /// the SQL preview and notifies GPUI.
    pub(crate) fn rebuild_spec_and_notify(&mut self, cx: &mut Context<Self>) {
        self.rebuild_spec_pure();
        cx.emit(BuilderEvent::SpecChanged(Box::new(
            self.current_spec.clone(),
        )));
        cx.notify();
    }

    /// Recomputes the SQL preview from `current_spec` and notifies GPUI.
    pub(crate) fn refresh_preview_and_notify(&mut self, cx: &mut Context<Self>) {
        self.refresh_mutation_preview_pure();
        cx.emit(BuilderEvent::SpecChanged(Box::new(
            self.current_spec.clone(),
        )));
        cx.notify();
    }

    /// Returns the current limit as a `u64`, or `None` when zero / unparseable.
    pub fn current_limit(&self) -> Option<u64> {
        match self.limit_text.parse::<u64>() {
            Ok(0) | Err(_) => None,
            Ok(n) => Some(n),
        }
    }

    /// Returns the current offset as a `u64`.
    pub fn current_offset(&self) -> u64 {
        self.offset_text.parse::<u64>().unwrap_or(0)
    }

    /// Returns `true` when the panel has a valid spec that can be executed.
    pub fn is_runnable(&self) -> bool {
        self.current_spec.is_runnable().is_ok()
    }

    /// Returns `true` when the query is currently grouped (has at least one
    /// group-by or aggregate row).
    pub fn is_grouped(&self) -> bool {
        self.current_spec.is_grouped()
    }

    /// The weak handle to the owning `DataGridPanel`, if one was provided.
    pub fn data_grid(&self) -> Option<&WeakEntity<DataGridPanel>> {
        self.data_grid.as_ref()
    }

    // -----------------------------------------------------------------------
    // Mutation mode support
    // -----------------------------------------------------------------------

    /// Returns `true` when the mutation mode selector (SELECT / UPDATE / DELETE)
    /// should be rendered.
    ///
    /// Hidden when the connected driver uses a non-SQL query language or when
    /// the profile's mutation policy is `ReadOnly` (H-1, H-2, I-1, I-2).
    pub fn shows_mutation_selector(&self, cx: &App) -> bool {
        let profile_id = self.schema_profile_id;
        if profile_id.is_nil() {
            return false;
        }
        let Some(app_state) = self.app_state_weak.upgrade() else {
            return false;
        };
        let state = app_state.read(cx);
        let Some(connected) = state.connections().get(&profile_id) else {
            return false;
        };
        if connected.mutation_policy == dbflux_core::MutationPolicy::ReadOnly {
            return false;
        }
        connected.connection.metadata().query_language == dbflux_core::QueryLanguage::Sql
    }

    /// Reads the connected driver's `QueryCapabilities`.
    ///
    /// Drives builder section visibility and sort UX so the builder stays
    /// driver-agnostic: every section gate keys off these capability flags
    /// rather than a driver id.
    pub(crate) fn query_capabilities(&self, cx: &App) -> Option<dbflux_core::QueryCapabilities> {
        let app_state = self.app_state_weak.upgrade()?;
        let state = app_state.read(cx);
        let connected = state.connections().get(&self.schema_profile_id)?;
        connected.connection.metadata().query.clone()
    }

    /// Whether the builder should render the JOINS section for this driver.
    pub(crate) fn shows_joins_section(&self, cx: &App) -> bool {
        self.query_capabilities(cx)
            .map(|q| q.supports_joins)
            .unwrap_or(false)
    }

    /// Whether the builder should render the GROUP BY / aggregates section.
    pub(crate) fn shows_group_by_section(&self, cx: &App) -> bool {
        self.query_capabilities(cx)
            .map(|q| q.supports_group_by)
            .unwrap_or(false)
    }

    /// Whether the builder should render the HAVING section.
    pub(crate) fn shows_having_section(&self, cx: &App) -> bool {
        self.query_capabilities(cx)
            .map(|q| q.supports_having)
            .unwrap_or(false)
    }

    /// The driver's result-ordering mode, defaulting to multi-column ordering.
    pub(crate) fn order_by_mode(&self, cx: &App) -> dbflux_core::OrderByMode {
        self.query_capabilities(cx)
            .map(|q| q.order_by_mode)
            .unwrap_or(dbflux_core::OrderByMode::AnyColumns)
    }

    /// The name of the single orderable column for a `SortKeyOnly` driver.
    ///
    /// Returns the key resolved once at panel open from the source's key-schema
    /// metadata (`cached_sort_key_column`), not from the live result — a builder
    /// read can replace the grid with a result that no longer carries key markers
    /// (e.g. a PartiQL `SELECT *`). `None` when the source has no orderable key
    /// (e.g. a partition-key-only table), in which case no column is seeded and
    /// no ORDER BY is emitted.
    pub(crate) fn sort_key_column(&self) -> Option<String> {
        self.cached_sort_key_column.clone()
    }

    /// Transitions into grouped mode: snapshots the current projection and
    /// switches projection to `Explicit([])`. Also drops sort entries that
    /// won't survive the GROUP BY validation.
    pub(crate) fn enter_grouped_mode(&mut self) {
        if self.pre_group_projection.is_none() {
            self.pre_group_projection = Some(self.current_spec.projection.clone());
        }
        self.current_spec.projection = Projection::Explicit(Vec::new());
        self.projection_mode = ProjectionMode::Explicit;
        self.projection_rows.clear();
        self.drop_invalid_sort_for_grouped();
    }

    /// Transitions out of grouped mode: restores the pre-group projection
    /// snapshot. Any sort entries that reference aggregate aliases are dropped.
    pub(crate) fn exit_grouped_mode(&mut self) {
        if let Some(prev) = self.pre_group_projection.take() {
            self.current_spec.projection = prev.clone();
            match &prev {
                Projection::All => {
                    self.projection_mode = ProjectionMode::All;
                    self.projection_rows.clear();
                }
                Projection::Explicit(cols) => {
                    self.projection_mode = ProjectionMode::Explicit;
                    self.projection_rows = cols
                        .iter()
                        .map(|c| ProjectionRow {
                            source_alias: c.source_alias.clone(),
                            column: c.column.clone(),
                            alias: c.alias.clone(),
                        })
                        .collect();
                }
            }
        }
        self.drop_invalid_sort_for_ungrouped();
        // Clear any HAVING state since there is nothing to have without grouping.
        self.current_spec.having = None;
        self.having_predicate_input_states.clear();
        self.having_predicate_column_input_states.clear();
        self.having_predicate_comparator_dropdowns.clear();
    }

    /// Drops sort rows whose column is not in the current grouped valid set
    /// (group-by columns union aggregate aliases).
    pub(crate) fn drop_invalid_sort_for_grouped(&mut self) {
        let valid: HashSet<String> = self
            .group_by_rows
            .iter()
            .map(|g| g.column.clone())
            .chain(self.aggregate_rows.iter().map(|a| a.alias.clone()))
            .collect();

        self.sort_rows.retain(|s| valid.contains(&s.column));
        self.current_spec.sort = self.sort_rows.iter().map(|r| r.to_sort_entry()).collect();
    }

    /// Drops sort rows whose (source_alias, column) pair is not present in the
    /// restored projection.
    pub(crate) fn drop_invalid_sort_for_ungrouped(&mut self) {
        let valid: HashSet<(String, String)> = match &self.current_spec.projection {
            Projection::All => {
                self.sort_rows.clear();
                self.current_spec.sort.clear();
                return;
            }
            Projection::Explicit(cols) => cols
                .iter()
                .map(|c| (c.source_alias.clone(), c.column.clone()))
                .collect(),
        };

        self.sort_rows
            .retain(|s| valid.contains(&(s.source_alias.clone(), s.column.clone())));
        self.current_spec.sort = self.sort_rows.iter().map(|r| r.to_sort_entry()).collect();
    }

    /// Generates a default alias for an aggregate row.
    ///
    /// Returns `count_star` for `CountStar`, `fn_col` for others (e.g.
    /// `sum_amount`). When the generated alias collides with an existing alias,
    /// appends `_2`, `_3`, etc. until unique.
    pub(crate) fn generate_aggregate_alias(&self, function: AggFn, column: &str) -> String {
        let fn_name = match function {
            AggFn::Count => "count",
            AggFn::CountStar => "count_star",
            AggFn::CountDistinct => "count_distinct",
            AggFn::Sum => "sum",
            AggFn::Avg => "avg",
            AggFn::Min => "min",
            AggFn::Max => "max",
        };

        let base = if function == AggFn::CountStar || column.is_empty() {
            fn_name.to_string()
        } else {
            let sanitized: String = column
                .chars()
                .map(|c| {
                    if c.is_alphanumeric() || c == '_' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();
            format!("{}_{}", fn_name, sanitized)
        };

        let existing: HashSet<&str> = self
            .aggregate_rows
            .iter()
            .map(|r| r.alias.as_str())
            .collect();

        if !existing.contains(base.as_str()) {
            return base;
        }

        let mut counter = 2usize;
        loop {
            let candidate = format!("{}_{}", base, counter);
            if !existing.contains(candidate.as_str()) {
                return candidate;
            }
            counter += 1;
        }
    }

    /// Returns `true` if `alias` looks like an auto-generated default alias
    /// for any aggregate function.
    pub(crate) fn is_auto_alias(&self, alias: &str) -> bool {
        let auto_prefixes = [
            "count_star",
            "count_distinct",
            "count",
            "sum",
            "avg",
            "min",
            "max",
        ];
        auto_prefixes
            .iter()
            .any(|prefix| alias == *prefix || alias.starts_with(&format!("{}_", prefix)))
    }

    // -----------------------------------------------------------------------
    // Completion support
    // -----------------------------------------------------------------------
}
