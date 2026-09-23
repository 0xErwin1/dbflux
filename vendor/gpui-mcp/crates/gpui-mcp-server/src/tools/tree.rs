use super::{
    DiffCurrentArgs, DiffSnapshotsArgs, Duration, ElementArgs, FindArgs, GpuiMcp, Instant, Json,
    MAX_TREE_SNAPSHOTS, Parameters, SnapshotArgs, ToolRouter, Value, WaitElementArgs,
    WaitStateArgs, encode_error, find_nodes, get_node, json, map_wait_error, object_output,
    require_bounds, state_matches, tool, tool_router, tree_diff, validate_name, validate_timeout,
};

#[tool_router(router = tree_router)]
impl GpuiMcp {
    #[tool(description = "Return the latest rendered GPUI semantic tree with real layout bounds")]
    async fn get_ui_tree(&self) -> Result<Json<Value>, String> {
        let tree = self.tree().await?;
        Ok(object_output(
            serde_json::to_value(tree).map_err(encode_error)?,
        ))
    }

    #[tool(
        description = "Find GPUI semantic elements by label substring or exact label and optional role"
    )]
    async fn find_elements(
        &self,
        Parameters(args): Parameters<FindArgs>,
    ) -> Result<Json<Value>, String> {
        let tree = self.tree().await?;
        let nodes = find_nodes(&tree, &args);
        Ok(object_output(
            json!({ "count": nodes.len(), "elements": nodes }),
        ))
    }

    #[tool(description = "Return one semantic element by its stable identifier")]
    async fn get_element(
        &self,
        Parameters(args): Parameters<ElementArgs>,
    ) -> Result<Json<Value>, String> {
        let tree = self.tree().await?;
        let node = get_node(&tree, &args.id)?;
        Ok(object_output(
            serde_json::to_value(node).map_err(encode_error)?,
        ))
    }

    #[tool(description = "Return window-relative logical bounds for one semantic element")]
    async fn get_element_bounds(
        &self,
        Parameters(args): Parameters<ElementArgs>,
    ) -> Result<Json<Value>, String> {
        let tree = self.tree().await?;
        let node = get_node(&tree, &args.id)?;
        let bounds = require_bounds(node)?;
        Ok(object_output(
            json!({ "id": args.id, "bounds": bounds, "center": bounds.center() }),
        ))
    }

    #[tool(description = "Wait until a label/role query matches a visible semantic element")]
    async fn wait_for_element(
        &self,
        Parameters(args): Parameters<WaitElementArgs>,
    ) -> Result<Json<Value>, String> {
        validate_timeout(args.timeout_ms)?;
        let started = Instant::now();
        let mut tree = self.tree().await?;
        loop {
            let query = FindArgs {
                query: Some(args.query.clone()),
                role: args.role,
                exact: args.exact,
                visible_only: true,
                limit: 1,
            };
            if let Some(node) = find_nodes(&tree, &query).into_iter().next() {
                return Ok(object_output(json!({
                    "elapsed_ms": started.elapsed().as_millis(),
                    "element": node,
                })));
            }
            let Some(remaining) =
                Duration::from_millis(args.timeout_ms).checked_sub(started.elapsed())
            else {
                return Err("timed out waiting for the element".to_owned());
            };
            tree = self
                .wait_for_tree(tree.generation, remaining)
                .await
                .map_err(|error| map_wait_error(error, "element"))?;
        }
    }

    #[tool(description = "Wait until all specified state predicates match a semantic element")]
    async fn wait_for_state(
        &self,
        Parameters(args): Parameters<WaitStateArgs>,
    ) -> Result<Json<Value>, String> {
        validate_timeout(args.timeout_ms)?;
        if args.visible.is_none()
            && args.enabled.is_none()
            && args.focused.is_none()
            && args.checked.is_none()
            && args.selected.is_none()
            && args.expanded.is_none()
        {
            return Err("at least one expected state must be specified".to_owned());
        }
        let started = Instant::now();
        let mut tree = self.tree().await?;
        loop {
            if let Ok(node) = get_node(&tree, &args.id)
                && state_matches(&node.state, &args)
            {
                return Ok(object_output(json!({
                    "elapsed_ms": started.elapsed().as_millis(),
                    "state": node.state,
                })));
            }
            let Some(remaining) =
                Duration::from_millis(args.timeout_ms).checked_sub(started.elapsed())
            else {
                return Err("timed out waiting for the requested state".to_owned());
            };
            tree = self
                .wait_for_tree(tree.generation, remaining)
                .await
                .map_err(|error| map_wait_error(error, "requested state"))?;
        }
    }

    #[tool(description = "Save the current semantic tree under a bounded in-memory name")]
    async fn save_ui_snapshot(
        &self,
        Parameters(args): Parameters<SnapshotArgs>,
    ) -> Result<Json<Value>, String> {
        validate_name(&args.name)?;
        let tree = self.tree().await?;
        let generation = tree.generation;
        let node_count = tree.nodes.len();
        let mut snapshots = self.snapshots.write().await;
        if !snapshots.trees.contains_key(&args.name) && snapshots.trees.len() >= MAX_TREE_SNAPSHOTS
        {
            return Err("tree snapshot capacity (32) has been reached".to_owned());
        }
        snapshots.trees.insert(args.name.clone(), tree);
        Ok(object_output(
            json!({ "name": args.name, "generation": generation, "node_count": node_count }),
        ))
    }

    #[tool(description = "Load a saved in-memory semantic tree snapshot")]
    async fn load_ui_snapshot(
        &self,
        Parameters(args): Parameters<SnapshotArgs>,
    ) -> Result<Json<Value>, String> {
        let snapshots = self.snapshots.read().await;
        let tree = snapshots
            .trees
            .get(&args.name)
            .ok_or_else(|| format!("tree snapshot {:?} was not found", args.name))?;
        Ok(object_output(
            serde_json::to_value(tree).map_err(encode_error)?,
        ))
    }

    #[tool(
        description = "Diff two saved semantic tree snapshots by stable node identifier and content"
    )]
    async fn diff_ui_snapshots(
        &self,
        Parameters(args): Parameters<DiffSnapshotsArgs>,
    ) -> Result<Json<Value>, String> {
        let snapshots = self.snapshots.read().await;
        let left = snapshots
            .trees
            .get(&args.left)
            .ok_or_else(|| format!("tree snapshot {:?} was not found", args.left))?;
        let right = snapshots
            .trees
            .get(&args.right)
            .ok_or_else(|| format!("tree snapshot {:?} was not found", args.right))?;
        Ok(object_output(tree_diff(left, right)))
    }

    #[tool(description = "Diff a saved semantic tree snapshot against the current GPUI tree")]
    async fn diff_current_ui(
        &self,
        Parameters(args): Parameters<DiffCurrentArgs>,
    ) -> Result<Json<Value>, String> {
        let current = self.tree().await?;
        let snapshots = self.snapshots.read().await;
        let saved = snapshots
            .trees
            .get(&args.name)
            .ok_or_else(|| format!("tree snapshot {:?} was not found", args.name))?;
        Ok(object_output(tree_diff(saved, &current)))
    }
}

pub(super) fn router() -> ToolRouter<GpuiMcp> {
    GpuiMcp::tree_router()
}
