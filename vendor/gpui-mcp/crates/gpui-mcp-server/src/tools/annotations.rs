use gpui_mcp_protocol::{
    Annotation, AnnotationSpec, AnnotationStyle, AnnotationTarget, DEFAULT_ANNOTATION_COLOR,
    MAX_ANNOTATION_LABEL_BYTES, MAX_ANNOTATIONS,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::time::Duration;

use super::{
    BridgeResult, GpuiMcp, HighlightArgs, Json, Operation, Parameters, Rect, ToolRouter, Value,
    get_node, json, object_output, tool, tool_router,
};

/// Group the highlight tools write to, shared with the bridge's legacy operation.
const HIGHLIGHT_GROUP: &str = "highlights";

#[derive(Debug, Deserialize, JsonSchema)]
struct AnnotationArgs {
    /// Optional identifier. Reusing one replaces that annotation; omit it to
    /// have one assigned. Use it to update or remove the annotation later.
    id: Option<String>,
    /// Semantic node to mark. The annotation follows it through layout
    /// changes, scrolling and zoom, and hides while it is absent. Give either
    /// `node_id` or `rect`.
    node_id: Option<String>,
    /// Fixed window-relative logical rectangle to mark instead of a node.
    rect: Option<Rect>,
    /// Short tag drawn at the top-left corner, at most 128 bytes.
    label: Option<String>,
    /// Eight-digit `#RRGGBBAA` color; defaults to `#00A8FFFF`.
    color: Option<String>,
    /// `outline` (default), `fill` (use a translucent color) or `outline_fill`.
    #[serde(default)]
    style: AnnotationStyle,
    /// Group name, so related annotations can be cleared together.
    group: Option<String>,
    /// Remove automatically this many milliseconds after it is first drawn,
    /// from 1 through 3600000. Prefer a lifetime so annotations do not go stale.
    ttl_ms: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct AnnotateArgs {
    /// One through 128 annotations to add or replace, atomically.
    annotations: Vec<AnnotationArgs>,
    /// Remove every annotation in this group first, in the same step.
    replace_group: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct RemoveAnnotationsArgs {
    /// Identifiers to remove.
    #[serde(default)]
    ids: Vec<String>,
    /// Remove every annotation in this group.
    group: Option<String>,
    /// Remove every annotation, including the application's own.
    #[serde(default)]
    all: bool,
}

impl AnnotationArgs {
    fn into_spec(self) -> Result<AnnotationSpec, String> {
        let target = match (self.node_id, self.rect) {
            (Some(node_id), None) => AnnotationTarget::Node { node_id },
            (None, Some(rect)) => AnnotationTarget::Rect { rect },
            _ => return Err("give each annotation exactly one of node_id or rect".to_owned()),
        };
        let spec = AnnotationSpec {
            id: self.id,
            target,
            label: self.label,
            color: self
                .color
                .unwrap_or_else(|| DEFAULT_ANNOTATION_COLOR.to_owned()),
            style: self.style,
            group: self.group,
            ttl_ms: self.ttl_ms,
        };
        spec.validate().map_err(str::to_owned)?;
        Ok(spec)
    }
}

#[tool_router(router = annotation_router)]
impl GpuiMcp {
    #[tool(
        description = "Add or replace labelled annotations on semantic elements or rectangles. Node annotations re-resolve every frame, so they follow the element through layout changes, scrolling and zoom, and hide while it is absent. Returns every annotation with the bounds it was drawn at. The application can observe and mirror these."
    )]
    async fn annotate_elements(
        &self,
        Parameters(args): Parameters<AnnotateArgs>,
    ) -> Result<Json<Value>, String> {
        if args.annotations.is_empty() || args.annotations.len() > MAX_ANNOTATIONS {
            return Err("annotations must contain between 1 and 128 entries".to_owned());
        }
        let annotations = args
            .annotations
            .into_iter()
            .map(AnnotationArgs::into_spec)
            .collect::<Result<Vec<_>, _>>()?;
        let applied = self
            .upsert_annotations(annotations, args.replace_group)
            .await?;
        let ids: Vec<_> = applied
            .into_iter()
            .map(|annotation| annotation.id)
            .collect();
        let annotations = self.list_annotations_settled().await?;
        Ok(object_output(
            json!({ "applied": ids, "annotations": annotations }),
        ))
    }

    #[tool(description = "Remove annotations by id, by group, or all of them with all=true")]
    async fn remove_annotations(
        &self,
        Parameters(args): Parameters<RemoveAnnotationsArgs>,
    ) -> Result<Json<Value>, String> {
        if args.ids.is_empty() && args.group.is_none() && !args.all {
            return Err("name ids, a group, or set all=true".to_owned());
        }
        if !args.ids.is_empty() {
            self.annotation_call(Operation::RemoveAnnotations { ids: args.ids })
                .await?;
        }
        if args.group.is_some() || args.all {
            let group = if args.all { None } else { args.group };
            self.annotation_call(Operation::ClearAnnotations { group })
                .await?;
        }
        let annotations = self.list_annotations_settled().await?;
        Ok(object_output(json!({ "annotations": annotations })))
    }

    #[tool(
        description = "List current annotations, who set them, and the window-relative bounds each was drawn at in the latest frame (null when its element is absent, hidden or scrolled out of view)"
    )]
    async fn list_annotations(&self) -> Result<Json<Value>, String> {
        let annotations = self.annotation_call(Operation::ListAnnotations).await?;
        Ok(object_output(json!({ "annotations": annotations })))
    }

    #[tool(
        description = "Outline semantic elements with their labels. A thin wrapper over annotate_elements: the outlines follow the elements and replace the previous highlights"
    )]
    async fn highlight_elements(
        &self,
        Parameters(args): Parameters<HighlightArgs>,
    ) -> Result<Json<Value>, String> {
        if args.ids.is_empty() || args.ids.len() > 64 {
            return Err("ids must contain between 1 and 64 elements".to_owned());
        }
        let tree = self.tree().await?;
        let annotations = args
            .ids
            .iter()
            .enumerate()
            .map(|(index, id)| {
                let node = get_node(&tree, id)?;
                let label = node.label.clone().unwrap_or_else(|| id.clone());
                let mut spec = AnnotationSpec::node(id.clone())
                    .with_id(format!("highlight-{index}"))
                    .with_color(args.color.clone())
                    .with_group(HIGHLIGHT_GROUP)
                    .with_label(truncate_label(&label));
                spec.ttl_ms = args.ttl_ms;
                spec.validate().map_err(str::to_owned)?;
                Ok(spec)
            })
            .collect::<Result<Vec<_>, String>>()?;
        self.upsert_annotations(annotations, Some(HIGHLIGHT_GROUP.to_owned()))
            .await?;
        Ok(super::ack_json("highlighted"))
    }

    #[tool(description = "Clear the outlines drawn by highlight_elements")]
    async fn clear_highlights(&self) -> Result<Json<Value>, String> {
        self.annotation_call(Operation::ClearAnnotations {
            group: Some(HIGHLIGHT_GROUP.to_owned()),
        })
        .await?;
        self.settle_pending(Duration::from_secs(2)).await?;
        Ok(super::ack_json("highlights_cleared"))
    }
}

impl GpuiMcp {
    async fn annotation_call(&self, operation: Operation) -> Result<Vec<Annotation>, String> {
        match self.call(operation).await? {
            BridgeResult::Annotations(annotations) => Ok(annotations),
            _ => Err("bridge returned the wrong result for annotations".to_owned()),
        }
    }

    async fn upsert_annotations(
        &self,
        annotations: Vec<AnnotationSpec>,
        replace_group: Option<String>,
    ) -> Result<Vec<Annotation>, String> {
        let applied = self
            .annotation_call(Operation::UpsertAnnotations {
                annotations,
                replace_group,
            })
            .await?;
        self.settle_pending(Duration::from_secs(2)).await?;
        Ok(applied)
    }

    /// List annotations after the frame that draws the latest change.
    async fn list_annotations_settled(&self) -> Result<Vec<Annotation>, String> {
        self.settle_pending(Duration::from_secs(2)).await?;
        self.annotation_call(Operation::ListAnnotations).await
    }
}

/// Cut a label to the annotation bound at a character boundary.
fn truncate_label(label: &str) -> String {
    let label: String = label.chars().filter(|c| !c.is_control()).collect();
    if label.len() <= MAX_ANNOTATION_LABEL_BYTES {
        return label;
    }
    let mut end = MAX_ANNOTATION_LABEL_BYTES - '…'.len_utf8();
    while !label.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &label[..end])
}

pub(super) fn router() -> ToolRouter<GpuiMcp> {
    GpuiMcp::annotation_router()
}

#[cfg(test)]
mod tests {
    use gpui_mcp_protocol::{AnnotationTarget, MAX_ANNOTATION_LABEL_BYTES};
    use serde_json::json;

    use super::{AnnotationArgs, truncate_label};

    fn args(value: serde_json::Value) -> Result<AnnotationArgs, serde_json::Error> {
        serde_json::from_value(value)
    }

    #[test]
    fn each_annotation_names_one_target() -> Result<(), Box<dyn std::error::Error>> {
        let spec = args(json!({ "node_id": "save", "label": "Save" }))?.into_spec()?;
        assert!(matches!(spec.target, AnnotationTarget::Node { node_id } if node_id == "save"));
        assert!(args(json!({}))?.into_spec().is_err());
        assert!(
            args(json!({
                "node_id": "save",
                "rect": { "x": 0, "y": 0, "width": 1, "height": 1 }
            }))?
            .into_spec()
            .is_err()
        );
        assert!(
            args(json!({ "node_id": "save", "color": "blue" }))?
                .into_spec()
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn long_labels_are_cut_at_a_character_boundary() {
        let label = truncate_label(&"é".repeat(200));
        assert!(label.len() <= MAX_ANNOTATION_LABEL_BYTES);
        assert!(label.ends_with('…'));
        assert_eq!(truncate_label("Save\n"), "Save");
    }
}
