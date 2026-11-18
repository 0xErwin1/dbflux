//! The bridge's annotation set. GPUI-independent so its rules are unit tested.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use gpui_mcp_protocol::{
    Annotation, AnnotationSource, AnnotationSpec, AnnotationStyle, AnnotationTarget, BridgeError,
    ErrorCode, MAX_ANNOTATIONS, Rect, parse_rgba,
};

/// Group that the legacy highlight operations write to.
pub(crate) const HIGHLIGHT_GROUP: &str = "highlights";

#[derive(Debug)]
struct Entry {
    annotation: Annotation,
    ttl: Option<Duration>,
    /// Set from `ttl` by the first frame that sees the annotation, on the
    /// window executor's clock.
    expires_at: Option<Instant>,
}

/// What one annotation needs to be drawn in a frame.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PaintItem {
    pub(crate) id: String,
    pub(crate) target: AnnotationTarget,
    pub(crate) color: u32,
    pub(crate) style: AnnotationStyle,
    pub(crate) label: Option<String>,
}

#[derive(Debug, Default)]
pub(crate) struct AnnotationStore {
    /// Draw order: earlier entries are drawn first.
    entries: Vec<Entry>,
    next_id: u64,
}

impl AnnotationStore {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Add or replace annotations atomically: either every spec applies or none does.
    pub(crate) fn upsert(
        &mut self,
        specs: Vec<AnnotationSpec>,
        replace_group: Option<&str>,
        source: AnnotationSource,
        now_ms: u64,
    ) -> Result<Vec<Annotation>, BridgeError> {
        let mut seen = BTreeSet::new();
        for spec in &specs {
            spec.validate()
                .map_err(|message| BridgeError::new(ErrorCode::InvalidRequest, message))?;
            if let Some(id) = &spec.id
                && !seen.insert(id.as_str())
            {
                return Err(BridgeError::new(
                    ErrorCode::InvalidRequest,
                    "annotation ids must be unique within one request",
                ));
            }
        }
        if replace_group.is_some_and(|group| !gpui_mcp_protocol::is_valid_annotation_name(group)) {
            return Err(BridgeError::new(
                ErrorCode::InvalidRequest,
                "annotation group is invalid",
            ));
        }
        let survivors = self
            .entries
            .iter()
            .filter(|entry| {
                replace_group.is_none_or(|group| entry.annotation.group.as_deref() != Some(group))
            })
            .filter(|entry| !seen.contains(entry.annotation.id.as_str()))
            .count();
        let added = specs.iter().filter(|spec| spec.id.is_none()).count() + seen.len();
        if survivors + added > MAX_ANNOTATIONS {
            return Err(BridgeError::new(
                ErrorCode::Busy,
                format!("no more than {MAX_ANNOTATIONS} annotations may exist at once"),
            ));
        }

        if let Some(group) = replace_group {
            self.entries
                .retain(|entry| entry.annotation.group.as_deref() != Some(group));
        }
        let mut applied = Vec::with_capacity(specs.len());
        for spec in specs {
            let id = match spec.id {
                Some(id) => id,
                None => self.generate_id(),
            };
            let annotation = Annotation {
                id: id.clone(),
                target: spec.target,
                label: spec.label,
                color: spec.color,
                style: spec.style,
                group: spec.group,
                source,
                updated_ms: now_ms,
                expires_ms: spec.ttl_ms.map(|ttl| now_ms.saturating_add(ttl)),
                resolved: None,
            };
            applied.push(annotation.clone());
            let entry = Entry {
                annotation,
                ttl: spec.ttl_ms.map(Duration::from_millis),
                expires_at: None,
            };
            // Replacing keeps the annotation's place in the draw order.
            match self
                .entries
                .iter_mut()
                .find(|existing| existing.annotation.id == id)
            {
                Some(existing) => *existing = entry,
                None => self.entries.push(entry),
            }
        }
        Ok(applied)
    }

    fn generate_id(&mut self) -> String {
        loop {
            self.next_id += 1;
            let id = format!("annotation-{}", self.next_id);
            if !self.entries.iter().any(|entry| entry.annotation.id == id) {
                return id;
            }
        }
    }

    /// Remove annotations by id and return how many were removed.
    pub(crate) fn remove(&mut self, ids: &[impl AsRef<str>]) -> usize {
        let before = self.entries.len();
        self.entries.retain(|entry| {
            !ids.iter()
                .any(|id| id.as_ref() == entry.annotation.id.as_str())
        });
        before - self.entries.len()
    }

    /// Remove every annotation, or every annotation in `group`.
    pub(crate) fn clear(&mut self, group: Option<&str>) -> usize {
        let before = self.entries.len();
        match group {
            Some(group) => self
                .entries
                .retain(|entry| entry.annotation.group.as_deref() != Some(group)),
            None => self.entries.clear(),
        }
        before - self.entries.len()
    }

    pub(crate) fn list(&self) -> Vec<Annotation> {
        self.entries
            .iter()
            .map(|entry| entry.annotation.clone())
            .collect()
    }

    /// Start the lifetime of annotations seen for the first time, then drop
    /// expired annotations and return whether any were dropped.
    pub(crate) fn prune_expired(&mut self, now: Instant) -> bool {
        for entry in &mut self.entries {
            if entry.expires_at.is_none()
                && let Some(ttl) = entry.ttl
            {
                entry.expires_at = Some(now + ttl);
            }
        }
        let before = self.entries.len();
        self.entries
            .retain(|entry| entry.expires_at.is_none_or(|deadline| deadline > now));
        before != self.entries.len()
    }

    /// The soonest moment an annotation expires.
    pub(crate) fn next_expiry(&self) -> Option<Instant> {
        self.entries
            .iter()
            .filter_map(|entry| entry.expires_at)
            .min()
    }

    pub(crate) fn paint_items(&self) -> Vec<PaintItem> {
        self.entries
            .iter()
            .filter_map(|entry| {
                let annotation = &entry.annotation;
                Some(PaintItem {
                    id: annotation.id.clone(),
                    target: annotation.target.clone(),
                    color: parse_rgba(&annotation.color)?,
                    style: annotation.style,
                    label: annotation.label.clone(),
                })
            })
            .collect()
    }

    /// Record the bounds each annotation was drawn at in the latest frame.
    pub(crate) fn set_resolved(&mut self, resolved: &BTreeMap<String, Option<Rect>>) {
        for entry in &mut self.entries {
            if let Some(rect) = resolved.get(&entry.annotation.id) {
                entry.annotation.resolved = *rect;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};

    use gpui_mcp_protocol::{AnnotationSource, AnnotationSpec, ErrorCode, MAX_ANNOTATIONS, Rect};

    use super::AnnotationStore;

    fn upsert(
        store: &mut AnnotationStore,
        specs: Vec<AnnotationSpec>,
        group: Option<&str>,
    ) -> Result<Vec<String>, ErrorCode> {
        store
            .upsert(specs, group, AnnotationSource::Agent, 1)
            .map(|applied| {
                applied
                    .into_iter()
                    .map(|annotation| annotation.id)
                    .collect()
            })
            .map_err(|error| error.code)
    }

    #[test]
    fn upsert_replaces_by_id_and_keeps_draw_order() -> Result<(), ErrorCode> {
        let mut store = AnnotationStore::default();
        upsert(
            &mut store,
            vec![
                AnnotationSpec::node("a").with_id("first"),
                AnnotationSpec::node("b").with_id("second"),
            ],
            None,
        )?;
        upsert(
            &mut store,
            vec![
                AnnotationSpec::node("c")
                    .with_id("first")
                    .with_label("moved"),
            ],
            None,
        )?;
        let list = store.list();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].id, "first");
        assert_eq!(list[0].label.as_deref(), Some("moved"));
        assert_eq!(list[1].id, "second");
        Ok(())
    }

    #[test]
    fn generated_ids_never_collide_with_chosen_ones() -> Result<(), ErrorCode> {
        let mut store = AnnotationStore::default();
        upsert(
            &mut store,
            vec![AnnotationSpec::node("a").with_id("annotation-1")],
            None,
        )?;
        let ids = upsert(&mut store, vec![AnnotationSpec::node("b")], None)?;
        assert_eq!(ids, ["annotation-2"]);
        Ok(())
    }

    #[test]
    fn replace_group_is_atomic_with_the_upsert() -> Result<(), ErrorCode> {
        let mut store = AnnotationStore::default();
        upsert(
            &mut store,
            vec![
                AnnotationSpec::node("a").with_group("review"),
                AnnotationSpec::node("b").with_id("keep"),
            ],
            None,
        )?;
        upsert(
            &mut store,
            vec![AnnotationSpec::node("c").with_group("review")],
            Some("review"),
        )?;
        let targets: Vec<_> = store.list().into_iter().map(|a| a.id).collect();
        assert_eq!(targets, ["keep", "annotation-2"]);

        // An invalid spec leaves everything untouched, including the group.
        assert_eq!(
            upsert(
                &mut store,
                vec![AnnotationSpec::node("d").with_color("red")],
                Some("review"),
            ),
            Err(ErrorCode::InvalidRequest)
        );
        assert_eq!(store.list().len(), 2);
        Ok(())
    }

    #[test]
    fn capacity_is_bounded() -> Result<(), ErrorCode> {
        let mut store = AnnotationStore::default();
        let specs = (0..MAX_ANNOTATIONS)
            .map(|index| AnnotationSpec::node(format!("n{index}")))
            .collect();
        upsert(&mut store, specs, None)?;
        assert_eq!(
            upsert(&mut store, vec![AnnotationSpec::node("x")], None),
            Err(ErrorCode::Busy)
        );
        // Replacing an existing id needs no extra room.
        upsert(
            &mut store,
            vec![AnnotationSpec::node("x").with_id("annotation-1")],
            None,
        )?;
        assert_eq!(
            upsert(
                &mut store,
                vec![
                    AnnotationSpec::node("x").with_id("dup"),
                    AnnotationSpec::node("y").with_id("dup"),
                ],
                None,
            ),
            Err(ErrorCode::InvalidRequest)
        );
        Ok(())
    }

    #[test]
    fn expiry_prunes_only_due_annotations() -> Result<(), ErrorCode> {
        let mut store = AnnotationStore::default();
        let start = Instant::now();
        store
            .upsert(
                vec![
                    AnnotationSpec::node("a").with_id("short").with_ttl_ms(100),
                    AnnotationSpec::node("b")
                        .with_id("long")
                        .with_ttl_ms(10_000),
                    AnnotationSpec::node("c").with_id("forever"),
                ],
                None,
                AnnotationSource::App,
                1_000,
            )
            .map_err(|error| error.code)?;
        assert_eq!(store.list()[0].expires_ms, Some(1_100));
        assert_eq!(
            store.next_expiry(),
            None,
            "lifetimes start when first drawn"
        );
        assert!(!store.prune_expired(start));
        assert_eq!(
            store.next_expiry(),
            Some(start + Duration::from_millis(100))
        );
        assert!(!store.prune_expired(start + Duration::from_millis(99)));
        assert!(store.prune_expired(start + Duration::from_millis(100)));
        let ids: Vec<_> = store.list().into_iter().map(|a| a.id).collect();
        assert_eq!(ids, ["long", "forever"]);
        Ok(())
    }

    #[test]
    fn remove_clear_and_resolution() -> Result<(), ErrorCode> {
        let mut store = AnnotationStore::default();
        upsert(
            &mut store,
            vec![
                AnnotationSpec::node("a").with_id("a").with_group("g"),
                AnnotationSpec::node("b").with_id("b").with_group("g"),
                AnnotationSpec::node("c").with_id("c"),
            ],
            None,
        )?;
        let rect = Rect {
            x: 1.0,
            y: 2.0,
            width: 3.0,
            height: 4.0,
        };
        store.set_resolved(&BTreeMap::from([
            ("a".to_owned(), Some(rect)),
            ("c".to_owned(), None),
        ]));
        assert_eq!(store.list()[0].resolved, Some(rect));
        assert_eq!(store.remove(&["a", "missing"]), 1);
        assert_eq!(store.clear(Some("g")), 1);
        assert_eq!(store.paint_items().len(), 1);
        assert_eq!(store.clear(None), 1);
        assert_eq!(store.len(), 0);
        Ok(())
    }
}
