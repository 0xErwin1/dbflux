//! Two-way sync between the builder and the query slots.
//!
//! The slots stay the query that runs. The builder reads them whenever they
//! change and writes back only the parts an edit changed. A slot holding
//! clauses the builder could not read is never written by an edit: the edit
//! is held until the user keeps the text or rewrites the slot from the
//! builder.

use dbflux_core::{
    DocumentFindSlots, DocumentQuerySpec, DocumentSlot, DocumentSlotParse, UnrepresentableClause,
};

/// Slot texts to write; `None` leaves a slot as it is.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SlotWrite {
    pub filter: Option<String>,
    pub projection: Option<String>,
    pub sort: Option<String>,
    pub limit: Option<String>,
}

impl SlotWrite {
    pub fn is_empty(&self) -> bool {
        self.filter.is_none()
            && self.projection.is_none()
            && self.sort.is_none()
            && self.limit.is_none()
    }
}

/// What the slots and the builder last agreed on.
#[derive(Debug, Clone, Default)]
pub struct SlotSync {
    /// Slot texts as last read or written.
    texts: DocumentFindSlots,
    /// The query those texts hold, part by part.
    spec: DocumentQuerySpec,
    unrepresentable: Vec<UnrepresentableClause>,
    /// Slots an edit would change but may not write.
    held: Vec<DocumentSlot>,
}

impl SlotSync {
    /// Records the slots and what the builder read from them.
    pub fn read(&mut self, slots: &DocumentFindSlots, parse: &DocumentSlotParse) {
        self.texts = slots.clone();
        self.spec = parse.spec.clone();
        self.unrepresentable = parse.unrepresentable.clone();
        self.held.clear();
    }

    /// Whether `slots` hold exactly what was last read or written, so a
    /// change notification carries nothing new.
    pub fn is_echo(&self, slots: &DocumentFindSlots) -> bool {
        self.texts.filter == slots.filter
            && self.texts.projection == slots.projection
            && self.texts.sort == slots.sort
            && self.texts.limit == slots.limit
    }

    /// Whether some slot holds clauses the builder could not read.
    pub fn is_conflicted(&self) -> bool {
        !self.unrepresentable.is_empty()
    }

    /// Whether `slot` holds clauses the builder could not read.
    pub fn locks(&self, slot: DocumentSlot) -> bool {
        self.unrepresentable
            .iter()
            .any(|clause| clause.slot == slot)
    }

    /// Slot texts as last read or written.
    pub fn texts(&self) -> &DocumentFindSlots {
        &self.texts
    }

    /// The query the slots held when last read or written.
    pub fn spec(&self) -> &DocumentQuerySpec {
        &self.spec
    }

    pub fn unrepresentable(&self) -> &[UnrepresentableClause] {
        &self.unrepresentable
    }

    /// Slots whose builder edits wait for "Keep the text" or "Rewrite from
    /// builder".
    pub fn held(&self) -> &[DocumentSlot] {
        &self.held
    }

    /// Texts to write for an edit that produced `spec`, rendered as
    /// `rendered`. Only the parts that changed are written, and never a slot
    /// the builder could not fully read.
    pub fn plan(&mut self, spec: &DocumentQuerySpec, rendered: &DocumentFindSlots) -> SlotWrite {
        let mut write = SlotWrite::default();

        if spec.filter != self.spec.filter {
            if self.locks(DocumentSlot::Filter) {
                self.hold(DocumentSlot::Filter);
            } else {
                write.filter = Some(self.write_filter(spec, rendered));
            }
        } else {
            self.release(DocumentSlot::Filter);
        }

        if spec.projection != self.spec.projection {
            if self.locks(DocumentSlot::Projection) {
                self.hold(DocumentSlot::Projection);
            } else {
                write.projection = Some(self.write_projection(spec, rendered));
            }
        } else {
            self.release(DocumentSlot::Projection);
        }

        if spec.sort != self.spec.sort {
            if self.locks(DocumentSlot::Sort) {
                self.hold(DocumentSlot::Sort);
            } else {
                write.sort = Some(self.write_sort(spec, rendered));
            }
        } else {
            self.release(DocumentSlot::Sort);
        }

        if spec.limit != self.spec.limit {
            write.limit = Some(self.write_limit(spec));
        }

        write
    }

    /// Texts that replace every slot with what the builder holds, dropping
    /// the clauses it could not read. Every part is written: the slots may
    /// no longer hold what was last read, so a part that looks unchanged
    /// here is not known to be unchanged there.
    pub fn rewrite(&mut self, spec: &DocumentQuerySpec, rendered: &DocumentFindSlots) -> SlotWrite {
        let write = SlotWrite {
            filter: Some(self.write_filter(spec, rendered)),
            projection: Some(self.write_projection(spec, rendered)),
            sort: Some(self.write_sort(spec, rendered)),
            limit: Some(self.write_limit(spec)),
        };

        self.unrepresentable.clear();
        self.held.clear();

        write
    }

    fn write_filter(&mut self, spec: &DocumentQuerySpec, rendered: &DocumentFindSlots) -> String {
        let text = part_text(spec.filter.is_empty(), &rendered.filter);
        self.texts.filter = text.clone();
        self.spec.filter = spec.filter.clone();
        self.release(DocumentSlot::Filter);
        text
    }

    fn write_projection(
        &mut self,
        spec: &DocumentQuerySpec,
        rendered: &DocumentFindSlots,
    ) -> String {
        let text = part_text(spec.projection.is_empty(), &rendered.projection);
        self.texts.projection = text.clone();
        self.spec.projection = spec.projection.clone();
        self.release(DocumentSlot::Projection);
        text
    }

    fn write_sort(&mut self, spec: &DocumentQuerySpec, rendered: &DocumentFindSlots) -> String {
        let text = part_text(spec.sort.is_empty(), &rendered.sort);
        self.texts.sort = text.clone();
        self.spec.sort = spec.sort.clone();
        self.release(DocumentSlot::Sort);
        text
    }

    fn write_limit(&mut self, spec: &DocumentQuerySpec) -> String {
        self.texts.limit = spec.limit;
        self.spec.limit = spec.limit;
        spec.limit
            .map(|limit| limit.to_string())
            .unwrap_or_default()
    }

    fn hold(&mut self, slot: DocumentSlot) {
        if !self.held.contains(&slot) {
            self.held.push(slot);
        }
    }

    fn release(&mut self, slot: DocumentSlot) {
        self.held.retain(|held| *held != slot);
    }
}

/// An empty part leaves its slot empty instead of writing the driver's
/// empty document, so the slot shows its placeholder again.
fn part_text(empty: bool, rendered: &str) -> String {
    if empty {
        String::new()
    } else {
        rendered.to_string()
    }
}
