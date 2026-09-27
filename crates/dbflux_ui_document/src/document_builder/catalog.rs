//! Fields the builder offers, read from the collection's schema sample.

use std::collections::HashMap;

use dbflux_core::{CollectionSchemaSample, DocumentFieldType, NULL_TYPE_NAME};

/// One sampled field path.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogField {
    /// Dotted path, relative to the catalog's scope.
    pub path: String,
    /// Last segment of the path.
    pub name: String,
    /// Nesting below the top level of the scope.
    pub depth: usize,
    /// Builder types seen for the field, most common first, without null.
    pub types: Vec<DocumentFieldType>,
    /// Share of the sampled documents that hold the field.
    pub presence_percent: u32,
}

/// Sampled fields in tree order: every nested path right after its parent,
/// siblings in the order the sample first met them.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FieldCatalog {
    fields: Vec<CatalogField>,
}

impl FieldCatalog {
    /// Reads `sample`, mapping its native type names through `field_type`.
    /// Types the builder has no operators for are left out.
    pub fn new(
        sample: &CollectionSchemaSample,
        field_type: impl Fn(&str) -> Option<DocumentFieldType>,
    ) -> Self {
        let fields = sample
            .fields
            .iter()
            .map(|stats| {
                let mut types = Vec::new();
                for share in &stats.types {
                    if share.type_name == NULL_TYPE_NAME {
                        continue;
                    }
                    if let Some(field_type) = field_type(&share.type_name)
                        && !types.contains(&field_type)
                    {
                        types.push(field_type);
                    }
                }

                let ratio = stats.presence_ratio(sample.sampled_documents);
                CatalogField {
                    path: stats.path.clone(),
                    name: String::new(),
                    depth: 0,
                    types,
                    presence_percent: (ratio * 100.0).round().clamp(0.0, 100.0) as u32,
                }
            })
            .collect();

        Self::in_tree_order(fields)
    }

    /// Orders `fields` depth-first. A path whose parent was not sampled is
    /// placed at the top level.
    fn in_tree_order(fields: Vec<CatalogField>) -> Self {
        let known: HashMap<String, usize> = fields
            .iter()
            .enumerate()
            .map(|(index, field)| (field.path.clone(), index))
            .collect();

        let mut children: HashMap<Option<usize>, Vec<usize>> = HashMap::new();
        for (index, field) in fields.iter().enumerate() {
            let parent = field
                .path
                .rsplit_once('.')
                .and_then(|(parent, _)| known.get(parent).copied());
            children.entry(parent).or_default().push(index);
        }

        let mut ordered = Vec::with_capacity(fields.len());
        let mut stack: Vec<(usize, usize)> = children
            .get(&None)
            .map(|roots| roots.iter().rev().map(|index| (*index, 0)).collect())
            .unwrap_or_default();

        while let Some((index, depth)) = stack.pop() {
            let Some(field) = fields.get(index) else {
                continue;
            };

            let name = match field.path.rsplit_once('.') {
                Some((_, name)) if depth > 0 => name.to_string(),
                _ => field.path.clone(),
            };

            ordered.push(CatalogField {
                name,
                depth,
                ..field.clone()
            });

            if let Some(nested) = children.get(&Some(index)) {
                stack.extend(nested.iter().rev().map(|child| (*child, depth + 1)));
            }
        }

        Self { fields: ordered }
    }

    /// A flat catalog of computed output fields, such as the group keys and
    /// accumulators after a group stage: every one present in every row.
    pub fn from_outputs(outputs: Vec<(String, Vec<DocumentFieldType>)>) -> Self {
        let fields = outputs
            .into_iter()
            .map(|(path, types)| CatalogField {
                name: path.clone(),
                path,
                depth: 0,
                types,
                presence_percent: 100,
            })
            .collect();

        Self { fields }
    }

    /// The fields sampled with a number type, flattened to the top level:
    /// what a `$sum` or `$avg` can read.
    pub fn numeric(&self) -> FieldCatalog {
        let fields = self
            .fields
            .iter()
            .filter(|field| {
                field.types.iter().any(|field_type| {
                    matches!(
                        field_type,
                        DocumentFieldType::Integer | DocumentFieldType::Decimal
                    )
                })
            })
            .map(|field| CatalogField {
                name: field.path.clone(),
                depth: 0,
                ..field.clone()
            })
            .collect();

        Self { fields }
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    #[cfg(test)]
    pub fn fields(&self) -> &[CatalogField] {
        &self.fields
    }

    pub fn field(&self, path: &str) -> Option<&CatalogField> {
        self.fields.iter().find(|field| field.path == path)
    }

    /// Types sampled for `path`; empty for a path the sample never saw.
    pub fn types(&self, path: &str) -> Vec<DocumentFieldType> {
        self.field(path)
            .map(|field| field.types.clone())
            .unwrap_or_default()
    }

    pub fn is_sampled(&self, path: &str) -> bool {
        self.field(path).is_some()
    }

    /// Fields whose path contains `query`, ignoring case; every field for an
    /// empty query.
    pub fn search(&self, query: &str) -> Vec<&CatalogField> {
        let query = query.trim().to_lowercase();

        self.fields
            .iter()
            .filter(|field| query.is_empty() || field.path.to_lowercase().contains(&query))
            .collect()
    }

    /// The fields below `prefix`, with paths relative to it: what an
    /// `$elemMatch` on the array at `prefix` can test.
    pub fn scoped(&self, prefix: &str) -> FieldCatalog {
        if prefix.is_empty() {
            return self.clone();
        }

        let lead = format!("{prefix}.");
        let fields = self
            .fields
            .iter()
            .filter_map(|field| {
                field.path.strip_prefix(&lead).map(|relative| CatalogField {
                    path: relative.to_string(),
                    ..field.clone()
                })
            })
            .collect();

        Self::in_tree_order(fields)
    }
}
