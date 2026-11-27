//! Key condition of a SHOWPLAN operator: its seek predicates, or its residual
//! predicate when it has none.

use dbflux_core::PlanCondition;

use super::{Attributes, attribute, unbracket};

pub(super) const SEEK_LABEL: &str = "Seek Predicates";
const PREDICATE_LABEL: &str = "Predicate";

struct PredicateScope {
    owner: usize,
    depth: usize,
    scalar: Option<String>,
}

#[derive(Default)]
struct SeekScope {
    owner: usize,
    depth: usize,
    groups: Vec<String>,
    operator: String,
    columns: Vec<String>,
    expressions: Vec<String>,
    expressions_depth: Option<usize>,
}

/// Follows `Predicate` and `SeekPredicates` elements while the parser walks the
/// XML. `owner` is the number of open operators, so a predicate inside a
/// nested subquery plan is credited to that plan's operator, not the outer one.
#[derive(Default)]
pub(super) struct ConditionCapture {
    predicates: Vec<PredicateScope>,
    seeks: Vec<SeekScope>,
}

impl ConditionCapture {
    /// `path` already ends with the element being opened.
    pub(super) fn open(
        &mut self,
        name: &str,
        attributes: &Attributes,
        path: &[String],
        owner: usize,
    ) {
        if owner == 0 {
            return;
        }

        match name {
            "Predicate" => self.predicates.push(PredicateScope {
                owner,
                depth: path.len(),
                scalar: None,
            }),
            "SeekPredicates" => self.seeks.push(SeekScope {
                owner,
                depth: path.len(),
                ..SeekScope::default()
            }),
            _ => self.open_inner(name, attributes, path, owner),
        }
    }

    fn open_inner(&mut self, name: &str, attributes: &Attributes, path: &[String], owner: usize) {
        let scalar = attribute(attributes, "ScalarString").map(str::to_string);

        if let Some(seek) = self.seeks.last_mut().filter(|seek| seek.owner == owner) {
            match name {
                "Prefix" | "StartRange" | "EndRange" => {
                    seek.operator = attribute(attributes, "ScanType")
                        .unwrap_or("EQ")
                        .to_string();
                    seek.columns.clear();
                    seek.expressions.clear();
                }
                "RangeExpressions" => seek.expressions_depth = Some(path.len()),
                "ColumnReference" if parent(path) == Some("RangeColumns") => {
                    seek.columns.push(column_text(attributes));
                }
                "ScalarOperator" if seek.expressions_depth == Some(path.len() - 1) => {
                    seek.expressions.extend(scalar);
                }
                _ => {}
            }
            return;
        }

        if name == "ScalarOperator"
            && let Some(predicate) = self
                .predicates
                .last_mut()
                .filter(|predicate| predicate.owner == owner && predicate.scalar.is_none())
        {
            predicate.scalar = scalar;
        }
    }

    /// `path` still ends with the element being closed. Returns the condition
    /// the element completed, if any.
    pub(super) fn close(&mut self, name: &str, path: &[String]) -> Option<PlanCondition> {
        match name {
            "Prefix" | "StartRange" | "EndRange" => {
                if let Some(seek) = self.seeks.last_mut() {
                    seek.flush_group();
                }
                None
            }
            "RangeExpressions" => {
                if let Some(seek) = self.seeks.last_mut() {
                    seek.expressions_depth = None;
                }
                None
            }
            "SeekPredicates" if self.seeks.last().is_some_and(|s| s.depth == path.len()) => {
                let seek = self.seeks.pop()?;
                (!seek.groups.is_empty()).then(|| PlanCondition {
                    label: SEEK_LABEL.to_string(),
                    value: seek.groups.join(" AND "),
                })
            }
            "Predicate"
                if self
                    .predicates
                    .last()
                    .is_some_and(|p| p.depth == path.len()) =>
            {
                let predicate = self.predicates.pop()?;
                predicate.scalar.map(|value| PlanCondition {
                    label: PREDICATE_LABEL.to_string(),
                    value,
                })
            }
            _ => None,
        }
    }
}

impl SeekScope {
    fn flush_group(&mut self) {
        let operator = match self.operator.as_str() {
            "EQ" => "=",
            "GT" => ">",
            "GE" => ">=",
            "LT" => "<",
            "LE" => "<=",
            other => other,
        };

        for (column, expression) in self.columns.iter().zip(&self.expressions) {
            self.groups
                .push(format!("{column} {operator} {expression}"));
        }
        self.columns.clear();
        self.expressions.clear();
    }
}

fn parent(path: &[String]) -> Option<&str> {
    path.len()
        .checked_sub(2)
        .and_then(|index| path.get(index))
        .map(String::as_str)
}

fn column_text(attributes: &Attributes) -> String {
    let column = format!(
        "[{}]",
        unbracket(attribute(attributes, "Column").unwrap_or("?"))
    );

    match attribute(attributes, "Alias").or_else(|| attribute(attributes, "Table")) {
        Some(owner) => format!("[{}].{column}", unbracket(owner)),
        None => column,
    }
}
