//! Storage layer and page model of the delimited (CSV and TSV) document.
//!
//! The storage layer answers four questions and nothing else: where a
//! delimited file lives, how its bytes are read by range, which version of it
//! was opened, and how an edited copy replaces it. Every storage function
//! blocks on file or network I/O and touches no GPUI state, so callers run it
//! on the background executor and report the returned errors themselves.
//!
//! The page model holds the records loaded so far, builds the table model
//! from them and turns the table's pending edits into an edit set. It does no
//! I/O.

mod page_model;
mod save;
mod source;

#[cfg(test)]
mod page_model_tests;
#[cfg(test)]
mod tests;

pub use page_model::{PageModel, PageModelError};
pub use save::{SaveOutcome, save_edited};
pub use source::{
    DelimitedLocation, DelimitedSource, ObjectSource, SourceVersion, StorageError,
    has_changed_since, open_source, read_version,
};
