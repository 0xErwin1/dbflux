//! The delimited (CSV and TSV) document, its storage layer and its page model.
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
//!
//! The document is the tab: it opens a file through the storage layer on the
//! background executor and shows the page model in a table, which edits the
//! loaded records by position and saves them through the storage layer. Its
//! toolbar shows the dialect the file was read with and overrides parts of
//! it.

mod document;
mod editing;
mod page_model;
mod pane;
mod render;
mod save;
mod source;
mod toolbar;

#[cfg(test)]
mod document_tests;
#[cfg(test)]
mod editing_tests;
#[cfg(test)]
mod page_model_tests;
#[cfg(test)]
mod tests;

pub use document::{DelimitedDocument, DelimitedWarning};
pub use page_model::{PageModel, PageModelError};
pub use save::{SaveOutcome, save_edited};
pub use source::{
    DelimitedLocation, DelimitedSource, ObjectSource, SourceVersion, StorageError,
    has_changed_since, open_source, read_version,
};
