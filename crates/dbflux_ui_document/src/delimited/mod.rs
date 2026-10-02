//! Storage layer of the delimited (CSV and TSV) document.
//!
//! It answers four questions and nothing else: where a delimited file lives,
//! how its bytes are read by range, which version of it was opened, and how
//! an edited copy replaces it. Every function here blocks on file or network
//! I/O and touches no GPUI state, so callers run it on the background
//! executor and report the returned errors themselves.

mod save;
mod source;

#[cfg(test)]
mod tests;

pub use save::{SaveOutcome, save_edited};
pub use source::{
    DelimitedLocation, DelimitedSource, ObjectSource, SourceVersion, StorageError,
    has_changed_since, open_source, read_version,
};
