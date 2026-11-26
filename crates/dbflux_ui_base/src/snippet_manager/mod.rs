//! `SnippetManager` — SQLite-backed manager for code snippets.
//!
//! Wraps `SnippetRepo` from `dbflux_storage` and caches the full snippet list
//! for synchronous reads, loaded lazily on first use. Writes go to the
//! repository first; the cache is rebuilt only after a successful write. A
//! write that committed succeeds even when that rebuild fails: the failure is
//! logged and the next read goes to the repository again, which reports it.

use std::sync::Arc;

use dbflux_storage::{SnippetDraft, SnippetError, SnippetRecord, SnippetRepo};

/// Code snippets of every language.
pub struct SnippetManager {
    repo: Arc<SnippetRepo>,
    cache: Option<Vec<SnippetRecord>>,
}

impl SnippetManager {
    pub fn new(repo: Arc<SnippetRepo>) -> Self {
        Self { repo, cache: None }
    }

    /// Every snippet, ordered by language then prefix.
    pub fn all(&mut self) -> Result<Vec<SnippetRecord>, SnippetError> {
        Ok(self.loaded()?.clone())
    }

    /// The snippets of one language, ordered by prefix.
    pub fn snippets_for_language(
        &mut self,
        language: &str,
    ) -> Result<Vec<SnippetRecord>, SnippetError> {
        let language = language.trim().to_lowercase();

        Ok(self
            .loaded()?
            .iter()
            .filter(|snippet| snippet.language == language)
            .cloned()
            .collect())
    }

    pub fn create(&mut self, draft: &SnippetDraft) -> Result<SnippetRecord, SnippetError> {
        let record = self.repo.insert(draft)?;
        self.reload_after_write();

        Ok(record)
    }

    pub fn update(
        &mut self,
        id: &str,
        draft: &SnippetDraft,
    ) -> Result<SnippetRecord, SnippetError> {
        let record = self.repo.update(id, draft)?;
        self.reload_after_write();

        Ok(record)
    }

    /// Deletes snippet `id`. Returns whether it existed.
    pub fn delete(&mut self, id: &str) -> Result<bool, SnippetError> {
        let deleted = self.repo.delete(id)?;
        self.reload_after_write();

        Ok(deleted)
    }

    fn loaded(&mut self) -> Result<&Vec<SnippetRecord>, SnippetError> {
        if self.cache.is_none() {
            self.cache = Some(self.repo.list_all()?);
        }

        Ok(self.cache.get_or_insert_with(Vec::new))
    }

    /// Rebuilds the cache after a committed write. On failure the cache stays
    /// empty, so the next read asks the repository again.
    fn reload_after_write(&mut self) {
        self.cache = None;

        match self.repo.list_all() {
            Ok(rows) => self.cache = Some(rows),
            Err(error) => log::warn!("snippets could not be reloaded after a write: {error}"),
        }
    }
}

#[cfg(test)]
mod tests;
