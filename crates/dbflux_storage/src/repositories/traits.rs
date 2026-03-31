//! Common repository trait for DBFlux storage.
//!
//! All domain repositories implement this trait for a consistent API.

use crate::error::RepositoryError;

/// A repository provides CRUD operations for a specific entity type.
///
/// Implementors must be Send + Sync to allow concurrent access.
pub trait Repository: Send + Sync {
    /// The entity type stored by this repository.
    type Entity;
    /// The identifier type for entities.
    type Id;

    /// Returns all entities.
    fn all(&self) -> Result<Vec<Self::Entity>, RepositoryError>;

    /// Finds an entity by its ID.
    fn find_by_id(&self, id: &Self::Id) -> Result<Option<Self::Entity>, RepositoryError>;

    /// Inserts or updates an entity.
    fn upsert(&self, entity: &Self::Entity) -> Result<(), RepositoryError>;

    /// Deletes an entity by its ID.
    fn delete(&self, id: &Self::Id) -> Result<(), RepositoryError>;
}
