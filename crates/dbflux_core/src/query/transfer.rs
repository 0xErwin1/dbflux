//! Column metadata shared by the data-transfer engine (`dbflux_transfer`) and
//! by [`crate::query::generator::CreateTableSpec`].
//!
//! Lives in `dbflux_core` rather than `dbflux_transfer` because
//! `CreateTableSpec` is part of the `QueryGenerator` trait (implemented by
//! every SQL driver, which depends on `dbflux_core`, not the reverse).

/// One column as seen by an Export/Import/Migration flow: enough to render a
/// same-engine `CREATE TABLE` column definition and to preserve nullability
/// and primary-key membership across the transfer.
///
/// Serializable so it can be embedded verbatim in a `TransferManifest`
/// (`dbflux_transfer::manifest`), letting Import recreate a table without
/// re-querying the source driver's schema.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransferColumn {
    pub name: String,
    pub type_name: Option<String>,
    pub nullable: bool,
    pub is_primary_key: bool,
}
