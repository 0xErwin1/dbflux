//! Guards against the reported client identity drifting from the app version.
//!
//! `dbflux_core::client_identity()` bakes in `dbflux_core`'s compile-time
//! version. Every crate inherits the single `[workspace.package]` version, so
//! the two must always match; this test fails if a crate ever stops
//! inheriting it.

#[test]
fn client_identity_matches_app_version() {
    let expected = format!("dbflux/{}", env!("CARGO_PKG_VERSION"));
    assert_eq!(dbflux_core::client_identity(), expected);
}
