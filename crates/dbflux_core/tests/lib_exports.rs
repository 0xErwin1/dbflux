use dbflux_core::SavedQuery;

#[test]
fn saved_query_exports_compile() {
    let _ = SavedQuery::new("Name".to_string(), "SELECT 1".to_string(), None);
}
