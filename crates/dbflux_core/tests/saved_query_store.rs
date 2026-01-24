use dbflux_core::{SavedQuery, SavedQueryStore};
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

fn temp_path() -> PathBuf {
    std::env::temp_dir().join(format!("dbflux_saved_queries_test_{}.json", Uuid::new_v4()))
}

#[test]
fn saves_and_loads_queries() {
    let path = temp_path();
    let mut store = SavedQueryStore::from_path(path.clone()).expect("store create");

    let query = SavedQuery::new("Users".to_string(), "SELECT * FROM users".to_string(), None);
    store.add(query.clone());
    store.save().expect("save");

    let store = SavedQueryStore::from_path(path.clone()).expect("reload");
    assert_eq!(store.get_all().len(), 1);
    assert_eq!(store.get_all()[0].name, query.name);
    assert_eq!(store.get_all()[0].sql, query.sql);

    let _ = fs::remove_file(path);
}

#[test]
fn updates_and_removes_queries() {
    let path = temp_path();
    let mut store = SavedQueryStore::from_path(path.clone()).expect("store create");

    let query = SavedQuery::new("Old".to_string(), "SELECT 1".to_string(), None);
    let id = query.id;
    store.add(query.clone());

    assert!(store.update(id, "New".to_string(), "SELECT 2".to_string()));
    assert_eq!(store.get_all()[0].name, "New");
    assert_eq!(store.get_all()[0].sql, "SELECT 2");

    assert!(store.remove(id));
    assert!(store.get_all().is_empty());

    let _ = fs::remove_file(path);
}

#[test]
fn searches_and_favorites_queries() {
    let path = temp_path();
    let mut store = SavedQueryStore::from_path(path.clone()).expect("store create");

    let query_a = SavedQuery::new("Users".to_string(), "SELECT * FROM users".to_string(), None);
    let query_b = SavedQuery::new(
        "Orders".to_string(),
        "SELECT * FROM orders".to_string(),
        None,
    );

    let id_a = query_a.id;
    store.add(query_a);
    store.add(query_b);

    let results = store.search("users");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "Users");

    assert!(store.toggle_favorite(id_a));
    let favorites = store.favorites();
    assert_eq!(favorites.len(), 1);
    assert!(favorites[0].is_favorite);

    let _ = fs::remove_file(path);
}
