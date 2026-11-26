#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use super::*;
use dbflux_storage::bootstrap::StorageRuntime;

fn manager_and_connection() -> (SnippetManager, Arc<std::sync::Mutex<rusqlite::Connection>>) {
    let runtime = StorageRuntime::in_memory().expect("runtime");
    let conn = runtime.viz_connection().expect("connection");
    let manager = SnippetManager::new(Arc::new(SnippetRepo::new(Arc::clone(&conn))));
    (manager, conn)
}

fn draft(prefix: &str, language: &str) -> SnippetDraft {
    SnippetDraft {
        name: format!("Snippet {prefix}"),
        prefix: prefix.to_string(),
        language: language.to_string(),
        body: "SELECT $0".to_string(),
    }
}

#[test]
fn the_built_ins_are_listed_per_language() {
    let mut manager = manager_and_connection().0;

    assert_eq!(manager.snippets_for_language("sql").unwrap().len(), 6);
    assert!(manager.snippets_for_language("mongodb").unwrap().is_empty());
    assert_eq!(manager.all().unwrap().len(), 6);
}

#[test]
fn create_update_and_delete_refresh_the_cache() {
    let mut manager = manager_and_connection().0;
    assert_eq!(manager.all().unwrap().len(), 6);

    let created = manager.create(&draft("top", "sql")).unwrap();
    assert_eq!(manager.snippets_for_language("sql").unwrap().len(), 7);

    manager.update(&created.id, &draft("topn", "sql")).unwrap();
    assert!(
        manager
            .snippets_for_language("sql")
            .unwrap()
            .iter()
            .any(|snippet| snippet.prefix == "topn")
    );

    assert!(manager.delete(&created.id).unwrap());
    assert_eq!(manager.all().unwrap().len(), 6);
}

#[test]
fn a_failed_write_leaves_the_cache_untouched() {
    let mut manager = manager_and_connection().0;
    let before = manager.all().unwrap();

    assert!(matches!(
        manager.create(&draft("sel", "sql")),
        Err(SnippetError::DuplicatePrefix { .. })
    ));
    assert!(matches!(
        manager.create(&draft(" ", "sql")),
        Err(SnippetError::EmptyPrefix)
    ));
    assert!(matches!(
        manager.update("missing", &draft("x", "sql")),
        Err(SnippetError::NotFound(_))
    ));

    assert_eq!(manager.all().unwrap(), before);
    assert_eq!(manager.snippets_for_language("sql").unwrap().len(), 6);
}

#[test]
fn a_committed_write_succeeds_when_the_cache_cannot_be_reloaded() {
    let (mut manager, conn) = manager_and_connection();
    manager.all().unwrap();

    conn.lock()
        .unwrap()
        .execute(
            "INSERT INTO snp_snippets
                 (id, name, prefix, language, body, builtin_key, created_at, updated_at)
             VALUES ('bad', 'bad', 'bad', 'sql', 'x', NULL, 1, 'not a timestamp')",
            [],
        )
        .unwrap();
    assert!(manager.repo.list_all().is_err());

    let created = manager.create(&draft("top", "sql")).unwrap();
    assert_eq!(created.prefix, "top");
    assert!(
        manager.all().is_err(),
        "the next read hits the repository again"
    );
}
