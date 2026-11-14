use std::collections::HashSet;

use uuid::Uuid;

use super::profiles_to_reopen;

fn ids(count: usize) -> Vec<Uuid> {
    (0..count).map(|_| Uuid::new_v4()).collect()
}

#[test]
fn saved_profiles_reopen_without_restored_tabs() {
    let saved = ids(2);
    let existing: HashSet<Uuid> = saved.iter().copied().collect();

    let selected = profiles_to_reopen(&saved, &[], &existing, &HashSet::new());

    assert_eq!(selected, saved);
}

#[test]
fn restored_tab_connections_still_reopen() {
    let restored = ids(1);
    let existing: HashSet<Uuid> = restored.iter().copied().collect();

    let selected = profiles_to_reopen(&[], &restored, &existing, &HashSet::new());

    assert_eq!(selected, restored);
}

#[test]
fn saved_and_restored_are_merged_once() {
    let profiles = ids(3);
    let saved = vec![profiles[0], profiles[1]];
    let restored = vec![profiles[1], profiles[2], profiles[2]];
    let existing: HashSet<Uuid> = profiles.iter().copied().collect();

    let selected = profiles_to_reopen(&saved, &restored, &existing, &HashSet::new());

    assert_eq!(selected, profiles);
}

#[test]
fn deleted_and_connected_profiles_are_skipped() {
    let profiles = ids(3);
    let existing: HashSet<Uuid> = [profiles[0], profiles[1]].into_iter().collect();
    let connected: HashSet<Uuid> = [profiles[1]].into_iter().collect();

    let selected = profiles_to_reopen(&profiles, &[], &existing, &connected);

    assert_eq!(selected, vec![profiles[0]]);
}

#[test]
fn nothing_to_reopen_when_nothing_was_recorded() {
    let existing: HashSet<Uuid> = ids(2).into_iter().collect();

    assert!(profiles_to_reopen(&[], &[], &existing, &HashSet::new()).is_empty());
}
