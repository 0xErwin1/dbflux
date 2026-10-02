//! Closest-name lookup for identifiers that were not found.
//!
//! Used to answer "did you mean" for table, collection and column names. The
//! match ignores case and separators, so `userId` is close to `user_id`.

/// Upper bound on how many names [`suggest_names`] returns.
pub const MAX_NAME_SUGGESTIONS: usize = 3;

/// Longest wanted name, in characters, that [`suggest_names`] compares. It is
/// above the identifier limit of the supported engines, so a longer name is
/// not an identifier and is not worth the comparison cost.
pub const MAX_SUGGESTED_NAME_LENGTH: usize = 128;

/// Returns the candidates closest to `wanted`, best first.
///
/// Names are compared case-insensitively with separators removed, so
/// `camelCase`, `snake_case` and `kebab-case` spellings of the same words are
/// equal. A candidate is kept when its edit distance to `wanted` is at most a
/// third of the longer name, which keeps unrelated names out. Ties are ordered
/// by name, and at most [`MAX_NAME_SUGGESTIONS`] names are returned. A wanted
/// name longer than [`MAX_SUGGESTED_NAME_LENGTH`] gets no suggestions.
pub fn suggest_names<'a>(
    wanted: &str,
    candidates: impl IntoIterator<Item = &'a str>,
) -> Vec<String> {
    if wanted.chars().count() > MAX_SUGGESTED_NAME_LENGTH {
        return Vec::new();
    }

    let wanted = comparable(wanted);

    let mut ranked: Vec<(usize, &str)> = candidates
        .into_iter()
        .filter_map(|candidate| {
            let other = comparable(candidate);
            let limit = wanted.len().max(other.len()) / 3;

            // The distance is at least the difference in length.
            if wanted.len().abs_diff(other.len()) > limit {
                return None;
            }

            let distance = edit_distance(&wanted, &other);

            (distance <= limit).then_some((distance, candidate))
        })
        .collect();

    ranked.sort_unstable();
    ranked.dedup();

    ranked
        .into_iter()
        .take(MAX_NAME_SUGGESTIONS)
        .map(|(_, candidate)| candidate.to_string())
        .collect()
}

/// Lowercases a name and drops everything that is not a letter or a digit.
fn comparable(name: &str) -> Vec<char> {
    name.chars()
        .filter(|character| character.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

/// Optimal string alignment distance: insertions, deletions, substitutions and
/// swaps of two adjacent characters each cost one edit.
fn edit_distance(left: &[char], right: &[char]) -> usize {
    let mut before_previous: Vec<usize> = vec![0; right.len() + 1];
    let mut previous: Vec<usize> = (0..=right.len()).collect();

    for (left_index, left_char) in left.iter().enumerate() {
        let mut current = Vec::with_capacity(right.len() + 1);
        current.push(left_index + 1);

        for (right_index, right_char) in right.iter().enumerate() {
            let substitution = previous[right_index] + usize::from(left_char != right_char);
            let deletion = previous[right_index + 1] + 1;
            let insertion = current[right_index] + 1;

            let mut best = substitution.min(deletion).min(insertion);

            let swapped = left_index > 0
                && right_index > 0
                && *left_char == right[right_index - 1]
                && left[left_index - 1] == *right_char;

            if swapped {
                best = best.min(before_previous[right_index - 1] + 1);
            }

            current.push(best);
        }

        before_previous = previous;
        previous = current;
    }

    previous[right.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn suggest(wanted: &str, candidates: &[&str]) -> Vec<String> {
        suggest_names(wanted, candidates.iter().copied())
    }

    #[test]
    fn a_misspelled_name_suggests_the_closest_candidate() {
        assert_eq!(suggest("usres", &["orders", "users", "items"]), ["users"]);
    }

    #[test]
    fn matching_ignores_case() {
        assert_eq!(suggest("USERS", &["users", "orders"]), ["users"]);
    }

    #[test]
    fn camel_case_and_snake_case_are_equivalent() {
        assert_eq!(suggest("userId", &["id", "user_id", "email"]), ["user_id"]);
        assert_eq!(
            suggest("UserAccounts", &["user_accounts", "orders"]),
            ["user_accounts"]
        );
        assert_eq!(suggest("user-id", &["userId"]), ["userId"]);
    }

    #[test]
    fn unrelated_names_are_not_suggested() {
        assert!(suggest("invoices", &["users", "orders", "items"]).is_empty());
        assert!(suggest("id", &["at", "is", "no"]).is_empty());
    }

    #[test]
    fn no_candidates_gives_no_suggestions() {
        assert!(suggest("users", &[]).is_empty());
    }

    #[test]
    fn at_most_three_names_are_returned_best_first() {
        let suggestions = suggest(
            "order",
            &[
                "orders_x", "ordersx", "orders", "order_", "orderz", "ordery",
            ],
        );

        assert_eq!(suggestions, ["order_", "orders", "ordery"]);
    }

    #[test]
    fn ties_are_ordered_by_name_whatever_the_input_order() {
        let forward = suggest("item", &["items", "itemz", "itema"]);
        let backward = suggest("item", &["itema", "itemz", "items"]);

        assert_eq!(forward, ["itema", "items", "itemz"]);
        assert_eq!(forward, backward);
    }

    #[test]
    fn a_transposition_counts_as_one_edit() {
        assert_eq!(suggest("eamil", &["email", "name"]), ["email"]);
    }

    #[test]
    fn an_overlong_wanted_name_gets_no_suggestions() {
        let longest_checked = "a".repeat(MAX_SUGGESTED_NAME_LENGTH);
        let overlong = "a".repeat(MAX_SUGGESTED_NAME_LENGTH + 1);

        assert_eq!(
            suggest(&longest_checked, &[longest_checked.as_str()]),
            [longest_checked.clone()]
        );
        assert!(suggest(&overlong, &[overlong.as_str(), longest_checked.as_str()]).is_empty());
    }

    #[test]
    fn a_candidate_of_a_very_different_length_is_skipped() {
        let long = "user_account_settings_history";

        assert!(suggest("user", &[long]).is_empty());
        assert!(suggest(long, &["user"]).is_empty());
        assert_eq!(
            suggest("user_acount_settings_history", &[long, "user"]),
            [long]
        );
    }

    #[test]
    fn duplicate_candidates_are_suggested_once() {
        assert_eq!(suggest("usres", &["users", "users"]), ["users"]);
    }
}
