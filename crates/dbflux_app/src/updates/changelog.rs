//! Parser for the repository `CHANGELOG.md` (Keep a Changelog format).
//!
//! The file is bundled into the binary at build time, so the "What is new"
//! and welcome dialogs work offline and always describe the build that is
//! running. Parsing is lenient: headings that are not releases, sub-headings
//! (`####`) and free paragraphs are skipped rather than rejected, because the
//! file is written by hand and its older sections use different shapes.

use std::cmp::Ordering;
use std::sync::OnceLock;

use semver::Version;

const BUNDLED_CHANGELOG: &str = include_str!("../../../../CHANGELOG.md");

/// Longest title shown for an entry that has no bold lead-in.
const MAX_TITLE_CHARS: usize = 110;

/// Longest one-line summary shown under an entry title.
const MAX_SUMMARY_CHARS: usize = 140;

/// What a `## [...]` release heading names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseHeading {
    /// `## [Unreleased]`: work that ships with the next release.
    Unreleased,
    Version(Version),
}

/// One `### ...` group inside a release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SectionKind {
    Added,
    Changed,
    Deprecated,
    Removed,
    Fixed,
    Security,
    /// A heading outside the Keep a Changelog set, kept verbatim.
    Other(String),
}

impl SectionKind {
    /// Maps a section heading to its kind. The older `Features` / `Fixes`
    /// headings produced by the previous release tooling fold into `Added` /
    /// `Fixed`.
    pub fn from_heading(heading: &str) -> Self {
        match heading.trim().to_ascii_lowercase().as_str() {
            "added" | "features" => Self::Added,
            "changed" => Self::Changed,
            "deprecated" => Self::Deprecated,
            "removed" => Self::Removed,
            "fixed" | "fixes" | "bug fixes" => Self::Fixed,
            "security" => Self::Security,
            _ => Self::Other(heading.trim().to_string()),
        }
    }
}

/// A single bullet of a section, reduced to plain text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangelogEntry {
    pub title: String,
    pub summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangelogSection {
    pub kind: SectionKind,
    pub entries: Vec<ChangelogEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangelogRelease {
    pub heading: ReleaseHeading,
    /// Release date as written in the heading (`YYYY-MM-DD`).
    pub date: Option<String>,
    pub sections: Vec<ChangelogSection>,
}

impl ChangelogRelease {
    pub fn version(&self) -> Option<&Version> {
        match &self.heading {
            ReleaseHeading::Version(version) => Some(version),
            ReleaseHeading::Unreleased => None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.sections
            .iter()
            .all(|section| section.entries.is_empty())
    }

    fn section_mut(&mut self, kind: SectionKind) -> &mut ChangelogSection {
        let index = match self
            .sections
            .iter()
            .position(|section| section.kind == kind)
        {
            Some(index) => index,
            None => {
                self.sections.push(ChangelogSection {
                    kind,
                    entries: Vec::new(),
                });
                self.sections.len() - 1
            }
        };

        &mut self.sections[index]
    }
}

/// The changelog compiled into this build, parsed once.
pub fn bundled() -> &'static [ChangelogRelease] {
    static PARSED: OnceLock<Vec<ChangelogRelease>> = OnceLock::new();

    PARSED.get_or_init(|| parse(BUNDLED_CHANGELOG))
}

/// Parses Keep a Changelog markdown into releases in file order.
///
/// Repeated `### Added` (or any other) headings inside one release merge into
/// a single section, and sections or releases without entries are dropped.
pub fn parse(markdown: &str) -> Vec<ChangelogRelease> {
    let mut releases: Vec<ChangelogRelease> = Vec::new();
    let mut current_section: Option<SectionKind> = None;
    let mut pending_entry: Option<String> = None;

    for line in markdown.lines() {
        if let Some(rest) = line.strip_prefix("## ") {
            flush_entry(&mut releases, &current_section, &mut pending_entry);
            current_section = None;

            if let Some(release) = parse_release_heading(rest) {
                releases.push(release);
            }
            continue;
        }

        if let Some(rest) = line.strip_prefix("### ") {
            flush_entry(&mut releases, &current_section, &mut pending_entry);
            current_section = Some(SectionKind::from_heading(rest));
            continue;
        }

        if line.starts_with('#') {
            flush_entry(&mut releases, &current_section, &mut pending_entry);
            continue;
        }

        if let Some(text) = bullet_text(line) {
            flush_entry(&mut releases, &current_section, &mut pending_entry);
            pending_entry = Some(text.to_string());
            continue;
        }

        if line.trim().is_empty() {
            flush_entry(&mut releases, &current_section, &mut pending_entry);
            continue;
        }

        if let Some(entry) = pending_entry.as_mut() {
            entry.push(' ');
            entry.push_str(line.trim());
        }
    }

    flush_entry(&mut releases, &current_section, &mut pending_entry);

    for release in &mut releases {
        release
            .sections
            .retain(|section| !section.entries.is_empty());
    }
    releases.retain(|release| !release.is_empty());

    releases
}

/// Releases a user moving from `since` to `current` has not seen yet, newest
/// first: every versioned release in `(since, current]`, plus `[Unreleased]`
/// when `current` is a pre-release build (rc, nightly, dev), whose changes are
/// only described there.
pub fn releases_between<'a>(
    releases: &'a [ChangelogRelease],
    since: &Version,
    current: &Version,
) -> Vec<&'a ChangelogRelease> {
    let mut selected: Vec<&ChangelogRelease> = releases
        .iter()
        .filter(|release| match &release.heading {
            ReleaseHeading::Unreleased => !current.pre.is_empty(),
            ReleaseHeading::Version(version) => {
                version.cmp_precedence(since) == Ordering::Greater
                    && version.cmp_precedence(current) != Ordering::Greater
            }
        })
        .collect();

    selected.sort_by(|left, right| compare_headings(&right.heading, &left.heading));
    selected
}

/// The release that describes the running build: the section named after its
/// exact version, else `[Unreleased]` for a pre-release build, else the newest
/// release not newer than it.
pub fn release_for_version<'a>(
    releases: &'a [ChangelogRelease],
    current: &Version,
) -> Option<&'a ChangelogRelease> {
    let exact = releases.iter().find(|release| {
        release
            .version()
            .is_some_and(|version| version.cmp_precedence(current) == Ordering::Equal)
    });
    if exact.is_some() {
        return exact;
    }

    if !current.pre.is_empty() {
        let unreleased = releases
            .iter()
            .find(|release| release.heading == ReleaseHeading::Unreleased);
        if unreleased.is_some() {
            return unreleased;
        }
    }

    releases
        .iter()
        .filter(|release| {
            release
                .version()
                .is_some_and(|version| version.cmp_precedence(current) != Ordering::Greater)
        })
        .max_by(|left, right| compare_headings(&left.heading, &right.heading))
}

/// Orders headings by version precedence, with `[Unreleased]` above every
/// versioned release.
fn compare_headings(left: &ReleaseHeading, right: &ReleaseHeading) -> Ordering {
    match (left, right) {
        (ReleaseHeading::Unreleased, ReleaseHeading::Unreleased) => Ordering::Equal,
        (ReleaseHeading::Unreleased, ReleaseHeading::Version(_)) => Ordering::Greater,
        (ReleaseHeading::Version(_), ReleaseHeading::Unreleased) => Ordering::Less,
        (ReleaseHeading::Version(left), ReleaseHeading::Version(right)) => {
            left.cmp_precedence(right)
        }
    }
}

/// Parses the text after `## ` into a release, or `None` for any heading that
/// does not name a release in brackets.
fn parse_release_heading(rest: &str) -> Option<ChangelogRelease> {
    let rest = rest.trim();
    let inner_start = rest.strip_prefix('[')?;
    let close = inner_start.find(']')?;
    let label = inner_start[..close].trim();
    let after = inner_start[close + 1..].trim();

    let heading = if label.eq_ignore_ascii_case("unreleased") {
        ReleaseHeading::Unreleased
    } else {
        ReleaseHeading::Version(super::parse_version(label)?)
    };

    let date = after
        .trim_start_matches(['-', '\u{2013}', '\u{2014}'])
        .split_whitespace()
        .next()
        .map(str::to_string);

    Some(ChangelogRelease {
        heading,
        date,
        sections: Vec::new(),
    })
}

/// The text of a top-level `* ` or `- ` bullet. Indented bullets belong to the
/// entry above them and are treated as continuation text.
fn bullet_text(line: &str) -> Option<&str> {
    line.strip_prefix("* ")
        .or_else(|| line.strip_prefix("- "))
        .map(str::trim)
}

fn flush_entry(
    releases: &mut [ChangelogRelease],
    section: &Option<SectionKind>,
    pending_entry: &mut Option<String>,
) {
    let Some(text) = pending_entry.take() else {
        return;
    };

    let (Some(release), Some(kind)) = (releases.last_mut(), section.clone()) else {
        return;
    };

    if let Some(entry) = entry_from_text(&text) {
        release.section_mut(kind).entries.push(entry);
    }
}

/// Splits a bullet into a title and a one-line summary.
///
/// A bold lead-in (`**Title** — details`) becomes the title and the first
/// sentence of the details the summary. A plain bullet uses its first
/// sentence as the title and has no summary.
fn entry_from_text(text: &str) -> Option<ChangelogEntry> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }

    if let Some(after_open) = text.strip_prefix("**")
        && let Some(close) = after_open.find("**")
    {
        let title = plain_text(&after_open[..close]);
        let title = title.trim().trim_end_matches(['.', ':']).trim().to_string();
        let details = after_open[close + 2..]
            .trim()
            .trim_start_matches(['\u{2014}', '\u{2013}', '-', ':'])
            .trim();

        if !title.is_empty() {
            let summary = Some(truncate(
                &first_sentence(&plain_text(details)),
                MAX_SUMMARY_CHARS,
            ))
            .filter(|summary| !summary.is_empty());

            return Some(ChangelogEntry { title, summary });
        }
    }

    let title = truncate(&first_sentence(&plain_text(text)), MAX_TITLE_CHARS);

    Some(ChangelogEntry {
        title,
        summary: None,
    })
}

/// Strips inline markdown: emphasis markers, code ticks, and link targets.
fn plain_text(markdown: &str) -> String {
    let mut output = String::with_capacity(markdown.len());
    let mut characters = markdown.chars().peekable();

    while let Some(character) = characters.next() {
        match character {
            '*' if characters.peek() == Some(&'*') => {
                characters.next();
            }
            '`' => {}
            ']' if characters.peek() == Some(&'(') => {
                for skipped in characters.by_ref() {
                    if skipped == ')' {
                        break;
                    }
                }
            }
            '[' => {}
            _ => output.push(character),
        }
    }

    output.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Text up to the first sentence end (a period followed by a space and an
/// uppercase letter), or the whole text when there is none.
fn first_sentence(text: &str) -> String {
    let characters: Vec<char> = text.chars().collect();

    for index in 0..characters.len() {
        let is_period = characters[index] == '.';
        let followed_by_space = characters.get(index + 1) == Some(&' ');
        let next_is_upper = characters
            .get(index + 2)
            .is_some_and(|character| character.is_uppercase());

        if is_period && followed_by_space && next_is_upper {
            let sentence: String = characters[..index].iter().collect();
            if !ends_with_abbreviation(&sentence) {
                return sentence;
            }
        }
    }

    text.trim_end_matches('.').to_string()
}

/// Whether `text` ends with an abbreviation whose period does not end a
/// sentence.
fn ends_with_abbreviation(text: &str) -> bool {
    let last_word = text
        .rsplit([' ', '('])
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();

    matches!(last_word.as_str(), "e.g" | "i.e" | "etc" | "vs")
}

/// Shortens `text` to at most `limit` characters at a word boundary, adding an
/// ellipsis when anything was cut.
fn truncate(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_string();
    }

    let cut: String = text.chars().take(limit).collect();
    let at_word = match cut.rfind(' ') {
        Some(space) => &cut[..space],
        None => cut.as_str(),
    };

    format!("{}\u{2026}", at_word.trim_end_matches([',', ';', ':', ' ']))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "# Changelog

All notable changes.

## [Unreleased]

### Added

* **Welcome screen** — updates choices on first run. More text here.
* Plain entry without bold. Second sentence.

### Fixed

* **Audit export** — asks where to save
  instead of writing to Downloads.

### Added

- **Update notices** — a check against `GitHub` releases at startup.

## [0.8.0] - 2026-10-15

### Fixed

* **Ctrl Shift N.** Opens a new connection again.

## [0.7.1] \u{2013} 2026-08-20

Initial words that belong to no entry.

### Fixes

* Toast bubble no longer overflows the screen.

#### Sub heading

- Nested group entry

### Empty

## [0.6.0-dev.1] - 2026-05-14

### Chores

* Adopt trunk.

## [not-a-version] - 2026-01-01

### Added

* Must be ignored.

## Not a release heading
";

    fn version(text: &str) -> Version {
        Version::parse(text).expect("valid version")
    }

    #[test]
    fn parses_releases_sections_and_entries() {
        let releases = parse(SAMPLE);

        let headings: Vec<_> = releases.iter().map(|release| &release.heading).collect();
        assert_eq!(
            headings,
            vec![
                &ReleaseHeading::Unreleased,
                &ReleaseHeading::Version(version("0.8.0")),
                &ReleaseHeading::Version(version("0.7.1")),
                &ReleaseHeading::Version(version("0.6.0-dev.1")),
            ]
        );

        assert_eq!(releases[0].date, None);
        assert_eq!(releases[1].date.as_deref(), Some("2026-10-15"));
        assert_eq!(releases[2].date.as_deref(), Some("2026-08-20"));
    }

    #[test]
    fn repeated_section_headings_merge_in_order() {
        let releases = parse(SAMPLE);
        let unreleased = &releases[0];

        let kinds: Vec<_> = unreleased
            .sections
            .iter()
            .map(|section| &section.kind)
            .collect();
        assert_eq!(kinds, vec![&SectionKind::Added, &SectionKind::Fixed]);

        let added: Vec<_> = unreleased.sections[0]
            .entries
            .iter()
            .map(|entry| entry.title.as_str())
            .collect();
        assert_eq!(
            added,
            vec![
                "Welcome screen",
                "Plain entry without bold",
                "Update notices"
            ]
        );
    }

    #[test]
    fn bold_lead_in_becomes_title_and_first_sentence_the_summary() {
        let releases = parse(SAMPLE);
        let added = &releases[0].sections[0].entries;

        assert_eq!(
            added[0],
            ChangelogEntry {
                title: "Welcome screen".to_string(),
                summary: Some("updates choices on first run".to_string()),
            }
        );
        assert_eq!(added[1].summary, None);
        assert_eq!(
            added[2].summary.as_deref(),
            Some("a check against GitHub releases at startup")
        );

        let fixed = &releases[0].sections[1].entries[0];
        assert_eq!(
            fixed.summary.as_deref(),
            Some("asks where to save instead of writing to Downloads")
        );

        let trailing_period = &releases[1].sections[0].entries[0];
        assert_eq!(trailing_period.title, "Ctrl Shift N");
        assert_eq!(
            trailing_period.summary.as_deref(),
            Some("Opens a new connection again")
        );
    }

    #[test]
    fn legacy_headings_map_and_unknown_headings_are_kept() {
        let releases = parse(SAMPLE);

        let legacy = &releases[2];
        assert_eq!(legacy.sections.len(), 1);
        assert_eq!(legacy.sections[0].kind, SectionKind::Fixed);
        assert_eq!(
            legacy.sections[0]
                .entries
                .iter()
                .map(|entry| entry.title.as_str())
                .collect::<Vec<_>>(),
            vec![
                "Toast bubble no longer overflows the screen",
                "Nested group entry"
            ]
        );

        assert_eq!(
            releases[3].sections[0].kind,
            SectionKind::Other("Chores".to_string())
        );
    }

    #[test]
    fn empty_input_and_heading_only_input_yield_nothing() {
        assert!(parse("").is_empty());
        assert!(parse("## [Unreleased]\n\n### Added\n").is_empty());
    }

    #[test]
    fn plain_text_strips_markdown() {
        assert_eq!(
            plain_text("Use `cargo` and [the docs](https://example.com) **now**"),
            "Use cargo and the docs now"
        );
        assert_eq!(plain_text("SELECT * FROM t"), "SELECT * FROM t");
    }

    #[test]
    fn abbreviations_do_not_end_the_first_sentence() {
        assert_eq!(
            first_sentence("Long hosts (e.g. Example.com) broke it. Now fixed."),
            "Long hosts (e.g. Example.com) broke it"
        );
    }

    #[test]
    fn long_titles_are_cut_at_a_word() {
        let long = "word ".repeat(60);
        let entry = entry_from_text(&long).expect("entry");

        assert!(entry.title.chars().count() <= MAX_TITLE_CHARS + 1);
        assert!(entry.title.ends_with('\u{2026}'));
    }

    #[test]
    fn releases_between_covers_the_half_open_range_newest_first() {
        let releases = parse(SAMPLE);

        let stable: Vec<_> = releases_between(&releases, &version("0.7.0"), &version("0.8.0"))
            .into_iter()
            .map(|release| release.heading.clone())
            .collect();
        assert_eq!(
            stable,
            vec![
                ReleaseHeading::Version(version("0.8.0")),
                ReleaseHeading::Version(version("0.7.1")),
            ]
        );

        let none = releases_between(&releases, &version("0.8.0"), &version("0.8.0"));
        assert!(none.is_empty());
    }

    #[test]
    fn prerelease_builds_include_unreleased() {
        let releases = parse(SAMPLE);

        let nightly: Vec<_> = releases_between(
            &releases,
            &version("0.7.1"),
            &version("0.9.0-nightly+abc1234"),
        )
        .into_iter()
        .map(|release| release.heading.clone())
        .collect();

        assert_eq!(
            nightly,
            vec![
                ReleaseHeading::Unreleased,
                ReleaseHeading::Version(version("0.8.0")),
            ]
        );
    }

    #[test]
    fn release_for_version_prefers_exact_then_unreleased_then_older() {
        let releases = parse(SAMPLE);

        let exact = release_for_version(&releases, &version("0.8.0")).expect("exact");
        assert_eq!(exact.heading, ReleaseHeading::Version(version("0.8.0")));

        let nightly =
            release_for_version(&releases, &version("0.9.0-nightly+abc")).expect("nightly");
        assert_eq!(nightly.heading, ReleaseHeading::Unreleased);

        let patch = release_for_version(&releases, &version("0.8.3")).expect("older");
        assert_eq!(patch.heading, ReleaseHeading::Version(version("0.8.0")));

        assert!(release_for_version(&releases, &version("0.0.1")).is_none());
    }

    #[test]
    fn bundled_changelog_parses_every_release_heading() {
        let releases = bundled();
        let heading_count = BUNDLED_CHANGELOG
            .lines()
            .filter(|line| line.starts_with("## ["))
            .count();

        assert!(!releases.is_empty());
        assert!(releases.len() <= heading_count, "parser invented releases");
        assert!(
            releases.len() + 2 >= heading_count,
            "parser dropped release headings: {} of {heading_count}",
            releases.len()
        );

        for release in releases {
            assert!(!release.is_empty(), "{:?} has no entries", release.heading);
            for section in &release.sections {
                for entry in &section.entries {
                    assert!(!entry.title.is_empty());
                    assert!(!entry.title.contains("**"));
                    assert!(!entry.title.contains('`'));
                }
            }
        }
    }

    #[test]
    fn bundled_changelog_contains_known_releases() {
        let releases = bundled();

        let release_070 = releases
            .iter()
            .find(|release| release.version() == Some(&version("0.7.0")))
            .expect("0.7.0 is in the changelog");
        assert_eq!(release_070.date.as_deref(), Some("2026-07-31"));

        let added = release_070
            .sections
            .iter()
            .find(|section| section.kind == SectionKind::Added)
            .expect("0.7.0 has an Added section");
        assert!(
            added
                .entries
                .iter()
                .any(|entry| entry.title.starts_with("Amazon S3 driver"))
        );

        assert!(
            releases
                .iter()
                .any(|release| release.version() == Some(&version("0.1.0")))
        );
    }
}
