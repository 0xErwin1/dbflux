//! Loads the system fonts off the startup path. See `odd/tasks/gpui-cold-start.md`, S3.
//!
//! Building the font database (`fontdb::Database::load_system_fonts`) parses every face
//! fontconfig reports; on the reference machine that is 594 faces and about half a second with
//! a cold page cache. `CosmicTextSystem::new` paid for it inside `LinuxCommon::new`, before the
//! first window could even be created, so the text system now starts empty
//! (`CosmicTextSystem::new_without_system_fonts`) and this module builds the database on a
//! thread of its own. The files it finds are memory-mapped and handed to the app, which adds
//! them to the text system and draws again once they are installed
//! ([`gpui::Platform::take_system_font_receiver`]).
//!
//! The bytes are borrowed from the mappings rather than read into owned buffers: several
//! desktops ship a CJK family whose files add up to hundreds of megabytes, and loading those
//! into anonymous memory would make the resident set grow by the whole font set. The mappings
//! are leaked, so they live as long as the process and need no owner.

use std::{borrow::Cow, collections::BTreeSet, path::Path, time::Instant};

use futures::channel::mpsc;

/// The system fonts, as font file contents the text system accepts.
pub(crate) type FontReceiver = mpsc::UnboundedReceiver<Vec<Cow<'static, [u8]>>>;

/// The loader, started at most once per process.
#[derive(Default)]
pub(crate) struct SystemFonts {
    receiver: Option<FontReceiver>,
    started: bool,
}

impl SystemFonts {
    /// Hands over the channel the fonts arrive on, starting the loader the first time.
    ///
    /// A later call returns nothing, so only one consumer can take the fonts.
    pub(crate) fn take(&mut self) -> Option<FontReceiver> {
        if !self.started {
            self.started = true;
            self.receiver = Some(load_in_background());
        }
        self.receiver.take()
    }
}

/// Builds the system font database on its own thread.
fn load_in_background() -> FontReceiver {
    let (sender, receiver) = mpsc::unbounded();
    let spawned = std::thread::Builder::new()
        .name("system-fonts".to_owned())
        .spawn(move || {
            let fonts = load();
            if fonts.is_empty() {
                log::warn!("found no system fonts; text can only use the fonts the app adds");
                return;
            }
            if sender.unbounded_send(fonts).is_err() {
                // No consumer is left, so the process draws the whole session with the fonts it
                // added itself. A warning, not a debug line: the load succeeded and its result was
                // thrown away, which a cold-start log has to show.
                log::warn!(
                    "the system fonts were loaded after their receiver went away; text keeps the fonts the app adds"
                );
            }
        });
    if let Err(error) = spawned {
        log::error!("failed to start the system font loader: {error}");
    }
    receiver
}

/// Every system font file, mapped into this process.
fn load() -> Vec<Cow<'static, [u8]>> {
    let started = Instant::now();
    let mut database = fontdb::Database::new();
    database.load_system_fonts();
    let fonts = map_faces(&database);
    log::info!(
        "loaded {} system font files in {:?}",
        fonts.len(),
        started.elapsed()
    );
    fonts
}

/// Maps every distinct file behind the database's faces.
///
/// A file can hold several faces (a variable font reports one per instance), and face order
/// within the database is not stable, so the paths are collected into a set first: each file is
/// mapped once and the result is deterministic.
fn map_faces(database: &fontdb::Database) -> Vec<Cow<'static, [u8]>> {
    let paths: BTreeSet<&Path> = database
        .faces()
        .filter_map(|face| match &face.source {
            fontdb::Source::File(path) => Some(path.as_path()),
            _ => None,
        })
        .collect();
    paths.into_iter().filter_map(map_font_file).collect()
}

/// Maps `path` and leaks the mapping, so the bytes are `'static`.
fn map_font_file(path: &Path) -> Option<Cow<'static, [u8]>> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) => {
            log::warn!("cannot open the font {path:?}: {error}");
            return None;
        }
    };
    // SAFETY: the mapping is read-only and the text system only reads the bytes. It is leaked
    // below, so it is never unmapped behind a `'static` borrow, and nothing in this crate hands
    // out a way to truncate or rewrite the file underneath it. `fontdb`, which mapped these
    // same files before this module existed, keeps its mappings alive the same way.
    let map = match unsafe { memmap2::Mmap::map(&file) } {
        Ok(map) => map,
        Err(error) => {
            log::warn!("cannot map the font {path:?}: {error}");
            return None;
        }
    };
    let map: &'static memmap2::Mmap = Box::leak(Box::new(map));
    Some(Cow::Borrowed(&map[..]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::{Font, SharedString, TextSystem};
    use std::{io::Write as _, path::PathBuf, sync::Arc};

    /// The text system the platform starts with has no system fonts; the fonts a platform loads
    /// later have to reach it, and the next layout has to be the family that was asked for.
    ///
    /// This is the S3 chain under the app: `add_fonts` hands over the bytes the loader mapped,
    /// the platform text system parses them into its database, the cached font ids are dropped,
    /// and a line resolved afterwards names the requested family instead of the stand-in the
    /// first pass had to draw with.
    #[test]
    fn a_font_added_after_startup_is_the_family_the_next_layout_shapes_with() {
        let directory = fixture_font_dir();
        let text_system = Arc::new(TextSystem::new(Arc::new(
            crate::linux::CosmicTextSystem::new_without_system_fonts("Archivo"),
        )));

        // The family the application adds itself: the state the first frame draws in.
        text_system
            .add_fonts(vec![map_font_file(&directory.join("Archivo-Regular.ttf"))
                .expect("the bundled interface font should map")])
            .expect("the bundled interface font should install");
        let requested: Font = gpui::font("JetBrains Mono");
        assert!(
            text_system.try_resolve_font(&requested).is_none(),
            "a family only the system provides is not resolvable yet"
        );
        assert_eq!(
            resolved_family(&text_system, &requested).as_deref(),
            Some("Archivo"),
            "the layout before the fonts arrive draws the installed stand-in"
        );

        text_system
            .add_fonts(vec![map_font_file(&directory.join("JetBrainsMono-Regular.ttf"))
                .expect("the bundled monospace font should map")])
            .expect("the bundled monospace font should install");

        assert!(
            text_system.try_resolve_font(&requested).is_some(),
            "the installed family is resolvable after the fonts arrive"
        );
        assert_eq!(
            resolved_family(&text_system, &requested).as_deref(),
            Some("JetBrains Mono"),
            "the layout after the fonts arrive is the family that was asked for"
        );
    }

    /// The loader hands its fonts to the app once.
    ///
    /// A second `take` must not start a second load or answer with an empty channel: the app that
    /// asks second would receive nothing and draw the whole session with the fonts it added
    /// itself, which is the state `odd/tasks/gpui-cold-start.md` S3 rules out.
    #[test]
    fn hands_the_fonts_to_one_consumer_only() {
        let mut system_fonts = SystemFonts::default();

        let first = system_fonts.take();
        assert!(first.is_some(), "the first take starts the loader");
        assert!(
            system_fonts.take().is_none(),
            "a later take answers nothing instead of an empty channel"
        );
    }

    /// The font files this repository ships, standing in for an installed system font set.
    ///
    /// The vendored crate is tested from the DBFlux repository, whose `crates/bolt/assets/fonts`
    /// holds real TrueType files. A fake font would not do: the point of these tests is that
    /// `fontdb` finds the files as faces and that their bytes come back unmapped correctly.
    fn fixture_font_dir() -> PathBuf {
        let directory =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../crates/bolt/assets/fonts");
        let directory = directory.canonicalize().unwrap_or(directory);
        assert!(
            directory.is_dir(),
            "the fixture fonts are missing from {directory:?}; run these tests from the DBFlux repository"
        );
        directory
    }

    fn fixture_files(directory: &Path) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = std::fs::read_dir(directory)
            .expect("the fixture directory should be readable")
            .map(|entry| entry.expect("a readable dir entry").path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "ttf"))
            .collect();
        files.sort();
        assert!(!files.is_empty(), "{directory:?} holds no fonts");
        files
    }

    /// Serializes the tests that change the process environment, which other threads read.
    fn fontconfig_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The family a layout is drawn with, going through the same resolution the renderer uses.
    fn resolved_family(text_system: &Arc<TextSystem>, requested: &Font) -> Option<SharedString> {
        let font_id = text_system.resolve_font(requested);
        text_system.get_font_for_id(font_id).map(|font| font.family)
    }

    fn database_from_dir(directory: &Path) -> fontdb::Database {
        let mut database = fontdb::Database::new();
        database.load_fonts_dir(directory);
        assert!(
            database.faces().next().is_some(),
            "fontdb found no face in {directory:?}"
        );
        database
    }

    #[test]
    fn maps_the_file_behind_every_face_once() {
        let directory = fixture_font_dir();
        let database = database_from_dir(&directory);

        let fonts = map_faces(&database);

        assert_eq!(
            fonts.len(),
            fixture_files(&directory).len(),
            "every font file should be mapped once, not once per face"
        );
        for font in &fonts {
            assert!(
                font.starts_with(&[0x00, 0x01, 0x00, 0x00]) || font.starts_with(b"true"),
                "the mapped bytes should be a TrueType file"
            );
        }

        let file = &fixture_files(&directory)[0];
        let mapped = map_font_file(file).expect("an existing font should map");
        assert_eq!(
            &mapped[..],
            &std::fs::read(file).expect("a readable font")[..],
            "the mapping should expose the file's own bytes"
        );
    }

    #[test]
    fn maps_the_faces_found_through_fontconfig() {
        let directory = fixture_font_dir();
        let scratch = std::env::temp_dir().join(format!(
            "gpui-pre-linux-system-fonts-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&scratch).expect("a scratch directory in the temp dir");
        let config = scratch.join("fonts.conf");
        let mut file =
            std::fs::File::create(&config).expect("a fontconfig file in the scratch dir");
        write!(
            file,
            "<?xml version=\"1.0\"?>\n<!DOCTYPE fontconfig SYSTEM \"fonts.dtd\">\n\
             <fontconfig>\n  <dir>{}</dir>\n</fontconfig>\n",
            directory.display()
        )
        .expect("the fontconfig file should be writable");
        drop(file);

        let _lock = fontconfig_lock();
        let previous = std::env::var_os("FONTCONFIG_FILE");
        // SAFETY: the tests that touch the environment hold `fontconfig_lock` for as long as they
        // read or write it, and nothing else in this test binary reads `FONTCONFIG_FILE`.
        unsafe { std::env::set_var("FONTCONFIG_FILE", &config) };

        let mut database = fontdb::Database::new();
        database.load_system_fonts();
        let fonts = map_faces(&database);

        match previous {
            // SAFETY: as above.
            Some(previous) => unsafe { std::env::set_var("FONTCONFIG_FILE", previous) },
            // SAFETY: as above.
            None => unsafe { std::env::remove_var("FONTCONFIG_FILE") },
        }
        std::fs::remove_dir_all(&scratch).expect("the scratch directory should be removed");

        // Exactly the fixture's files: had the config been ignored, the machine's own font
        // directories would have been walked and many more faces found.
        assert_eq!(
            fonts.len(),
            fixture_files(&directory).len(),
            "the faces the fontconfig file points at should all be mapped"
        );
    }
}
