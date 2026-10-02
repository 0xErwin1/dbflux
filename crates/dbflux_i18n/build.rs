use std::fs;
use std::path::PathBuf;

fn main() {
    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR"));
    let locales_dir = manifest_dir.join("locales");
    println!("cargo:rerun-if-changed=locales");

    let mut catalogs: Vec<(String, PathBuf)> = fs::read_dir(&locales_dir)
        .expect("locales directory must exist")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.is_file()
                && path
                    .extension()
                    .and_then(|extension| extension.to_str())
                    .is_some_and(|extension| extension == "yml" || extension == "yaml")
        })
        .filter_map(|path| {
            let stem = path.file_stem()?.to_str()?.to_string();
            Some((stem, path))
        })
        .collect();
    catalogs.sort_by(|a, b| a.0.cmp(&b.0));

    for (_, path) in &catalogs {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    let mut code = String::from("static LOCALE_SOURCES: &[(&str, &str)] = &[\n");
    for (stem, path) in &catalogs {
        let stem = stem.replace('\\', "\\\\").replace('"', "\\\"");
        let path = path
            .display()
            .to_string()
            .replace('\\', "\\\\")
            .replace('"', "\\\"");
        code.push_str(&format!("    (\"{stem}\", include_str!(\"{path}\")),\n"));
    }
    code.push_str("];\n");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("cargo sets OUT_DIR"));
    fs::write(out_dir.join("locales.rs"), code).expect("failed to write locales.rs");
}
