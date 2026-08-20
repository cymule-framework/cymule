//! Explicit offline directory migration from the v1 whole-state file.

use std::path::PathBuf;

use cymule_directory_store::DirectoryStore;

fn main() {
    let mut arguments = std::env::args_os();
    let program = arguments.next().unwrap_or_default();
    let root = arguments.next().map_or_else(
        || {
            eprintln!(
                "usage: {} <offline-store-directory>",
                PathBuf::from(program).display()
            );
            std::process::exit(2);
        },
        PathBuf::from,
    );
    if arguments.next().is_some() {
        eprintln!("migration accepts exactly one store directory");
        std::process::exit(2);
    }
    let receipt = DirectoryStore::migrate_v1(&root).unwrap_or_else(|error| {
        eprintln!("migration failed: {error}");
        std::process::exit(1);
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&receipt).expect("migration receipt serializes")
    );
}
