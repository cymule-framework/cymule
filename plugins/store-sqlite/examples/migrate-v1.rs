//! Explicit offline migration from the `SQLite` v1 whole-state table.

use std::path::PathBuf;

use cymule_store_sqlite::SqliteStore;

fn main() {
    let mut arguments = std::env::args_os();
    let program = arguments.next().unwrap_or_default();
    let path = arguments.next().map_or_else(
        || {
            eprintln!(
                "usage: {} <offline-sqlite-path>",
                PathBuf::from(program).display()
            );
            std::process::exit(2);
        },
        PathBuf::from,
    );
    if arguments.next().is_some() {
        eprintln!("migration accepts exactly one SQLite path");
        std::process::exit(2);
    }
    let receipts = SqliteStore::migrate_v1(&path).unwrap_or_else(|error| {
        eprintln!("migration failed: {error}");
        std::process::exit(1);
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&receipts).expect("migration receipts serialize")
    );
}
