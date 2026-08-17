//! Atomic writers for repository-generated artifacts.
//!
//! Every write uses the shared-cache contract: write `<name>.tmp.<pid>` in the
//! destination directory, then rename it over the final path.

use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

pub fn write_json_atomic<T: Serialize>(
    path: impl AsRef<Path>,
    value: &T,
    pretty: bool,
) -> Result<()> {
    let payload = if pretty {
        serde_json::to_vec_pretty(value)
    } else {
        serde_json::to_vec(value)
    }
    .context("serialize JSON artifact")?;
    write_bytes_atomic(path.as_ref(), &payload)
}

pub fn write_json_artifact_atomic<T: Serialize>(path: impl AsRef<Path>, value: &T) -> Result<()> {
    let mut payload = serde_json::to_vec_pretty(value).context("serialize artifact JSON")?;
    payload.push(b'\n');
    write_bytes_atomic(path.as_ref(), &payload)
}

pub fn write_jsonl_atomic<T: Serialize>(path: impl AsRef<Path>, rows: &[T]) -> Result<()> {
    let mut payload = Vec::new();
    for row in rows {
        serde_json::to_writer(&mut payload, row).context("serialize artifact JSONL row")?;
        payload.push(b'\n');
    }
    write_bytes_atomic(path.as_ref(), &payload)
}

fn write_bytes_atomic(path: &Path, payload: &[u8]) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create artifact directory {}", parent.display()))?;
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("artifact");
    let temp_path = path.with_file_name(format!("{file_name}.tmp.{}", std::process::id()));
    if let Err(error) = std::fs::write(&temp_path, payload) {
        let _ = std::fs::remove_file(&temp_path);
        return Err(error).with_context(|| format!("write artifact temp {}", temp_path.display()));
    }
    if let Err(error) = std::fs::rename(&temp_path, path) {
        let _ = std::fs::remove_file(&temp_path);
        return Err(error).with_context(|| {
            format!(
                "rename artifact {} into {}",
                temp_path.display(),
                path.display()
            )
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_write_replaces_existing_file_without_leaving_temp() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("artifact.json");
        std::fs::write(&path, "stale").unwrap();

        write_json_artifact_atomic(&path, &serde_json::json!({"version": 2})).unwrap();

        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "{\n  \"version\": 2\n}\n"
        );
        assert!(!temp
            .path()
            .join(format!("artifact.json.tmp.{}", std::process::id()))
            .exists());
    }

    #[test]
    fn jsonl_write_emits_one_compact_row_per_line() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("ledger.jsonl");

        write_jsonl_atomic(
            &path,
            &[
                serde_json::json!({"generation": 1}),
                serde_json::json!({"generation": 2}),
            ],
        )
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(path).unwrap(),
            "{\"generation\":1}\n{\"generation\":2}\n"
        );
    }
}
