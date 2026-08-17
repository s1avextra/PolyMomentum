//! Strategy-registry persistence and immutable evidence archiving.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{StrategyRegistry, StrategyRegistryMarkInput};
use crate::artifact::write_json_artifact_atomic;
use crate::strategy::spec::stable_json_hash;

#[derive(Serialize)]
struct StrategyRegistryFingerprint<'a> {
    strategy_id: &'a str,
    parent_id: &'a Option<String>,
    artifact_path: &'a Option<String>,
    metrics_path: &'a Option<String>,
}

pub(super) fn strategy_version_id(input: &StrategyRegistryMarkInput) -> String {
    let fingerprint = StrategyRegistryFingerprint {
        strategy_id: input.strategy_id.trim(),
        parent_id: &input.parent_id,
        artifact_path: &input.artifact_path,
        metrics_path: &input.metrics_path,
    };
    let hash = stable_json_hash(&fingerprint);
    format!("sv_{}", &hash[..16])
}

pub(super) fn read_strategy_registry(path: &Path) -> Result<StrategyRegistry> {
    if !path.exists() {
        return Ok(StrategyRegistry {
            schema_version: 1,
            updated_at: Utc::now().to_rfc3339(),
            entries: Vec::new(),
        });
    }
    let data = std::fs::read_to_string(path)
        .with_context(|| format!("read strategy registry {}", path.display()))?;
    let registry: StrategyRegistry = serde_json::from_str(&data)
        .with_context(|| format!("parse strategy registry {}", path.display()))?;
    if registry.schema_version != 1 {
        bail!(
            "unsupported strategy registry schema_version {}; expected 1",
            registry.schema_version
        );
    }
    Ok(registry)
}

pub(super) fn write_strategy_registry_atomic(
    path: &Path,
    registry: &StrategyRegistry,
) -> Result<()> {
    write_json_artifact_atomic(path, registry).context("write strategy registry")
}

fn archived_evidence_path(strategy_dir: &Path, role: &str, source_path: &str) -> PathBuf {
    let source = Path::new(source_path);
    let extension = source
        .extension()
        .and_then(|extension| extension.to_str())
        .filter(|extension| !extension.is_empty())
        .map(|extension| format!(".{}", safe_path_component(extension)))
        .unwrap_or_default();
    let source_hash = stable_json_hash(&source_path);
    strategy_dir.join(format!(
        "{}_{}{}",
        safe_path_component(role),
        &source_hash[..16],
        extension
    ))
}

pub(super) fn archive_evidence_file(
    source: &Path,
    out_dir: &Path,
    strategy_dir: &Path,
    role: &str,
    source_path: &str,
) -> Result<(PathBuf, u64, String)> {
    std::fs::create_dir_all(out_dir)
        .with_context(|| format!("create evidence archive dir {}", out_dir.display()))?;
    let source_canonical = source
        .canonicalize()
        .with_context(|| format!("canonicalize evidence source {}", source.display()))?;
    let out_canonical = out_dir
        .canonicalize()
        .with_context(|| format!("canonicalize evidence archive dir {}", out_dir.display()))?;
    if source_canonical.starts_with(&out_canonical) {
        let (bytes, sha256) = file_sha256(source)?;
        return Ok((source.to_path_buf(), bytes, sha256));
    }

    let archived_path = archived_evidence_path(strategy_dir, role, source_path);
    let (bytes, sha256) = copy_file_atomic_with_sha256(source, &archived_path)?;
    Ok((archived_path, bytes, sha256))
}

fn copy_file_atomic_with_sha256(source: &Path, dest: &Path) -> Result<(u64, String)> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create evidence archive dir {}", parent.display()))?;
    }
    let payload =
        std::fs::read(source).with_context(|| format!("read evidence {}", source.display()))?;
    let sha256 = sha256_bytes(&payload);
    let file_name = dest
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("evidence");
    let tmp_path = dest.with_file_name(format!("{file_name}.tmp.{}", std::process::id()));
    let write_result = (|| -> Result<()> {
        let mut file = std::fs::File::create(&tmp_path)
            .with_context(|| format!("create evidence temp {}", tmp_path.display()))?;
        file.write_all(&payload)
            .with_context(|| format!("write evidence temp {}", tmp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("sync evidence temp {}", tmp_path.display()))?;
        Ok(())
    })();
    if let Err(error) = write_result {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(error);
    }
    if let Err(error) = std::fs::rename(&tmp_path, dest) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(error).context("rename evidence archive");
    }
    Ok((payload.len() as u64, sha256))
}

fn file_sha256(path: &Path) -> Result<(u64, String)> {
    let payload =
        std::fs::read(path).with_context(|| format!("read evidence {}", path.display()))?;
    Ok((payload.len() as u64, sha256_bytes(&payload)))
}

fn sha256_bytes(payload: &[u8]) -> String {
    hex::encode(Sha256::digest(payload))
}

pub(super) fn safe_path_component(value: &str) -> String {
    let cleaned = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if cleaned.is_empty() {
        "unnamed".to_string()
    } else {
        cleaned
    }
}

pub(super) fn merge_unique_strings(target: &mut Vec<String>, source: &[String]) {
    for value in source {
        if !target.iter().any(|existing| existing == value) {
            target.push(value.clone());
        }
    }
}
