//! Data provenance manifests for research and promotion artifacts.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataSourceManifest {
    pub name: String,
    pub kind: String,
    pub path: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub row_count: Option<u64>,
    pub checksum_sha256: Option<String>,
    pub complete: bool,
    pub metadata: BTreeMap<String, String>,
}

impl DataSourceManifest {
    pub fn new(name: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: kind.into(),
            path: None,
            start: None,
            end: None,
            row_count: None,
            checksum_sha256: None,
            complete: false,
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataManifest {
    pub schema_version: u32,
    pub complete: bool,
    pub sources: Vec<DataSourceManifest>,
    pub notes: Vec<String>,
    pub manifest_hash: String,
}

impl DataManifest {
    pub fn new(sources: Vec<DataSourceManifest>, notes: Vec<String>) -> Self {
        let complete = !sources.is_empty() && sources.iter().all(|s| s.complete);
        let mut manifest = Self {
            schema_version: 1,
            complete,
            sources,
            notes,
            manifest_hash: String::new(),
        };
        manifest.manifest_hash = manifest.compute_hash();
        manifest
    }

    pub fn compute_hash(&self) -> String {
        #[derive(Serialize)]
        struct Hashable<'a> {
            schema_version: u32,
            complete: bool,
            sources: &'a [DataSourceManifest],
            notes: &'a [String],
        }
        let payload = Hashable {
            schema_version: self.schema_version,
            complete: self.complete,
            sources: &self.sources,
            notes: &self.notes,
        };
        let bytes = serde_json::to_vec(&payload).expect("serialize DataManifest hash payload");
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hex::encode(hasher.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_hash_is_stable() {
        let mut src = DataSourceManifest::new("pmxt", "archive");
        src.path = Some("/tmp/cache".to_string());
        src.complete = true;
        src.row_count = Some(10);
        let a = DataManifest::new(vec![src.clone()], vec!["note".to_string()]);
        let b = DataManifest::new(vec![src], vec!["note".to_string()]);
        assert_eq!(a.manifest_hash, b.manifest_hash);
        assert_eq!(a.manifest_hash.len(), 64);
        assert!(a.complete);
    }

    #[test]
    fn incomplete_source_marks_manifest_incomplete() {
        let src = DataSourceManifest::new("btc", "kline");
        let manifest = DataManifest::new(vec![src], Vec::new());
        assert!(!manifest.complete);
    }
}
