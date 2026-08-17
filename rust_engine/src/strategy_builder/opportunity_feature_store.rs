//! Reusable outcome-free feature-store contract for strategy-family plugins.
//!
//! Rows carry only sealed observation coordinates, complementary token
//! identity, and a plugin-owned causal payload. Outcomes remain in the
//! physically separate labels artifact and are never accepted by builders.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::data::models::Market;

use super::opportunity_table::HashedSource;

pub const OPPORTUNITY_FEATURE_STORE_SCHEMA_VERSION: &str = "opportunity_feature_store_v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeaturePluginDescriptor {
    pub plugin_id: String,
    pub plugin_version: String,
    pub configuration_sha256: String,
    pub causal_windows_ms: Vec<i64>,
    pub payload_schema: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeatureStorePmxtSource {
    pub hour: String,
    pub pmxt_parquet: HashedSource,
    pub target_condition_count: usize,
    pub streamed_target_events: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityFeatureStoreManifest {
    pub schema_version: String,
    pub generated_at: String,
    pub dataset_seal: HashedSource,
    pub dataset_sha256: String,
    pub market_catalog: HashedSource,
    pub output: HashedSource,
    pub plugin: FeaturePluginDescriptor,
    pub source_opportunity_rows: usize,
    pub output_rows: usize,
    pub complete_pair_rows: usize,
    pub source_pmxt_hours: Vec<FeatureStorePmxtSource>,
    pub source_pmxt_scans: usize,
    pub outcome_columns_present: bool,
    pub gamma_outcome_prices_influence_output: bool,
    pub external_price_or_model_features_influence_output: bool,
    pub feature_semantics: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OpportunityFeatureStoreRow<T> {
    pub source_opportunity_id: String,
    pub condition_id: String,
    pub chronological_window: String,
    pub window_start_ms: i64,
    pub observed_at_ms: i64,
    pub decision_seconds: u16,
    pub up_token_id: String,
    pub down_token_id: String,
    pub features: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeNeutralPair {
    pub up_token_id: String,
    pub down_token_id: String,
}

pub fn load_outcome_neutral_pairs(path: &Path) -> Result<HashMap<String, OutcomeNeutralPair>> {
    let catalog: BTreeMap<String, Market> = serde_json::from_reader(
        File::open(path).with_context(|| format!("open market catalog {}", path.display()))?,
    )
    .with_context(|| format!("parse market catalog {}", path.display()))?;
    let mut pairs = HashMap::new();
    for (key, market) in catalog {
        if key != market.condition_id {
            bail!("market catalog key does not match condition_id");
        }
        let mut up = None;
        let mut down = None;
        for outcome in market.outcomes {
            if (outcome.price - 0.5).abs() > 1e-12 {
                bail!("feature store requires an outcome-price-neutralized market catalog");
            }
            match outcome.name.to_ascii_lowercase().as_str() {
                "up" => up = Some(outcome.token_id),
                "down" => down = Some(outcome.token_id),
                _ => {}
            }
        }
        if let (Some(up_token_id), Some(down_token_id)) = (up, down) {
            pairs.insert(
                market.condition_id,
                OutcomeNeutralPair {
                    up_token_id,
                    down_token_id,
                },
            );
        }
    }
    if pairs.is_empty() {
        bail!("market catalog contains no Up/Down token pairs");
    }
    Ok(pairs)
}

pub fn read_feature_store_rows<T: DeserializeOwned>(
    path: &Path,
) -> Result<Vec<OpportunityFeatureStoreRow<T>>> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("read feature store {}", path.display()))?;
    let mut rows = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            bail!("feature-store line {} is blank", index + 1);
        }
        rows.push(
            serde_json::from_str(line)
                .with_context(|| format!("parse feature-store line {}", index + 1))?,
        );
    }
    if rows.is_empty() {
        bail!("feature-store file contains no rows");
    }
    Ok(rows)
}

pub fn validate_outcome_free_manifest(
    manifest: &OpportunityFeatureStoreManifest,
    plugin_id: &str,
    plugin_version: &str,
    dataset_sha256: &str,
    dataset_seal_sha256: &str,
) -> Result<()> {
    if manifest.schema_version != OPPORTUNITY_FEATURE_STORE_SCHEMA_VERSION
        || manifest.plugin.plugin_id != plugin_id
        || manifest.plugin.plugin_version != plugin_version
        || manifest.dataset_sha256 != dataset_sha256
        || manifest.dataset_seal.sha256 != dataset_seal_sha256
        || manifest.source_pmxt_scans != manifest.source_pmxt_hours.len()
        || manifest.outcome_columns_present
        || manifest.gamma_outcome_prices_influence_output
        || manifest.external_price_or_model_features_influence_output
    {
        bail!("feature-store manifest violates the outcome-free plugin contract");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generic_row_keeps_plugin_payload_nested() {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        #[serde(deny_unknown_fields)]
        struct Payload {
            trades: usize,
        }
        let row = OpportunityFeatureStoreRow {
            source_opportunity_id: "o1".to_string(),
            condition_id: "c1".to_string(),
            chronological_window: "older".to_string(),
            window_start_ms: 1,
            observed_at_ms: 2,
            decision_seconds: 120,
            up_token_id: "up".to_string(),
            down_token_id: "down".to_string(),
            features: Payload { trades: 3 },
        };
        let json = serde_json::to_value(&row).unwrap();
        assert_eq!(json["features"]["trades"], 3);
        assert!(json.get("won").is_none());
        assert_eq!(
            serde_json::from_value::<OpportunityFeatureStoreRow<Payload>>(json).unwrap(),
            row
        );
    }
}
