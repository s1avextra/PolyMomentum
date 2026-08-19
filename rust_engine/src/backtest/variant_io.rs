//! Validated loading of exact strategy variants from replay artifacts.

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{bail, Context, Result};

use super::strategies::StrategyVariant;

/// Read either one `StrategyVariant` or a non-empty array of uniquely named
/// variants from JSON.
pub fn read_variants(path: impl AsRef<Path>) -> Result<Vec<StrategyVariant>> {
    let path = path.as_ref();
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let value: serde_json::Value = serde_json::from_str(&text)
        .with_context(|| format!("parse JSON from {}", path.display()))?;
    let variants = if value.is_array() {
        serde_json::from_value::<Vec<StrategyVariant>>(value)
            .with_context(|| format!("parse StrategyVariant array from {}", path.display()))?
    } else {
        vec![serde_json::from_value::<StrategyVariant>(value)
            .with_context(|| format!("parse StrategyVariant JSON from {}", path.display()))?]
    };

    if variants.is_empty() {
        bail!("StrategyVariant array must not be empty");
    }
    let mut names = BTreeSet::new();
    for variant in &variants {
        if variant.name.trim().is_empty() {
            bail!("StrategyVariant name must not be empty");
        }
        if !names.insert(variant.name.clone()) {
            bail!("duplicate StrategyVariant name `{}`", variant.name);
        }
    }
    Ok(variants)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::spec::stable_json_hash;

    #[test]
    fn accepts_single_and_batched_variants() {
        let temp = tempfile::TempDir::new().unwrap();
        let single_path = temp.path().join("single.json");
        let batch_path = temp.path().join("batch.json");
        let first = StrategyVariant::baseline();
        let mut second = first.clone();
        second.name = "baseline_with_exit".to_string();
        second.exit.settlement_basis_enabled = true;
        std::fs::write(&single_path, serde_json::to_vec(&first).unwrap()).unwrap();
        std::fs::write(
            &batch_path,
            serde_json::to_vec(&vec![first.clone(), second.clone()]).unwrap(),
        )
        .unwrap();

        let single = read_variants(&single_path).unwrap();
        let batch = read_variants(&batch_path).unwrap();

        assert_eq!(single.len(), 1);
        assert_eq!(single[0].name, first.name);
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[1].name, second.name);
        assert!(batch[1].exit.settlement_basis_enabled);
    }

    #[test]
    fn rejects_empty_and_duplicate_batches() {
        let temp = tempfile::TempDir::new().unwrap();
        let empty_path = temp.path().join("empty.json");
        let duplicate_path = temp.path().join("duplicate.json");
        let variant = StrategyVariant::baseline();
        std::fs::write(&empty_path, "[]").unwrap();
        std::fs::write(
            &duplicate_path,
            serde_json::to_vec(&vec![variant.clone(), variant]).unwrap(),
        )
        .unwrap();

        assert!(read_variants(&empty_path)
            .unwrap_err()
            .to_string()
            .contains("must not be empty"));
        assert!(read_variants(&duplicate_path)
            .unwrap_err()
            .to_string()
            .contains("duplicate StrategyVariant name"));
    }

    #[test]
    fn registered_complete_set_lock_variant_is_exactly_parseable() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "../deploy/promotions/evidence/strategy_registry/20260718_complete_set_lock_v1_variant.json",
        );
        let variants = read_variants(path).unwrap();

        assert_eq!(variants.len(), 1);
        assert_eq!(
            variants[0].name,
            "primary_v6_volfloor_300_complete_set_lock_v1"
        );
        assert!(variants[0].exit.complete_set_lock_enabled);
        assert_eq!(variants[0].exit.complete_set_min_profit_usd, 0.10);
        assert_eq!(variants[0].exit.complete_set_arm_profit_usd, 0.0);
        assert!(serde_json::to_value(variants[0].exit)
            .unwrap()
            .get("complete_set_arm_profit_usd")
            .is_none());
        assert_eq!(
            stable_json_hash(&variants[0]),
            "c25aa94ad592b6274150e48be7765ace8fa3beba85595e48225906ca01c01363"
        );
    }

    #[test]
    fn registered_settlement_anchor_baseline_is_exactly_parseable() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "../deploy/promotions/evidence/strategy_registry/20260721_settlement_source_anchor_baseline_variant.json",
        );
        let variants = read_variants(path).unwrap();

        assert_eq!(variants.len(), 1);
        assert_eq!(variants[0].name, "primary_v6_volfloor_300");
        assert_eq!(
            stable_json_hash(&variants[0]),
            "a5d67641653ae85a853aab531060a240eade257e32fd5bf0e46392c7934302d5"
        );
    }

    #[test]
    fn registered_complete_set_pair_pins_baseline_and_candidate_hashes() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "../deploy/promotions/evidence/strategy_registry/20260718_complete_set_lock_v1_pair.json",
        );
        let variants = read_variants(path).unwrap();

        assert_eq!(variants.len(), 2);
        assert_eq!(variants[0].name, "primary_v6_volfloor_300");
        assert_eq!(
            stable_json_hash(&variants[0]),
            "a5d67641653ae85a853aab531060a240eade257e32fd5bf0e46392c7934302d5"
        );
        assert_eq!(
            stable_json_hash(&variants[1]),
            "c25aa94ad592b6274150e48be7765ace8fa3beba85595e48225906ca01c01363"
        );
    }

    #[test]
    fn registered_trailing_complete_set_lock_variant_is_exactly_parseable() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "../deploy/promotions/evidence/strategy_registry/20260718_trailing_complete_set_lock_v2_variant.json",
        );
        let variants = read_variants(path).unwrap();

        assert_eq!(variants.len(), 1);
        assert_eq!(
            variants[0].name,
            "primary_v6_volfloor_300_trailing_complete_set_lock_v2"
        );
        assert!(variants[0].exit.complete_set_lock_enabled);
        assert_eq!(variants[0].exit.complete_set_min_profit_usd, 0.10);
        assert_eq!(variants[0].exit.complete_set_arm_profit_usd, 0.50);
        assert_eq!(
            stable_json_hash(&variants[0]),
            "8554587b2e8bca78c504f3fbb8840737fee1d384567b173ba8efe8d909a4bb11"
        );
    }

    #[test]
    fn registered_trailing_complete_set_pair_pins_baseline_and_candidate_hashes() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "../deploy/promotions/evidence/strategy_registry/20260718_trailing_complete_set_lock_v2_pair.json",
        );
        let variants = read_variants(path).unwrap();

        assert_eq!(variants.len(), 2);
        assert_eq!(
            stable_json_hash(&variants[0]),
            "a5d67641653ae85a853aab531060a240eade257e32fd5bf0e46392c7934302d5"
        );
        assert_eq!(
            stable_json_hash(&variants[1]),
            "8554587b2e8bca78c504f3fbb8840737fee1d384567b173ba8efe8d909a4bb11"
        );
    }
}
