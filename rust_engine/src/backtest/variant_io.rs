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
}
