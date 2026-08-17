//! Seal causal opportunity tables and create a separately keyed label table.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use chrono::{SecondsFormat, Utc};
use flate2::read::GzDecoder;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::artifact::write_json_artifact_atomic;
use crate::strategy::spec::stable_json_hash;

use super::opportunity_table::{HashedSource, OpportunityTableManifest};

pub const OPPORTUNITY_DATASET_SCHEMA_VERSION: &str = "opportunity_dataset_v1";
pub const OPPORTUNITY_LABEL_SCHEMA_VERSION: &str = "opportunity_labels_v1";

#[derive(Debug, Clone)]
pub struct OpportunityDatasetSealInput {
    pub opportunity_manifest_paths: Vec<PathBuf>,
    pub output_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityDatasetEntry {
    pub hour: String,
    pub manifest: HashedSource,
    pub opportunity_table: HashedSource,
    pub row_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityDatasetSeal {
    pub schema_version: String,
    pub generated_at: String,
    pub causal_feature_semantics_version: String,
    pub stake_usd: f64,
    pub fee_rate: f64,
    pub entries: Vec<OpportunityDatasetEntry>,
    pub total_rows: usize,
    pub unique_opportunity_ids: usize,
    pub dataset_sha256: String,
    pub outcome_columns_present: bool,
}

#[derive(Debug, Clone)]
pub struct OpportunityLabelsInput {
    pub dataset_seal_path: PathBuf,
    pub label_source_path: PathBuf,
    pub output_path: PathBuf,
    pub manifest_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityLabelsManifest {
    pub schema_version: String,
    pub generated_at: String,
    pub dataset_seal: HashedSource,
    pub dataset_sha256: String,
    pub label_source: HashedSource,
    pub output: HashedSource,
    pub total_opportunities: usize,
    pub labeled_rows: usize,
    pub tie_rows: usize,
    pub missing_label_rows: usize,
    pub fresh_holdout_rows_excluded: usize,
    pub fresh_holdout_labels_present: bool,
    pub join_key: String,
    pub resolution_semantics: String,
}

#[derive(Debug, Clone)]
pub(crate) struct CausalOpportunity {
    pub opportunity_id: String,
    pub condition_id: String,
    pub token_id: String,
    pub chronological_window: String,
    pub window_start_ms: i64,
    pub observed_at_ms: i64,
    pub signal_direction: String,
    pub strike_price: f64,
    pub btc_observed: f64,
    pub elapsed_seconds: f64,
    pub remaining_seconds: f64,
    pub move_2m_usd: Option<f64>,
    pub path_2m_aligned: Option<bool>,
    pub path_3m_aligned: Option<bool>,
    pub path_4m_aligned: Option<bool>,
    pub directional_distance_to_strike_usd: f64,
    pub causal_volatility: f64,
    pub book_observable: bool,
    pub best_ask: Option<f64>,
    pub top_book_pressure: Option<f64>,
    pub stake_fully_executable: bool,
    pub fee_aware_break_even_probability: Option<f64>,
    pub fee_aware_net_win_usd: Option<f64>,
    pub fee_aware_max_loss_usd: Option<f64>,
    pub btc_open: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct OpportunityLabel {
    pub opportunity_id: String,
    pub terminal_btc: f64,
    pub terminal_direction: String,
    pub won: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WindowLabel {
    window_start: i64,
    terminal: f64,
}

pub fn seal_dataset(input: OpportunityDatasetSealInput) -> Result<OpportunityDatasetSeal> {
    if input.opportunity_manifest_paths.is_empty() {
        bail!("at least one opportunity manifest is required");
    }
    if input
        .opportunity_manifest_paths
        .contains(&input.output_path)
    {
        bail!("dataset seal must not replace an input manifest");
    }

    let mut entries = Vec::new();
    let mut hours = HashSet::new();
    let mut opportunity_ids = HashSet::new();
    let mut semantics = None;
    let mut stake_usd = None;
    let mut fee_rate = None;
    for path in &input.opportunity_manifest_paths {
        let manifest: OpportunityTableManifest = serde_json::from_reader(
            File::open(path).with_context(|| format!("open manifest {}", path.display()))?,
        )
        .with_context(|| format!("parse manifest {}", path.display()))?;
        if manifest.schema_version != super::opportunity_table::OPPORTUNITY_TABLE_SCHEMA_VERSION {
            bail!("unsupported opportunity-table schema in {}", path.display());
        }
        if manifest.outcome_columns_present {
            bail!("opportunity manifest claims outcome columns are present");
        }
        if !hours.insert(manifest.hour.clone()) {
            bail!("duplicate opportunity hour {}", manifest.hour);
        }
        match &semantics {
            None => semantics = Some(manifest.causal_feature_semantics_version.clone()),
            Some(expected) if *expected != manifest.causal_feature_semantics_version => {
                bail!("mixed causal feature semantics in dataset")
            }
            _ => {}
        }
        validate_same_f64("stake_usd", &mut stake_usd, manifest.stake_usd)?;
        validate_same_f64("fee_rate", &mut fee_rate, manifest.fee_rate)?;

        let table_path = PathBuf::from(&manifest.output.path);
        let actual_table_hash = sha256_file(&table_path)?;
        if actual_table_hash != manifest.output.sha256 {
            bail!(
                "opportunity table hash mismatch at {}",
                table_path.display()
            );
        }
        let rows = read_opportunities(&table_path)?;
        if rows.len() != manifest.row_count {
            bail!("opportunity row count mismatch at {}", table_path.display());
        }
        for row in rows {
            if !opportunity_ids.insert(row.opportunity_id) {
                bail!("duplicate opportunity_id across dataset");
            }
        }
        entries.push(OpportunityDatasetEntry {
            hour: manifest.hour,
            manifest: HashedSource {
                path: path.display().to_string(),
                sha256: sha256_file(path)?,
            },
            opportunity_table: HashedSource {
                path: table_path.display().to_string(),
                sha256: actual_table_hash,
            },
            row_count: manifest.row_count,
        });
    }
    entries.sort_by(|left, right| left.hour.cmp(&right.hour));
    let total_rows = entries.iter().map(|entry| entry.row_count).sum();
    let semantics = semantics.context("dataset semantics missing")?;
    let stake_usd = stake_usd.context("dataset stake missing")?;
    let fee_rate = fee_rate.context("dataset fee rate missing")?;
    let dataset_sha256 = stable_json_hash(&serde_json::json!({
        "schema_version": OPPORTUNITY_DATASET_SCHEMA_VERSION,
        "causal_feature_semantics_version": semantics,
        "stake_usd": stake_usd,
        "fee_rate": fee_rate,
        "entries": entries,
    }));
    let seal = OpportunityDatasetSeal {
        schema_version: OPPORTUNITY_DATASET_SCHEMA_VERSION.to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        causal_feature_semantics_version: semantics,
        stake_usd,
        fee_rate,
        entries,
        total_rows,
        unique_opportunity_ids: opportunity_ids.len(),
        dataset_sha256,
        outcome_columns_present: false,
    };
    write_json_artifact_atomic(&input.output_path, &seal)?;
    Ok(seal)
}

fn validate_same_f64(name: &str, expected: &mut Option<f64>, value: f64) -> Result<()> {
    if !value.is_finite() {
        bail!("{name} must be finite");
    }
    match expected {
        Some(previous) if (*previous - value).abs() > 1e-12 => {
            bail!("mixed {name} values in dataset")
        }
        None => *expected = Some(value),
        _ => {}
    }
    Ok(())
}

pub fn create_labels(input: OpportunityLabelsInput) -> Result<OpportunityLabelsManifest> {
    if input.output_path == input.manifest_path
        || input.output_path == input.dataset_seal_path
        || input.output_path == input.label_source_path
        || input.manifest_path == input.dataset_seal_path
        || input.manifest_path == input.label_source_path
    {
        bail!("label outputs must not replace inputs or each other");
    }
    let seal_sha256 = sha256_file(&input.dataset_seal_path)?;
    let (seal, opportunities) = load_sealed_opportunities(&input.dataset_seal_path)?;
    let source_sha256 = sha256_file(&input.label_source_path)?;
    let labels_by_window = read_window_labels(&input.label_source_path)?;
    let (mut labels, tie_rows, missing_label_rows, fresh_holdout_rows_excluded) =
        join_discovery_labels(&opportunities, &labels_by_window);
    labels.sort_by(|left, right| left.opportunity_id.cmp(&right.opportunity_id));
    write_labels_parquet_atomic(&input.output_path, &labels)?;
    let manifest = OpportunityLabelsManifest {
        schema_version: OPPORTUNITY_LABEL_SCHEMA_VERSION.to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        dataset_seal: HashedSource {
            path: input.dataset_seal_path.display().to_string(),
            sha256: seal_sha256,
        },
        dataset_sha256: seal.dataset_sha256,
        label_source: HashedSource {
            path: input.label_source_path.display().to_string(),
            sha256: source_sha256,
        },
        output: HashedSource {
            path: input.output_path.display().to_string(),
            sha256: sha256_file(&input.output_path)?,
        },
        total_opportunities: opportunities.len(),
        labeled_rows: labels.len(),
        tie_rows,
        missing_label_rows,
        fresh_holdout_rows_excluded,
        fresh_holdout_labels_present: false,
        join_key: "opportunity_id".to_string(),
        resolution_semantics: "Binance one-minute window terminal close proxy; official settlement parity remains a later gate".to_string(),
    };
    write_json_artifact_atomic(&input.manifest_path, &manifest)?;
    Ok(manifest)
}

fn join_discovery_labels(
    opportunities: &[CausalOpportunity],
    labels_by_window: &HashMap<i64, f64>,
) -> (Vec<OpportunityLabel>, usize, usize, usize) {
    let mut labels = Vec::with_capacity(opportunities.len());
    let mut tie_rows = 0usize;
    let mut missing_label_rows = 0usize;
    let mut fresh_holdout_rows_excluded = 0usize;
    for opportunity in opportunities {
        // Discovery labels are physically incapable of exposing the fresh
        // holdout. A separately locked post-selection command must own any
        // eventual fresh scoring step.
        if opportunity.chronological_window == "fresh_holdout" {
            fresh_holdout_rows_excluded += 1;
            continue;
        }
        let window_start = opportunity.window_start_ms / 1_000;
        let Some(terminal_btc) = labels_by_window.get(&window_start).copied() else {
            missing_label_rows += 1;
            continue;
        };
        let (terminal_direction, won) = if terminal_btc > opportunity.btc_open {
            ("up", Some(opportunity.signal_direction == "up"))
        } else if terminal_btc < opportunity.btc_open {
            ("down", Some(opportunity.signal_direction == "down"))
        } else {
            tie_rows += 1;
            ("tie", None)
        };
        labels.push(OpportunityLabel {
            opportunity_id: opportunity.opportunity_id.clone(),
            terminal_btc,
            terminal_direction: terminal_direction.to_string(),
            won,
        });
    }
    (
        labels,
        tie_rows,
        missing_label_rows,
        fresh_holdout_rows_excluded,
    )
}

pub(crate) fn load_sealed_opportunities(
    seal_path: &Path,
) -> Result<(OpportunityDatasetSeal, Vec<CausalOpportunity>)> {
    let seal: OpportunityDatasetSeal = serde_json::from_reader(
        File::open(seal_path).with_context(|| format!("open seal {}", seal_path.display()))?,
    )
    .with_context(|| format!("parse seal {}", seal_path.display()))?;
    if seal.schema_version != OPPORTUNITY_DATASET_SCHEMA_VERSION || seal.outcome_columns_present {
        bail!("invalid or outcome-bearing opportunity dataset seal");
    }
    let expected_dataset_hash = stable_json_hash(&serde_json::json!({
        "schema_version": OPPORTUNITY_DATASET_SCHEMA_VERSION,
        "causal_feature_semantics_version": seal.causal_feature_semantics_version,
        "stake_usd": seal.stake_usd,
        "fee_rate": seal.fee_rate,
        "entries": seal.entries,
    }));
    if expected_dataset_hash != seal.dataset_sha256 {
        bail!("opportunity dataset seal hash is not reproducible");
    }
    let mut rows = Vec::new();
    let mut ids = HashSet::new();
    for entry in &seal.entries {
        let manifest_path = PathBuf::from(&entry.manifest.path);
        if sha256_file(&manifest_path)? != entry.manifest.sha256 {
            bail!(
                "sealed opportunity manifest hash drift at {}",
                manifest_path.display()
            );
        }
        let manifest: OpportunityTableManifest = serde_json::from_reader(
            File::open(&manifest_path)
                .with_context(|| format!("open manifest {}", manifest_path.display()))?,
        )
        .with_context(|| format!("parse manifest {}", manifest_path.display()))?;
        if manifest.outcome_columns_present
            || manifest.hour != entry.hour
            || manifest.row_count != entry.row_count
            || manifest.output != entry.opportunity_table
            || manifest.causal_feature_semantics_version != seal.causal_feature_semantics_version
            || (manifest.stake_usd - seal.stake_usd).abs() > 1e-12
            || (manifest.fee_rate - seal.fee_rate).abs() > 1e-12
        {
            bail!(
                "sealed opportunity manifest contract drift at {}",
                manifest_path.display()
            );
        }
        let path = PathBuf::from(&entry.opportunity_table.path);
        if sha256_file(&path)? != entry.opportunity_table.sha256 {
            bail!("sealed opportunity table hash drift at {}", path.display());
        }
        let entry_rows = read_opportunities(&path)?;
        if entry_rows.len() != entry.row_count {
            bail!(
                "sealed opportunity table row count drift at {}",
                path.display()
            );
        }
        for row in entry_rows {
            if !ids.insert(row.opportunity_id.clone()) {
                bail!("duplicate opportunity_id in sealed dataset");
            }
            rows.push(row);
        }
    }
    if rows.len() != seal.total_rows || ids.len() != seal.unique_opportunity_ids {
        bail!("sealed dataset aggregate counts drifted");
    }
    rows.sort_by(|left, right| {
        left.observed_at_ms
            .cmp(&right.observed_at_ms)
            .then_with(|| left.opportunity_id.cmp(&right.opportunity_id))
    });
    Ok((seal, rows))
}

pub(crate) fn read_labels(path: &Path) -> Result<Vec<OpportunityLabel>> {
    let file = File::open(path).with_context(|| format!("open labels {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut labels = Vec::new();
    for batch in reader {
        let batch = batch?;
        for row in 0..batch.num_rows() {
            labels.push(OpportunityLabel {
                opportunity_id: required_string(&batch, "opportunity_id", row)?,
                terminal_btc: required_f64(&batch, "terminal_btc", row)?,
                terminal_direction: required_string(&batch, "terminal_direction", row)?,
                won: optional_bool(&batch, "won", row)?,
            });
        }
    }
    Ok(labels)
}

fn read_opportunities(path: &Path) -> Result<Vec<CausalOpportunity>> {
    let file = File::open(path).with_context(|| format!("open table {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch?;
        for forbidden in [
            "won",
            "outcome",
            "terminal_btc",
            "terminal_direction",
            "pnl",
        ] {
            if batch.column_by_name(forbidden).is_some() {
                bail!("outcome column {forbidden} found in causal opportunity table");
            }
        }
        for row in 0..batch.num_rows() {
            rows.push(CausalOpportunity {
                opportunity_id: required_string(&batch, "opportunity_id", row)?,
                condition_id: required_string(&batch, "condition_id", row)?,
                token_id: required_string(&batch, "token_id", row)?,
                chronological_window: required_string(&batch, "chronological_window", row)?,
                window_start_ms: required_i64(&batch, "window_start_ms", row)?,
                observed_at_ms: required_i64(&batch, "observed_at_ms", row)?,
                signal_direction: required_string(&batch, "signal_direction", row)?,
                strike_price: required_f64(&batch, "strike_price", row)?,
                btc_observed: required_f64(&batch, "btc_observed", row)?,
                elapsed_seconds: required_f64(&batch, "elapsed_seconds", row)?,
                remaining_seconds: required_f64(&batch, "remaining_seconds", row)?,
                move_2m_usd: optional_f64(&batch, "move_2m_usd", row)?,
                path_2m_aligned: optional_bool(&batch, "path_2m_aligned", row)?,
                path_3m_aligned: optional_bool(&batch, "path_3m_aligned", row)?,
                path_4m_aligned: optional_bool(&batch, "path_4m_aligned", row)?,
                directional_distance_to_strike_usd: required_f64(
                    &batch,
                    "directional_distance_to_strike_usd",
                    row,
                )?,
                causal_volatility: required_f64(&batch, "causal_volatility", row)?,
                book_observable: required_bool(&batch, "book_observable", row)?,
                best_ask: optional_f64(&batch, "best_ask", row)?,
                top_book_pressure: optional_f64(&batch, "top_book_pressure", row)?,
                stake_fully_executable: required_bool(&batch, "stake_fully_executable", row)?,
                fee_aware_break_even_probability: optional_f64(
                    &batch,
                    "fee_aware_break_even_probability",
                    row,
                )?,
                fee_aware_net_win_usd: optional_f64(&batch, "fee_aware_net_win_usd", row)?,
                fee_aware_max_loss_usd: optional_f64(&batch, "fee_aware_max_loss_usd", row)?,
                btc_open: required_f64(&batch, "btc_open", row)?,
            });
        }
    }
    Ok(rows)
}

fn read_window_labels(path: &Path) -> Result<HashMap<i64, f64>> {
    let file = File::open(path).with_context(|| format!("open label source {}", path.display()))?;
    let reader: Box<dyn BufRead> =
        if path.extension().and_then(|value| value.to_str()) == Some("gz") {
            Box::new(BufReader::new(GzDecoder::new(file)))
        } else {
            Box::new(BufReader::new(file))
        };
    let mut labels = HashMap::new();
    for (index, line) in reader.lines().enumerate() {
        let row: WindowLabel =
            serde_json::from_str(&line.with_context(|| format!("read label line {}", index + 1))?)
                .with_context(|| format!("parse strict label line {}", index + 1))?;
        if !row.terminal.is_finite() || row.terminal <= 0.0 {
            bail!("terminal label must be finite and positive");
        }
        if labels.insert(row.window_start, row.terminal).is_some() {
            bail!("duplicate label window_start {}", row.window_start);
        }
    }
    Ok(labels)
}

fn write_labels_parquet_atomic(path: &Path, rows: &[OpportunityLabel]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("opportunity_id", DataType::Utf8, false),
        Field::new("terminal_btc", DataType::Float64, false),
        Field::new("terminal_direction", DataType::Utf8, false),
        Field::new("won", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.opportunity_id.as_str()),
            )) as ArrayRef,
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|row| row.terminal_btc),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.terminal_direction.as_str()),
            )),
            Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.won).collect::<Vec<_>>(),
            )),
        ],
    )?;
    write_record_batch_atomic(path, schema, &batch)
}

fn write_record_batch_atomic(path: &Path, schema: Arc<Schema>, batch: &RecordBatch) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("table.parquet");
    let temporary = path.with_file_name(format!("{name}.tmp.{}", std::process::id()));
    let result = (|| -> Result<()> {
        let file = File::create(&temporary)?;
        let properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(properties))?;
        writer.write(batch)?;
        writer.close()?;
        std::fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

fn required_string(batch: &RecordBatch, name: &str, row: usize) -> Result<String> {
    let array = batch
        .column_by_name(name)
        .with_context(|| format!("missing {name}"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .with_context(|| format!("{name} is not Utf8"))?;
    if array.is_null(row) {
        bail!("{name} is unexpectedly null");
    }
    Ok(array.value(row).to_string())
}

fn required_i64(batch: &RecordBatch, name: &str, row: usize) -> Result<i64> {
    let array = batch
        .column_by_name(name)
        .with_context(|| format!("missing {name}"))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .with_context(|| format!("{name} is not Int64"))?;
    if array.is_null(row) {
        bail!("{name} is unexpectedly null");
    }
    Ok(array.value(row))
}

fn required_f64(batch: &RecordBatch, name: &str, row: usize) -> Result<f64> {
    optional_f64(batch, name, row)?.with_context(|| format!("{name} is unexpectedly null"))
}

fn optional_f64(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<f64>> {
    let array = batch
        .column_by_name(name)
        .with_context(|| format!("missing {name}"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .with_context(|| format!("{name} is not Float64"))?;
    Ok((!array.is_null(row)).then(|| array.value(row)))
}

fn required_bool(batch: &RecordBatch, name: &str, row: usize) -> Result<bool> {
    optional_bool(batch, name, row)?.with_context(|| format!("{name} is unexpectedly null"))
}

fn optional_bool(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<bool>> {
    let array = batch
        .column_by_name(name)
        .with_context(|| format!("missing {name}"))?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .with_context(|| format!("{name} is not Boolean"))?;
    Ok((!array.is_null(row)).then(|| array.value(row)))
}

pub(crate) fn sha256_file(path: &Path) -> Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("open {} for hashing", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opportunity(
        id: &str,
        chronological_window: &str,
        window_start_ms: i64,
    ) -> CausalOpportunity {
        CausalOpportunity {
            opportunity_id: id.to_string(),
            condition_id: "condition".to_string(),
            token_id: "token".to_string(),
            chronological_window: chronological_window.to_string(),
            window_start_ms,
            observed_at_ms: window_start_ms + 120_000,
            signal_direction: "up".to_string(),
            strike_price: 100.0,
            btc_observed: 101.0,
            elapsed_seconds: 120.0,
            remaining_seconds: 180.0,
            move_2m_usd: Some(100.0),
            path_2m_aligned: Some(true),
            path_3m_aligned: None,
            path_4m_aligned: None,
            directional_distance_to_strike_usd: 100.0,
            causal_volatility: 0.5,
            book_observable: true,
            best_ask: Some(0.5),
            top_book_pressure: Some(0.0),
            stake_fully_executable: true,
            fee_aware_break_even_probability: Some(0.51),
            fee_aware_net_win_usd: Some(4.0),
            fee_aware_max_loss_usd: Some(5.0),
            btc_open: 100.0,
        }
    }

    #[test]
    fn strict_label_source_rejects_causal_or_extra_fields() {
        let value = serde_json::json!({
            "window_start": 1,
            "terminal": 100.0,
            "p0": 99.0
        });
        assert!(serde_json::from_value::<WindowLabel>(value).is_err());
    }

    #[test]
    fn label_parquet_round_trips_nullable_tie() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("labels.parquet");
        let rows = vec![
            OpportunityLabel {
                opportunity_id: "a".to_string(),
                terminal_btc: 101.0,
                terminal_direction: "up".to_string(),
                won: Some(true),
            },
            OpportunityLabel {
                opportunity_id: "b".to_string(),
                terminal_btc: 100.0,
                terminal_direction: "tie".to_string(),
                won: None,
            },
        ];
        write_labels_parquet_atomic(&path, &rows).unwrap();
        assert_eq!(read_labels(&path).unwrap(), rows);
    }

    #[test]
    fn discovery_label_join_physically_excludes_fresh_holdout() {
        let opportunities = vec![
            opportunity("older", "older", 1_000),
            opportunity("fresh", "fresh_holdout", 2_000),
        ];
        let labels_by_window = HashMap::from([(1, 101.0), (2, 1_000_000.0)]);
        let (labels, ties, missing, fresh_excluded) =
            join_discovery_labels(&opportunities, &labels_by_window);
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].opportunity_id, "older");
        assert_eq!(ties, 0);
        assert_eq!(missing, 0);
        assert_eq!(fresh_excluded, 1);
    }
}
