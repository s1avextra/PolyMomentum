//! Candle window length parsing.
//!
//! Polymarket questions look like:
//!   "Bitcoin Up or Down - April 4, 3:45AM-4:00AM ET"  → 15-minute window
//!   "Bitcoin Up or Down - April 4, 3AM ET"            → hourly window
//!
//! Returns 0.0 when we can't parse — the caller treats that as "skip" rather
//! than guessing a default window length.

use chrono::{DateTime, NaiveTime, Utc};
use once_cell::sync::Lazy;
use regex::Regex;

static AM_PM_RANGE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(\d{1,2}(?::\d{2})?\s*(?:am|pm))\s*-\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm))")
        .expect("range regex")
});

static HOUR_TOKEN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(\d{1,2})\s*(am|pm)\b").expect("hour regex"));

pub fn estimate_window_minutes(description: &str) -> f64 {
    let s = description.to_lowercase();
    if let Some(caps) = AM_PM_RANGE.captures(&s) {
        let t1 = caps.get(1).map(|m| m.as_str().trim()).unwrap_or("");
        let t2 = caps.get(2).map(|m| m.as_str().trim()).unwrap_or("");
        if let (Some(n1), Some(n2)) = (parse_clock(t1), parse_clock(t2)) {
            let diff_min = ((n2 - n1).num_minutes()) as f64;
            let diff_min = if diff_min < 0.0 {
                diff_min + 1440.0
            } else {
                diff_min
            };
            if diff_min > 0.0 {
                return diff_min;
            }
        }
    }
    if HOUR_TOKEN.is_match(&s) {
        return 60.0;
    }
    0.0
}

pub fn btc_updown_slug_step_seconds(window_minutes: f64) -> Option<i64> {
    if (window_minutes - 5.0).abs() <= 1e-6 {
        Some(5 * 60)
    } else if (window_minutes - 15.0).abs() <= 1e-6 {
        Some(15 * 60)
    } else {
        None
    }
}

pub fn btc_updown_slugs_for_range(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    step_seconds: i64,
) -> Vec<String> {
    let Some(prefix) = btc_updown_slug_prefix(step_seconds) else {
        return Vec::new();
    };
    let start_seconds = start.timestamp();
    let end_exclusive_seconds = end.timestamp() + 3_600;
    let mut timestamp = start_seconds - start_seconds.rem_euclid(step_seconds);
    let mut slugs = Vec::new();
    while timestamp < end_exclusive_seconds {
        if timestamp + step_seconds > start_seconds {
            slugs.push(format!("{prefix}-{timestamp}"));
        }
        timestamp += step_seconds;
    }
    slugs
}

pub fn btc_updown_slugs_for_live_horizon(
    now: DateTime<Utc>,
    step_seconds: i64,
    horizon_minutes: i64,
) -> Vec<String> {
    let Some(prefix) = btc_updown_slug_prefix(step_seconds) else {
        return Vec::new();
    };
    let now_seconds = now.timestamp();
    let mut timestamp = (now_seconds - step_seconds).div_euclid(step_seconds) * step_seconds;
    let end_seconds = now_seconds + horizon_minutes.max(1) * 60 + step_seconds;
    let mut slugs = Vec::new();
    while timestamp <= end_seconds {
        slugs.push(format!("{prefix}-{timestamp}"));
        timestamp += step_seconds;
    }
    slugs
}

fn btc_updown_slug_prefix(step_seconds: i64) -> Option<&'static str> {
    match step_seconds {
        300 => Some("btc-updown-5m"),
        900 => Some("btc-updown-15m"),
        _ => None,
    }
}

fn parse_clock(s: &str) -> Option<chrono::NaiveDateTime> {
    let s = s.replace(' ', "").to_lowercase();
    for fmt in ["%I:%M%p", "%I%p"] {
        if let Ok(t) = NaiveTime::parse_from_str(&s, fmt) {
            return Some(chrono::NaiveDate::from_ymd_opt(2000, 1, 1)?.and_time(t));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hourly() {
        assert!((estimate_window_minutes("April 4, 3AM ET") - 60.0).abs() < 1e-6);
        assert!((estimate_window_minutes("April 4, 3PM ET") - 60.0).abs() < 1e-6);
    }

    #[test]
    fn parses_15_minute() {
        assert!((estimate_window_minutes("April 4, 3:45AM-4:00AM ET") - 15.0).abs() < 1e-6);
    }

    #[test]
    fn parses_5_minute() {
        assert!((estimate_window_minutes("April 4, 3:55AM-4:00AM ET") - 5.0).abs() < 1e-6);
    }

    #[test]
    fn handles_overnight() {
        // 11:55PM-12:00AM is 5 minutes (rollover)
        assert!((estimate_window_minutes("April 4, 11:55PM-12:00AM ET") - 5.0).abs() < 1e-6);
    }

    #[test]
    fn returns_zero_when_unparseable() {
        assert!(estimate_window_minutes("garbage").abs() < 1e-9);
    }

    #[test]
    fn slug_generation_only_accepts_supported_windows() {
        assert_eq!(btc_updown_slug_step_seconds(5.0), Some(300));
        assert_eq!(btc_updown_slug_step_seconds(15.0), Some(900));
        assert_eq!(btc_updown_slug_step_seconds(60.0), None);

        let now = DateTime::from_timestamp(1_779_539_390, 0).unwrap();
        let slugs = btc_updown_slugs_for_live_horizon(now, 300, 15);
        assert!(slugs.contains(&"btc-updown-5m-1779538800".to_string()));
        assert!(slugs.contains(&"btc-updown-5m-1779539100".to_string()));
        assert!(slugs.len() <= 6);
        assert!(btc_updown_slugs_for_live_horizon(now, 60, 15).is_empty());
    }
}
