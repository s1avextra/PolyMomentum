//! Strategy variants — each is a `ZoneConfig` + dead-zone toggle + label.
//!
//! Add new variants here, then they appear in `polymomentum-engine sweep`.

use crate::strategy::decision::ZoneConfig;
use crate::strategy::microstructure::MicrostructureConfig;

#[derive(Debug, Clone)]
pub struct Strategy {
    pub name: String,
    pub zone_config: ZoneConfig,
    pub skip_dead_zone: bool,
    pub min_confidence: f64,
    pub min_edge: f64,
    pub prefer_maker: bool,
    pub microstructure: MicrostructureConfig,
}

pub fn baseline() -> Strategy {
    Strategy {
        name: "baseline".into(),
        zone_config: ZoneConfig::default(),
        skip_dead_zone: true,
        min_confidence: 0.60,
        min_edge: 0.07,
        prefer_maker: false,
        microstructure: MicrostructureConfig::disabled(),
    }
}

/// Disable early/primary/late zones via unreachable thresholds. Only terminal
/// (≥95% elapsed) zone trades fire.
pub fn terminal_only() -> Strategy {
    let cfg = ZoneConfig {
        early_min_confidence: 1.1, // unreachable
        early_min_z: 100.0,
        late_min_confidence: 1.1,
        late_min_z: 100.0,
        primary_min_z: 100.0,
        ..ZoneConfig::default()
    };
    Strategy {
        name: "terminal_only".into(),
        zone_config: cfg,
        skip_dead_zone: true,
        min_confidence: 0.60,
        min_edge: 0.07,
        prefer_maker: false,
        microstructure: MicrostructureConfig::disabled(),
    }
}

/// Looser terminal entry — wider eligibility, see if the extra trades hold up.
pub fn aggressive_terminal() -> Strategy {
    let cfg = ZoneConfig {
        early_min_confidence: 1.1,
        early_min_z: 100.0,
        late_min_confidence: 1.1,
        late_min_z: 100.0,
        primary_min_z: 100.0,
        terminal_min_confidence: 0.50,
        terminal_min_z: 0.20,
        terminal_min_edge: 0.02,
        min_ev_buffer: 0.03,
        ..ZoneConfig::default()
    };
    Strategy {
        name: "aggressive_terminal".into(),
        zone_config: cfg,
        skip_dead_zone: true,
        min_confidence: 0.60,
        min_edge: 0.07,
        prefer_maker: false,
        microstructure: MicrostructureConfig::disabled(),
    }
}

/// Tighter terminal — require strong agreement before firing.
pub fn conservative_terminal() -> Strategy {
    let cfg = ZoneConfig {
        early_min_confidence: 1.1,
        early_min_z: 100.0,
        late_min_confidence: 1.1,
        late_min_z: 100.0,
        primary_min_z: 100.0,
        terminal_min_confidence: 0.65,
        terminal_min_z: 0.50,
        terminal_min_edge: 0.07,
        min_ev_buffer: 0.07,
        ..ZoneConfig::default()
    };
    Strategy {
        name: "conservative_terminal".into(),
        zone_config: cfg,
        skip_dead_zone: true,
        min_confidence: 0.60,
        min_edge: 0.07,
        prefer_maker: false,
        microstructure: MicrostructureConfig::disabled(),
    }
}

/// Disable dead-zone filter (allow 0.80-0.90 confidence trades).
pub fn no_dead_zone() -> Strategy {
    Strategy {
        name: "no_dead_zone".into(),
        zone_config: ZoneConfig::default(),
        skip_dead_zone: false,
        min_confidence: 0.60,
        min_edge: 0.07,
        prefer_maker: false,
        microstructure: MicrostructureConfig::disabled(),
    }
}

/// Disable the entry-price EV filter — see how many of those rejected trades
/// would actually have lost money.
pub fn ev_off() -> Strategy {
    let cfg = ZoneConfig {
        min_ev_buffer: -1.0,
        ..ZoneConfig::default()
    };
    Strategy {
        name: "ev_off".into(),
        zone_config: cfg,
        skip_dead_zone: true,
        min_confidence: 0.60,
        min_edge: 0.07,
        prefer_maker: false,
        microstructure: MicrostructureConfig::disabled(),
    }
}

/// Tighter EV filter — require larger expected-value buffer.
pub fn ev_strict() -> Strategy {
    let cfg = ZoneConfig {
        min_ev_buffer: 0.10,
        ..ZoneConfig::default()
    };
    Strategy {
        name: "ev_strict".into(),
        zone_config: cfg,
        skip_dead_zone: true,
        min_confidence: 0.60,
        min_edge: 0.07,
        prefer_maker: false,
        microstructure: MicrostructureConfig::disabled(),
    }
}

/// Maker-first — assumes a posted limit gets filled at improvement, with 0%
/// fee. Optimistic; a real maker route needs a fallback and timing.
pub fn maker_first() -> Strategy {
    Strategy {
        name: "maker_first".into(),
        zone_config: ZoneConfig::default(),
        skip_dead_zone: true,
        min_confidence: 0.60,
        min_edge: 0.07,
        prefer_maker: true,
        microstructure: MicrostructureConfig::disabled(),
    }
}

pub fn paper_a_plus_floor() -> Strategy {
    let cfg = ZoneConfig {
        early_min_confidence: 0.35,
        late_min_confidence: 0.35,
        terminal_min_confidence: 0.35,
        early_min_z: 0.50,
        primary_min_z: 0.50,
        late_min_z: 0.50,
        terminal_min_z: 0.50,
        early_min_edge: 0.02,
        late_min_edge: 0.02,
        terminal_min_edge: 0.02,
        min_ev_buffer: -1.0,
        max_price: 0.75,
        settlement_guard_minutes: 5.0,
        settlement_min_abs_move_usd: 25.0,
        settlement_sigma_buffer: 0.20,
        ..ZoneConfig::default()
    };
    Strategy {
        name: "paper_a_plus_floor".into(),
        zone_config: cfg,
        skip_dead_zone: true,
        min_confidence: 0.35,
        min_edge: 0.02,
        prefer_maker: true,
        microstructure: MicrostructureConfig {
            max_spread: 0.02,
            min_book_depth: 20.0,
            min_book_pressure: 0.0,
        },
    }
}

/// Default sweep set.
pub fn default_strategies() -> Vec<Strategy> {
    vec![
        baseline(),
        terminal_only(),
        aggressive_terminal(),
        conservative_terminal(),
        no_dead_zone(),
        ev_off(),
        ev_strict(),
        maker_first(),
        paper_a_plus_floor(),
    ]
}
