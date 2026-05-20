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

#[derive(Debug, Clone)]
pub struct GridConfig {
    pub conf: Vec<f64>,
    pub z: Vec<f64>,
    pub edge: Vec<f64>,
    pub ev_buffer: Vec<f64>,
    pub min_price: Vec<f64>,
    pub max_price: Vec<f64>,
    pub settlement_min_abs_move_usd: Vec<f64>,
    pub settlement_guard_minutes: Vec<f64>,
    pub settlement_sigma_buffer: Vec<f64>,
    pub micro_max_spread: Vec<f64>,
    pub micro_min_depth: Vec<f64>,
    pub micro_min_pressure: Vec<f64>,
    pub also_maker: bool,
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

pub fn grid_strategies(grid: &GridConfig) -> Vec<Strategy> {
    let maker_sides: Vec<bool> = if grid.also_maker {
        vec![false, true]
    } else {
        vec![false]
    };
    let mut out = Vec::new();
    for &conf in &grid.conf {
        for &z in &grid.z {
            for &edge in &grid.edge {
                for &ev in &grid.ev_buffer {
                    for &min_price in &grid.min_price {
                        for &max_price in &grid.max_price {
                            if min_price > max_price {
                                continue;
                            }
                            for &floor in &grid.settlement_min_abs_move_usd {
                                for &guard in &grid.settlement_guard_minutes {
                                    for &sigma in &grid.settlement_sigma_buffer {
                                        for &micro_spread in &grid.micro_max_spread {
                                            for &micro_depth in &grid.micro_min_depth {
                                                for &micro_pressure in &grid.micro_min_pressure {
                                                    for &maker in &maker_sides {
                                                        let zone_config = ZoneConfig {
                                                            early_min_confidence: conf,
                                                            late_min_confidence: conf,
                                                            terminal_min_confidence: conf,
                                                            early_min_z: z,
                                                            primary_min_z: z,
                                                            late_min_z: z,
                                                            terminal_min_z: z,
                                                            early_min_edge: edge,
                                                            late_min_edge: edge,
                                                            terminal_min_edge: edge,
                                                            min_price,
                                                            max_price,
                                                            min_ev_buffer: ev,
                                                            settlement_min_abs_move_usd: floor,
                                                            settlement_guard_minutes: guard,
                                                            settlement_sigma_buffer: sigma,
                                                            ..ZoneConfig::default()
                                                        };
                                                        out.push(Strategy {
                                                            name: format!(
                                                                "c{:.2}_z{:.2}_e{:.2}_ev{:+.2}_p{:.2}-{:.2}_sf{:.0}_sg{:.1}_ss{:.2}_ms{:.2}_md{:.0}_mp{:.2}_{}",
                                                                conf,
                                                                z,
                                                                edge,
                                                                ev,
                                                                min_price,
                                                                max_price,
                                                                floor,
                                                                guard,
                                                                sigma,
                                                                micro_spread,
                                                                micro_depth,
                                                                micro_pressure,
                                                                if maker { "mk" } else { "tk" },
                                                            ),
                                                            zone_config,
                                                            skip_dead_zone: true,
                                                            min_confidence: conf,
                                                            min_edge: edge,
                                                            prefer_maker: maker,
                                                            microstructure: MicrostructureConfig {
                                                                max_spread: micro_spread,
                                                                min_book_depth: micro_depth,
                                                                min_book_pressure: micro_pressure,
                                                            },
                                                        });
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_grid() -> GridConfig {
        GridConfig {
            conf: vec![0.20, 0.30],
            z: vec![0.0, 0.5],
            edge: vec![0.02],
            ev_buffer: vec![-1.0],
            min_price: vec![0.10],
            max_price: vec![0.75],
            settlement_min_abs_move_usd: vec![25.0],
            settlement_guard_minutes: vec![5.0],
            settlement_sigma_buffer: vec![0.20],
            micro_max_spread: vec![0.02],
            micro_min_depth: vec![20.0],
            micro_min_pressure: vec![0.0],
            also_maker: true,
        }
    }

    #[test]
    fn grid_expands_cartesian_product() {
        let strategies = grid_strategies(&small_grid());
        assert_eq!(strategies.len(), 2 * 2 * 2);
        assert!(strategies.iter().any(|s| s.prefer_maker));
        assert!(strategies.iter().any(|s| !s.prefer_maker));
    }

    #[test]
    fn grid_names_are_unique() {
        let strategies = grid_strategies(&small_grid());
        let names: std::collections::HashSet<&str> =
            strategies.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names.len(), strategies.len());
    }
}
