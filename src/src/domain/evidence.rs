use chrono::{DateTime, Duration, Utc};

use super::failure::FailureClass;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestStage {
    Static,
    DnsTcp,
    Relay,
    Http,
    Download,
}
impl TestStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Static => "STATIC",
            Self::DnsTcp => "DNS_TCP",
            Self::Relay => "RELAY",
            Self::Http => "HTTP",
            Self::Download => "DOWNLOAD",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EvidenceDelta {
    pub alpha: f64,
    pub beta: f64,
    pub half_life: Duration,
}

pub fn delta(stage: TestStage, class: FailureClass, fast_download: bool) -> EvidenceDelta {
    let hour = |hours| Duration::hours(hours);
    match (stage, class, fast_download) {
        (_, FailureClass::LocalOverload | FailureClass::EndpointFailure, _) => EvidenceDelta {
            alpha: 0.0,
            beta: 0.0,
            half_life: hour(2),
        },
        (_, FailureClass::InvalidConfig, _) => EvidenceDelta {
            alpha: 0.0,
            beta: 8.0,
            half_life: hour(12),
        },
        (TestStage::Download, FailureClass::Success, true) => EvidenceDelta {
            alpha: 8.0,
            beta: 0.0,
            half_life: hour(24),
        },
        (TestStage::Download, FailureClass::Success, false) => EvidenceDelta {
            alpha: 5.0,
            beta: 0.0,
            half_life: hour(24),
        },
        (TestStage::Http, FailureClass::Success, _) => EvidenceDelta {
            alpha: 3.0,
            beta: 0.0,
            half_life: hour(6),
        },
        (TestStage::Relay, FailureClass::Success, _) => EvidenceDelta {
            alpha: 1.0,
            beta: 0.0,
            half_life: hour(6),
        },
        (_, FailureClass::TlsTimeout, _) => EvidenceDelta {
            alpha: 0.0,
            beta: 0.5,
            half_life: hour(2),
        },
        (
            _,
            FailureClass::TcpTimeout
            | FailureClass::DnsFailure
            | FailureClass::RelayTimeout
            | FailureClass::DownloadTimeout,
            _,
        ) => EvidenceDelta {
            alpha: 0.0,
            beta: 1.0,
            half_life: hour(2),
        },
        (_, FailureClass::ConnectionRefused, _) => EvidenceDelta {
            alpha: 0.0,
            beta: 2.0,
            half_life: hour(12),
        },
        (TestStage::Download, FailureClass::DownloadTooSlow, _) => EvidenceDelta {
            alpha: 0.0,
            beta: 3.0,
            half_life: hour(12),
        },
        (_, FailureClass::XrayStartFailed | FailureClass::HttpFailure, _) => EvidenceDelta {
            alpha: 0.0,
            beta: 1.0,
            half_life: hour(12),
        },
        _ => EvidenceDelta {
            alpha: 0.0,
            beta: 0.0,
            half_life: hour(2),
        },
    }
}

pub fn decay(value: f64, elapsed: Duration, half_life: Duration) -> f64 {
    if value <= 0.0 || elapsed <= Duration::zero() {
        return value.max(0.0);
    }
    let ratio = elapsed.num_milliseconds() as f64 / half_life.num_milliseconds().max(1) as f64;
    value * 0.5_f64.powf(ratio)
}

pub fn health(alpha: f64, beta: f64) -> f64 {
    let total = alpha + beta;
    if total <= 0.0 {
        0.5
    } else {
        (alpha / total).clamp(0.0, 1.0)
    }
}

pub fn lease_until(stage: TestStage, fast_download: bool, now: DateTime<Utc>) -> DateTime<Utc> {
    now + match stage {
        TestStage::Download if fast_download => Duration::hours(12),
        TestStage::Download => Duration::hours(6),
        TestStage::Http => Duration::hours(2),
        TestStage::Relay => Duration::minutes(30),
        _ => Duration::zero(),
    }
}

pub fn full_jitter_delay(failure_streak: u32, had_real_success: bool, dormant: bool) -> Duration {
    use rand::Rng;

    let (base_seconds, cap_seconds) = if dormant {
        // Dormant means low priority, not permanently dead.  A two-hour
        // retry floor gives recently failing upstream entries a practical
        // chance to recover without occupying the hot queue.
        (2 * 60 * 60_u64, 12 * 60 * 60_u64)
    } else if had_real_success {
        (5 * 60_u64, 6 * 60 * 60_u64)
    } else {
        (30 * 60_u64, 24 * 60 * 60_u64)
    };
    let shift = failure_streak.min(20);
    let ceiling = base_seconds.saturating_mul(1_u64 << shift).min(cap_seconds);
    Duration::seconds(rand::rng().random_range(0..=ceiling) as i64)
}
