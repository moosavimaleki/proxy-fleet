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

pub fn full_jitter_delay(
    failure_streak: u32,
    had_real_success: bool,
    dormant: bool,
    class: FailureClass,
) -> Duration {
    use rand::Rng;

    let (base_seconds, cap_seconds) = if dormant {
        // Dormant is a recovery queue, never a graveyard. The successive
        // ceilings are 6h, 12h and 24h; full jitter prevents many revived
        // records from waking in one scheduler tick.
        let recovery_ceiling = match failure_streak {
            0 => 6 * 60 * 60_u64,
            1 => 12 * 60 * 60_u64,
            _ => 24 * 60 * 60_u64,
        };
        (recovery_ceiling, recovery_ceiling)
    } else if had_real_success {
        (5 * 60_u64, 6 * 60 * 60_u64)
    } else {
        (30 * 60_u64, 24 * 60 * 60_u64)
    };
    // A refused connection is stronger evidence than a timeout.  Advance its
    // exponential backoff by one step without changing its evidence class or
    // prematurely revoking its publication lease.
    let shift = failure_streak
        .saturating_add(u32::from(class == FailureClass::ConnectionRefused))
        .min(20);
    let ceiling = base_seconds.saturating_mul(1_u64 << shift).min(cap_seconds);
    Duration::seconds(rand::rng().random_range(0..=ceiling) as i64)
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use proptest::prelude::*;

    use super::{FailureClass, TestStage, decay, delta, full_jitter_delay, health, lease_until};

    #[test]
    fn decay_halves_exactly_at_the_half_life() {
        assert!((decay(8.0, Duration::hours(24), Duration::hours(24)) - 4.0).abs() < 1e-9);
        assert!((decay(8.0, Duration::zero(), Duration::hours(24)) - 8.0).abs() < 1e-9);
    }

    #[test]
    fn leases_match_the_publication_policy() {
        let now = Utc.with_ymd_and_hms(2026, 8, 5, 9, 0, 0).unwrap();
        assert_eq!(
            lease_until(TestStage::Download, true, now),
            now + Duration::hours(12)
        );
        assert_eq!(
            lease_until(TestStage::Download, false, now),
            now + Duration::hours(6)
        );
        assert_eq!(
            lease_until(TestStage::Http, false, now),
            now + Duration::hours(2)
        );
        assert_eq!(
            lease_until(TestStage::Relay, false, now),
            now + Duration::minutes(30)
        );
    }

    #[test]
    fn evidence_weights_and_inconclusive_classes_follow_the_model() {
        let fast = delta(TestStage::Download, FailureClass::Success, true);
        assert_eq!((fast.alpha, fast.beta), (8.0, 0.0));
        let slow = delta(TestStage::Download, FailureClass::DownloadTooSlow, false);
        assert_eq!((slow.alpha, slow.beta), (0.0, 3.0));
        for class in [FailureClass::LocalOverload, FailureClass::EndpointFailure] {
            let inconclusive = delta(TestStage::Http, class, false);
            assert_eq!((inconclusive.alpha, inconclusive.beta), (0.0, 0.0));
            assert!(class.inconclusive());
        }
    }

    proptest! {
        #[test]
        fn health_is_always_finite_and_bounded(alpha in -1.0e12_f64..1.0e12, beta in -1.0e12_f64..1.0e12) {
            let score = health(alpha, beta);
            prop_assert!(score.is_finite());
            prop_assert!((0.0..=1.0).contains(&score));
        }

        #[test]
        fn jitter_is_never_negative_or_above_its_cap(streak in 0_u32..30, successful in any::<bool>(), dormant in any::<bool>()) {
            let delay = full_jitter_delay(streak, successful, dormant, FailureClass::TcpTimeout);
            let cap = if dormant { if streak == 0 { 6 * 60 * 60 } else if streak == 1 { 12 * 60 * 60 } else { 24 * 60 * 60 } } else if successful { 6 * 60 * 60 } else { 24 * 60 * 60 };
            prop_assert!((0..=cap).contains(&delay.num_seconds()));
        }
    }

    #[test]
    fn refused_connection_has_a_stronger_first_backoff_ceiling_than_timeout() {
        // With no prior success, TCP starts at 30 min while a refused port
        // advances one exponential step to one hour.
        let timeout = full_jitter_delay(0, false, false, FailureClass::TcpTimeout);
        let refused = full_jitter_delay(0, false, false, FailureClass::ConnectionRefused);
        assert!(timeout <= Duration::minutes(30));
        assert!(refused <= Duration::hours(1));
    }
}
