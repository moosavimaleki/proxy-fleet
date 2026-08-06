use chrono::{DateTime, Duration, Utc};

use crate::domain::{
    evidence::{EvidenceDelta, TestStage, full_jitter_delay, health, lease_until},
    failure::FailureClass,
    proxy::LifecycleState,
};

#[derive(Debug, Clone)]
pub struct HealthDecision {
    pub lifecycle: LifecycleState,
    pub score: f64,
    pub failure_streak: u32,
    pub independent_failures: u32,
    pub lease_until: Option<DateTime<Utc>>,
    pub next_test_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct HealthInput {
    pub prior_lifecycle: LifecycleState,
    pub prior_lease_until: Option<DateTime<Utc>>,
    pub activated_at: Option<DateTime<Utc>>,
    pub alpha: f64,
    pub beta: f64,
    pub failure_streak: u32,
    pub independent_failures: u32,
    /// A cascade can emit more than one observation. Only failures from a
    /// different run are independent evidence for demoting an ACTIVE proxy.
    pub new_independent_failure: bool,
    pub had_real_download: bool,
    pub stage: TestStage,
    pub class: FailureClass,
    pub fast_download: bool,
    pub now: DateTime<Utc>,
    pub active_relay_interval: Duration,
    pub active_download_interval: Duration,
    pub active_min_residence: Duration,
}

pub fn decide(input: HealthInput) -> HealthDecision {
    let succeeded = input.class == FailureClass::Success;
    let inconclusive = input.class.inconclusive();
    let score = health(input.alpha, input.beta);
    let real_download = succeeded && input.stage == TestStage::Download;
    let failure_streak = if succeeded {
        0
    } else if inconclusive {
        input.failure_streak
    } else {
        input.failure_streak.saturating_add(1)
    };
    let independent_failures = if succeeded {
        0
    } else if inconclusive {
        input.independent_failures
    } else if input.new_independent_failure {
        input.independent_failures.saturating_add(1)
    } else {
        input.independent_failures
    };
    let active_resident = input
        .activated_at
        .map(|time| input.now - time >= input.active_min_residence)
        .unwrap_or(false);

    let lifecycle = if input.class == FailureClass::InvalidConfig {
        LifecycleState::Invalid
    } else if real_download {
        LifecycleState::Active
    } else {
        match input.prior_lifecycle {
            LifecycleState::Active if !active_resident => LifecycleState::Active,
            LifecycleState::Active if score < 0.35 && independent_failures >= 2 => {
                LifecycleState::Probation
            }
            LifecycleState::Active => LifecycleState::Active,
            LifecycleState::Probation if score >= 0.70 => LifecycleState::Active,
            LifecycleState::Probation if score < 0.15 && failure_streak >= 3 => {
                LifecycleState::Dormant
            }
            LifecycleState::Probation => LifecycleState::Probation,
            LifecycleState::Dormant if score >= 0.70 && succeeded => LifecycleState::Probation,
            LifecycleState::Dormant => LifecycleState::Dormant,
            LifecycleState::Retired => LifecycleState::Retired,
            LifecycleState::Invalid => LifecycleState::Invalid,
            LifecycleState::WaitingForPort => LifecycleState::WaitingForPort,
            LifecycleState::Testing => LifecycleState::Testing,
            LifecycleState::Candidate => LifecycleState::Candidate,
        }
    };

    let candidate_lease = if succeeded {
        Some(lease_until(input.stage, input.fast_download, input.now))
    } else {
        None
    };
    let lease_until = match (input.prior_lease_until, candidate_lease) {
        (Some(current), Some(candidate)) => Some(current.max(candidate)),
        (current, candidate) => current.or(candidate),
    };
    let next_test_at = if succeeded {
        match lifecycle {
            LifecycleState::Active if input.stage == TestStage::Download => {
                input.now + input.active_download_interval
            }
            LifecycleState::Active => input.now + input.active_relay_interval,
            LifecycleState::Probation => input.now + Duration::minutes(15),
            LifecycleState::Dormant => input.now + Duration::hours(6),
            _ => input.now + Duration::minutes(5),
        }
    } else if inconclusive {
        input.now + Duration::minutes(2)
    } else {
        input.now
            + full_jitter_delay(
                failure_streak,
                input.had_real_download,
                lifecycle == LifecycleState::Dormant,
                input.class,
            )
    };
    HealthDecision {
        lifecycle,
        score,
        failure_streak,
        independent_failures,
        lease_until,
        next_test_at,
    }
}

pub fn event_contribution(
    stage: TestStage,
    class: FailureClass,
    fast_download: bool,
) -> EvidenceDelta {
    crate::domain::evidence::delta(stage, class, fast_download)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(
        prior_lifecycle: LifecycleState,
        stage: TestStage,
        class: FailureClass,
    ) -> HealthInput {
        HealthInput {
            prior_lifecycle,
            prior_lease_until: None,
            activated_at: None,
            alpha: 8.0,
            beta: 1.0,
            failure_streak: 0,
            independent_failures: 0,
            new_independent_failure: true,
            had_real_download: false,
            stage,
            class,
            fast_download: true,
            now: Utc::now(),
            active_relay_interval: Duration::seconds(10),
            active_download_interval: Duration::minutes(5),
            active_min_residence: Duration::minutes(30),
        }
    }

    #[test]
    fn candidate_requires_a_real_download_to_become_active() {
        let relay = decide(input(
            LifecycleState::Candidate,
            TestStage::Relay,
            FailureClass::Success,
        ));
        assert_eq!(relay.lifecycle, LifecycleState::Candidate);
        let download = decide(input(
            LifecycleState::Candidate,
            TestStage::Download,
            FailureClass::Success,
        ));
        assert_eq!(download.lifecycle, LifecycleState::Active);
        assert!(download.lease_until.is_some());
    }

    #[test]
    fn one_tls_timeout_cannot_evict_an_active_proxy() {
        let mut event = input(
            LifecycleState::Active,
            TestStage::Relay,
            FailureClass::TlsTimeout,
        );
        event.alpha = 1.0;
        event.beta = 9.0;
        event.activated_at = Some(event.now - event.active_min_residence - Duration::minutes(1));
        event.independent_failures = 0;
        event.had_real_download = true;
        let decision = decide(event);
        assert_eq!(decision.lifecycle, LifecycleState::Active);
    }

    #[test]
    fn active_demotes_only_after_residence_and_two_independent_failures() {
        let mut event = input(
            LifecycleState::Active,
            TestStage::Relay,
            FailureClass::TcpTimeout,
        );
        event.alpha = 1.0;
        event.beta = 9.0;
        event.activated_at = Some(event.now - event.active_min_residence - Duration::minutes(1));
        event.independent_failures = 1;
        assert_eq!(decide(event).lifecycle, LifecycleState::Probation);
    }

    #[test]
    fn inconclusive_event_preserves_failure_counters_and_lease() {
        let mut event = input(
            LifecycleState::Active,
            TestStage::Http,
            FailureClass::EndpointFailure,
        );
        event.failure_streak = 4;
        event.independent_failures = 3;
        event.prior_lease_until = Some(event.now + Duration::hours(4));
        let result = decide(event);
        assert_eq!(result.lifecycle, LifecycleState::Active);
        assert_eq!(result.failure_streak, 4);
        assert_eq!(result.independent_failures, 3);
        assert!(result.lease_until.is_some());
    }

    #[test]
    fn active_relay_revalidation_does_not_wait_for_the_download_interval() {
        let mut relay = input(
            LifecycleState::Active,
            TestStage::Relay,
            FailureClass::Success,
        );
        relay.active_relay_interval = Duration::seconds(11);
        relay.active_download_interval = Duration::minutes(5);
        let now = relay.now;
        assert_eq!(decide(relay).next_test_at, now + Duration::seconds(11));
    }

    #[test]
    fn configured_residence_blocks_normal_active_demotion_until_it_expires() {
        let mut event = input(
            LifecycleState::Active,
            TestStage::Relay,
            FailureClass::TcpTimeout,
        );
        event.alpha = 1.0;
        event.beta = 9.0;
        event.independent_failures = 5;
        event.active_min_residence = Duration::minutes(15);
        event.activated_at = Some(event.now - Duration::minutes(14));
        assert_eq!(decide(event).lifecycle, LifecycleState::Active);
    }
}
