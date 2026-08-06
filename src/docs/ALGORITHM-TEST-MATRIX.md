# Algorithm-to-test matrix

This matrix is the acceptance trace for `ALGORITHM.md`.  Test names are Rust
unit, property, or integration tests and are run with `cargo test --workspace`.
It intentionally references no raw proxy configuration or credential.

| Algorithm rule | Automated evidence |
|---|---|
| Canonical identity ignores a remark and preserves a reappearing identity | `parser::tests::remark_does_not_change_vless_identity`; `storage::tests::complete_upstream_reappearance_revives_a_retired_identity` |
| Lifecycle is limited to candidate/testing/active/probation/dormant/invalid/retired/waiting-for-port | `health::tests::remaining_lifecycle_transitions_preserve_hysteresis_rules`; `storage::tests::port_exhaustion_is_a_temporary_execution_state` |
| Health, next test and publication lease are separate | `storage::tests::publication_snapshot_reads_generation_and_leased_configs_together`; `storage::tests::endpoint_incident_does_not_demote_or_shorten_an_active_lease` |
| Results are append-only, stage-aware and idempotent | `storage::tests::repeated_stage_in_one_run_is_idempotent`; `storage::tests::frequent_successes_do_not_make_health_evidence_unbounded` |
| Error classification marks local/endpoint failures inconclusive | `domain::evidence::tests::evidence_weights_and_inconclusive_classes_follow_the_model`; `storage::tests::endpoint_incident_does_not_demote_or_shorten_an_active_lease` |
| Bayesian score, weighted evidence and asymmetric time decay remain safe | `domain::evidence::tests::decay_halves_exactly_at_the_half_life`; `domain::evidence::tests::health_is_always_finite_and_bounded`; `domain::evidence::tests::evidence_weights_and_inconclusive_classes_follow_the_model` |
| Hysteresis and active residence prevent transient removal | `health::tests::candidate_requires_a_real_download_to_become_active`; `health::tests::one_tls_timeout_cannot_evict_an_active_proxy`; `health::tests::active_demotes_only_after_residence_and_two_independent_failures`; `health::tests::configured_residence_blocks_normal_active_demotion_until_it_expires` |
| Publication is controlled by time-bounded lease rather than just ACTIVE | `domain::evidence::tests::leases_match_the_publication_policy`; `publisher::tests::empty_first_snapshot_never_replaces_existing_subscription`; `storage::tests::partial_refresh_never_counts_missing_and_leased_active_survives_complete_misses` |
| Retests use bounded full-jitter backoff and dormant recovery | `domain::evidence::tests::jitter_is_never_negative_or_above_its_cap`; `domain::evidence::tests::refused_connection_has_a_stronger_first_backoff_ceiling_than_timeout`; `health::tests::remaining_lifecycle_transitions_preserve_hysteresis_rules` |
| Four scheduler queues retain their shares and do not starve | `scheduler::tests::queue_quota_conserves_capacity`; `scheduler::tests::persistent_quota_debt_prevents_active_starvation`; `scheduler::tests::high_pressure_demotes_heavy_work_with_a_deterministic_tie_breaker`; `scheduler::tests::normal_scheduler_queue_uses_due_index_not_a_table_scan` |
| Tests are cascaded and bounded from preflight through limited download | `probe::tests::socks_http_mocks_cover_success_timeout_refusal_and_tls_failure`; `probe::tests::global_budget_never_returns_a_non_positive_timeout`; `probe::tests::cancelling_a_download_releases_its_connection_without_a_worker_leak` |
| Adaptive concurrency and correlated incidents do not create false demotions | `scheduler::tests::correlated_batch_failure_is_detected_but_single_failure_is_not`; `scheduler::tests::high_pressure_demotes_heavy_work_with_a_deterministic_tie_breaker`; `storage::tests::endpoint_incident_does_not_demote_or_shorten_an_active_lease` |
| Upstream changes require complete generations before retirement and preserve tombstones | `storage::tests::cached_source_members_are_carried_into_a_not_modified_generation`; `storage::tests::complete_upstream_reappearance_revives_a_retired_identity`; `storage::tests::partial_refresh_never_counts_missing_and_leased_active_survives_complete_misses` |

The focused tests and the full workspace suite are required before changing a
rule or marking algorithm parity complete.
