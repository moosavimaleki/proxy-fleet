# Dependency acceptance record

The Rust rewrite uses only the dependencies listed and accepted in section 7
of `TASKs.md`.  `Cargo.toml` was reviewed on 2026-08-06: no unplanned runtime
dependency was introduced for the scheduler index, observability endpoint, or
service-layer refactor.

If a future dependency is proposed, its change must include all of:

1. the concrete capability missing from the current dependency set;
2. security/licensing and maintenance rationale;
3. a reproducible benchmark or measurement showing why the existing standard
   library/dependency cannot meet the need; and
4. a focused test proving the integration does not change subscription or
   health behaviour.

This is deliberately an acceptance gate, not an invitation to add a general
framework for a small convenience function.
