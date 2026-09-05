# Rate Limiter

Production-ready, configurable rate limiter. Usable as a Go library, as HTTP
middleware for an API gateway, or as a standalone service. Built distributed-first.

## Objectives
1. Correct under concurrency across multiple nodes. Races, clock skew and store
   round-trips are the subject, not an edge case.
2. Five algorithms, each independently testable: token bucket, leaky bucket,
   fixed window counter, sliding window log, sliding window counter.
3. Rules resolved at runtime from a RuleSource port, hot-reloadable, matched on
   extracted keys with explicit precedence. Each rule names its algorithm and params.
4. One core, two driving adapters: embedded library and standalone service.
   No logic lives in only one of them.
5. Observable from the first commit. Traces, metrics and logs are part of the
   definition of done for every increment, not a later pass.
6. Every tradeoff claim backed by a runnable benchmark or a test that fails
   without the property.

## Architecture (hexagonal)
- Domain: `Rule`, `Key`, `Decision` (allowed, remaining, retryAfter, resetAt),
  and one `Limiter` per algorithm. Pure, no I/O, no clock reads.
- Driven ports: `Store` (counter state), `RuleSource`, `Clock`, `Observer`.
- Driven adapters: redis + memory, file + remote, real + fake, otel + noop.
- Driving adapters: HTTP middleware, service handler, RPC client.
  The middleware holds a `Limiter`; embedded and remote are the same interface.
- Dependencies point inward. Adapters are swappable in tests without mocks of
  the domain.

## Observability
- OpenTelemetry only, behind the `Observer` port. The domain emits through the
  port; it never imports an SDK. `noop` is the default so the library adds no
  exporter to a consumer that did not ask for one.
- Traces: one span per decision, with the algorithm, rule id and outcome as
  attributes. Store round-trips are child spans, so network cost is visible
  separately from algorithm cost.
- Metrics: decisions by outcome, decision latency, store latency and errors,
  rule-reload success and age. Latencies as histograms with exemplars linking
  to traces.
- Logs: structured, correlated by trace id. Errors and rule reloads only;
  a decision is a metric, never a log line.
- Cardinality is a hard constraint. The rate limit key (IP, user, API key) is
  unbounded and MUST NOT appear as a metric label. Labels carry the rule id and
  algorithm, which are bounded by config.
- Semantic conventions where they exist; project-specific names only where
  they do not, declared in one place.
- Instrumentation is asserted in tests: an increment that emits nothing it
  claimed to emit is not done.

## Evidence
- Each algorithm ships a benchmark and a test demonstrating its weakness
  (fixed-window boundary spike, sliding-log memory growth, token-bucket burst).
- `docs/benchmarks.md` holds evidence tables produced by real runs, each with
  the command that reproduces it. Nothing asserted that is not measured.
- Benchmarks report the observability overhead separately, so the cost of
  instrumentation is a measured number rather than an assumption.
- `docs/decisions/` holds a short ADR only where the alternative was genuinely
  viable (store atomicity strategy, key layout, rule precedence).

## Out of scope (v1)
Quota billing, auth, per-tenant admin UI, non-HTTP transports beyond gRPC.

## Working rules
- Small increments. Each merges green and standalone.
- Tests prove behaviour, not coverage. Delete tests that stop discriminating.
- Comments, docs and PRs: describe current behaviour, as short as accuracy
  allows. Link a known source rather than restating it. No history, no rationale
  narratives, no stale numbers.
- Find the existing pattern before inventing one.

Concrete shapes behind this charter live in `docs/superpowers/specs/`.
