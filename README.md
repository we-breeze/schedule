# brz-schedule

Provider-independent durable jobs and periodic scheduling for Rust. The core
contains no Redis or MySQL driver. `JobProvider` owns atomic persistence;
`LockProvider` independently supplies token-owned mutual exclusion. Delivery is
at least once: handlers must make external effects idempotent.

```rust,no_run
use std::sync::Arc;
use brz_schedule::{memory::MemoryProvider, Completion, Job, Registry, Worker, WorkerOptions, JobProvider};
use serde_json::json;

# async fn example() -> brz_schedule::Result<()> {
let provider = Arc::new(MemoryProvider::new(100)?);
let mut registry = Registry::default();
registry.register("email", |context, payload| async move {
    if context.cancellation.is_cancelled() {
        return Ok(Completion::ContinueAt { run_at_ms: brz_schedule::now_ms(), payload: None });
    }
    Ok(Completion::Success(json!({"recipient": payload["to"]})))
})?;
provider.submit(Job::new("email-42", "email", json!({"to":"person@example.com"}))?).await?;
Worker::new(provider, registry, WorkerOptions::default())?.run_once().await?;
# Ok(()) }
```

`register_typed` accepts serde inputs. Persistent envelopes retain a stable job
ID, kind, JSON payload, due time, failure budget and optional overlap scope.
An optional `expires_at_ms` is a start deadline: queued jobs are cancelled on
claim, including future-due jobs, and the worker rechecks before dispatch.
Already-started handlers continue under their timeout and lease. A continuation
or failed retry after the deadline becomes cancelled on its next claim.
`ContinueAt` is a successful continuation and preserves the failure budget.
`Failure` consumes that budget and optionally retries at a durable timestamp.
Expired claims are recovered by the next claim call. Renew and finish reject
expired or replaced tokens. Terminal status never becomes queued implicitly.

## Periodic schedules

`Schedule::new` accepts `Once`, `FixedRate`, `FixedDelay`, or `Cron`. Fixed rate
uses the occurrence time; fixed delay uses the previous terminal completion
time and waits across continuations/retries. Initial timestamps are registration
hints: changing only `first_run_at_ms` or the template job's due time preserves
the existing cursor across replicas/restarts. Changing the trigger, payload or
policies intentionally resets its cursor. Fixed-delay schedules do not replay
missed intervals; one occurrence becomes due after its predecessor completes.

Cron expressions are evaluated by `croner` in an explicit IANA timezone with
explicit DOM/DOW `Or`/`And`. Croner fixed-time DST-gap occurrences run at the first
real instant after the gap. UTC occurrence timestamps are durable identities.
`FireOnce` coalesces missed ticks; `Skip` discards ticks older than its grace;
`CatchUp` replays a bounded number of oldest ticks. `OverlapPolicy::Skip` refuses
to enqueue a second nonterminal occurrence. Cursor advancement and occurrence
enqueue are atomic, so full queues leave the cursor available for later recovery.
`Schedule.start_deadline_after_ms` gives every occurrence a deadline relative
to its scheduled time; an absolute template deadline remains an upper bound.

`SchedulerOptions.allowed_kinds` filters persisted schedules before pagination.
`None` permits every kind; an explicit empty set disables all scheduling. Worker
claims independently filter to its registered kinds. Removing a local handler
therefore does not consume its persisted jobs.

## Runtime and persistence

The runtime uses Tokio's bounded task sets, timers and cooperative signals.
Apalis was evaluated, but mapping its persistence state, cancellation and
acknowledgement layers would duplicate these provider contracts. Cron parsing
and calendar calculation reuse croner rather than a custom parser.

`run_until` stops claiming on shutdown, signals cooperative cancellation, and
drains up to `shutdown_grace`. Remaining handler futures are aborted, without
acknowledging their jobs; leases recover them. Provider failures stop the worker
and propagate to its owner. A bounded claim batch prevents unbounded in-flight
work. In-memory providers are explicit single-process tools, never an automatic
fallback for unavailable durable storage.
`HandlerOptions.timeout` defaults to `Some(300 seconds)`; `None` keeps lease
renewal and cancellation active without imposing an overall handler deadline.

See [`brz-schedule-redis`](providers/redis/README.md) for durable providers. Each
provider must satisfy the shared `test-support` conformance suite, ownership
fencing, atomic schedule advancement and capacity contracts.

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --features test-support
BRZ_SCHEDULE_TEST_REDIS=127.0.0.1:6379:0 cargo test -p brz-schedule-redis --features integration-tests
```

Live Redis integration tests are opt-in locally and are mandatory in GitHub CI.
Release workflows are prepared for explicit manual dispatch; they publish core
before the Redis adapter and verify matching artifacts when resuming a tag.
