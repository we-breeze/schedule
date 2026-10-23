# brz-schedule-redis

Independent Redis `JobProvider` and `LockProvider` adapters for `brz-schedule`.
Uses `brz-redis = 0.0.16` and a single logical writer shard. Scheduling core
depends on neither Redis nor this crate.

```rust,no_run
use brz_schedule_redis::{RedisConfig, RedisProvider, LockKeyPolicy};
# async fn example() -> brz_schedule::Result<()> {
let provider = RedisProvider::connect(RedisConfig {
    endpoint: "127.0.0.1:6379:0".into(),
    namespace: "my-service:schedule:v1".into(),
    ..Default::default()
}).await?;
let locks = provider.locks(LockKeyPolicy::Raw);
# let _ = locks;
# Ok(()) }
```

`RedisLockProvider::new` also accepts an application-owned `RedisService`.
Choose `Raw` for complete application keys or `Prefix` for an explicit prefix.
The combined `RedisProvider` implements locks with a namespaced `:lock:` prefix;
it must not be used for existing full-key protocols without choosing `Raw`.
Password Debug output is redacted. Callers supply configuration; no environment
variables, service discovery rules or product resource names are embedded.

All claims, lease renewal, completion, overlap checks, capacity checks and
schedule cursor advancement use atomic Lua. Redis TIME provides lease authority.
Unknown transport outcomes remain recoverable; clients may safely retry submit
with the same ID. User JSON is stored as opaque text to preserve empty arrays,
objects, strings and nested data across Lua cjson boundaries. No persistent
record is removed by a claim. Server failures are errors, never queue misses.

`capacity` bounds queued and running jobs. `retained_terminal` bounds completed
records and the duplicate-ID horizon; current periodic predecessors remain
pinned until replaced. Schedule definitions are bounded by capacity. Backend
data keys and format belong to this adapter, not any external queue protocol.

Redis durability also depends on the server's persistence and replication
configuration. Lua atomicity alone does not survive Redis data loss. The adapter
does not claim exactly-once delivery or transactional external effects.
