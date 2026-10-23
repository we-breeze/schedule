use brz_schedule::{memory::MemoryProvider, *};
use chrono::{TimeZone, Utc};
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

fn time(text: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(text)
        .unwrap()
        .timestamp_millis()
}

#[tokio::test]
async fn each_occurrence_receives_its_own_start_deadline() {
    let provider = Arc::new(MemoryProvider::with_clock(16, Arc::new(|| 1000)).unwrap());
    let scheduler = Scheduler::new(provider.clone(), SchedulerOptions::default()).unwrap();
    let mut schedule = Schedule::new(
        "deadline",
        Trigger::FixedRate { interval_ms: 1000 },
        Job::new("template", "kind", ()).unwrap(),
        0,
    )
    .unwrap();
    schedule.start_deadline_after_ms = Some(400);
    scheduler.register(schedule).await.unwrap();
    scheduler.run_once_at(0).await.unwrap();
    assert_eq!(
        provider
            .get("schedule:deadline:0")
            .await
            .unwrap()
            .unwrap()
            .job
            .expires_at_ms,
        Some(400)
    );
    assert!(
        provider
            .claim("cleanup", &["kind".into()], 1, 5000)
            .await
            .unwrap()
            .is_empty()
    );
    scheduler.run_once_at(1000).await.unwrap();
    let claim = provider
        .claim("worker", &["kind".into()], 1, 5000)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(claim.record.job.expires_at_ms, Some(1400));
}

#[test]
fn cron_honors_iana_timezone_and_explicit_dom_dow_policy() {
    let cron = Trigger::Cron {
        expression: "0 9 * * *".into(),
        timezone: "Asia/Shanghai".into(),
        day_match: DayMatch::Or,
    };
    assert_eq!(
        cron.next_after(time("2026-10-23T00:00:00Z")).unwrap(),
        Some(time("2026-10-23T01:00:00Z"))
    );
    let start = time("2026-10-02T00:00:00Z");
    let make = |day_match| Trigger::Cron {
        expression: "0 9 1 * MON".into(),
        timezone: "UTC".into(),
        day_match,
    };
    assert_eq!(
        make(DayMatch::Or).next_after(start).unwrap(),
        Some(time("2026-10-05T09:00:00Z"))
    );
    assert_eq!(
        make(DayMatch::And).next_after(start).unwrap(),
        Some(time("2027-02-01T09:00:00Z"))
    );
    let dst = Trigger::Cron {
        expression: "30 2 * * *".into(),
        timezone: "America/New_York".into(),
        day_match: DayMatch::Or,
    };
    let next = dst
        .next_after(time("2026-03-08T06:00:00Z"))
        .unwrap()
        .unwrap();
    // Croner runs a fixed-time occurrence inside a DST gap at the first real
    // instant after the gap, rather than silently discarding that occurrence.
    assert_eq!(
        Utc.timestamp_millis_opt(next).unwrap(),
        Utc.timestamp_millis_opt(time("2026-03-08T07:00:00Z"))
            .unwrap()
    );
}

#[tokio::test]
async fn missed_occurrences_are_bounded_and_duplicate_schedulers_use_cas() {
    let clock = Arc::new(AtomicI64::new(10_000));
    let read = clock.clone();
    let provider = Arc::new(
        MemoryProvider::with_clock(32, Arc::new(move || read.load(Ordering::SeqCst))).unwrap(),
    );
    let scheduler = Scheduler::new(provider.clone(), SchedulerOptions::default()).unwrap();
    let mut schedule = Schedule::new(
        "catch",
        Trigger::FixedRate { interval_ms: 1000 },
        Job::new("template", "kind", ()).unwrap(),
        1000,
    )
    .unwrap();
    schedule.overlap = OverlapPolicy::Allow;
    schedule.misfire = MisfirePolicy::CatchUp { max_per_tick: 3 };
    scheduler.register(schedule.clone()).await.unwrap();
    assert_eq!(scheduler.run_once_at(10_000).await.unwrap(), 3);
    let due = provider.due_schedules(None, 10).await.unwrap().remove(0);
    assert_eq!(due.next_run_at_ms, Some(4000));
    schedule.first_run_at_ms = 999_000;
    schedule.job.run_at_ms = 999_000;
    scheduler.register(schedule).await.unwrap();
    let restarted = provider.due_schedules(None, 10).await.unwrap().remove(0);
    assert_eq!(restarted.revision, due.revision);
    assert_eq!(restarted.next_run_at_ms, due.next_run_at_ms);
    let (a, b) = tokio::join!(
        provider.advance_schedule("catch", due.revision, None, Some(11_000)),
        provider.advance_schedule("catch", due.revision, None, Some(12_000))
    );
    assert_ne!(a.unwrap(), b.unwrap());
    assert!(provider.due_schedules(None, 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn skip_preserves_a_current_tick_and_overlap_skip_keeps_one_nonterminal_job() {
    let provider = Arc::new(MemoryProvider::with_clock(16, Arc::new(|| 20_000)).unwrap());
    let scheduler = Scheduler::new(provider.clone(), SchedulerOptions::default()).unwrap();
    let mut schedule = Schedule::new(
        "skip",
        Trigger::FixedRate { interval_ms: 1000 },
        Job::new("template", "kind", ()).unwrap(),
        1000,
    )
    .unwrap();
    schedule.misfire = MisfirePolicy::Skip;
    schedule.misfire_grace_ms = 100;
    scheduler.register(schedule).await.unwrap();
    // Old missed ticks are ignored while the current occurrence remains due.
    assert_eq!(scheduler.run_once_at(10_000).await.unwrap(), 1);
    assert!(provider.get("schedule:skip:1000").await.unwrap().is_none());
    assert!(provider.get("schedule:skip:10000").await.unwrap().is_some());
    // The first job is still queued, so the next occurrence advances without
    // adding a second job in the same overlap scope.
    assert_eq!(scheduler.run_once_at(11_000).await.unwrap(), 1);
    assert!(provider.get("schedule:skip:11000").await.unwrap().is_none());
    // A missed occurrence beyond grace is discarded and moves the cursor on.
    assert_eq!(scheduler.run_once_at(12_500).await.unwrap(), 0);
    assert!(provider.get("schedule:skip:12000").await.unwrap().is_none());
}

#[tokio::test]
async fn fixed_delay_measures_from_completion_and_filtering_precedes_limit() {
    let clock = Arc::new(AtomicI64::new(1000));
    let read = clock.clone();
    let provider = Arc::new(
        MemoryProvider::with_clock(16, Arc::new(move || read.load(Ordering::SeqCst))).unwrap(),
    );
    let scheduler = Scheduler::new(
        provider.clone(),
        SchedulerOptions {
            batch_size: 1,
            allowed_kinds: Some(BTreeSet::from(["allowed".into()])),
            ..Default::default()
        },
    )
    .unwrap();
    scheduler
        .register(
            Schedule::new(
                "disabled",
                Trigger::Once,
                Job::new("t", "disabled", ()).unwrap(),
                0,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    scheduler
        .register(
            Schedule::new(
                "delay",
                Trigger::FixedDelay { delay_ms: 1000 },
                Job::new("t", "allowed", ()).unwrap(),
                1000,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(scheduler.run_once_at(1000).await.unwrap(), 1);
    let claim = provider
        .claim("w", &["allowed".into()], 1, 10_000)
        .await
        .unwrap()
        .remove(0);
    clock.store(5000, Ordering::SeqCst);
    assert_eq!(scheduler.run_once_at(5000).await.unwrap(), 0);
    provider
        .finish(
            &claim.record.job.id,
            &claim.owner_token,
            Completion::Success(serde_json::Value::Null),
        )
        .await
        .unwrap();
    assert_eq!(scheduler.run_once_at(5000).await.unwrap(), 0);
    clock.store(6000, Ordering::SeqCst);
    assert_eq!(scheduler.run_once_at(6000).await.unwrap(), 1);
    assert!(provider.get("schedule:delay:6000").await.unwrap().is_some());
    let blocked = Scheduler::new(
        provider.clone(),
        SchedulerOptions {
            allowed_kinds: Some(BTreeSet::new()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(blocked.run_once_at(99_000).await.unwrap(), 0);
}
