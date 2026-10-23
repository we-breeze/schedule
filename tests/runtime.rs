use brz_schedule::{memory::MemoryProvider, *};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::oneshot;

#[derive(Serialize, Deserialize)]
struct Input {
    value: u32,
}

#[tokio::test]
async fn deadline_is_rechecked_before_handler_dispatch() {
    // Simulate time advancing between provider claim and worker dispatch.
    let provider = Arc::new(MemoryProvider::with_clock(4, Arc::new(|| 0)).unwrap());
    let mut job = Job::new("expired-dispatch", "kind", ()).unwrap();
    job.run_at_ms = 0;
    job.expires_at_ms = Some(1);
    provider.submit(job).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let mut registry = Registry::default();
    registry
        .register("kind", move |_, _| {
            called.fetch_add(1, Ordering::SeqCst);
            async { Ok(Completion::Success(json!(true))) }
        })
        .unwrap();
    Worker::new(provider.clone(), registry, WorkerOptions::default())
        .unwrap()
        .run_once()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        provider
            .get("expired-dispatch")
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Cancelled
    );
}

#[tokio::test]
async fn typed_handlers_are_bounded_and_unregistered_work_is_retained() {
    let provider = Arc::new(MemoryProvider::new(16).unwrap());
    for i in 0..5 {
        provider
            .submit(Job::new(i.to_string(), "typed", Input { value: i }).unwrap())
            .await
            .unwrap();
    }
    provider
        .submit(Job::new("untouched", "not-registered", ()).unwrap())
        .await
        .unwrap();
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry
        .register_typed("typed", {
            let running = running.clone();
            let peak = peak.clone();
            move |_, input: Input| {
                let running = running.clone();
                let peak = peak.clone();
                async move {
                    let active = running.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(active, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    running.fetch_sub(1, Ordering::SeqCst);
                    Ok(Completion::Success(json!(input.value * 2)))
                }
            }
        })
        .unwrap();
    let worker = Worker::new(
        provider.clone(),
        registry,
        WorkerOptions {
            concurrency: 2,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(worker.run_once().await.unwrap(), 2);
    assert_eq!(worker.run_once().await.unwrap(), 2);
    assert_eq!(worker.run_once().await.unwrap(), 1);
    assert!(peak.load(Ordering::SeqCst) <= 2);
    assert_eq!(
        provider.get("4").await.unwrap().unwrap().result,
        Some(json!(8))
    );
    assert_eq!(
        provider.get("untouched").await.unwrap().unwrap().status,
        JobStatus::Queued
    );
}

#[tokio::test(start_paused = true)]
async fn long_handlers_renew_claims_until_owned_completion() {
    let base = now_ms();
    let start = tokio::time::Instant::now();
    let provider = Arc::new(
        MemoryProvider::with_clock(
            4,
            Arc::new(move || base + start.elapsed().as_millis() as i64),
        )
        .unwrap(),
    );
    let mut job = Job::new("long", "long", ()).unwrap();
    job.run_at_ms = base;
    provider.submit(job).await.unwrap();
    let mut registry = Registry::default();
    registry
        .register("long", |_, _| async {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok(Completion::Success(json!(true)))
        })
        .unwrap();
    let worker = Worker::new(
        provider.clone(),
        registry,
        WorkerOptions {
            lease: Duration::from_millis(90),
            ..Default::default()
        },
    )
    .unwrap();
    worker.run_once().await.unwrap();
    let record = provider.get("long").await.unwrap().unwrap();
    assert_eq!(record.status, JobStatus::Succeeded);
    assert_eq!(record.deliveries, 1);
}

#[tokio::test(start_paused = true)]
async fn handlers_can_disable_the_overall_deadline_without_disabling_lease_renewal() {
    let base = now_ms();
    let start = tokio::time::Instant::now();
    let provider = Arc::new(
        MemoryProvider::with_clock(
            4,
            Arc::new(move || base + start.elapsed().as_millis() as i64),
        )
        .unwrap(),
    );
    let mut job = Job::new("no-deadline", "kind", ()).unwrap();
    job.run_at_ms = base;
    provider.submit(job).await.unwrap();
    let mut registry = Registry::default();
    registry
        .register_with(
            "kind",
            HandlerOptions {
                timeout: None,
                ..Default::default()
            },
            |_, _| async {
                tokio::time::sleep(Duration::from_secs(600)).await;
                Ok(Completion::Success(json!(true)))
            },
        )
        .unwrap();
    Worker::new(
        provider.clone(),
        registry,
        WorkerOptions {
            lease: Duration::from_secs(30),
            ..Default::default()
        },
    )
    .unwrap()
    .run_once()
    .await
    .unwrap();
    assert_eq!(
        provider.get("no-deadline").await.unwrap().unwrap().status,
        JobStatus::Succeeded
    );
}

struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn forced_shutdown_aborts_handler_and_leaves_recoverable_unacked_claim() {
    let base = now_ms();
    let start = tokio::time::Instant::now();
    let provider = Arc::new(
        MemoryProvider::with_clock(
            4,
            Arc::new(move || base + start.elapsed().as_millis() as i64),
        )
        .unwrap(),
    );
    let mut job = Job::new("shutdown", "slow", ()).unwrap();
    job.run_at_ms = base;
    provider.submit(job).await.unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = oneshot::channel();
    let started = Arc::new(Mutex::new(Some(started_tx)));
    let mut registry = Registry::default();
    registry
        .register("slow", {
            let dropped = dropped.clone();
            move |_, _| {
                let dropped = dropped.clone();
                let started = started.clone();
                async move {
                    let _guard = Dropped(dropped);
                    if let Some(sender) = started.lock().unwrap().take() {
                        let _ = sender.send(());
                    }
                    std::future::pending::<()>().await;
                    Ok(Completion::Success(json!(null)))
                }
            }
        })
        .unwrap();
    let worker = Worker::new(
        provider.clone(),
        registry,
        WorkerOptions {
            lease: Duration::from_millis(300),
            shutdown_grace: Duration::from_millis(50),
            ..Default::default()
        },
    )
    .unwrap();
    let (stop_tx, stop_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        worker
            .run_until(async {
                let _ = stop_rx.await;
            })
            .await
    });
    started_rx.await.unwrap();
    stop_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
    // Child handlers must not outlive the worker that owns their lease.
    tokio::task::yield_now().await;
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        provider.get("shutdown").await.unwrap().unwrap().status,
        JobStatus::Running
    );
    tokio::time::advance(Duration::from_millis(400)).await;
    assert_eq!(
        provider
            .claim("recovery", &["slow".into()], 1, 5000)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn panic_is_a_failed_attempt_and_registry_rejects_duplicate_names() {
    let provider = Arc::new(MemoryProvider::new(4).unwrap());
    provider
        .submit(Job::new("panic", "panic", ()).unwrap())
        .await
        .unwrap();
    let mut registry = Registry::default();
    registry
        .register("panic", |_, _| async {
            panic!("intentional handler panic");
            #[allow(unreachable_code)]
            Ok(Completion::Cancelled)
        })
        .unwrap();
    assert!(
        registry
            .register("panic", |_, _| async { Ok(Completion::Cancelled) })
            .is_err()
    );
    Worker::new(provider.clone(), registry, WorkerOptions::default())
        .unwrap()
        .run_once()
        .await
        .unwrap();
    let record = provider.get("panic").await.unwrap().unwrap();
    assert_eq!(record.failures, 1);
    assert_eq!(record.status, JobStatus::Queued);
    assert!(
        record
            .last_error
            .unwrap()
            .contains("intentional handler panic")
    );
}
