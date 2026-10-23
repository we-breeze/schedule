use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc, time::Duration};

use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::{sync::watch, task::JoinSet};

use crate::{Claim, Completion, Error, Job, JobProvider, Result, SubmitResult};

type HandlerFuture = Pin<Box<dyn Future<Output = Result<Completion>> + Send>>;
type Handler = Arc<dyn Fn(JobContext, Value) -> HandlerFuture + Send + Sync>;

/// Cooperative cancellation delivered on shutdown, timeout or ownership loss.
#[derive(Clone, Debug)]
pub struct Cancellation(watch::Receiver<bool>);

impl Cancellation {
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }
    pub async fn cancelled(&self) {
        let mut receiver = self.0.clone();
        let _ = receiver.wait_for(|cancelled| *cancelled).await;
    }
}

#[derive(Clone, Debug)]
pub struct JobContext {
    pub job_id: String,
    pub failures: u32,
    pub deliveries: u32,
    pub cancellation: Cancellation,
}

/// Runtime-only handler configuration; no persistence or driver types leak out.
#[derive(Clone, Debug)]
pub struct HandlerOptions {
    /// None removes the overall wall-clock deadline; cancellation and lease
    /// renewal remain active. Bound individual external operations separately.
    pub timeout: Option<Duration>,
    pub retry_delay: Duration,
    pub max_retry_delay: Duration,
}

impl Default for HandlerOptions {
    fn default() -> Self {
        Self {
            timeout: Some(Duration::from_secs(300)),
            retry_delay: Duration::from_secs(5),
            max_retry_delay: Duration::from_secs(300),
        }
    }
}

#[derive(Clone, Default)]
pub struct Registry {
    handlers: BTreeMap<String, (Handler, HandlerOptions)>,
}

impl Registry {
    pub fn register<F, Fut>(&mut self, kind: impl Into<String>, handler: F) -> Result<()>
    where
        F: Fn(JobContext, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Completion>> + Send + 'static,
    {
        self.register_with(kind, HandlerOptions::default(), handler)
    }

    pub fn register_with<F, Fut>(
        &mut self,
        kind: impl Into<String>,
        options: HandlerOptions,
        handler: F,
    ) -> Result<()>
    where
        F: Fn(JobContext, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Completion>> + Send + 'static,
    {
        let kind = kind.into();
        if kind.is_empty()
            || self.handlers.contains_key(&kind)
            || options.timeout.is_some_and(|timeout| timeout.is_zero())
            || options.retry_delay.is_zero()
            || options.max_retry_delay < options.retry_delay
        {
            return Err(Error::Invalid(
                "duplicate/empty handler or invalid handler timing".into(),
            ));
        }
        self.handlers.insert(
            kind,
            (
                Arc::new(move |ctx, input| Box::pin(handler(ctx, input))),
                options,
            ),
        );
        Ok(())
    }

    pub fn register_typed<T, F, Fut>(&mut self, kind: impl Into<String>, handler: F) -> Result<()>
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(JobContext, T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Completion>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.register(kind, move |ctx, input| {
            let handler = handler.clone();
            async move { handler(ctx, serde_json::from_value(input)?).await }
        })
    }
}

#[derive(Clone)]
pub struct TaskClient {
    provider: Arc<dyn JobProvider>,
}

impl TaskClient {
    pub fn new(provider: Arc<dyn JobProvider>) -> Self {
        Self { provider }
    }
    pub async fn submit(&self, job: Job) -> Result<SubmitResult> {
        self.provider.submit(job).await
    }
}

#[derive(Clone, Debug)]
pub struct WorkerOptions {
    pub name: String,
    pub concurrency: usize,
    pub lease: Duration,
    pub poll_interval: Duration,
    pub shutdown_grace: Duration,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            name: "worker".into(),
            concurrency: 8,
            lease: Duration::from_secs(30),
            poll_interval: Duration::from_millis(250),
            shutdown_grace: Duration::from_secs(30),
        }
    }
}

/// A bounded Tokio worker; all jobs remain durable until owned completion.
pub struct Worker {
    provider: Arc<dyn JobProvider>,
    registry: Registry,
    options: WorkerOptions,
}

impl Worker {
    pub fn new(
        provider: Arc<dyn JobProvider>,
        registry: Registry,
        options: WorkerOptions,
    ) -> Result<Self> {
        if options.name.is_empty()
            || options.concurrency == 0
            || options.lease < Duration::from_millis(30)
            || options.lease.as_millis() > i64::MAX as u128
            || options.poll_interval.is_zero()
            || options.shutdown_grace.is_zero()
            || registry.handlers.is_empty()
        {
            return Err(Error::Invalid(
                "worker requires handlers, capacity and positive timing".into(),
            ));
        }
        Ok(Self {
            provider,
            registry,
            options,
        })
    }

    async fn fill(
        &self,
        tasks: &mut JoinSet<Result<()>>,
        stop: watch::Receiver<bool>,
    ) -> Result<usize> {
        let free = self.options.concurrency.saturating_sub(tasks.len());
        if free == 0 {
            return Ok(0);
        }
        let kinds = self.registry.handlers.keys().cloned().collect::<Vec<_>>();
        let claims = self
            .provider
            .claim(
                &self.options.name,
                &kinds,
                free,
                self.options.lease.as_millis() as u64,
            )
            .await?;
        let count = claims.len();
        for claim in claims {
            let (handler, options) = self
                .registry
                .handlers
                .get(&claim.record.job.kind)
                .ok_or_else(|| Error::UnknownHandler(claim.record.job.kind.clone()))?
                .clone();
            tasks.spawn(execute(
                self.provider.clone(),
                claim,
                handler,
                options,
                self.options.lease,
                stop.clone(),
            ));
        }
        Ok(count)
    }

    /// Execute one bounded batch. Useful for explicit manual verification.
    pub async fn run_once(&self) -> Result<usize> {
        let (_stop, receiver) = watch::channel(false);
        let mut tasks = JoinSet::new();
        let count = self.fill(&mut tasks, receiver).await?;
        while let Some(result) = tasks.join_next().await {
            join_result(result)?;
        }
        Ok(count)
    }

    pub async fn run_until<F: Future<Output = ()> + Send>(&self, shutdown: F) -> Result<()> {
        let (stop, receiver) = watch::channel(false);
        let mut tasks = JoinSet::new();
        tokio::pin!(shutdown);
        let result = loop {
            tokio::select! {
                biased;
                () = &mut shutdown => break Ok(()),
                result = self.fill(&mut tasks, receiver.clone()) => {
                    if let Err(error) = result { break Err(error); }
                }
            }
            tokio::select! {
                biased;
                () = &mut shutdown => break Ok(()),
                result = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(result) = result && let Err(error) = join_result(result) { break Err(error); }
                }
                () = tokio::time::sleep(self.options.poll_interval) => {}
            }
        };
        // Stop claiming immediately; cooperative handlers can finish or yield.
        let _ = stop.send(true);
        let drain = async {
            while let Some(joined) = tasks.join_next().await {
                join_result(joined)?;
            }
            Ok(())
        };
        let drained = tokio::time::timeout(self.options.shutdown_grace, drain).await;
        match drained {
            Ok(drain_result) => {
                result?;
                drain_result
            }
            Err(_) => {
                tasks.abort_all();
                // Unacknowledged jobs retain leases and are recovered by claim.
                while tasks.join_next().await.is_some() {}
                result
            }
        }
    }
}

fn join_result(result: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    result.map_err(|error| Error::Worker(error.to_string()))?
}

#[tracing::instrument(name = "schedule.execute", skip_all,
    fields(job_id = %claim.record.job.id, job_kind = %claim.record.job.kind,
        delivery = claim.record.deliveries))]
async fn execute(
    provider: Arc<dyn JobProvider>,
    claim: Claim,
    handler: Handler,
    options: HandlerOptions,
    lease: Duration,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    if claim
        .record
        .job
        .expires_at_ms
        .is_some_and(|at| at <= crate::now_ms())
    {
        return if provider
            .finish(
                &claim.record.job.id,
                &claim.owner_token,
                Completion::Cancelled,
            )
            .await?
        {
            Ok(())
        } else {
            Err(Error::OwnershipLost)
        };
    }
    let (cancel, cancellation) = watch::channel(*stop.borrow());
    let context = JobContext {
        job_id: claim.record.job.id.clone(),
        failures: claim.record.failures,
        deliveries: claim.record.deliveries,
        cancellation: Cancellation(cancellation),
    };
    let future = handler(context, claim.record.job.payload.clone());
    let mut running = AbortOnDrop(tokio::spawn(future));
    let timeout = async {
        match options.timeout {
            Some(timeout) => tokio::time::sleep(timeout).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(timeout);
    let mut renew = tokio::time::interval(lease / 3);
    renew.tick().await;
    let completion = loop {
        tokio::select! {
            result = &mut running.0 => break match result {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(error)) => failure(&claim, error.to_string(), &options),
                Err(error) => failure(&claim, format!("handler panicked: {error}"), &options),
            },
            () = &mut timeout => {
                let _ = cancel.send(true);
                running.0.abort();
                let _ = (&mut running.0).await;
                break failure(&claim, "handler timed out".into(), &options);
            }
            _ = renew.tick() => {
                match provider.renew(&claim.record.job.id, &claim.owner_token, lease.as_millis() as u64).await {
                    Ok(true) => {}
                    result => {
                        let _ = cancel.send(true);
                        running.0.abort();
                        let _ = (&mut running.0).await;
                        return match result { Ok(false) => Err(Error::OwnershipLost), Err(error) => Err(error), Ok(true) => unreachable!() };
                    }
                }
            }
            result = stop.changed(), if !*stop.borrow() => {
                if result.is_ok() && *stop.borrow() { let _ = cancel.send(true); }
            }
        }
    };
    if provider
        .finish(&claim.record.job.id, &claim.owner_token, completion)
        .await?
    {
        Ok(())
    } else {
        Err(Error::OwnershipLost)
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<Result<Completion>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn failure(claim: &Claim, error: String, options: &HandlerOptions) -> Completion {
    let delay = options
        .retry_delay
        .saturating_mul(2_u32.saturating_pow(claim.record.failures))
        .min(options.max_retry_delay)
        .as_millis()
        .min(i64::MAX as u128) as i64;
    Completion::Failure {
        error,
        retry_at_ms: Some(crate::now_ms().saturating_add(delay)),
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;
    use crate::{Job, JobProvider, memory::MemoryProvider};

    #[tokio::test]
    async fn a_claimed_handler_sees_shutdown_that_arrived_before_its_first_poll() {
        let provider = Arc::new(MemoryProvider::new(2).unwrap());
        provider
            .submit(Job::new("claim", "kind", ()).unwrap())
            .await
            .unwrap();
        let claim = provider
            .claim("worker", &["kind".into()], 1, 5000)
            .await
            .unwrap()
            .remove(0);
        let mut registry = Registry::default();
        registry
            .register("kind", |context, _| async move {
                assert!(context.cancellation.is_cancelled());
                Ok(Completion::ContinueAt {
                    run_at_ms: crate::now_ms() + 1000,
                    payload: None,
                })
            })
            .unwrap();
        let (handler, options) = registry.handlers.remove("kind").unwrap();
        let (_sender, receiver) = watch::channel(true);
        execute(
            provider.clone(),
            claim,
            handler,
            options,
            Duration::from_secs(5),
            receiver,
        )
        .await
        .unwrap();
        assert_eq!(
            provider.get("claim").await.unwrap().unwrap().status,
            crate::JobStatus::Queued
        );
    }
}
