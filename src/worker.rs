//! Async worker helper.
//!
//! A [`Worker`] polls one or more queues, dispatches each message to a per-queue handler,
//! runs the handler with automatic visibility-heartbeat (so a slow handler can outlive the
//! queue's `vt` without redelivery), and optionally subscribes to rbmq realtime PUBLISHes
//! to wake up immediately instead of polling.
//!
//! Requires the `worker` feature (default-on, tokio-only for now).
//!
//! ```no_run
//! # use rbmq::{RbmqMessage, RbmqOptions, Worker};
//! # use std::convert::Infallible;
//! # async fn _example() -> Result<(), Box<dyn std::error::Error>> {
//! let worker = Worker::builder(RbmqOptions::default())
//!     .route("emails", |msg: RbmqMessage<String>| async move {
//!         println!("got email job: {}", msg.message);
//!         Ok::<(), Infallible>(())
//!     })
//!     .route("billing", |msg: RbmqMessage<Vec<u8>>| async move {
//!         println!("got billing job: {} bytes", msg.message.len());
//!         Ok::<(), Infallible>(())
//!     })
//!     .build()
//!     .await?;
//!
//! worker.run().await?;
//! # Ok(()) }
//! ```

use crate::types::RedisBytes;
use crate::{Rbmq, RbmqError, RbmqMessage, RbmqOptions, RbmqResult};
use core::convert::TryFrom;
use futures_util::StreamExt;
use log::{error, warn};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

type HandlerError = Box<dyn StdError + Send + Sync>;
type HandlerResult = Result<(), HandlerError>;
type HandlerFuture = Pin<Box<dyn Future<Output = HandlerResult> + Send>>;
type ErasedHandler = Box<dyn Fn(RbmqMessage<Vec<u8>>) -> HandlerFuture + Send + Sync>;

/// Error wrapping a message-decoding failure; used so handlers can surface a typed error
/// instead of a panic when `TryFrom<RedisBytes>` rejects the bytes.
#[derive(Debug)]
pub struct DecodeError(pub Vec<u8>);

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "could not decode message bytes into the handler's expected type ({} bytes)",
            self.0.len()
        )
    }
}

impl StdError for DecodeError {}

/// Dead-letter queue configuration. Messages whose `rc` (receive count) exceeds
/// `max_failures` after a handler error are atomically moved to `queue` instead of
/// being left for redelivery. `max_failures = 0` means: route on the first failure.
#[derive(Debug, Clone)]
struct DlqConfig {
    queue: String,
    max_failures: u64,
}

/// Builds a [`Worker`].
pub struct WorkerBuilder {
    client: RbmqResult<redis::Client>,
    namespace: String,
    routes: HashMap<String, ErasedHandler>,
    poll_interval: Duration,
    heartbeat_interval: Duration,
    visibility_extension: Duration,
    use_realtime: bool,
    global_dlq: Option<DlqConfig>,
    route_dlqs: HashMap<String, DlqConfig>,
}

impl WorkerBuilder {
    fn new(client: RbmqResult<redis::Client>, namespace: String) -> Self {
        Self {
            client,
            namespace,
            routes: HashMap::new(),
            poll_interval: Duration::from_secs(1),
            heartbeat_interval: Duration::from_secs(10),
            visibility_extension: Duration::from_secs(30),
            use_realtime: false,
            global_dlq: None,
            route_dlqs: HashMap::new(),
        }
    }

    /// Register a handler for messages on `qname`. The message is decoded into `T` via
    /// `TryFrom<RedisBytes>`. If decoding fails, the handler is *not* called and the message
    /// is left in the queue (will be redelivered after `vt`).
    ///
    /// Adding the same queue twice replaces the previous handler.
    pub fn route<F, Fut, T, E>(mut self, qname: impl Into<String>, handler: F) -> Self
    where
        F: Fn(RbmqMessage<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        T: TryFrom<RedisBytes, Error = Vec<u8>> + Send + 'static,
        E: StdError + Send + Sync + 'static,
    {
        let erased: ErasedHandler = Box::new(move |raw: RbmqMessage<Vec<u8>>| {
            let message = match T::try_from(RedisBytes::from(raw.message)) {
                Ok(message) => message,
                Err(bytes) => {
                    return Box::pin(std::future::ready(Err(
                        Box::new(DecodeError(bytes)) as HandlerError
                    )));
                }
            };
            let future = handler(RbmqMessage {
                id: raw.id,
                message,
                rc: raw.rc,
                fr: raw.fr,
                sent: raw.sent,
            });
            Box::pin(async move {
                future
                    .await
                    .map_err(|error| Box::new(error) as HandlerError)
            })
        });
        self.routes.insert(qname.into(), erased);
        self
    }

    /// Set the queue namespace. The default for a URL-configured worker is `rbmq`.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// How long to sleep between empty polling rounds. Default: 1s.
    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    /// How often to extend the in-flight message's visibility. Default: 10s. Should be smaller
    /// than [`Self::visibility_extension`] so each tick comfortably renews the visibility window.
    pub fn heartbeat_interval(mut self, d: Duration) -> Self {
        self.heartbeat_interval = d;
        self
    }

    /// Minimum visibility when a job is received and the window renewed by each heartbeat.
    ///
    /// Defaults to 30s. Handler failures retry using the queue's configured visibility delay.
    pub fn visibility_extension(mut self, d: Duration) -> Self {
        self.visibility_extension = d;
        self
    }

    /// Configure a global dead-letter queue used by every route that doesn't have its own
    /// override (see [`Self::dlq_for`]). After a handler returns `Err`, if the message's
    /// receive count `rc` exceeds `max_failures`, the worker atomically moves it from the
    /// source queue to `queue` instead of leaving it for redelivery.
    ///
    /// `max_failures = 0` ⇒ DLQ on the first failure (no retries).
    /// `max_failures = N` ⇒ DLQ on the (N+1)-th failure.
    ///
    /// The DLQ must already exist (call `create_queue` on it before building the worker).
    pub fn dlq(mut self, queue: impl Into<String>, max_failures: u64) -> Self {
        self.global_dlq = Some(DlqConfig {
            queue: queue.into(),
            max_failures,
        });
        self
    }

    /// Per-route DLQ override. Same semantics as [`Self::dlq`] but applies only to the named
    /// route, taking precedence over the global DLQ.
    pub fn dlq_for(
        mut self,
        route: impl Into<String>,
        queue: impl Into<String>,
        max_failures: u64,
    ) -> Self {
        self.route_dlqs.insert(
            route.into(),
            DlqConfig {
                queue: queue.into(),
                max_failures,
            },
        );
        self
    }

    /// Subscribe to rbmq realtime PUBLISHes so the worker wakes up on new messages instead of
    /// strictly polling. Off by default. Senders must use `RbmqOptions { realtime: true, .. }`
    /// for any realtime channel to fire — when off, this option does nothing harmful, the
    /// worker just always waits the full poll interval between checks.
    pub fn use_realtime(mut self, enabled: bool) -> Self {
        self.use_realtime = enabled;
        self
    }

    /// Build the [`Worker`], opening connections to Redis. Returns an error if any of the
    /// connections fail to open, if heartbeat settings are invalid,
    /// if a route is configured with its own queue as DLQ
    /// (would loop forever), or, when `use_realtime` is on, if subscribing fails.
    pub async fn build(mut self) -> RbmqResult<Worker> {
        if self.routes.is_empty() {
            return Err(RbmqError::NoAttributeSupplied);
        }
        let visibility_ms = crate::functions::duration_milliseconds(self.visibility_extension)?;
        let heartbeat_ms = self.heartbeat_interval.as_millis();
        if heartbeat_ms == 0 || heartbeat_ms >= u128::from(visibility_ms) {
            return Err(RbmqError::InvalidFormat(
                "heartbeat interval must be at least 1ms and shorter than visibility extension"
                    .into(),
            ));
        }
        // Reject self-loops (route X with DLQ X) up front.
        for route in self.routes.keys() {
            let dlq = self
                .route_dlqs
                .get(route)
                .or(self.global_dlq.as_ref())
                .map(|d| d.queue.as_str());
            if dlq == Some(route.as_str()) {
                return Err(RbmqError::InvalidFormat(format!(
                    "DLQ for route {route:?} is the same queue (would loop)"
                )));
            }
        }
        let client = self.client?;
        let rbmq = Rbmq::from_client(&client, false, &self.namespace).await?;
        let pubsub = if self.use_realtime {
            let mut pubsub = client.get_async_pubsub().await?;
            for qname in self.routes.keys() {
                pubsub
                    .subscribe(format!("{}:rt:{qname}", self.namespace))
                    .await?;
            }
            Some(pubsub)
        } else {
            None
        };
        let routes = self
            .routes
            .into_iter()
            .map(|(qname, handler)| {
                let dlq = self
                    .route_dlqs
                    .remove(&qname)
                    .or_else(|| self.global_dlq.clone());
                Route {
                    qname,
                    handler,
                    dlq,
                }
            })
            .collect();
        Ok(Worker {
            rbmq,
            pubsub,
            routes,
            config: WorkerConfig {
                poll_interval: self.poll_interval,
                heartbeat_interval: self.heartbeat_interval,
                visibility_extension: self.visibility_extension,
            },
        })
    }
}

/// Polls registered queues and dispatches to per-queue handlers. Build via [`Worker::builder`].
pub struct Worker {
    rbmq: Rbmq,
    pubsub: Option<redis::aio::PubSub>,
    routes: Vec<Route>,
    config: WorkerConfig,
}

struct Route {
    qname: String,
    handler: ErasedHandler,
    dlq: Option<DlqConfig>,
}

struct WorkerConfig {
    poll_interval: Duration,
    heartbeat_interval: Duration,
    visibility_extension: Duration,
}

impl Worker {
    /// Start a [`WorkerBuilder`].
    pub fn builder(options: RbmqOptions) -> WorkerBuilder {
        let client = options.redis_client().map_err(RbmqError::from);
        WorkerBuilder::new(client, options.ns)
    }

    /// Start a worker from a Redis URL, in the default namespace.
    pub fn from_url(url: &str) -> WorkerBuilder {
        let client = redis::Client::open(url).map_err(RbmqError::from);
        WorkerBuilder::new(client, "rbmq".into())
    }

    /// Run forever. Returns only on a fatal Redis error.
    pub async fn run(self) -> RbmqResult<()> {
        self.run_until(std::future::pending::<()>()).await
    }

    /// Run until `shutdown` resolves. The current in-flight handler (if any) is allowed to
    /// finish before the worker returns; handlers are never cancelled mid-flight. Shutdown is
    /// checked before each polling round. A round handles up to one message per route.
    pub async fn run_until<S>(mut self, shutdown: S) -> RbmqResult<()>
    where
        S: Future<Output = ()> + Send,
    {
        let mut shutdown = Box::pin(shutdown);
        loop {
            // Cheap non-blocking shutdown check between rounds.
            tokio::select! {
                biased;
                _ = &mut shutdown => return Ok(()),
                _ = std::future::ready(()) => {}
            }

            if !self.poll_round().await? {
                let mut wait = Box::pin(self.wait_for_work());
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    _ = &mut wait => {}
                }
            }
        }
    }

    /// One pass over all routes. Returns true if any message was processed.
    async fn poll_round(&mut self) -> RbmqResult<bool> {
        let mut did_work = false;
        for route in &self.routes {
            let raw = self
                .rbmq
                .receive_for_worker(&route.qname, self.config.visibility_extension)
                .await?;
            let Some(raw) = raw else {
                continue;
            };
            did_work = true;
            dispatch(&mut self.rbmq, &self.config, route, raw).await?;
        }
        Ok(did_work)
    }

    async fn wait_for_work(&mut self) {
        if let Some(pubsub) = self.pubsub.as_mut() {
            let mut stream = pubsub.on_message();
            tokio::select! {
                _ = tokio::time::sleep(self.config.poll_interval) => return,
                signal = stream.next() => {
                    if signal.is_some() {
                        return;
                    }
                },
            }
            drop(stream);
            self.pubsub = None;
        }
        tokio::time::sleep(self.config.poll_interval).await;
    }
}

async fn dispatch(
    rbmq: &mut Rbmq,
    config: &WorkerConfig,
    route: &Route,
    msg: RbmqMessage<Vec<u8>>,
) -> RbmqResult<()> {
    let qname = &route.qname;
    let msg_id = msg.id.clone();
    let receive_count = msg.rc;
    let mut handler_fut = (route.handler)(msg);
    let first_heartbeat = tokio::time::Instant::now() + config.heartbeat_interval;
    let mut ticker = tokio::time::interval_at(first_heartbeat, config.heartbeat_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let outcome = loop {
        tokio::select! {
            biased;
            result = &mut handler_fut => break result,
            _ = ticker.tick() => {
                match rbmq
                    .update_lease(qname, &msg_id, receive_count, Some(config.visibility_extension))
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        // The attempt is stale. Finish its handler without mutating Redis.
                        let _ = handler_fut.await;
                        return Ok(());
                    }
                    Err(error) => warn!("rbmq worker: heartbeat failed for {qname}/{msg_id}: {error}"),
                }
            }
        }
    };

    let Err(handler_error) = outcome else {
        rbmq.acknowledge(qname, &msg_id, receive_count).await?;
        return Ok(());
    };

    if let Some(dlq) = route
        .dlq
        .as_ref()
        .filter(|dlq| receive_count > dlq.max_failures)
    {
        match rbmq.dead_letter(qname, &msg_id, receive_count, &dlq.queue).await {
            Ok(moved) => {
                if moved {
                    warn!(
                        "rbmq worker: routed {qname}/{msg_id} to DLQ {} after {receive_count} failure(s): {handler_error}",
                        dlq.queue,
                    );
                }
                return Ok(());
            }
            Err(error) => error!(
                "rbmq worker: failed to move {qname}/{msg_id} to DLQ {}: {error} (handler error: {handler_error})",
                dlq.queue,
            ),
        }
    } else {
        warn!("rbmq worker: handler failed for {qname}/{msg_id} (rc={receive_count}): {handler_error}");
    }

    if let Err(error) = rbmq.update_lease(qname, &msg_id, receive_count, None).await {
        warn!("rbmq worker: could not schedule retry for {qname}/{msg_id}: {error}");
    }
    Ok(())
}
