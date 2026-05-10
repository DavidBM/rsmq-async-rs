//! Async worker helper.
//!
//! A [`Worker`] polls one or more queues, dispatches each message to a per-queue handler,
//! runs the handler with automatic visibility-heartbeat (so a slow handler can outlive the
//! queue's `vt` without redelivery), and optionally subscribes to RSMQ realtime PUBLISHes
//! to wake up immediately instead of polling.
//!
//! Requires the `worker` feature (default-on, tokio-only for now).
//!
//! ```no_run
//! # use rsmq_async::{RsmqMessage, RsmqOptions, Worker};
//! # use std::convert::Infallible;
//! # async fn _example() -> Result<(), Box<dyn std::error::Error>> {
//! let worker = Worker::builder(RsmqOptions::default())
//!     .route("emails", |msg: RsmqMessage<String>| async move {
//!         println!("got email job: {}", msg.message);
//!         Ok::<(), Infallible>(())
//!     })
//!     .route("billing", |msg: RsmqMessage<Vec<u8>>| async move {
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
use crate::{Rsmq, RsmqConnection, RsmqError, RsmqMessage, RsmqOptions, RsmqResult};
use core::convert::TryFrom;
use futures_util::StreamExt;
use log::{error, warn};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

type HandlerError = Box<dyn StdError + Send + Sync>;
type HandlerResult = Result<(), HandlerError>;
type HandlerFuture = Pin<Box<dyn Future<Output = HandlerResult> + Send>>;
type ErasedHandler = Arc<dyn Fn(RsmqMessage<Vec<u8>>) -> HandlerFuture + Send + Sync>;

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
    options: RsmqOptions,
    routes: HashMap<String, ErasedHandler>,
    poll_interval: Duration,
    heartbeat_interval: Duration,
    visibility_extension: Duration,
    use_realtime: bool,
    global_dlq: Option<DlqConfig>,
    route_dlqs: HashMap<String, DlqConfig>,
}

impl WorkerBuilder {
    fn new(options: RsmqOptions) -> Self {
        Self {
            options,
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
        F: Fn(RsmqMessage<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        T: TryFrom<RedisBytes, Error = Vec<u8>> + Send + 'static,
        E: StdError + Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        let erased: ErasedHandler = Arc::new(move |raw: RsmqMessage<Vec<u8>>| {
            let handler = handler.clone();
            Box::pin(async move {
                let bytes = RedisBytes::from(raw.message);
                let typed = T::try_from(bytes)
                    .map_err(|b| Box::new(DecodeError(b)) as Box<dyn StdError + Send + Sync>)?;
                let msg = RsmqMessage {
                    id: raw.id,
                    message: typed,
                    rc: raw.rc,
                    fr: raw.fr,
                    sent: raw.sent,
                };
                handler(msg)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn StdError + Send + Sync>)
            })
        });
        self.routes.insert(qname.into(), erased);
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

    /// How far into the future each heartbeat pushes the message's visibility. Default: 30s.
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

    /// Subscribe to RSMQ realtime PUBLISHes so the worker wakes up on new messages instead of
    /// strictly polling. Off by default. Senders must use `RsmqOptions { realtime: true, .. }`
    /// for any realtime channel to fire — when off, this option does nothing harmful, the
    /// worker just always waits the full poll interval between checks.
    pub fn use_realtime(mut self, enabled: bool) -> Self {
        self.use_realtime = enabled;
        self
    }

    /// Build the [`Worker`], opening connections to Redis. Returns an error if any of the
    /// connections fail to open, if a route is configured with its own queue as DLQ
    /// (would loop forever), or, when `use_realtime` is on, if subscribing fails.
    pub async fn build(self) -> RsmqResult<Worker> {
        if self.routes.is_empty() {
            return Err(RsmqError::NoAttributeSupplied);
        }
        // Reject self-loops (route X with DLQ X) up front.
        for route in self.routes.keys() {
            let dlq = self
                .route_dlqs
                .get(route)
                .or(self.global_dlq.as_ref())
                .map(|d| d.queue.as_str());
            if dlq == Some(route.as_str()) {
                return Err(RsmqError::InvalidFormat(format!(
                    "DLQ for route {route:?} is the same queue (would loop)"
                )));
            }
        }
        let rsmq = Rsmq::new(self.options.clone()).await?;
        let pubsub = if self.use_realtime {
            Some(open_pubsub(&self.options, self.routes.keys()).await?)
        } else {
            None
        };
        Ok(Worker {
            rsmq,
            pubsub,
            routes: self.routes.into_iter().collect(),
            config: WorkerConfig {
                poll_interval: self.poll_interval,
                heartbeat_interval: self.heartbeat_interval,
                visibility_extension: self.visibility_extension,
            },
            global_dlq: self.global_dlq,
            route_dlqs: self.route_dlqs,
        })
    }
}

/// Polls registered queues and dispatches to per-queue handlers. Build via [`Worker::builder`].
pub struct Worker {
    rsmq: Rsmq,
    pubsub: Option<redis::aio::PubSub>,
    routes: Vec<(String, ErasedHandler)>,
    config: WorkerConfig,
    global_dlq: Option<DlqConfig>,
    route_dlqs: HashMap<String, DlqConfig>,
}

struct WorkerConfig {
    poll_interval: Duration,
    heartbeat_interval: Duration,
    visibility_extension: Duration,
}

impl Worker {
    /// Start a [`WorkerBuilder`].
    pub fn builder(options: RsmqOptions) -> WorkerBuilder {
        WorkerBuilder::new(options)
    }

    /// Run forever. Returns only on a fatal Redis error.
    pub async fn run(self) -> RsmqResult<()> {
        self.run_until(std::future::pending::<()>()).await
    }

    /// Run until `shutdown` resolves. The current in-flight handler (if any) is allowed to
    /// finish before the worker returns; handlers are never cancelled mid-flight. Shutdown is
    /// checked between polling rounds — if the queues are saturated, exit may be delayed by
    /// up to one round (≈ one message per registered queue).
    pub async fn run_until<S>(mut self, shutdown: S) -> RsmqResult<()>
    where
        S: Future<Output = ()> + Send,
    {
        let mut shutdown = Box::pin(shutdown);
        loop {
            let did_work = self.poll_round().await?;

            // Cheap non-blocking shutdown check between rounds.
            tokio::select! {
                biased;
                _ = &mut shutdown => return Ok(()),
                _ = std::future::ready(()) => {}
            }

            if !did_work {
                let mut wait = Box::pin(self.wait_for_work());
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    _ = &mut wait => {}
                }
            }
        }
    }

    /// One pass over all routes. Returns true if any message was processed.
    async fn poll_round(&mut self) -> RsmqResult<bool> {
        let mut did_work = false;
        for idx in 0..self.routes.len() {
            // Avoid borrowing self.routes for the whole iteration so we can mutably use self.rsmq.
            let qname = self.routes[idx].0.clone();
            let handler = self.routes[idx].1.clone();
            let raw = self.rsmq.receive_message::<Vec<u8>>(&qname, None).await?;
            let Some(raw) = raw else {
                continue;
            };
            did_work = true;
            self.dispatch(&qname, &handler, raw).await?;
        }
        Ok(did_work)
    }

    async fn dispatch(
        &mut self,
        qname: &str,
        handler: &ErasedHandler,
        msg: RsmqMessage<Vec<u8>>,
    ) -> RsmqResult<()> {
        let msg_id = msg.id.clone();
        let msg_rc = msg.rc;
        let mut handler_fut = (handler)(msg);
        let mut ticker = tokio::time::interval(self.config.heartbeat_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await; // skip the immediate tick

        let outcome = loop {
            tokio::select! {
                result = &mut handler_fut => break result,
                _ = ticker.tick() => {
                    // Heartbeat failures are logged but don't abort the handler; the message
                    // will simply be redelivered if the handler runs past its visibility.
                    if let Err(e) = self
                        .rsmq
                        .change_message_visibility(qname, &msg_id, self.config.visibility_extension)
                        .await
                    {
                        warn!("rsmq worker: heartbeat failed for {qname}/{msg_id}: {e}");
                    }
                }
            }
        };

        match outcome {
            Ok(()) => {
                self.rsmq.delete_message(qname, &msg_id).await?;
            }
            Err(e) => {
                let dlq_target = self
                    .dlq_for_route(qname)
                    .filter(|d| msg_rc > d.max_failures)
                    .map(|d| d.queue.clone());
                if let Some(dlq_queue) = dlq_target {
                    match self.rsmq.move_message(qname, &msg_id, &dlq_queue).await {
                        Ok(true) => {
                            warn!(
                                "rsmq worker: routed {qname}/{msg_id} to DLQ {dlq_queue} after {msg_rc} failure(s): {e}"
                            );
                        }
                        Ok(false) => {
                            // Message disappeared (concurrent delete?). Nothing to do.
                        }
                        Err(move_err) => {
                            error!(
                                "rsmq worker: failed to move {qname}/{msg_id} to DLQ {dlq_queue}: {move_err} (handler error: {e})"
                            );
                        }
                    }
                    return Ok(());
                }
                warn!(
                    "rsmq worker: handler failed for {qname}/{msg_id} (rc={msg_rc}): {e} (will redeliver)"
                );
                // Leave the message hidden — it will be redelivered after the queue's vt.
            }
        }
        Ok(())
    }

    fn dlq_for_route(&self, qname: &str) -> Option<&DlqConfig> {
        self.route_dlqs.get(qname).or(self.global_dlq.as_ref())
    }

    async fn wait_for_work(&mut self) {
        match self.pubsub.as_mut() {
            Some(pubsub) => {
                let mut stream = pubsub.on_message();
                let timer = tokio::time::sleep(self.config.poll_interval);
                tokio::pin!(timer);
                tokio::select! {
                    _ = &mut timer => {},
                    _ = stream.next() => {},
                }
            }
            None => {
                tokio::time::sleep(self.config.poll_interval).await;
            }
        }
    }
}

async fn open_pubsub<'a, I>(opts: &RsmqOptions, channels: I) -> RsmqResult<redis::aio::PubSub>
where
    I: IntoIterator<Item = &'a String>,
{
    let mut redis_info = redis::RedisConnectionInfo::default()
        .set_db(opts.db.into())
        .set_protocol(opts.protocol);
    if let Some(username) = opts.username.clone() {
        redis_info = redis_info.set_username(username);
    }
    if let Some(password) = opts.password.clone() {
        redis_info = redis_info.set_password(password);
    }
    let conn_info = format!("redis://{}:{}", opts.host, opts.port)
        .parse::<redis::ConnectionInfo>()?
        .set_redis_settings(redis_info);
    let client = redis::Client::open(conn_info)?;
    let mut pubsub = client.get_async_pubsub().await?;
    for qname in channels {
        pubsub
            .subscribe(format!("{}:rt:{}", opts.ns, qname))
            .await?;
    }
    Ok(pubsub)
}
