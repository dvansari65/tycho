use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot, Mutex,
};
use tokio_retry::strategy::ExponentialBackoff;
use tracing::{error, info, warn};
use tycho_common::models::ExtractorIdentity;

pub use crate::extractor::factory::{DCIType, ExtractorConfig, ProtocolTypeConfig};
use crate::extractor::{
    factory::ExtractorFactory,
    runner::{ControlMessage, ExtractorHandle, SubscriptionsMap},
    DeltaCommand, ExtractionError,
};

/// Upper bound for the restart backoff delay.
const MAX_RESTART_BACKOFF: Duration = Duration::from_secs(4 * 60 * 60);

/// Long-lived per-extractor task that owns the factory and manages restart lifecycle.
///
/// The supervisor:
/// - Builds an extractor and runner via its factory.
/// - Runs the runner and waits for it to exit.
/// - On failure: sends `DeltaCommand::ExtractorRestarted` to all subscribers, applies exponential
///   backoff, then rebuilds from scratch. Each subscriber decides how to handle the restart.
/// - Forwards `ControlMessage::Subscribe` from the `ExtractorHandle` to the subscription map.
/// - Forwards `ControlMessage::Stop` by signalling the runner's stop channel.
pub struct ExtractorSupervisor {
    factory: ExtractorFactory,
    ctrl_tx: Sender<ControlMessage>,
    control_rx: Receiver<ControlMessage>,
    subscriptions: Arc<Mutex<SubscriptionsMap>>,
    id: ExtractorIdentity,
    max_restarts: Option<u32>,
    next_subscriber_id: u64,
}

impl ExtractorSupervisor {
    pub fn new(factory: ExtractorFactory) -> Self {
        let id = factory.extractor_id();
        let max_restarts: Option<u32> = factory.config.max_restarts;
        let (ctrl_tx, control_rx) = mpsc::channel(128);
        Self {
            factory,
            ctrl_tx,
            control_rx,
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            id,
            max_restarts,
            next_subscriber_id: 0,
        }
    }

    /// Registers a subscriber before the supervision loop starts.
    ///
    /// The subscriber receives every [`DeltaCommand`] the extractor emits, across restarts.
    /// Subscribers joining at runtime go through [`ExtractorHandle::subscribe`] instead.
    pub async fn add_subscriber(&mut self, sender: Sender<DeltaCommand>) {
        let subscriber_id = self.next_subscriber_id;
        self.next_subscriber_id += 1;
        self.subscriptions
            .lock()
            .await
            .insert(subscriber_id, sender);
    }

    /// Returns an [`ExtractorHandle`] that can be used to subscribe or stop this extractor.
    pub fn handle(&self) -> ExtractorHandle {
        ExtractorHandle::new(self.id.clone(), self.ctrl_tx.clone())
    }

    /// Runs the supervision loop. Returns when the extractor has been stopped via
    /// `ControlMessage::Stop` or has exhausted all restart attempts.
    pub async fn run(mut self) -> Result<(), ExtractionError> {
        let mut restart_count: u32 = 0;
        // Exponential backoff: 1s, 2s, 4s, ... capped at 4 hours.
        let mut backoff_strategy = ExponentialBackoff::from_millis(2)
            .factor(500)
            .max_delay(MAX_RESTART_BACKOFF);

        loop {
            let (stop_tx, stop_rx) = oneshot::channel();
            let runner = match self
                .factory
                .build_runner(self.subscriptions.clone(), stop_rx)
                .await
            {
                Ok(r) => r,
                Err(err) => {
                    error!(extractor = %self.id, error = %err, "Failed to build extractor");
                    metrics::counter!(
                        "extractor_restart_failed",
                        "extractor" => self.id.name.clone()
                    )
                    .increment(1);
                    return Err(err);
                }
            };

            let mut run_handle = runner.run();

            // Drive the runner, handling control messages in parallel.
            let runner_result = loop {
                tokio::select! {
                    result = &mut run_handle => {
                        break result;
                    }
                    Some(ctrl) = self.control_rx.recv() => {
                        match ctrl {
                            ControlMessage::Stop => {
                                info!(extractor = %self.id, "Stop signal received by supervisor");
                                let _ = stop_tx.send(());
                                let result = run_handle.await;
                                return match result {
                                    Ok(Ok(())) => Ok(()),
                                    Ok(Err(e)) => Err(e),
                                    Err(join_err) => Err(ExtractionError::Unknown(
                                        format!("Runner panicked: {join_err}")
                                    )),
                                };
                            }
                            ControlMessage::Subscribe(sender) => {
                                let subscriber_id = self.next_subscriber_id;
                                self.next_subscriber_id += 1;
                                info!(
                                    extractor = %self.id,
                                    subscriber_id,
                                    "New subscription via supervisor"
                                );
                                self.subscriptions
                                    .lock()
                                    .await
                                    .insert(subscriber_id, sender);
                            }
                        }
                    }
                }
            };

            // Runner exited — classify the result.
            match runner_result {
                Ok(Ok(())) => {
                    info!(extractor = %self.id, "Extractor exited gracefully");
                    metrics::counter!(
                        "extractor_stopped",
                        "extractor" => self.id.name.clone(),
                        "reason" => "graceful"
                    )
                    .increment(1);
                    return Ok(());
                }
                Ok(Err(ref err)) => {
                    error!(
                        extractor = %self.id,
                        error = %err,
                        restart_count,
                        "Extractor failed"
                    );
                    metrics::counter!(
                        "extractor_stopped",
                        "extractor" => self.id.name.clone(),
                        "reason" => err.variant_name()
                    )
                    .increment(1);
                }
                Err(ref join_err) => {
                    error!(
                        extractor = %self.id,
                        error = %join_err,
                        "Extractor task panicked"
                    );
                    metrics::counter!(
                        "extractor_stopped",
                        "extractor" => self.id.name.clone(),
                        "reason" => "panic"
                    )
                    .increment(1);
                }
            }

            if self
                .max_restarts
                .is_some_and(|max| restart_count >= max)
            {
                error!(
                    extractor = %self.id,
                    max_restarts = ?self.max_restarts,
                    "Extractor permanently stopped — restart limit reached"
                );
                metrics::counter!(
                    "extractor_permanently_stopped",
                    "extractor" => self.id.name.clone()
                )
                .increment(1);
                return runner_result
                    .map_err(|e| ExtractionError::Unknown(format!("Runner panicked: {e}")))?;
            }

            // Notify all subscribers of the restart. Sent on the same channels as block
            // messages, so it is guaranteed to arrive after all blocks the runner emitted
            // before failing. Each subscriber decides how to react: `PendingDeltas` resets its
            // buffer, the WS service ends the affected client subscriptions.
            {
                let mut subs = self.subscriptions.lock().await;
                let mut closed = Vec::new();
                for (subscriber_id, sender) in subs.iter() {
                    if sender
                        .send(DeltaCommand::ExtractorRestarted(self.id.name.clone()))
                        .await
                        .is_err()
                    {
                        closed.push(*subscriber_id);
                    }
                }
                for subscriber_id in closed {
                    subs.remove(&subscriber_id);
                    info!(
                        extractor = %self.id,
                        subscriber_id,
                        "Removed closed subscriber during restart"
                    );
                }
            }

            let backoff = backoff_strategy
                .next()
                .expect("backoff strategy is infinite");
            warn!(
                extractor = %self.id,
                ?backoff,
                restart_count,
                "Waiting for backoff before restarting extractor"
            );
            tokio::time::sleep(backoff).await;
            warn!(
                extractor = %self.id,
                ?backoff,
                restart_count,
                "Restarting extractor after backoff"
            );
            restart_count += 1;
        }
    }
}
