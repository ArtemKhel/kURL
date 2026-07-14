use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use common::events::ClickEvent;
use itertools::Itertools;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info, instrument};

use crate::db;

pub struct ClickCounter {
    tx: UnboundedSender<ClickEvent>,
}

impl ClickCounter {
    pub fn spawn(
        config: &crate::Config,
        db: sqlx::PgPool,
        task_tracker: &TaskTracker,
        shutdown: CancellationToken,
    ) -> ClickCounter {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<ClickEvent>();
        let flush_interval = config.analytics.flush_interval.clone();
        task_tracker.spawn(async move {
            let mut worker = ClickCounterWorker::new(db, rx, flush_interval);
            worker.run(shutdown).await;
        });
        Self { tx }
    }

    pub fn notify(&self, event: ClickEvent) {
        if let Err(e) = self.tx.send(event) {
            error!(error = %e, "Failed to send click event to ClickCounter");
        }
    }
}

#[derive(Debug)]
struct ClickData {
    count: i64,
    at: DateTime<Utc>,
}

struct ClickCounterWorker {
    db: sqlx::PgPool,
    rx: UnboundedReceiver<ClickEvent>,
    buffer: HashMap<String, ClickData>,
    flush_interval: Duration,
}

impl ClickCounterWorker {
    pub fn new(db: sqlx::PgPool, rx: UnboundedReceiver<ClickEvent>, flush_interval: Duration) -> Self {
        Self {
            db,
            rx,
            buffer: HashMap::new(),
            flush_interval,
        }
    }

    #[instrument(skip_all)]
    async fn run(&mut self, shutdown: CancellationToken) {
        let mut ticker = tokio::time::interval(self.flush_interval);

        loop {
            tokio::select! {
                event = self.rx.recv() => {
                    match event{
                        Some(event) => {
                            let entry = self.buffer.entry(event.short_code).or_insert(ClickData { count: 0, at: event.at });
                            entry.count += 1;
                            if event.at > entry.at {
                                entry.at = event.at;
                            }
                        }
                        None => {
                            info!("ClickCounter channel closed, exiting");
                            self.flush().await;
                            break;
                        }
                    }
                }
                _ = ticker.tick() => {
                    self.flush().await;
                }
                _ = shutdown.cancelled() => {
                    info!("Shutdown requested, flushing click buffer");
                    self.flush().await;
                    break;
                }
            }
        }
    }

    #[instrument(skip_all)]
    async fn flush(&mut self) {
        info!("Flushing click counters");
        if self.buffer.is_empty() {
            return;
        }

        let (short_codes, click_counts, click_ats): (Vec<_>, Vec<_>, Vec<_>) = self
            .buffer
            .drain()
            .map(|(code, ClickData { count, at })| (code.clone(), count, at))
            .multiunzip();

        let total = click_counts.iter().sum();

        // todo: may lose some data if db write fails
        //  should run in transaction, check for transient errors and retry, then wipe buffer
        db::update_link_total_clicks(&self.db, &short_codes, &click_counts, &click_ats)
            .await
            .unwrap_or_else(|e| error!(error=?e, "Failed to update click counts"));

        db::update_total_clicks(&self.db, total)
            .await
            .unwrap_or_else(|e| error!(error=?e, "Failed to update total click count"));
    }
}
