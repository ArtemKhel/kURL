use std::collections::HashMap;

use chrono::{DateTime, Utc};
use common::events::ClickEvent;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, instrument};

use crate::db;

pub struct ClickCounter {
    tx: UnboundedSender<ClickEvent>,
}

impl ClickCounter {
    pub fn new(db: sqlx::PgPool) -> ClickCounter {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<ClickEvent>();
        tokio::spawn(async move {
            let mut worker = ClickCounterWorker {
                db,
                rx,
                buffer: HashMap::new(),
            };
            worker.run().await
        });
        Self { tx }
    }

    pub fn notify(&self, event: ClickEvent) {
        if let Err(e) = self.tx.send(event) {
            error!(error = %e, "Failed to send click event to ClickCounter");
        };
    }
}

struct ClickCounterWorker {
    db: sqlx::PgPool,
    rx: UnboundedReceiver<ClickEvent>,
    buffer: HashMap<String, (i64, DateTime<Utc>)>,
}

impl ClickCounterWorker {
    pub fn new(db: sqlx::PgPool, rx: UnboundedReceiver<ClickEvent>, buffer: HashMap<String, (i64, DateTime<Utc>)>) -> Self {
        Self { db, rx, buffer }
    }

    #[instrument(skip_all)]
    async fn run(&mut self) {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(10));

        loop {
            tokio::select! {
                event = self.rx.recv() => {
                    match event{
                        Some(event) => {
                            let entry = self.buffer.entry(event.short_code).or_insert((0, event.at));
                            (*entry).0 += 1;
                            if event.at > (*entry).1 {
                                (*entry).1 = event.at;
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
                    info!("Tick");
                    self.flush().await;
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

        let mut short_codes = Vec::with_capacity(self.buffer.len());
        let mut click_counts = Vec::with_capacity(self.buffer.len());
        let mut click_ats = Vec::with_capacity(self.buffer.len());

        for (short_code, (click_count, click_at)) in self.buffer.drain() {
            short_codes.push(short_code);
            click_counts.push(click_count);
            click_ats.push(click_at);
        }

        let total = click_counts.iter().fold(0, |acc, count| acc + count);

        db::update_link_total_clicks(&self.db, &short_codes, &click_counts, &click_ats)
            .await
            .unwrap();

        db::update_total_clicks(&self.db, total).await.unwrap();
    }
}
