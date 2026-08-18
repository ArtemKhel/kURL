use std::collections::HashMap;

use anyhow::Context;
use chrono::{DateTime, TimeZone, Utc};
use common::{events::ClickEvent, redis_keys::RedisKeys};
use redis::{AsyncTypedCommands, RedisError, RedisResult, streams::StreamDeletionPolicy};
use tracing::instrument;

pub struct RedisStats {}

/// Atomic HMAX: set the field to be the greatest of current and incoming values
/// * KEYS: hash_key
/// * ARGV:
///   - field
///   - value
///
/// Returns: `true` if the value was updated, `false` if not
const HMAX_SCRIPT: &str = r#"
local current = redis.call("HGET", KEYS[1], ARGV[1])
if not current or tonumber(ARGV[2]) > tonumber(current) then
    redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
    return true
end
return false
"#;

/// Atomic compare-and-delete: if the current value matches the expected value, set it to a new value
/// * KEYS: hash_key
/// * ARGV:
///  - field
///  - expected_value
///
/// Returns: `true` if the value was updated, `false` if not
const COMPARE_AND_DELETE_SCRIPT: &str = r#"
local current = redis.call("HGET", KEYS[1], ARGV[1])
if current == argv[2] then
    redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
    return true
end
return false
"#;

/// Atomic click event processing script
/// * KEYS:
///   - event_stream
///   - global_stats_key
///   - link_stats_key
///   - link_last_clicked_at_key
/// * ARGV:
///   - consumer_group
///   - entry_id
///   - day_string
///   - timestamp
///
/// Returns: 1 if the event was processed, 0 if it was already handled
const RECORD_CLICK: &str = r#"
local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
if acked == 0 then return 0 end
redis.call('HINCRBY', KEYS[2], ARGV[3], 1)
redis.call('HINCRBY', KEYS[3], ARGV[3], 1)
local ts = tonumber(ARGV[4])
local cur_ts = tonumber(redis.call('GET', KEYS[4]) or '0')
if ts > cur_ts then
    redis.call('SET', KEYS[4], ARGV[4])
end
redis.call('XDEL', KEYS[1], ARGV[2])
return 1
"#;

#[derive(Debug, PartialEq, Eq)]
pub enum EventOutcome {
    Applied,
    AlreadyHandled,
}

impl RedisStats {
    pub async fn hmax(
        conn: &mut deadpool_redis::Connection,
        hash_key: &str,
        field: &str,
        value: i64,
    ) -> RedisResult<bool> {
        redis::Script::new(HMAX_SCRIPT)
            .key(hash_key)
            .arg(field)
            .arg(value)
            .invoke_async(conn)
            .await
    }

    pub async fn compare_and_delete(
        conn: &mut deadpool_redis::Connection,
        hash_key: &str,
        field: &str,
        expected_value: &str,
    ) -> RedisResult<bool> {
        redis::Script::new(COMPARE_AND_DELETE_SCRIPT)
            .key(hash_key)
            .arg(field)
            .arg(expected_value)
            .invoke_async(conn)
            .await?
    }

    pub async fn record_click_event(
        conn: &mut deadpool_redis::Connection,
        event_stream_key: &str,
        consumer_group: &str,
        entry_id: &str,
        event: &ClickEvent,
    ) -> RedisResult<EventOutcome> {
        let global_key = RedisKeys::global_stats_key();
        let link_key = RedisKeys::link_stats_key(&event.short_code);
        let last_click_key = RedisKeys::link_last_clicked_at_key(&event.short_code);

        let result: i64 = redis::Script::new(RECORD_CLICK)
            .key(event_stream_key)
            .key(global_key)
            .key(link_key)
            .key(last_click_key)
            .arg(consumer_group)
            .arg(entry_id)
            .arg(event.at.format("%Y-%m-%d").to_string())
            .arg(event.at.timestamp())
            .invoke_async(conn)
            .await?;

        match result {
            0 => Ok(EventOutcome::Applied),
            1 => Ok(EventOutcome::AlreadyHandled),
            _ => Err(RedisError::from(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Unexpected return value from RECORD_CLICK script",
            ))),
        }
    }

    pub async fn drop_click_event(
        conn: &mut deadpool_redis::Connection,
        event_stream_key: &str,
        consumer_group: &str,
        entry_id: &str,
    ) -> RedisResult<()> {
        let _ = conn
            .xack_del(
                event_stream_key,
                consumer_group,
                std::slice::from_ref(&entry_id),
                StreamDeletionPolicy::Acked,
            )
            .await;
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn last_clicked_at_batch(
        conn: &mut deadpool_redis::Connection,
        short_codes: &[String],
    ) -> anyhow::Result<HashMap<String, DateTime<Utc>>> {
        if short_codes.is_empty() {
            return Ok(HashMap::new());
        }

        let keys: Vec<String> = short_codes
            .iter()
            .map(|code| RedisKeys::link_last_clicked_at_key(code))
            .collect();

        let values: Vec<Option<String>> = conn
            .mget(keys)
            .await
            .context("failed to fetch last clicked timestamps")?;

        short_codes
            .iter()
            .cloned()
            .zip(values)
            .filter_map(|(code, value)| value.map(|value| (code, value)))
            .map(|(code, value)| {
                let ts = value
                    .parse::<i64>()
                    .with_context(|| format!("invalid Redis last_clicked_at timestamp for {code}: {value}"))?;
                let at = Utc
                    .timestamp_millis_opt(ts)
                    .single()
                    .with_context(|| format!("out-of-range timestamp for {code}: {ts}"))?;
                Ok((code, at))
            })
            .collect()
    }
}
