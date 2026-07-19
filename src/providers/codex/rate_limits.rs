//! Latest Codex rate-limit snapshot, captured from `codex.rate_limits`
//! stream events and served by the proxy's `GET /usage` endpoint.
//!
//! The upstream pushes a snapshot on responses whether or not a limit is
//! reached; before this module, the `limit_reached: false` snapshots were
//! discarded, so the only way to see remaining quota was an unofficial
//! side-channel API. The upstream payload shape is undocumented and can
//! change, so the `rate_limits` object is stored and served verbatim
//! rather than being re-modeled field by field.

use serde_json::{Value, json};
use std::sync::RwLock;

static LATEST: RwLock<Option<Value>> = RwLock::new(None);

/// Record the snapshot when `payload` is a `codex.rate_limits` event.
/// Returns true when a snapshot was captured.
pub fn record_event(payload: &Value) -> bool {
    if payload.get("type").and_then(Value::as_str) != Some("codex.rate_limits") {
        return false;
    }
    let Some(rate_limits) = payload.get("rate_limits") else {
        return false;
    };
    let snapshot = json!({
        "rate_limits": rate_limits.clone(),
        "captured_at": jiff::Timestamp::now().to_string(),
    });
    if let Ok(mut guard) = LATEST.write() {
        *guard = Some(snapshot);
        return true;
    }
    false
}

/// The most recent snapshot, or None when no request has produced one yet.
pub fn latest() -> Option<Value> {
    LATEST.read().ok().and_then(|guard| guard.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ignores_non_rate_limit_events() {
        assert!(!record_event(&json!({"type": "keepalive"})));
        assert!(!record_event(
            &json!({"type": "response.output_text.delta", "delta": "x"})
        ));
        assert!(!record_event(&json!({"type": "codex.rate_limits"})));
    }

    #[test]
    fn records_and_returns_latest_snapshot() {
        let first = json!({
            "type": "codex.rate_limits",
            "rate_limits": {
                "limit_reached": false,
                "primary": {"used_percent": 16.0, "window_minutes": 10080}
            }
        });
        assert!(record_event(&first));
        let stored = latest().expect("snapshot stored");
        assert_eq!(
            stored.pointer("/rate_limits/primary/used_percent"),
            Some(&json!(16.0))
        );
        assert!(stored.get("captured_at").and_then(Value::as_str).is_some());

        let second = json!({
            "type": "codex.rate_limits",
            "rate_limits": {"limit_reached": true}
        });
        assert!(record_event(&second));
        let stored = latest().expect("snapshot replaced");
        assert_eq!(
            stored.pointer("/rate_limits/limit_reached"),
            Some(&json!(true))
        );
    }
}
