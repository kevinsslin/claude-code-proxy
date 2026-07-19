//! Optional harness-level cap on Claude Code agent nesting depth.
//!
//! Claude Code identifies agent traffic with `x-claude-code-agent-id` and,
//! for nested agents, `x-claude-code-parent-agent-id`. Native Codex ships a
//! hard `max_depth` for agents; Claude Code does not expose one, and agent
//! self-recursion (an agent whose description matches the task it was given
//! delegating to itself) burns real quota. When `CCP_MAX_AGENT_DEPTH` is set,
//! requests from agents nested deeper than the cap are rejected before they
//! reach the upstream. Unset (the default) disables the check entirely.
//!
//! Depth semantics match native Codex: the main conversation is depth 0, a
//! subagent it spawns is depth 1, that agent's child is depth 2, and so on.
//! `CCP_MAX_AGENT_DEPTH=2` therefore allows one level of helpers under a
//! subagent and blocks the third level.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

/// Per-session `agent id -> parent agent id` maps, learned from request
/// headers. A parent always makes requests before it can spawn a child, so
/// the chain is normally complete by the time the child's request arrives.
type Tree = HashMap<String, Option<String>>;

const MAX_SESSIONS: usize = 512;
const MAX_CHAIN_WALK: u32 = 32;

fn trees() -> &'static RwLock<HashMap<String, Tree>> {
    static TREES: OnceLock<RwLock<HashMap<String, Tree>>> = OnceLock::new();
    TREES.get_or_init(|| RwLock::new(HashMap::new()))
}

/// The configured cap, read per request so tests and restarts stay simple.
pub fn max_depth_from_env() -> Option<u32> {
    std::env::var("CCP_MAX_AGENT_DEPTH")
        .ok()?
        .trim()
        .parse()
        .ok()
        .filter(|depth| *depth > 0)
}

/// Record this request's agent lineage and enforce `max`. Returns the
/// offending depth when the requester is nested deeper than the cap.
///
/// Fail-open by design: no cap, no agent header (main conversation), or an
/// evicted session all allow the request. An unknown parent still counts as
/// one level (a parent header proves the requester is nested). Cycles from
/// malformed headers hit the walk bound and are rejected.
pub fn check(
    max: Option<u32>,
    session: Option<&str>,
    agent: Option<&str>,
    parent: Option<&str>,
) -> Result<(), u32> {
    let Some(max) = max else { return Ok(()) };
    let Some(agent) = agent else { return Ok(()) };
    let session = session.unwrap_or("");

    let mut trees = trees().write().unwrap_or_else(|err| err.into_inner());
    if trees.len() > MAX_SESSIONS && !trees.contains_key(session) {
        trees.clear();
    }
    let tree = trees.entry(session.to_string()).or_default();
    tree.insert(agent.to_string(), parent.map(str::to_string));

    let mut depth = 1u32;
    let mut cursor = parent.map(str::to_string);
    while let Some(current) = cursor {
        depth += 1;
        if depth > MAX_CHAIN_WALK {
            break;
        }
        cursor = tree.get(&current).cloned().flatten();
    }

    if depth > max { Err(depth) } else { Ok(()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_cap_or_no_agent_allows() {
        assert!(check(None, Some("s0"), Some("a1"), Some("a0")).is_ok());
        assert!(check(Some(1), Some("s0"), None, None).is_ok());
    }

    #[test]
    fn depth_chain_is_walked_and_capped() {
        let session = Some("s-chain");
        // depth 1: subagent spawned by the main conversation
        assert!(check(Some(2), session, Some("a1"), None).is_ok());
        // depth 2: child of a1, allowed at cap 2
        assert!(check(Some(2), session, Some("a2"), Some("a1")).is_ok());
        // depth 3: child of a2, rejected
        assert_eq!(check(Some(2), session, Some("a3"), Some("a2")), Err(3));
        // depth 1 in the same session still fine afterwards
        assert!(check(Some(2), session, Some("b1"), None).is_ok());
    }

    #[test]
    fn unknown_parent_counts_as_one_nested_level() {
        let session = Some("s-unknown");
        assert!(check(Some(2), session, Some("x2"), Some("never-seen")).is_ok());
        assert_eq!(
            check(Some(1), session, Some("y2"), Some("also-never-seen")),
            Err(2)
        );
    }

    #[test]
    fn header_cycles_are_rejected_not_looped() {
        let session = Some("s-cycle");
        assert!(check(Some(10), session, Some("c1"), Some("c2")).is_ok());
        // c2 claims c1 as parent: cycle. Walk bound turns it into a rejection.
        assert_eq!(check(Some(10), session, Some("c2"), Some("c1")), Err(33));
    }

    #[test]
    fn sessions_are_isolated() {
        assert!(check(Some(2), Some("s-a"), Some("p1"), None).is_ok());
        assert!(check(Some(2), Some("s-a"), Some("p2"), Some("p1")).is_ok());
        // same ids in a different session start a fresh chain
        assert!(check(Some(2), Some("s-b"), Some("p2"), Some("p1")).is_ok());
    }
}
