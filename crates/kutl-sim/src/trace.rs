//! Structured trace logging for simulation debugging.
//!
//! Records events during a simulation run. Opt-in — zero overhead when disabled.

use std::path::Path;

use serde::Serialize;

use crate::PeerId;

/// Number of recent events included in failure dump output.
const FAILURE_TRACE_EVENTS: usize = 20;

/// Directory for trace output files.
const TRACE_DIR: &str = "target/sim-traces";

/// Reason a message was dropped.
#[derive(Debug, Clone, Serialize)]
pub enum DropReason {
    /// Peers are partitioned.
    Partition,
    /// Link loss probability hit.
    Loss,
}

/// A single event recorded during simulation.
#[derive(Debug, Clone, Serialize)]
pub enum TraceEvent {
    /// A peer edited its document.
    Edit {
        tick: i64,
        peer: PeerId,
        intent: String,
    },
    /// A message was sent (enqueued in the network).
    MessageSent {
        tick: i64,
        from: PeerId,
        to: PeerId,
        bytes: usize,
    },
    /// A message was delivered to a peer.
    MessageDelivered { tick: i64, from: PeerId, to: PeerId },
    /// A message was dropped.
    MessageDropped {
        tick: i64,
        from: PeerId,
        to: PeerId,
        reason: DropReason,
    },
    /// A network partition was created.
    Partition { tick: i64, a: PeerId, b: PeerId },
    /// A network partition was healed.
    Heal { tick: i64, a: PeerId, b: PeerId },
    /// A peer crashed.
    Crash { tick: i64, peer: PeerId },
    /// A peer restarted.
    Restart { tick: i64, peer: PeerId },
}

/// Collects trace events during a simulation run.
#[derive(Debug, Clone, Default)]
pub struct TraceLog {
    events: Vec<TraceEvent>,
}

impl TraceLog {
    /// Record a trace event.
    pub fn record(&mut self, event: TraceEvent) {
        self.events.push(event);
    }

    /// Return all recorded events.
    pub fn events(&self) -> &[TraceEvent] {
        &self.events
    }

    /// Return the last `n` events (or all if fewer than `n`).
    pub fn last_n(&self, n: usize) -> &[TraceEvent] {
        let start = self.events.len().saturating_sub(n);
        &self.events[start..]
    }

    /// Format a failure message with trace context.
    pub fn format_failure(
        &self,
        seed: u64,
        peer_states: &[(PeerId, String)],
        message: &str,
    ) -> String {
        use std::fmt::Write as _;

        let mut out = format!("=== SIMULATION FAILURE ===\nSeed: {seed}\nMessage: {message}\n");

        out.push_str("\n--- Peer States ---\n");
        for (id, content) in peer_states {
            let _ = writeln!(out, "  peer {id}: {content:?}");
        }

        let _ = writeln!(
            out,
            "\n--- Last {FAILURE_TRACE_EVENTS} Events (of {}) ---",
            self.events.len()
        );
        for event in self.last_n(FAILURE_TRACE_EVENTS) {
            let _ = writeln!(out, "  {event:?}");
        }

        out
    }

    /// Serialize the trace log to JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(&self.events)
    }

    /// Write the trace log to a file in `target/sim-traces/<seed>.json`.
    ///
    /// Creates the directory if it doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if directory creation or file writing fails.
    pub fn write_to_file(&self, seed: u64) -> std::io::Result<std::path::PathBuf> {
        let dir = Path::new(TRACE_DIR);
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("{seed}.json"));
        let json = self.to_json().map_err(std::io::Error::other)?;
        std::fs::write(&path, json)?;
        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_events() {
        let mut log = TraceLog::default();
        log.record(TraceEvent::Edit {
            tick: 1,
            peer: 0,
            intent: "test".to_owned(),
        });
        assert_eq!(log.events().len(), 1);
    }

    #[test]
    fn test_last_n() {
        let mut log = TraceLog::default();
        for i in 0..10 {
            log.record(TraceEvent::Edit {
                tick: i,
                peer: 0,
                intent: format!("edit-{i}"),
            });
        }
        assert_eq!(log.last_n(3).len(), 3);
        assert_eq!(log.last_n(100).len(), 10);
    }
}
