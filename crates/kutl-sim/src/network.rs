//! Virtual network for in-process message delivery between peers.

use std::collections::{HashMap, HashSet, VecDeque};

use rand::RngExt;
use rand_chacha::ChaChaRng;

use kutl_core::Change;

use crate::PeerId;
use crate::link::LinkConfig;

/// Maximum additional jitter (in ticks) applied when a message is reordered.
const MAX_REORDER_JITTER: u64 = 50;

/// A message in transit between two peers.
#[derive(Debug, Clone)]
pub struct Message {
    /// Sender peer id.
    pub from: PeerId,
    /// Receiver peer id.
    pub to: PeerId,
    /// Diamond-types delta bytes.
    pub dt_bytes: Vec<u8>,
    /// Change metadata associated with this delta.
    pub changes: Vec<Change>,
    /// Virtual tick at which this message becomes deliverable.
    pub delivery_tick: i64,
}

/// Outcome of a `send()` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendResult {
    /// Message was enqueued for delivery.
    Enqueued,
    /// Message was dropped because the peers are partitioned.
    DroppedPartition,
    /// Message was dropped due to link loss probability.
    DroppedLoss,
}

/// In-memory message queues with partition support and per-link adversity.
///
/// Messages sent between partitioned peers are silently dropped.
#[derive(Debug)]
pub struct VirtualNetwork {
    queue: VecDeque<Message>,
    /// Bidirectional partitions: if (a,b) is present, messages between a and b
    /// are dropped. We store both (a,b) and (b,a) for fast lookup.
    partitions: HashSet<(PeerId, PeerId)>,
    /// Per-directed-link configuration for latency, loss, and reorder.
    link_configs: HashMap<(PeerId, PeerId), LinkConfig>,
    /// PRNG for randomized network decisions.
    rng: ChaChaRng,
}

impl VirtualNetwork {
    /// Create a network with the given PRNG for randomized decisions.
    pub fn new(rng: ChaChaRng) -> Self {
        Self {
            queue: VecDeque::new(),
            partitions: HashSet::new(),
            link_configs: HashMap::new(),
            rng,
        }
    }

    /// Set the link configuration for a directed link from `from` to `to`.
    pub fn configure_link(&mut self, from: PeerId, to: PeerId, config: LinkConfig) {
        self.link_configs.insert((from, to), config);
    }

    /// Enqueue a message for delivery, subject to partitions and link config.
    ///
    /// Returns a [`SendResult`] indicating what happened to the message.
    pub fn send(&mut self, mut msg: Message) -> SendResult {
        if self.is_partitioned(msg.from, msg.to) {
            return SendResult::DroppedPartition;
        }

        if let Some(config) = self.link_configs.get(&(msg.from, msg.to)).cloned() {
            // Check loss probability.
            let roll: f64 = self.rng.random();
            if roll < config.loss {
                return SendResult::DroppedLoss;
            }

            // Add random latency from the configured range.
            if !config.latency.is_empty() {
                let range_size = config.latency.end - config.latency.start;
                let offset: i64 = if range_size > 0 {
                    let roll: u64 = self.rng.random();
                    config.latency.start + roll.cast_signed().rem_euclid(range_size)
                } else {
                    config.latency.start
                };
                msg.delivery_tick += offset;
            }

            // Apply reorder jitter: add extra random delay.
            let reorder_roll: f64 = self.rng.random();
            if reorder_roll < config.reorder {
                let jitter: u64 = self.rng.random();
                msg.delivery_tick += (jitter % MAX_REORDER_JITTER).cast_signed();
            }
        }

        self.queue.push_back(msg);
        SendResult::Enqueued
    }

    /// Deliver all messages whose `delivery_tick <= now`, returning them grouped by receiver.
    pub fn deliver_ready(&mut self, now: i64) -> HashMap<PeerId, Vec<Message>> {
        let mut ready: HashMap<PeerId, Vec<Message>> = HashMap::new();
        let mut remaining = VecDeque::new();

        let old_queue = std::mem::take(&mut self.queue);
        for msg in old_queue {
            if msg.delivery_tick <= now && !self.partitions.contains(&(msg.from, msg.to)) {
                ready.entry(msg.to).or_default().push(msg);
            } else {
                remaining.push_back(msg);
            }
        }

        self.queue = remaining;
        ready
    }

    /// Create a bidirectional network partition between peers `a` and `b`.
    pub fn partition(&mut self, a: PeerId, b: PeerId) {
        self.partitions.insert((a, b));
        self.partitions.insert((b, a));
    }

    /// Heal a partition between peers `a` and `b`.
    pub fn heal(&mut self, a: PeerId, b: PeerId) {
        self.partitions.remove(&(a, b));
        self.partitions.remove(&(b, a));
    }

    /// Clear all partitions.
    pub fn heal_all(&mut self) {
        self.partitions.clear();
    }

    /// Remove all per-link configurations, restoring default (no adversity) behaviour.
    pub fn clear_link_configs(&mut self) {
        self.link_configs.clear();
    }

    /// Drop all in-flight messages to or from the given peer.
    pub fn drop_messages_for(&mut self, peer: PeerId) {
        self.queue.retain(|m| m.from != peer && m.to != peer);
    }

    /// Return `true` if no messages are pending delivery.
    pub fn is_quiescent(&self) -> bool {
        self.queue.is_empty()
    }

    /// Return the number of messages currently in the queue.
    pub fn pending_count(&self) -> usize {
        self.queue.len()
    }

    fn is_partitioned(&self, a: PeerId, b: PeerId) -> bool {
        self.partitions.contains(&(a, b))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prng::net_rng;

    fn test_net() -> VirtualNetwork {
        VirtualNetwork::new(net_rng(42))
    }

    fn msg(from: PeerId, to: PeerId, tick: i64) -> Message {
        Message {
            from,
            to,
            dt_bytes: vec![1, 2, 3],
            changes: vec![],
            delivery_tick: tick,
        }
    }

    #[test]
    fn test_send_and_deliver() {
        let mut net = test_net();
        assert_eq!(net.send(msg(0, 1, 10)), SendResult::Enqueued);
        assert!(!net.is_quiescent());

        let ready = net.deliver_ready(10);
        assert_eq!(ready[&1].len(), 1);
        assert!(net.is_quiescent());
    }

    #[test]
    fn test_deliver_respects_tick() {
        let mut net = test_net();
        net.send(msg(0, 1, 10));
        let ready = net.deliver_ready(5);
        assert!(ready.is_empty());
        assert!(!net.is_quiescent());
    }

    #[test]
    fn test_partition_drops_messages() {
        let mut net = test_net();
        net.partition(0, 1);
        assert_eq!(net.send(msg(0, 1, 0)), SendResult::DroppedPartition);
        assert!(net.is_quiescent()); // dropped on send
    }

    #[test]
    fn test_heal_allows_messages() {
        let mut net = test_net();
        net.partition(0, 1);
        net.heal(0, 1);
        assert_eq!(net.send(msg(0, 1, 0)), SendResult::Enqueued);
        assert!(!net.is_quiescent());
    }

    #[test]
    fn test_heal_all_clears_partitions() {
        let mut net = test_net();
        net.partition(0, 1);
        net.partition(2, 3);
        net.heal_all();
        assert_eq!(net.send(msg(0, 1, 0)), SendResult::Enqueued);
        assert_eq!(net.send(msg(2, 3, 0)), SendResult::Enqueued);
    }

    #[test]
    fn test_drop_messages_for_peer() {
        let mut net = test_net();
        net.send(msg(0, 1, 10));
        net.send(msg(2, 1, 10));
        net.send(msg(1, 2, 10));
        net.send(msg(0, 2, 10));
        assert_eq!(net.pending_count(), 4);

        net.drop_messages_for(1);
        // Only msg(0, 2, 10) should remain.
        assert_eq!(net.pending_count(), 1);
    }

    #[test]
    fn test_link_config_loss() {
        let mut net = test_net();
        net.configure_link(
            0,
            1,
            LinkConfig {
                latency: 0..1,
                loss: 1.0, // always drop
                reorder: 0.0,
            },
        );
        assert_eq!(net.send(msg(0, 1, 0)), SendResult::DroppedLoss);
    }

    #[test]
    fn test_link_config_latency() {
        let mut net = test_net();
        net.configure_link(
            0,
            1,
            LinkConfig {
                latency: 10..20,
                loss: 0.0,
                reorder: 0.0,
            },
        );
        net.send(msg(0, 1, 0));
        // Message should not be deliverable at tick 0 (latency adds 10..20).
        let ready = net.deliver_ready(0);
        assert!(ready.is_empty());
        // Should be deliverable at tick 20.
        let ready = net.deliver_ready(20);
        assert_eq!(ready[&1].len(), 1);
    }
}
