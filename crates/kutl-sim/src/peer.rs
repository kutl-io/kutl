//! Simulation peer wrapping a [`Document`].

use kutl_core::{AgentId, Boundary, Document, EditContext, Result, SharedEnv};

use crate::store::DocSnapshot;

/// A simulated peer with a name, agent id, and document.
#[derive(Debug)]
pub struct Peer {
    pub(crate) name: String,
    pub(crate) agent: AgentId,
    pub(crate) doc: Document,
}

impl Peer {
    /// Create a new peer with the given name and environment.
    pub fn new(name: &str, env: SharedEnv) -> Result<Self> {
        let mut doc = Document::with_env(env);
        let agent = doc.register_agent(name)?;
        Ok(Self {
            name: name.to_owned(),
            agent,
            doc,
        })
    }

    /// Capture a snapshot of the peer's current document state.
    pub fn snapshot(&self) -> DocSnapshot {
        DocSnapshot {
            dt_bytes: self.doc.encode_full(),
            changes: self.doc.changes().to_vec(),
        }
    }

    /// Restore a peer from a snapshot with a fresh environment.
    ///
    /// Creates a new `Document`, merges the snapshot bytes and changes,
    /// and re-registers the agent.
    pub fn restore(name: &str, env: SharedEnv, snapshot: &DocSnapshot) -> Result<Self> {
        let mut doc = Document::with_env(env);
        doc.merge(&snapshot.dt_bytes, &snapshot.changes)?;
        let agent = doc.register_agent(name)?;
        Ok(Self {
            name: name.to_owned(),
            agent,
            doc,
        })
    }

    /// Edit the peer's document.
    pub fn edit(
        &mut self,
        intent: &str,
        boundary: Boundary,
        f: impl FnOnce(&mut EditContext<'_>) -> Result<()>,
    ) -> Result<()> {
        self.doc.edit(self.agent, &self.name, intent, boundary, f)
    }

    /// Return the current document content.
    pub fn content(&self) -> String {
        self.doc.content()
    }

    /// Encode operations since `version` as a delta.
    pub fn encode_since(&self, version: &[usize]) -> Vec<u8> {
        self.doc.encode_since(version)
    }

    /// Encode the full document state as a self-contained blob.
    pub fn encode_full(&self) -> Vec<u8> {
        self.doc.encode_full()
    }

    /// Return changes since `version`.
    pub fn changes_since(&self, version: &[usize]) -> Vec<kutl_core::Change> {
        self.doc.changes_since(version)
    }

    /// Merge a remote delta and its changes.
    pub fn merge(&mut self, dt_bytes: &[u8], changes: &[kutl_core::Change]) -> Result<()> {
        self.doc.merge(dt_bytes, changes)
    }

    /// Return a reference to the peer's change metadata.
    pub fn changes(&self) -> &[kutl_core::Change] {
        self.doc.changes()
    }

    /// Return the current local version.
    pub fn local_version(&self) -> Vec<usize> {
        self.doc.local_version()
    }
}
