//! Decoders for the shapes the text-tier files had before the ones they
//! have now. The text tier's JSON fallback never expires: every file it
//! covers is a source of truth, so the fallback costs three lines and a
//! stale file is never a rebuild. The shapes live here, not on the live
//! types, so a live type describes only what the current writer produces.

use serde::Deserialize;

use crate::space_registry::SpaceRegistry;

/// The space registry's original JSON shape: entries were `{path, relay_url}`
/// objects before the registry held plain paths.
#[derive(Deserialize)]
pub(crate) struct SpaceRegistryJsonV0 {
    #[serde(default)]
    spaces: Vec<SpaceRegistryEntryJsonV0>,
}

#[derive(Deserialize)]
struct SpaceRegistryEntryJsonV0 {
    path: String,
}

impl From<SpaceRegistryJsonV0> for SpaceRegistry {
    fn from(v0: SpaceRegistryJsonV0) -> Self {
        Self {
            spaces: v0.spaces.into_iter().map(|e| e.path).collect(),
        }
    }
}
