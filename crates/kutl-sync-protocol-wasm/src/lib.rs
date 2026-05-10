//! wasm-bindgen bridge for [`kutl_sync_protocol`] (RFD 0066).
//!
//! The pure rules live in `kutl-sync-protocol` and are shared by every
//! native client (daemon, relay). This crate re-exports the two
//! rules the browser needs — `resolve_client_classification` and
//! `should_allow_editing` — so the TypeScript sync module can gate
//! editor writability on the exact same invariants the Rust side
//! enforces.
//!
//! The `LoadClass` enum is exposed as opaque `u32` discriminants on
//! the JS side (see [`WasmLoadClass`]) because wasm-bindgen doesn't
//! round-trip `#[non_exhaustive]`/struct-variant enums cleanly. The
//! TS helper unpacks the discriminant plus the optional
//! `expected_version` from two separate exports; this keeps the
//! wire-level shape trivial and the API discoverable.

use kutl_sync_protocol::{LoadClass, resolve_client_classification, should_allow_editing};
use wasm_bindgen::prelude::*;

/// JS-friendly discriminant for [`LoadClass`]. Matches the proto
/// `LoadStatus` enum's integer values so TS can pun between the two
/// when it already has the proto value.
#[wasm_bindgen]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WasmLoadClass {
    /// Same wire value as `LoadStatus::UNSPECIFIED`. Only returned when
    /// an old relay sends no classification; the resolver treats it as
    /// `Unavailable` (fail-closed).
    Unspecified = 0,
    Found = 1,
    Empty = 2,
    Inconsistent = 3,
    Unavailable = 4,
}

impl WasmLoadClass {
    fn from_class(class: LoadClass) -> Self {
        match class {
            // Text and blob both surface as `Found` to the UI. The
            // editor-vs-viewer routing is driven by `doc.contentType`
            // from the HTTP API (the authoritative content-type source
            // since sideband magic-byte detection at upload time) — not
            // from the sync classification. If a future requirement
            // needs the classification layer itself to distinguish
            // text vs blob, un-flatten this arm and expose a separate
            // `FoundBlob` discriminant on the WASM enum.
            LoadClass::FoundContent | LoadClass::FoundBlob => WasmLoadClass::Found,
            LoadClass::Empty => WasmLoadClass::Empty,
            LoadClass::Inconsistent { .. } => WasmLoadClass::Inconsistent,
            LoadClass::Unavailable => WasmLoadClass::Unavailable,
        }
    }

    fn to_class(self, expected_version: u64) -> LoadClass {
        match self {
            WasmLoadClass::Found => LoadClass::FoundContent,
            WasmLoadClass::Empty => LoadClass::Empty,
            WasmLoadClass::Inconsistent => LoadClass::Inconsistent { expected_version },
            WasmLoadClass::Unavailable | WasmLoadClass::Unspecified => LoadClass::Unavailable,
        }
    }
}

/// Result of the client-side resolver (RFD 0066 Decision 4), as a
/// JS-friendly struct. `expected_version` is non-zero only when
/// `class == Inconsistent`.
#[wasm_bindgen]
#[derive(Clone, Copy, Debug)]
pub struct ResolvedClassification {
    class: WasmLoadClass,
    expected_version: u64,
}

#[wasm_bindgen]
impl ResolvedClassification {
    #[wasm_bindgen(getter)]
    #[must_use]
    pub fn class(&self) -> WasmLoadClass {
        self.class
    }

    /// The witness's claimed version when `class == Inconsistent`;
    /// `0` for any other class.
    #[wasm_bindgen(getter, js_name = expectedVersion)]
    #[must_use]
    pub fn expected_version(&self) -> u64 {
        self.expected_version
    }
}

/// Client-side symmetric fail-closed rule (RFD 0066 Decision 4).
///
/// Wraps [`kutl_sync_protocol::resolve_client_classification`] for
/// JS callers. See the native docs for rule details.
///
/// `server_class` and `server_expected_version` together encode the
/// server's `SubscribeStatus` payload. `has_local_content` is the
/// caller's answer to "does the in-memory adapter for this doc have
/// anything in it?".
#[wasm_bindgen(js_name = resolveClientClassification)]
#[must_use]
pub fn resolve_client_classification_wasm(
    server_class: WasmLoadClass,
    server_expected_version: u64,
    has_local_content: bool,
) -> ResolvedClassification {
    let server_native = server_class.to_class(server_expected_version);
    let resolved = resolve_client_classification(server_native, has_local_content);
    let expected_version = match resolved {
        LoadClass::Inconsistent { expected_version } => expected_version,
        _ => 0,
    };
    ResolvedClassification {
        class: WasmLoadClass::from_class(resolved),
        expected_version,
    }
}

/// Hard edit guard (RFD 0066 Decision 3).
///
/// Wraps [`kutl_sync_protocol::should_allow_editing`] for JS. Returns
/// `true` only for `Found` and `Empty`; every other class — including
/// `Unspecified` — fails closed.
#[wasm_bindgen(js_name = shouldAllowEditing)]
#[must_use]
pub fn should_allow_editing_wasm(class: WasmLoadClass, expected_version: u64) -> bool {
    let native = class.to_class(expected_version);
    should_allow_editing(native)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_resolve_symmetric_rule() {
        let result = resolve_client_classification_wasm(WasmLoadClass::Empty, 0, true);
        assert_eq!(result.class, WasmLoadClass::Inconsistent);
    }

    #[test]
    fn wraps_edit_guard() {
        assert!(should_allow_editing_wasm(WasmLoadClass::Found, 0));
        assert!(should_allow_editing_wasm(WasmLoadClass::Empty, 0));
        assert!(!should_allow_editing_wasm(WasmLoadClass::Inconsistent, 7));
        assert!(!should_allow_editing_wasm(WasmLoadClass::Unavailable, 0));
        // Unspecified fails closed.
        assert!(!should_allow_editing_wasm(WasmLoadClass::Unspecified, 0));
    }
}
