//! Read-through loading for the text-tier files under `$KUTL_HOME` and a
//! space's `.kutl/`: TOML on disk, with a JSON fallback for files written
//! before the TOML move.
//!
//! The fallback never expires. Every file that comes through here is a
//! source of truth — a device key, a bearer token, the space registry, relay
//! pins, a space's own identity — so a file that was never rewritten must keep
//! loading, and the three lines the fallback costs are cheaper than losing
//! any of them. A JSON hit is rewritten as TOML on the spot, best effort:
//! write the TOML through the caller's writer, re-read it, then remove the
//! JSON. Any failure after the JSON parse is logged and swallowed, so a
//! read-only home (an identity mounted from a secret) keeps working on its
//! JSON indefinitely. Every writer goes through [`save`], which retires the
//! JSON sibling once the TOML is in place, so the two spellings never both
//! stand as sources.
//!
//! A shape the JSON was written in before the current one is decoded by
//! [`load_with_legacy`] through a frozen type in [`crate::legacy`]; the
//! live types describe only what the current writer produces.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Serialize, de::DeserializeOwned};

/// The JSON sibling of a `.toml` path: same directory, same stem.
#[must_use]
pub fn json_sibling(toml_path: &Path) -> PathBuf {
    toml_path.with_extension("json")
}

/// The spelling of `toml_path` that exists, TOML preferred; `None` when
/// neither does.
#[must_use]
pub fn existing(toml_path: &Path) -> Option<PathBuf> {
    if toml_path.exists() {
        return Some(toml_path.to_path_buf());
    }
    let json = json_sibling(toml_path);
    json.exists().then_some(json)
}

/// Whether either spelling of the file exists.
#[must_use]
pub fn exists(toml_path: &Path) -> bool {
    existing(toml_path).is_some()
}

/// Whether `path` carries either spelling's extension: the filter a
/// directory listing of text-tier files applies.
#[must_use]
pub fn is_text_file(path: &Path) -> bool {
    path.extension()
        .is_some_and(|ext| ext == "toml" || ext == "json")
}

/// Load `toml_path`, falling back to its JSON sibling. `Ok(None)` when
/// neither exists; a present-but-unparseable file of either spelling is an
/// error, because these are source-of-truth files the user fixes. A TOML
/// file that is present but blank is damage (a crash truncated it), not an
/// empty document: an empty registry or trust record would silently reset
/// where the file it replaced was a parse error.
///
/// `write` is the caller's writer for the TOML replacement (a locked atomic
/// replace, or an owner-only secret file), so the rewrite keeps whatever
/// permission and locking contract the file has.
pub fn load<T>(toml_path: &Path, write: impl FnOnce(&Path, &str) -> Result<()>) -> Result<Option<T>>
where
    T: DeserializeOwned + Serialize,
{
    load_with_legacy::<T, T>(toml_path, write)
}

/// [`load`], with a second JSON shape: when the JSON sibling is not the
/// current shape `T`, it is decoded as the frozen earlier shape `L` and
/// converted. The TOML path never carries the old shape, because TOML was
/// only ever written from `T`.
pub fn load_with_legacy<T, L>(
    toml_path: &Path,
    write: impl FnOnce(&Path, &str) -> Result<()>,
) -> Result<Option<T>>
where
    T: DeserializeOwned + Serialize,
    L: DeserializeOwned + Into<T>,
{
    match std::fs::read_to_string(toml_path) {
        Ok(data) => {
            if data.trim().is_empty() {
                anyhow::bail!(
                    "{} is present but blank; it was likely truncated by a crash — restore it or delete it to start over",
                    toml_path.display()
                );
            }
            let value = toml::from_str(&data)
                .with_context(|| format!("failed to parse {}", toml_path.display()))?;
            return Ok(Some(value));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(e).with_context(|| format!("failed to read {}", toml_path.display()));
        }
    }
    let json_path = json_sibling(toml_path);
    let data = match std::fs::read_to_string(&json_path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e).with_context(|| format!("failed to read {}", json_path.display()));
        }
    };
    let value: T = serde_json::from_str::<T>(&data)
        .or_else(|current_err| {
            serde_json::from_str::<L>(&data)
                .map(Into::into)
                .map_err(|_| current_err)
        })
        .with_context(|| format!("failed to parse {}", json_path.display()))?;
    migrate(&value, toml_path, &json_path, write);
    Ok(Some(value))
}

/// The TOML text a text-tier value is written as: the one serializer, so a
/// file's spelling is decided in one place whether it is saved, rewritten
/// from JSON, or printed for a provisioning script.
pub fn to_toml<T: Serialize>(value: &T) -> Result<String> {
    toml::to_string_pretty(value).context("failed to serialize as toml")
}

/// Write `value` as TOML to `toml_path` through the caller's writer, then
/// retire the JSON sibling: once the TOML is in place the JSON is a second
/// source that a later read (with the TOML gone) would resurrect. A sibling
/// that cannot be removed is logged, not an error: the TOML is written and
/// wins every read that finds it.
pub fn save<T: Serialize>(
    toml_path: &Path,
    value: &T,
    write: impl FnOnce(&Path, &str) -> Result<()>,
) -> Result<()> {
    write(toml_path, &to_toml(value)?)?;
    let json_path = json_sibling(toml_path);
    if json_path == toml_path {
        return Ok(());
    }
    retire_json(toml_path, &json_path);
    Ok(())
}

/// Remove the JSON sibling of a TOML that is already in place and proven.
/// A sibling that is already gone is the expected state (the first rewrite,
/// or another process's, retired it), and one that cannot be removed is
/// logged, not an error: the TOML wins every read that finds it. Never
/// called before the TOML is proven, because the JSON is the only other
/// copy until then.
fn retire_json(toml_path: &Path, json_path: &Path) {
    match std::fs::remove_file(json_path) {
        Ok(()) => tracing::info!(
            from = %json_path.display(),
            to = %toml_path.display(),
            "retired the json copy after writing toml"
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => tracing::warn!(
            path = %json_path.display(),
            error = %e,
            "could not retire the json copy; the toml file wins every read"
        ),
    }
}

/// Rewrite a JSON-sourced value as TOML: write, re-read, then retire the
/// JSON, so the two spellings never both stand as sources and the JSON is
/// only removed once the TOML has been proven readable. A TOML that was
/// written but failed its re-read is removed again, so an unproven file
/// never shadows the JSON that still loads. Only the write and its
/// re-read are provable: the JSON removal comes after, and its outcome
/// never rolls the proven TOML back, because two processes can read the
/// same JSON before either rewrites it, and the second one finds the JSON
/// already gone. Rolling back there would leave neither spelling on disk.
fn migrate<T>(
    value: &T,
    toml_path: &Path,
    json_path: &Path,
    write: impl FnOnce(&Path, &str) -> Result<()>,
) where
    T: DeserializeOwned + Serialize,
{
    let mut written = false;
    let proven = (|| -> Result<()> {
        write(toml_path, &to_toml(value)?)?;
        written = true;
        let back = std::fs::read_to_string(toml_path)
            .with_context(|| format!("failed to re-read {}", toml_path.display()))?;
        toml::from_str::<T>(&back)
            .with_context(|| format!("failed to re-parse {}", toml_path.display()))?;
        Ok(())
    })();
    match proven {
        Ok(()) => {
            tracing::info!(
                from = %json_path.display(),
                to = %toml_path.display(),
                "rewrote as toml"
            );
            retire_json(toml_path, json_path);
        }
        Err(e) => {
            let unproven_removed = written
                && std::fs::remove_file(toml_path)
                    .map_or_else(|e| e.kind() == std::io::ErrorKind::NotFound, |()| true);
            tracing::warn!(
                from = %json_path.display(),
                to = %toml_path.display(),
                error = %e,
                unproven_toml_removed = unproven_removed,
                "could not rewrite as toml; the json copy stays in use"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use tempfile::TempDir;

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Sample {
        name: String,
        count: u32,
    }

    fn plain_write(p: &Path, s: &str) -> Result<()> {
        std::fs::write(p, s)?;
        Ok(())
    }

    #[test]
    fn test_load_missing_is_none() {
        let dir = TempDir::new().unwrap();
        let got: Option<Sample> = load(&dir.path().join("x.toml"), plain_write).unwrap();
        assert!(got.is_none());
    }

    #[test]
    fn test_load_toml_wins_over_json() {
        let dir = TempDir::new().unwrap();
        let toml_path = dir.path().join("x.toml");
        std::fs::write(&toml_path, "name = \"toml\"\ncount = 1\n").unwrap();
        std::fs::write(json_sibling(&toml_path), r#"{"name":"json","count":2}"#).unwrap();
        let got: Sample = load(&toml_path, plain_write).unwrap().unwrap();
        assert_eq!(got.name, "toml");
        assert!(
            json_sibling(&toml_path).exists(),
            "an unread json sibling is left alone"
        );
    }

    #[test]
    fn test_json_fallback_rewrites_and_removes() {
        let dir = TempDir::new().unwrap();
        let toml_path = dir.path().join("x.toml");
        let json_path = json_sibling(&toml_path);
        std::fs::write(&json_path, r#"{"name":"json","count":2}"#).unwrap();
        let got: Sample = load(&toml_path, plain_write).unwrap().unwrap();
        assert_eq!(
            got,
            Sample {
                name: "json".into(),
                count: 2
            }
        );
        assert!(toml_path.exists(), "the toml replacement was written");
        assert!(!json_path.exists(), "the json original was removed");
        let again: Sample = load(&toml_path, plain_write).unwrap().unwrap();
        assert_eq!(again, got, "the rewrite round-trips");
    }

    #[test]
    fn test_json_fallback_keeps_json_when_rewrite_fails() {
        let dir = TempDir::new().unwrap();
        let toml_path = dir.path().join("x.toml");
        let json_path = json_sibling(&toml_path);
        std::fs::write(&json_path, r#"{"name":"json","count":2}"#).unwrap();
        let refuse = |_: &Path, _: &str| -> Result<()> { anyhow::bail!("read-only home") };
        let got: Sample = load(&toml_path, refuse).unwrap().unwrap();
        assert_eq!(got.name, "json");
        assert!(!toml_path.exists());
        assert!(json_path.exists(), "the json copy stays in use");
    }

    #[test]
    fn test_unparseable_toml_is_an_error_not_a_fallback() {
        let dir = TempDir::new().unwrap();
        let toml_path = dir.path().join("x.toml");
        std::fs::write(&toml_path, "name = \n").unwrap();
        std::fs::write(json_sibling(&toml_path), r#"{"name":"json","count":2}"#).unwrap();
        let err = load::<Sample>(&toml_path, plain_write).unwrap_err();
        assert!(err.to_string().contains("failed to parse"), "{err}");
    }

    #[test]
    fn test_blank_toml_is_damage_not_an_empty_document() {
        let dir = TempDir::new().unwrap();
        let toml_path = dir.path().join("x.toml");
        std::fs::write(&toml_path, "  \n\n").unwrap();
        std::fs::write(json_sibling(&toml_path), r#"{"name":"json","count":2}"#).unwrap();
        let err = load::<Sample>(&toml_path, plain_write).unwrap_err();
        assert!(err.to_string().contains("blank"), "{err}");
    }

    /// A rewrite whose TOML fails its own re-read leaves no TOML behind:
    /// an unproven file must not shadow the JSON that still loads.
    #[test]
    fn test_failed_rewrite_removes_the_unproven_toml() {
        let dir = TempDir::new().unwrap();
        let toml_path = dir.path().join("x.toml");
        let json_path = json_sibling(&toml_path);
        std::fs::write(&json_path, r#"{"name":"json","count":2}"#).unwrap();
        let garble = |p: &Path, _: &str| -> Result<()> {
            std::fs::write(p, "name = \n")?;
            Ok(())
        };
        let got: Sample = load(&toml_path, garble).unwrap().unwrap();
        assert_eq!(got.name, "json");
        assert!(!toml_path.exists(), "the unproven toml was removed");
        assert!(json_path.exists(), "the json copy stays in use");
        let again: Sample = load(&toml_path, plain_write).unwrap().unwrap();
        assert_eq!(again, got, "the next load still works and migrates");
        assert!(toml_path.exists() && !json_path.exists());
    }

    /// Two processes read the same JSON before either rewrites it. The
    /// second one's rewrite lands after the first has already retired the
    /// JSON, so its own removal finds nothing; that is the finished state,
    /// not a failed rewrite, and the proven TOML must stay. The writer
    /// plays the first process: it completes the whole rewrite inside the
    /// second one's window.
    #[test]
    fn test_migrate_keeps_the_proven_toml_when_the_json_is_already_retired() {
        let dir = TempDir::new().unwrap();
        let toml_path = dir.path().join("x.toml");
        let json_path = json_sibling(&toml_path);
        std::fs::write(&json_path, r#"{"name":"json","count":2}"#).unwrap();
        let other_process_finished_first = |p: &Path, s: &str| -> Result<()> {
            std::fs::write(p, s)?;
            std::fs::remove_file(json_sibling(p))?;
            Ok(())
        };
        let got: Sample = load(&toml_path, other_process_finished_first)
            .unwrap()
            .unwrap();
        assert_eq!(got.name, "json");
        assert!(
            toml_path.exists(),
            "the proven toml is not rolled back over a json that is already gone"
        );
        let again: Sample = load(&toml_path, plain_write).unwrap().unwrap();
        assert_eq!(again, got, "the value survives on disk");
    }

    #[test]
    fn test_save_retires_the_json_sibling() {
        let dir = TempDir::new().unwrap();
        let toml_path = dir.path().join("x.toml");
        let json_path = json_sibling(&toml_path);
        std::fs::write(&json_path, r#"{"name":"json","count":2}"#).unwrap();
        save(
            &toml_path,
            &Sample {
                name: "toml".into(),
                count: 1,
            },
            plain_write,
        )
        .unwrap();
        assert!(toml_path.exists());
        assert!(!json_path.exists(), "the json sibling is retired");
        let got: Sample = load(&toml_path, plain_write).unwrap().unwrap();
        assert_eq!(got.name, "toml");
    }

    #[derive(Deserialize)]
    struct SampleV0 {
        name: String,
    }

    impl From<SampleV0> for Sample {
        fn from(v0: SampleV0) -> Self {
            Self {
                name: v0.name,
                count: 0,
            }
        }
    }

    #[test]
    fn test_legacy_json_shape_is_converted_and_rewritten_as_the_current_one() {
        let dir = TempDir::new().unwrap();
        let toml_path = dir.path().join("x.toml");
        let json_path = json_sibling(&toml_path);
        std::fs::write(&json_path, r#"{"name":"old"}"#).unwrap();
        let got: Sample = load_with_legacy::<Sample, SampleV0>(&toml_path, plain_write)
            .unwrap()
            .unwrap();
        assert_eq!(
            got,
            Sample {
                name: "old".into(),
                count: 0
            }
        );
        assert!(toml_path.exists() && !json_path.exists());
        let again: Sample = load(&toml_path, plain_write).unwrap().unwrap();
        assert_eq!(again, got, "the rewrite is the current shape");
    }

    #[test]
    fn test_unparseable_json_is_an_error() {
        let dir = TempDir::new().unwrap();
        let toml_path = dir.path().join("x.toml");
        std::fs::write(json_sibling(&toml_path), "{not json").unwrap();
        assert!(load::<Sample>(&toml_path, plain_write).is_err());
    }
}
