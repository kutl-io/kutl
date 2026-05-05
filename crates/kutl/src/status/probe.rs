//! Relay reachability probe — the only network-touching status step.

use super::schema::RelayInfo;

/// Per-relay reachability probe timeout.
const RELAY_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Dial each relay's HTTP equivalent in parallel and update reachability.
///
/// Uses a HEAD against the relay's HTTP base URL with a short timeout.
/// Any 2xx, 3xx, or 4xx response counts as reachable — we are testing
/// TCP/TLS reachability and the relay being responsive at all, not endpoint
/// correctness.
pub async fn probe_relays(relays: &mut [RelayInfo]) {
    let client = match reqwest::Client::builder()
        .timeout(RELAY_PROBE_TIMEOUT)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            for r in relays.iter_mut() {
                r.error = Some(format!("client init failed: {e}"));
            }
            return;
        }
    };

    let probes = relays.iter().map(|r| {
        let client = client.clone();
        let url = r.url.clone();
        async move {
            let http = kutl_client::ws_url_to_http(&url);
            match client.head(&http).send().await {
                Ok(resp) => Ok(resp.status().as_u16()),
                Err(e) => Err(e.to_string()),
            }
        }
    });

    let results = futures_util::future::join_all(probes).await;
    for (relay, result) in relays.iter_mut().zip(results) {
        match result {
            Ok(_status) => {
                relay.reachable = true;
                relay.error = None;
            }
            Err(e) => {
                relay.reachable = false;
                relay.error = Some(e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_probe_relays_marks_unreachable_url() {
        let mut relays = vec![RelayInfo {
            url: "ws://127.0.0.1:1/bogus".into(),
            reachable: false,
            error: None,
        }];

        probe_relays(&mut relays).await;

        assert!(!relays[0].reachable, "bogus URL should not be reachable");
        assert!(relays[0].error.is_some(), "should record an error string");
    }
}
