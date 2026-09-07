//! The service loop shared by every relay binary: serve HTTP until the
//! relay actor exits, and treat that exit as fatal.

use std::future::IntoFuture;

use axum::Router;
use axum::serve::ListenerExt;
use tokio::net::TcpListener;
use tokio::task::{JoinError, JoinHandle};

/// Serve `app` on `listener` until the HTTP server stops or the relay actor
/// exits, whichever comes first.
///
/// The actor is the process: every connection funnels into it, so an actor
/// that has stopped leaves a server that still accepts sockets and answers
/// health checks while no command is ever processed. Its exit, panic or
/// clean, is therefore an error here, so the process ends non-zero and the
/// supervisor restarts it. Accepted sockets get `TCP_NODELAY`.
pub async fn serve(
    listener: TcpListener,
    app: Router,
    relay_handle: JoinHandle<()>,
) -> anyhow::Result<()> {
    let listener = listener.tap_io(|tcp| {
        let _ = tcp.set_nodelay(true);
    });
    tokio::select! {
        served = axum::serve(listener, app).into_future() => served.map_err(Into::into),
        exited = relay_handle => Err(actor_exit_error(exited)),
    }
}

/// The error a stopped relay actor becomes. A panic keeps its message so
/// the exit line names the cause.
fn actor_exit_error(exit: Result<(), JoinError>) -> anyhow::Error {
    match exit {
        Ok(()) => {
            anyhow::anyhow!("relay actor stopped; exiting so the supervisor restarts the process")
        }
        Err(e) if e.is_panic() => {
            let message = kutl_core::panic_payload_message(e.into_panic().as_ref());
            anyhow::anyhow!(
                "relay actor panicked: {message}; exiting so the supervisor restarts the process"
            )
        }
        Err(e) => anyhow::anyhow!("relay actor task ended: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn loopback() -> TcpListener {
        TcpListener::bind("127.0.0.1:0").await.unwrap()
    }

    #[tokio::test]
    async fn test_serve_fails_when_the_actor_panics() {
        let actor = tokio::spawn(async { panic!("actor probe") });
        let err = serve(loopback().await, Router::new(), actor)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("relay actor panicked: actor probe"),
            "got {err}"
        );
    }

    #[tokio::test]
    async fn test_serve_fails_when_the_actor_stops_cleanly() {
        let actor = tokio::spawn(async {});
        let err = serve(loopback().await, Router::new(), actor)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("relay actor stopped"), "got {err}");
    }

    /// A live actor keeps the server up: the select must not resolve on its
    /// own while the actor is still running.
    #[tokio::test]
    async fn test_serve_keeps_running_while_the_actor_lives() {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let actor = tokio::spawn(async move {
            let _ = stop_rx.await;
        });
        let serving = serve(loopback().await, Router::new(), actor);
        tokio::pin!(serving);
        let early = tokio::time::timeout(std::time::Duration::from_millis(50), &mut serving).await;
        assert!(early.is_err(), "serve resolved while the actor was alive");
        drop(stop_tx);
        assert!(serving.await.is_err());
    }
}
