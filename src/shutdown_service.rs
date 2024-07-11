#[cfg(all(unix, feature = "unix-signals"))]
use tokio_util::sync::CancellationToken;

#[cfg(all(unix, feature = "unix-signals"))]
pub(crate) async fn shutdown_unix(cancellation_token: CancellationToken) {
    use log::{error, info};
    use signal_hook::iterator::Signals;

    let mut signals = match Signals::new(&[signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM, signal_hook::consts::SIGQUIT, signal_hook::consts::SIGKILL, signal_hook::consts::SIGSTOP]) {
        Ok(signals) => signals,
        Err(e) => {
            error!("Failed to create signal handler: {}", e);
            return;
        }
    };

    for signal in signals.forever() {
        match signal {
            signal_hook::consts::SIGINT => {
                // Handle SIGINT signal
                error!("Received SIGINT signal, shutting down");
                cancellation_token.cancel();
            }
            signal_hook::consts::SIGTERM => {
                // Handle SIGTERM signal
                error!("Received SIGTERM signal, shutting down");
                cancellation_token.cancel();
            }
            signal_hook::consts::SIGQUIT => {
                // Handle SIGQUIT signal
                error!("Received SIGQUIT signal, shutting down");
                cancellation_token.cancel();
            }
            signal_hook::consts::SIGKILL => {
                // Handle SIGKILL signal
                error!("Received SIGKILL signal, shutting down");
                cancellation_token.cancel();
            }
            signal_hook::consts::SIGSTOP => {
                // Handle SIGSTOP signal
                error!("Received SIGSTOP signal, shutting down");
                cancellation_token.cancel();
            }
            other => {
                // Handle other signals
                unreachable!("Other Signals Are Not Supported, Received Signal code: {}", other)
            }
        }
    }

    info!("Server Intiating Graceful Shutdown")
}