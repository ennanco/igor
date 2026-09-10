//! Logging and diagnostics initialization.

use tracing_subscriber::EnvFilter;

/// Installs Igor's process-wide tracing subscriber.
///
/// `RUST_LOG` overrides `default_directive` when it is present and valid.
pub fn init(default_directive: &str) -> anyhow::Result<()> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_directive));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .map_err(|error| anyhow::anyhow!("failed to initialize tracing: {error}"))?;
    Ok(())
}
