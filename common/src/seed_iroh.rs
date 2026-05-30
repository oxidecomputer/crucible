//! Seed iroh transport for Crucible Upstairs ↔ Downstairs.
//!
//! Replaces Crucible's native TCP on the seed mesh: both sides build an
//! iroh endpoint (secret key from env, injected by seed-agent at spawn) and
//! exchange the Crucible wire protocol over a bidirectional QUIC stream on
//! the `seed/crucible/v1` ALPN. The resulting `(SendStream, RecvStream)` is
//! fed to Crucible's existing generic `cmd_loop`/`proc` handlers — the wire
//! framing (tokio_util codec + bincode) is unchanged.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::OnceLock;

use anyhow::{Context, Result};
use iroh::endpoint::presets::Minimal;
use iroh::endpoint::{RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, PublicKey, SecretKey};

/// ALPN for Crucible block replication over the seed mesh.
pub const ALPN: &[u8] = b"seed/crucible/v1";

/// Process-global iroh endpoint, installed once at startup by the binary.
/// The Upstairs connect seam reads it; `None` means TCP transport.
static ENDPOINT: OnceLock<Endpoint> = OnceLock::new();

/// Upstairs target map: the placeholder `SocketAddr` Crucible still uses to
/// identify each Downstairs → that Downstairs's real iroh [`EndpointAddr`].
static TARGETS: OnceLock<HashMap<SocketAddr, EndpointAddr>> = OnceLock::new();

/// Install the process iroh endpoint and (for the Upstairs) the
/// placeholder-`SocketAddr` → iroh-`EndpointAddr` target map. Idempotent —
/// the binary calls this once at startup. Downstairs pass an empty `targets`.
pub fn init(endpoint: Endpoint, targets: Vec<(SocketAddr, EndpointAddr)>) {
    let _ = ENDPOINT.set(endpoint);
    let _ = TARGETS.set(targets.into_iter().collect());
}

/// The process iroh endpoint, if iroh transport is active.
#[must_use]
pub fn endpoint() -> Option<&'static Endpoint> {
    ENDPOINT.get()
}

/// The iroh target for the Downstairs identified by `addr` (the placeholder
/// `SocketAddr` Crucible threads through `CrucibleOpts.target`).
#[must_use]
pub fn iroh_target_for(addr: SocketAddr) -> Option<EndpointAddr> {
    TARGETS.get().and_then(|m| m.get(&addr).cloned())
}

/// Parse a `<node_id_hex>@<socket_addr>` target string into an
/// [`EndpointAddr`] with a direct IP address (localhost / direct dialing).
///
/// # Errors
///
/// Returns an error if the format, NodeId, or socket address is invalid.
pub fn parse_target(s: &str) -> Result<EndpointAddr> {
    let (id, addr) = s
        .split_once('@')
        .with_context(|| format!("target must be <node_id>@<addr>, got {s:?}"))?;
    let key = PublicKey::from_str(id.trim()).context("invalid node id in target")?;
    let socket: SocketAddr = addr.trim().parse().context("invalid socket addr in target")?;
    Ok(EndpointAddr::new(key).with_ip_addr(socket))
}

/// This endpoint's own `<node_id>@<addr>` string for the *first* bound socket,
/// for a peer to dial it directly. Returns `None` if no socket is bound.
#[must_use]
pub fn local_target_string(endpoint: &Endpoint) -> Option<String> {
    let socket = endpoint.bound_sockets().into_iter().next()?;
    // `bound_sockets()` reports the wildcard bind (e.g. 0.0.0.0:port), which
    // is not dialable; rewrite an unspecified IP to loopback so a same-host
    // peer can dial it directly (harness / co-located). Production reach uses
    // mesh discovery, not this string.
    let dial = if socket.ip().is_unspecified() {
        SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), socket.port())
    } else {
        socket
    };
    Some(format!("{}@{}", endpoint.id(), dial))
}

/// Env var carrying this process's hex-encoded (64-char) iroh secret key,
/// injected by seed-agent at spawn (the env-at-spawn subprocess model).
pub const SECRET_ENV: &str = "SEED_IROH_SECRET";

/// Parse the iroh secret key from [`SECRET_ENV`] (64 hex chars), if set.
///
/// # Errors
///
/// Returns an error if the variable is present but not 64 valid hex chars.
pub fn secret_from_env() -> Result<Option<SecretKey>> {
    let Ok(raw) = std::env::var(SECRET_ENV) else {
        return Ok(None);
    };
    if raw.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(secret_from_hex(&raw)?))
}

/// Parse a 64-char hex iroh secret key.
///
/// # Errors
///
/// Returns an error if `s` is not 64 valid hex chars.
pub fn secret_from_hex(s: &str) -> Result<SecretKey> {
    Ok(SecretKey::from_bytes(&hex32(s.trim())?))
}

/// Build a minimal iroh endpoint for the Crucible transport.
///
/// Uses the `Minimal` preset (no n0 relays/discovery); peers are dialed by
/// explicit [`EndpointAddr`]. Production mesh discovery is layered on later.
///
/// # Errors
///
/// Returns an error if the endpoint cannot bind.
pub async fn build_endpoint(secret: SecretKey) -> Result<Endpoint> {
    let mut builder = Endpoint::builder(Minimal)
        .secret_key(secret)
        .alpns(vec![ALPN.to_vec()]);
    // SEED_IROH_PORT pins the bind port so the control plane can compute a
    // Downstairs's dial address (NodeId@ip:port) deterministically from the
    // assigned secret — no addr-reporting handshake. Unset → ephemeral.
    if let Ok(p) = std::env::var("SEED_IROH_PORT") {
        if let Ok(port) = p.parse::<u16>() {
            builder = builder
                .bind_addr(std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, port))
                .map_err(|e| anyhow::anyhow!("bind iroh port {port}: {e}"))?;
        }
    }
    builder
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("bind iroh endpoint: {e}"))
}

/// Dial `addr` on the Crucible ALPN and open a bidirectional stream.
///
/// # Errors
///
/// Returns an error if the connection or stream cannot be established.
pub async fn connect(endpoint: &Endpoint, addr: EndpointAddr) -> Result<(SendStream, RecvStream)> {
    let conn = endpoint
        .connect(addr, ALPN)
        .await
        .map_err(|e| anyhow::anyhow!("iroh connect: {e}"))?;
    conn.open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("iroh open_bi: {e}"))
}

/// Accept the next inbound Crucible connection and its first bidirectional
/// stream. Returns `Ok(None)` once the endpoint is closed.
///
/// # Errors
///
/// Returns an error if accepting the connection or stream fails.
pub async fn accept(endpoint: &Endpoint) -> Result<Option<(SendStream, RecvStream)>> {
    let Some(incoming) = endpoint.accept().await else {
        return Ok(None);
    };
    let conn = incoming
        .await
        .map_err(|e| anyhow::anyhow!("iroh accept connection: {e}"))?;
    let stream = conn
        .accept_bi()
        .await
        .map_err(|e| anyhow::anyhow!("iroh accept_bi: {e}"))?;
    Ok(Some(stream))
}

fn hex32(s: &str) -> Result<[u8; 32]> {
    if s.len() != 64 {
        anyhow::bail!("{SECRET_ENV} must be 64 hex chars, got {}", s.len());
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .with_context(|| format!("invalid hex at byte {i}"))?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex32_roundtrips() {
        let key = [0xABu8; 32];
        let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex32(&hex).unwrap(), key);
    }

    #[test]
    fn hex32_rejects_wrong_length() {
        assert!(hex32("dead").is_err());
    }
}
