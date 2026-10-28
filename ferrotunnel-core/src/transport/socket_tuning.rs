//! Socket tuning for optimal TCP performance
//!
//! Applies low-latency and high-throughput optimizations:
//! - `TCP_NODELAY`: Disable Nagle's algorithm for lower latency
//! - Send/receive buffer sizes (see [`SOCKET_BUFFER_BYTES_ENV`])
//! - TCP keepalive: Detect dead connections faster

use socket2::SockRef;
use std::io;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::net::TcpStream;

const DEFAULT_BUFFER_BYTES: usize = 1024 * 1024;
const KEEPALIVE_TIME: Duration = Duration::from_secs(30);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Env var overriding the send and receive buffer size of every tunnel TCP
/// connection, in bytes; `0` leaves both to the OS. A fixed size turns off the
/// OS's buffer auto-tuning, and one TCP connection carries every stream of a
/// session, so it caps the session at about size / round-trip time.
pub const SOCKET_BUFFER_BYTES_ENV: &str = "FERROTUNNEL_SOCKET_BUFFER_BYTES";

static BUFFER_BYTES: OnceLock<usize> = OnceLock::new();

/// Set the buffer size for this process (`0` = OS default), for embedders that
/// configure the tunnel from their own settings. Must run before the first
/// connection; returns false if the size was already fixed, by an earlier call
/// or by a connection having read the env var or default.
pub fn set_buffer_bytes(bytes: usize) -> bool {
    BUFFER_BYTES.set(bytes).is_ok()
}

fn buffer_bytes() -> usize {
    *BUFFER_BYTES.get_or_init(|| {
        let Ok(raw) = std::env::var(SOCKET_BUFFER_BYTES_ENV) else {
            return DEFAULT_BUFFER_BYTES;
        };
        match raw.trim().parse::<usize>() {
            Ok(v) => {
                tracing::info!("Socket buffer override {SOCKET_BUFFER_BYTES_ENV}={v} (0 = OS default)");
                v
            }
            Err(e) => {
                tracing::warn!(
                    "Ignoring {SOCKET_BUFFER_BYTES_ENV}={raw:?} ({e}); using {DEFAULT_BUFFER_BYTES}"
                );
                DEFAULT_BUFFER_BYTES
            }
        }
    })
}

pub fn configure_socket(stream: &TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;

    let socket = SockRef::from(stream);

    let bytes = buffer_bytes();
    if bytes > 0 {
        socket.set_recv_buffer_size(bytes)?;
        socket.set_send_buffer_size(bytes)?;
    }

    let keepalive = socket2::TcpKeepalive::new()
        .with_time(KEEPALIVE_TIME)
        .with_interval(KEEPALIVE_INTERVAL);
    socket.set_tcp_keepalive(&keepalive)?;

    Ok(())
}

pub fn configure_socket_silent(stream: &TcpStream) {
    let _ = configure_socket(stream);
}
