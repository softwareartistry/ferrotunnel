//! Server-timed link probe to one client: heartbeat round trip, then burst streams over real tunnel frames.

use super::multiplexer::{Multiplexer, VirtualStream};
use ferrotunnel_common::{Result, TunnelError};
use ferrotunnel_protocol::frame::{Protocol, StreamPriority};
use rand::RngCore;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;
use tracing::debug;

/// Handshake capability a client advertises when it answers probes.
pub const PROBE_CAPABILITY: &str = "probe";

/// `OpenStream` header asking the client for a burst; the value is the byte count.
pub const BURST_HEADER: &str = "x-ferrotunnel-probe-burst";

/// Largest burst a client sends on one stream.
pub const MAX_BURST_BYTES: u64 = 1024 * 1024 * 1024;

/// Most burst streams one probe opens.
pub const MAX_BURST_STREAMS: u32 = 16;

// Bytes received before this point are TCP slow start, not link speed.
const WARMUP: Duration = Duration::from_secs(1);

const CHUNK_BYTES: usize = 64 * 1024;

/// The byte count of a burst request, if `headers` carry one.
pub(crate) fn burst_request(headers: &[(String, String)]) -> Option<u64> {
    headers
        .iter()
        .find(|(k, _)| k == BURST_HEADER)
        .and_then(|(_, v)| v.parse::<u64>().ok())
        .map(|n| n.min(MAX_BURST_BYTES))
}

/// Client side: write `bytes` of random filler into `stream`, then close it.
pub(crate) async fn serve_burst(mut stream: VirtualStream, bytes: u64) {
    // Random, not zeros, so nothing on the path can shrink the payload.
    let mut chunk = vec![0u8; CHUNK_BYTES];
    rand::rng().fill_bytes(&mut chunk);
    let mut left = bytes;
    while left > 0 {
        let n = usize::try_from(left).map_or(CHUNK_BYTES, |l| l.min(CHUNK_BYTES));
        if let Err(e) = stream.write_all(&chunk[..n]).await {
            debug!("probe burst stopped: {e}");
            return;
        }
        left -= n as u64;
    }
    let _ = stream.shutdown().await;
}

/// One burst stream as the server saw it; `elapsed` is first byte to last byte.
#[derive(Debug, Clone)]
pub struct StreamBurst {
    pub bytes: u64,
    pub elapsed: Duration,
}

/// All burst streams of one probe; `steady` excludes the warm-up second and is `None` if the burst ended inside it.
#[derive(Debug, Clone)]
pub struct BurstResult {
    pub bytes: u64,
    pub elapsed: Duration,
    pub steady: Option<(u64, Duration)>,
    pub streams: Vec<StreamBurst>,
}

/// Result of one probe: a round trip, then a burst.
#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub rtt: Duration,
    pub burst: BurstResult,
}

/// Megabits per second for `bytes` moved in `elapsed`.
pub fn mbps(bytes: u64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs <= 0.0 {
        return 0.0;
    }
    #[allow(clippy::cast_precision_loss)]
    let bits = bytes as f64 * 8.0;
    bits / secs / 1_000_000.0
}

// Clock shared by the burst streams of one probe.
#[derive(Default)]
struct Clock {
    first: Option<Instant>,
    last: Option<Instant>,
    steady_bytes: u64,
}

impl Clock {
    fn record(&mut self, now: Instant, n: u64) {
        let first = *self.first.get_or_insert(now);
        self.last = Some(now);
        if now >= first + WARMUP {
            self.steady_bytes += n;
        }
    }
}

// Server side: read one burst stream to its end, timing every chunk on the shared clock.
async fn drain(mut stream: VirtualStream, clock: Arc<Mutex<Clock>>) -> Result<StreamBurst> {
    let mut buf = vec![0u8; CHUNK_BYTES];
    let mut bytes = 0u64;
    let mut first = None;
    let mut last = None;
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        let now = Instant::now();
        first.get_or_insert(now);
        last = Some(now);
        bytes += n as u64;
        if let Ok(mut c) = clock.lock() {
            c.record(now, n as u64);
        }
    }
    let elapsed = match (first, last) {
        (Some(f), Some(l)) => l - f,
        _ => Duration::ZERO,
    };
    Ok(StreamBurst { bytes, elapsed })
}

impl Multiplexer {
    /// Ask the client for `streams` concurrent bursts of `bytes` each and time them.
    pub async fn burst(&self, bytes: u64, streams: u32) -> Result<BurstResult> {
        let bytes = bytes.min(MAX_BURST_BYTES);
        let streams = streams.clamp(1, MAX_BURST_STREAMS);
        let clock = Arc::new(Mutex::new(Clock::default()));
        let mut opened = Vec::new();
        for _ in 0..streams {
            if let Some(limiter) = self.rate_limiter() {
                limiter.check_stream_open()?;
            }
            let stream = self
                .open_stream_with_headers(
                    Protocol::TCP,
                    StreamPriority::default(),
                    vec![(BURST_HEADER.to_string(), bytes.to_string())],
                )
                .await?;
            opened.push(tokio::spawn(drain(stream, clock.clone())));
        }
        let mut per_stream = Vec::with_capacity(opened.len());
        for handle in opened {
            let burst = handle
                .await
                .map_err(|e| TunnelError::Protocol(format!("probe stream task failed: {e}")))??;
            per_stream.push(burst);
        }
        let clock = clock
            .lock()
            .map_err(|_| TunnelError::Protocol("probe clock poisoned".into()))?;
        let (elapsed, steady) = match (clock.first, clock.last) {
            (Some(first), Some(last)) => {
                let steady =
                    (last > first + WARMUP).then(|| (clock.steady_bytes, last - (first + WARMUP)));
                (last - first, steady)
            }
            _ => (Duration::ZERO, None),
        };
        Ok(BurstResult {
            bytes: per_stream.iter().map(|s| s.bytes).sum(),
            elapsed,
            steady,
            streams: per_stream,
        })
    }

    /// Time one heartbeat round trip, then a burst.
    pub async fn probe(&self, bytes: u64, streams: u32, wait: Duration) -> Result<ProbeResult> {
        let rtt = self.ping(wait).await?;
        let burst = self.burst(bytes, streams).await?;
        Ok(ProbeResult { rtt, burst })
    }
}
