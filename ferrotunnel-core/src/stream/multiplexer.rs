//! Stream multiplexer using lock-free data structures
//!
//! Manages multiple virtual streams over a single connection.
//!
//! ## Performance Optimizations (P1)
//! - Larger per-stream channel capacity (128) for better throughput
//! - Reduced backpressure with larger buffers

use super::pool::ObjectPool;
use bytes::Bytes;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use ferrotunnel_common::{Result, TunnelError};
use ferrotunnel_protocol::frame::{Frame, OpenStreamFrame, Protocol, StreamPriority};
use kanal::{bounded_async, AsyncReceiver, AsyncSender, ReceiveError};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tracing::warn;

/// Maximum time to keep retrying a send to the wire channel (`frame_tx`)
/// before treating the peer as unreachable and failing the send.
///
/// The `frame_tx` channel is drained by the batched sender. If that task exits
/// (teardown) or stalls (network partition with a full channel), an unbounded
/// wait would prevent the read loop from observing EOF and hang the session.
/// Sends retry `try_send_option` with a short backoff — polling only while the
/// channel is full — and fail once this deadline passes, so teardown completes
/// promptly and a delivered frame is never reported as timed out. The trade
/// for that accuracy: blocked senders poll on 1–10 ms timers instead of
/// parking in the channel's waitlist, so wakeup order among them is not
/// preserved under sustained backpressure. See #136.
const FRAME_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Pool for reusing read buffers in `VirtualStream`
pub type ReadBufferPool = ObjectPool<Vec<u8>>;

/// P1.2: Per-stream channel capacity (was 10, now 128 for better throughput)
/// This reduces backpressure and HOL blocking in multiplex scenarios.
const STREAM_CHANNEL_CAPACITY: usize = 128;

/// New stream queue capacity
const NEW_STREAM_QUEUE_CAPACITY: usize = 32;

type CachedSender = (u32, AsyncSender<Result<Frame>>);

/// Channel item for the batched sender: priority (send order) and frame.
pub type PrioritizedFrame = (StreamPriority, Frame);

/// Manages multiple virtual streams over a single connection
///
/// Uses lock-free data structures for high-concurrency performance:
/// - `DashMap` for concurrent stream access without global locks
/// - `AtomicU32` for lock-free stream ID allocation
/// - `kanal` for fast async channel throughput
/// - `ObjectPool` for read buffer reuse
#[derive(Clone, Debug)]
pub struct Multiplexer {
    streams: Arc<DashMap<u32, AsyncSender<Result<Frame>>>>,
    /// Stream priority for send scheduling (cleaned on CloseStream).
    stream_priorities: Arc<DashMap<u32, StreamPriority>>,
    last_sender: Arc<Mutex<Option<CachedSender>>>,
    /// Set by `close_all_streams`; a stream opened after it fails instead of hanging.
    closed: Arc<AtomicBool>,
    next_stream_id: Arc<AtomicU32>,
    frame_tx: AsyncSender<PrioritizedFrame>,
    new_stream_tx: AsyncSender<VirtualStream>,
    buffer_pool: ReadBufferPool,
    rate_limiter: Option<crate::rate_limit::SessionRateLimiter>,
    /// Client side of the session: answers probe heartbeats and probe bursts.
    is_client: bool,
    /// Outstanding [`Multiplexer::ping`] calls, keyed by the nonce they sent.
    pending_pings: Arc<DashMap<u64, tokio::sync::oneshot::Sender<()>>>,
    next_ping: Arc<AtomicU64>,
}

/// Send a prioritized frame to the wire channel with a bounded deadline.
///
/// Returns a `BrokenPipe` error if the receiver has been dropped and a
/// `TimedOut` error if the channel stays full past [`FRAME_SEND_TIMEOUT`].
/// Either way the caller fails fast instead of hanging on teardown (#136).
async fn send_prioritized_frame(
    frame_tx: &AsyncSender<PrioritizedFrame>,
    frame: PrioritizedFrame,
) -> Result<()> {
    let mut item = Some(frame);
    let deadline = tokio::time::Instant::now() + FRAME_SEND_TIMEOUT;
    let mut backoff = Duration::from_millis(1);
    loop {
        match frame_tx.try_send_option(&mut item) {
            Ok(true) => return Ok(()),
            Ok(false) => {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "timed out sending frame to wire channel",
                    )
                    .into());
                }
                tokio::time::sleep(backoff.min(deadline.duration_since(now))).await;
                backoff = (backoff * 2).min(Duration::from_millis(10));
            }
            Err(e) => return Err(io::Error::new(io::ErrorKind::BrokenPipe, e.to_string()).into()),
        }
    }
}

impl Multiplexer {
    pub fn new(
        frame_tx: AsyncSender<PrioritizedFrame>,
        is_client: bool,
    ) -> (Self, AsyncReceiver<VirtualStream>) {
        let (new_stream_tx, new_stream_rx) = bounded_async(NEW_STREAM_QUEUE_CAPACITY);
        let initial_stream_id = if is_client { 1 } else { 2 };
        (
            Self {
                streams: Arc::new(DashMap::new()),
                stream_priorities: Arc::new(DashMap::new()),
                last_sender: Arc::new(Mutex::new(None)),
                closed: Arc::new(AtomicBool::new(false)),
                next_stream_id: Arc::new(AtomicU32::new(initial_stream_id)),
                frame_tx,
                new_stream_tx,
                buffer_pool: ReadBufferPool::with_default_capacity(),
                rate_limiter: None,
                is_client,
                pending_pings: Arc::new(DashMap::new()),
                next_ping: Arc::new(AtomicU64::new(1)),
            },
            new_stream_rx,
        )
    }

    /// Apply `limiter` to the stream-open rate for streams opened through
    /// [`AnyMultiplexer`](crate::stream::AnyMultiplexer).
    ///
    /// Only stream opens are metered here. Byte rates on this transport are
    /// already applied by `TunnelServer::process_messages` as frames arrive.
    #[must_use]
    pub fn with_rate_limiter(mut self, limiter: crate::rate_limit::SessionRateLimiter) -> Self {
        self.rate_limiter = Some(limiter);
        self
    }

    /// The session rate limiter applied to streams from this multiplexer, if any.
    pub fn rate_limiter(&self) -> Option<&crate::rate_limit::SessionRateLimiter> {
        self.rate_limiter.as_ref()
    }

    /// Priority for a frame when sending (used by batched sender order).
    fn priority_for_frame(
        frame: &Frame,
        priorities: &DashMap<u32, StreamPriority>,
    ) -> StreamPriority {
        match frame {
            Frame::Data { stream_id, .. } => priorities
                .get(stream_id)
                .map_or(StreamPriority::Normal, |r| *r),
            Frame::Heartbeat { .. } | Frame::HeartbeatAck { .. } | Frame::HandshakeAck { .. } => {
                StreamPriority::Critical
            }
            Frame::CloseStream { stream_id, .. } => priorities
                .get(stream_id)
                .map_or(StreamPriority::Normal, |r| *r),
            _ => StreamPriority::Normal,
        }
    }

    /// Get the buffer pool for reusing read buffers
    pub fn buffer_pool(&self) -> &ReadBufferPool {
        &self.buffer_pool
    }

    /// Allocate a new stream ID atomically (lock-free).
    ///
    /// IDs advance by two to preserve client/server parity. Returns an error
    /// rather than wrapping past `u32::MAX`, so an exhausted ID space can never
    /// silently alias a live stream.
    #[inline]
    fn allocate_stream_id(&self) -> Result<u32> {
        self.next_stream_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(2))
            .map_err(|_| TunnelError::Protocol("stream ID space exhausted".into()))
    }

    /// Send a frame directly to the wire (priority derived from frame type and stream).
    ///
    /// Bounded by a fixed `FRAME_SEND_TIMEOUT` so a full or torn-down `frame_tx`
    /// channel fails fast instead of blocking the caller forever (issue #136).
    pub async fn send_frame(&self, frame: Frame) -> Result<()> {
        let priority = Self::priority_for_frame(&frame, &self.stream_priorities);
        send_prioritized_frame(&self.frame_tx, (priority, frame)).await
    }

    /// Process an incoming frame from the wire
    ///
    /// P1.2: Uses larger channel capacity (128) to reduce backpressure.
    /// Still uses async send to ensure reliable delivery (dropping data breaks protocols).
    pub async fn process_frame(&self, frame: Frame) -> Result<()> {
        match &frame {
            // A heartbeat that reaches the client's multiplexer came from the server: echo its nonce.
            Frame::Heartbeat { timestamp } if self.is_client => {
                self.send_frame(Frame::HeartbeatAck {
                    timestamp: *timestamp,
                })
                .await?;
            }
            Frame::HeartbeatAck { timestamp } => {
                if let Some((_, waiter)) = self.pending_pings.remove(timestamp) {
                    let _ = waiter.send(());
                }
            }
            Frame::OpenStream(open_stream) => {
                let stream_id = open_stream.stream_id;
                let priority = open_stream.priority;
                // P1.2: Use larger channel capacity
                let (tx, rx) = bounded_async(STREAM_CHANNEL_CAPACITY);

                match self.streams.entry(stream_id) {
                    Entry::Occupied(_) => {
                        warn!("Stream {} already exists", stream_id);
                        return Ok(());
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(tx);
                    }
                }
                self.stream_priorities.insert(stream_id, priority);

                let read_buffer = self.buffer_pool.try_acquire().unwrap_or_default();
                let stream = VirtualStream::new(
                    stream_id,
                    rx,
                    self.frame_tx.clone(),
                    priority,
                    read_buffer,
                    self.buffer_pool.clone(),
                    open_stream.protocol,
                );

                // A probe burst is answered by the tunnel itself, never by the local service.
                if self.is_client {
                    if let Some(bytes) = super::probe::burst_request(&open_stream.headers) {
                        tokio::spawn(super::probe::serve_burst(stream, bytes));
                        return Ok(());
                    }
                }

                // OpenStream is a control path - use async send for reliability
                if self.new_stream_tx.send(stream).await.is_err() {
                    warn!("Failed to queue new stream {}", stream_id);
                    self.streams.remove(&stream_id);
                }
            }
            Frame::Data { stream_id, .. } => {
                let stream_id = *stream_id;
                let tx = self
                    .cached_sender(stream_id)
                    .or_else(|| self.lookup_and_cache_sender(stream_id));

                if let Some(tx) = tx {
                    // Use async send - dropping data breaks protocols (HTTP, etc.)
                    // P1.2: Larger channel (128) reduces chance of blocking
                    if tx.send(Ok(frame)).await.is_err() {
                        self.streams.remove(&stream_id);
                    }
                }
            }
            Frame::CloseStream { stream_id, .. } => {
                let stream_id = *stream_id;
                let tx = self
                    .cached_sender(stream_id)
                    .or_else(|| self.lookup_and_cache_sender(stream_id));

                if let Some(tx) = tx {
                    // Best effort delivery of close frame
                    let _ = tx.send(Ok(frame)).await;
                }
                self.streams.remove(&stream_id);
                self.stream_priorities.remove(&stream_id);
            }
            _ => {}
        }
        Ok(())
    }

    /// Ends every stream on this multiplexer: the session carrying them is gone.
    /// Each stream's reader gets an error instead of waiting on senders that
    /// ingress handlers keep alive through their own multiplexer clones. An error,
    /// not EOF, so a cut close-delimited response can't pass as complete.
    pub fn close_all_streams(&self) {
        self.closed.store(true, Ordering::SeqCst);
        if let Ok(mut guard) = self.last_sender.lock() {
            *guard = None;
        }
        for entry in self.streams.iter() {
            // A full channel just drops: its reader sees EOF once the queue drains.
            let _ = entry
                .value()
                .try_send(Err(TunnelError::Protocol("tunnel session closed".into())));
        }
        self.streams.clear();
        self.stream_priorities.clear();
    }

    fn cached_sender(&self, stream_id: u32) -> Option<AsyncSender<Result<Frame>>> {
        let guard = self.last_sender.try_lock().ok()?;
        let (cached_id, tx) = guard.as_ref()?;
        if *cached_id == stream_id {
            return Some(tx.clone());
        }
        None
    }

    fn lookup_and_cache_sender(&self, stream_id: u32) -> Option<AsyncSender<Result<Frame>>> {
        let tx = self.streams.get(&stream_id).map(|r| r.clone())?;
        if let Ok(mut guard) = self.last_sender.try_lock() {
            *guard = Some((stream_id, tx.clone()));
        }
        Some(tx)
    }

    /// Open a new outbound stream with default priority.
    pub async fn open_stream(&self, protocol: Protocol) -> Result<VirtualStream> {
        self.open_stream_with_priority(protocol, StreamPriority::default())
            .await
    }

    /// Open a new outbound stream with the given priority (for QoS scheduling).
    pub async fn open_stream_with_priority(
        &self,
        protocol: Protocol,
        priority: StreamPriority,
    ) -> Result<VirtualStream> {
        self.open_stream_with_headers(protocol, priority, vec![])
            .await
    }

    /// Open a new outbound stream that carries `headers` in its `OpenStream` frame.
    pub async fn open_stream_with_headers(
        &self,
        protocol: Protocol,
        priority: StreamPriority,
        headers: Vec<(String, String)>,
    ) -> Result<VirtualStream> {
        let stream_id = self.allocate_stream_id()?;

        let (tx, rx) = bounded_async(STREAM_CHANNEL_CAPACITY);
        self.streams.insert(stream_id, tx);
        // Checked after the insert: a close racing this either sees the entry or
        // has already set the flag.
        if self.closed.load(Ordering::SeqCst) {
            self.streams.remove(&stream_id);
            return Err(TunnelError::Protocol("tunnel session closed".into()));
        }

        self.stream_priorities.insert(stream_id, priority);
        send_prioritized_frame(
            &self.frame_tx,
            (
                priority,
                Frame::OpenStream(Box::new(OpenStreamFrame {
                    stream_id,
                    protocol,
                    headers,
                    body_hint: None,
                    priority,
                })),
            ),
        )
        .await?;

        let read_buffer = self.buffer_pool.try_acquire().unwrap_or_default();
        Ok(VirtualStream::new(
            stream_id,
            rx,
            self.frame_tx.clone(),
            priority,
            read_buffer,
            self.buffer_pool.clone(),
            protocol,
        ))
    }

    /// One round trip to the peer via a nonce heartbeat; a peer without the `probe` capability never echoes, so this times out.
    pub async fn ping(&self, wait: Duration) -> Result<Duration> {
        let nonce = self.next_ping.fetch_add(1, Ordering::Relaxed);
        let (waiter, echoed) = tokio::sync::oneshot::channel();
        self.pending_pings.insert(nonce, waiter);
        let started = tokio::time::Instant::now();
        if let Err(e) = self.send_frame(Frame::Heartbeat { timestamp: nonce }).await {
            self.pending_pings.remove(&nonce);
            return Err(e);
        }
        let outcome = tokio::time::timeout(wait, echoed).await;
        self.pending_pings.remove(&nonce);
        match outcome {
            Ok(Ok(())) => Ok(started.elapsed()),
            Ok(Err(_)) => Err(TunnelError::Protocol("tunnel session closed".into())),
            Err(_) => Err(TunnelError::Timeout("no heartbeat echo from peer".into())),
        }
    }
}

/// P3.2: Maximum payload size per Frame::Data.
/// Increased to 64KB to reduce framing overhead in throughput tests.
/// Chunking large writes ensures reliable decoding on the wire.
const MAX_DATA_FRAME_PAYLOAD: usize = 64 * 1024; // 64KB

/// Boxed future type for receiving frames
type RecvFuture = Pin<
    Box<dyn std::future::Future<Output = std::result::Result<Result<Frame>, ReceiveError>> + Send>,
>;

/// Boxed future type for sending frames.
///
/// Resolves to an `io::Result` because the send is bounded by
/// [`FRAME_SEND_TIMEOUT`]: a full or closed `frame_tx` surfaces as an error
/// rather than blocking the writer forever (issue #136).
type SendFuture = Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;

/// Build the bounded send future stored in `pending_send`.
///
/// `poll_write` / `poll_shutdown` cannot `.await`, so the deadline is baked
/// into the future itself; a timeout or closed channel resolves to an error.
fn bounded_send_future(tx: AsyncSender<PrioritizedFrame>, frame: PrioritizedFrame) -> SendFuture {
    Box::pin(async move {
        let mut item = Some(frame);
        let deadline = tokio::time::Instant::now() + FRAME_SEND_TIMEOUT;
        let mut backoff = Duration::from_millis(1);
        loop {
            match tx.try_send_option(&mut item) {
                Ok(true) => return Ok(()),
                Ok(false) => {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "timed out sending frame to wire channel",
                        ));
                    }
                    tokio::time::sleep(backoff.min(deadline.duration_since(now))).await;
                    backoff = (backoff * 2).min(Duration::from_millis(10));
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::BrokenPipe, e.to_string())),
            }
        }
    })
}

/// A virtual stream that implements `AsyncRead` + `AsyncWrite`
///
/// Uses kanal channels for async communication.
/// The polling implementation uses boxed futures to bridge kanal's
/// async API with tokio's poll-based traits.
///
/// Read buffers are pooled and returned when the stream is dropped.
///
/// ## Performance Optimizations (P3)
/// - P3.1: Store `pending_send_len` instead of cloning the frame
/// - P3.3: Use `read_buffer_pos` cursor instead of drain() for O(1) reads
pub struct VirtualStream {
    stream_id: u32,
    rx: AsyncReceiver<Result<Frame>>,
    tx: AsyncSender<PrioritizedFrame>,
    priority: StreamPriority,
    read_buffer: Vec<u8>,
    read_buffer_bytes: Option<Bytes>,
    /// P3.3: Cursor position in read_buffer (avoids O(n) drain)
    read_buffer_pos: usize,
    /// Pool to return `read_buffer` to when dropped
    buffer_pool: Option<ReadBufferPool>,
    /// Pending receive future for `poll_read`
    pending_recv: Option<RecvFuture>,
    /// Pending send future for `poll_write`
    pending_send: Option<SendFuture>,
    /// P3.1: Store bytes written instead of cloning frame
    pending_send_len: usize,
    /// Protocol for this stream
    protocol: Protocol,
    /// The peer ended the stream normally; later reads stay EOF.
    peer_closed: bool,
    /// We sent our normal close; dropping needs no further signal.
    close_sent: bool,
    /// `pending_send` holds our close, not a data write.
    closing: bool,
}

impl std::fmt::Debug for VirtualStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VirtualStream")
            .field("stream_id", &self.stream_id)
            .field("read_buffer_len", &self.read_buffer.len())
            .finish_non_exhaustive()
    }
}

impl VirtualStream {
    /// Create a new `VirtualStream` with a pooled read buffer.
    pub fn new(
        stream_id: u32,
        rx: AsyncReceiver<Result<Frame>>,
        tx: AsyncSender<PrioritizedFrame>,
        priority: StreamPriority,
        read_buffer: Vec<u8>,
        buffer_pool: ReadBufferPool,
        protocol: Protocol,
    ) -> Self {
        Self {
            stream_id,
            rx,
            tx,
            priority,
            read_buffer,
            read_buffer_bytes: None,
            read_buffer_pos: 0,
            buffer_pool: Some(buffer_pool),
            pending_recv: None,
            pending_send: None,
            pending_send_len: 0,
            protocol,
            peer_closed: false,
            close_sent: false,
            closing: false,
        }
    }

    pub fn id(&self) -> u32 {
        self.stream_id
    }

    pub fn protocol(&self) -> Protocol {
        self.protocol
    }
}

impl Drop for VirtualStream {
    fn drop(&mut self) {
        // Return the read buffer to the pool if available
        if let Some(pool) = self.buffer_pool.take() {
            let buffer = std::mem::take(&mut self.read_buffer);
            pool.release(buffer);
        }
        // Dropped without our close: tell the peer, or it waits on this stream forever.
        // Always an error, even after the peer's close: that only ended *its* direction,
        // and ours may have been cut short.
        if !self.close_sent {
            let reason = ferrotunnel_protocol::frame::CloseReason::Error("stream dropped".into());
            let frame = (
                self.priority,
                Frame::CloseStream {
                    stream_id: self.stream_id,
                    reason,
                },
            );
            // Full channel: queue it behind the data instead of losing it. An error
            // means the session is gone, and its teardown already fails the peer.
            let mut item = Some(frame);
            if let Ok(false) = self.tx.try_send_option(&mut item) {
                if let (Some(frame), Ok(handle)) = (item, tokio::runtime::Handle::try_current()) {
                    let tx = self.tx.clone();
                    handle.spawn(async move {
                        let _ = send_prioritized_frame(&tx, frame).await;
                    });
                }
            }
        }
    }
}

impl AsyncRead for VirtualStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(buffered_bytes) = self.read_buffer_bytes.as_mut() {
            if !buffered_bytes.is_empty() {
                let len = std::cmp::min(buf.remaining(), buffered_bytes.len());
                let chunk = buffered_bytes.split_to(len);
                buf.put_slice(&chunk);
                if buffered_bytes.is_empty() {
                    self.read_buffer_bytes = None;
                }
                return Poll::Ready(Ok(()));
            }
            self.read_buffer_bytes = None;
        }

        // P3.3: First, return any buffered data using cursor (O(1) instead of O(n) drain)
        let buffered_remaining = self.read_buffer.len() - self.read_buffer_pos;
        if buffered_remaining > 0 {
            let len = std::cmp::min(buf.remaining(), buffered_remaining);
            let start = self.read_buffer_pos;
            buf.put_slice(&self.read_buffer[start..start + len]);
            self.read_buffer_pos += len;

            // If we've consumed all buffered data, reset the buffer
            if self.read_buffer_pos >= self.read_buffer.len() {
                self.read_buffer.clear();
                self.read_buffer_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        // A normal close already ended the stream; never wait on the channel again.
        if self.peer_closed {
            return Poll::Ready(Ok(()));
        }

        // Check if we have a pending receive future
        if self.pending_recv.is_none() {
            let rx = self.rx.clone();
            self.pending_recv = Some(Box::pin(async move { rx.recv().await }));
        }

        // Poll the pending future (we set it in the block above)
        let fut = match self.pending_recv.as_mut() {
            Some(f) => f,
            None => return Poll::Pending,
        };
        match fut.as_mut().poll(cx) {
            Poll::Ready(result) => {
                self.pending_recv = None;
                match result {
                    Ok(Ok(Frame::Data { data: bytes, .. })) => {
                        let len = std::cmp::min(buf.remaining(), bytes.len());
                        buf.put_slice(&bytes[..len]);
                        if len < bytes.len() {
                            self.read_buffer_bytes = Some(bytes.slice(len..));
                        }
                        Poll::Ready(Ok(()))
                    }
                    Ok(Ok(Frame::CloseStream {
                        reason: ferrotunnel_protocol::frame::CloseReason::Normal,
                        ..
                    })) => {
                        self.peer_closed = true;
                        Poll::Ready(Ok(())) // EOF
                    }
                    // Any other close is abnormal: an error, never a clean end of data.
                    Ok(Ok(Frame::CloseStream { reason, .. })) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        format!("stream closed: {reason:?}"),
                    ))),
                    Ok(Ok(_)) => Poll::Pending, // Ignore other frame types
                    Ok(Err(e)) => Poll::Ready(Err(io::Error::other(e.to_string()))),
                    // The channel went away without a normal close (session teardown,
                    // failed delivery): abnormal unless the peer already ended it.
                    Err(ReceiveError::SendClosed | ReceiveError::Closed) if self.peer_closed => {
                        Poll::Ready(Ok(()))
                    }
                    Err(ReceiveError::SendClosed | ReceiveError::Closed) => {
                        Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "stream ended without close",
                        )))
                    }
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for VirtualStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // If we have a pending send, poll it first
        if let Some(fut) = self.pending_send.as_mut() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    // P3.1: Use stored length instead of cloning frame
                    let bytes_written = self.pending_send_len;
                    self.pending_send = None;
                    self.pending_send_len = 0;
                    return Poll::Ready(result.map(|()| bytes_written));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // Chunk large writes to stay within protocol limits
        let chunk_size = buf.len().min(MAX_DATA_FRAME_PAYLOAD);

        // Zero-copy: use Bytes::copy_from_slice for optimal performance
        // This is still a copy, but avoids BytesMut allocation overhead
        let data = Bytes::copy_from_slice(&buf[..chunk_size]);

        // P3.1: Build frame directly for sending (no clone needed)
        let frame = Frame::Data {
            stream_id: self.stream_id,
            data,
            end_of_stream: false,
        };
        let priority = self.priority;

        let tx = self.tx.clone();
        // P3.1: Store length instead of frame, move frame into future
        self.pending_send_len = chunk_size;
        self.pending_send = Some(bounded_send_future(tx, (priority, frame)));

        // Poll the new future (we set it in the block above)
        let fut = match self.pending_send.as_mut() {
            Some(f) => f,
            None => return Poll::Pending,
        };
        match fut.as_mut().poll(cx) {
            Poll::Ready(result) => {
                self.pending_send = None;
                self.pending_send_len = 0;
                Poll::Ready(result.map(|()| chunk_size))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Kanal channels don't require explicit flushing
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.close_sent {
            return Poll::Ready(Ok(()));
        }
        // If we have a pending send, poll it first
        if let Some(fut) = self.pending_send.as_mut() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    self.pending_send = None;
                    self.pending_send_len = 0;
                    // A close that had to wait is now sent: don't send another.
                    if self.closing {
                        self.close_sent = result.is_ok();
                        return Poll::Ready(result);
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // Send close frame
        let frame = Frame::CloseStream {
            stream_id: self.stream_id,
            reason: ferrotunnel_protocol::frame::CloseReason::Normal,
        };
        let priority = self.priority;

        let tx = self.tx.clone();
        self.pending_send = Some(bounded_send_future(tx, (priority, frame)));
        self.closing = true;

        let fut = match self.pending_send.as_mut() {
            Some(f) => f,
            None => return Poll::Pending,
        };
        match fut.as_mut().poll(cx) {
            Poll::Ready(result) => {
                self.pending_send = None;
                self.close_sent = result.is_ok();
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrotunnel_protocol::frame::CloseReason;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_stream_id_allocation() {
        let (tx, _rx) = bounded_async(100);

        // Client (Odd IDs)
        let (client_mux, _client_streams) = Multiplexer::new(tx.clone(), true);

        let s1 = client_mux.open_stream(Protocol::HTTP).await.unwrap();
        assert_eq!(s1.id(), 1);

        let s2 = client_mux.open_stream(Protocol::HTTP).await.unwrap();
        assert_eq!(s2.id(), 3);

        // Server (Even IDs)
        let (server_mux, _server_streams) = Multiplexer::new(tx, false);

        let s3 = server_mux.open_stream(Protocol::HTTP).await.unwrap();
        assert_eq!(s3.id(), 2);

        let s4 = server_mux.open_stream(Protocol::HTTP).await.unwrap();
        assert_eq!(s4.id(), 4);
    }

    /// Issue #136: a send must fail fast when the receiver is gone, instead of
    /// blocking the session loop forever during teardown.
    #[tokio::test]
    async fn send_frame_fails_fast_when_receiver_dropped() {
        let (tx, rx) = bounded_async::<PrioritizedFrame>(1);
        drop(rx);
        let (mux, _streams) = Multiplexer::new(tx, true);

        let result = mux.send_frame(Frame::Heartbeat { timestamp: 0 }).await;

        assert!(
            result.is_err(),
            "send to a closed frame_tx must return an error"
        );
    }

    /// Issue #136: a send must time out (not hang) when the channel stays full
    /// because the batched sender stopped draining (e.g. network partition).
    ///
    /// `start_paused` lets tokio auto-advance virtual time to the send timeout,
    /// so the test asserts fail-fast behavior without waiting wall-clock.
    #[tokio::test(start_paused = true)]
    async fn send_frame_times_out_when_channel_full() {
        let (tx, _rx) = bounded_async::<PrioritizedFrame>(1);
        let (mux, _streams) = Multiplexer::new(tx, true);

        // Fill the single slot; `_rx` is kept alive but never drained.
        mux.send_frame(Frame::Heartbeat { timestamp: 0 })
            .await
            .expect("first send fills the only slot");

        let result = mux.send_frame(Frame::Heartbeat { timestamp: 1 }).await;

        assert!(
            result.is_err(),
            "send to a full frame_tx must time out rather than hang"
        );
    }

    /// Issue #136: `VirtualStream::poll_write` must not block forever when the
    /// wire channel is full; the bounded send surfaces an error instead.
    #[tokio::test(start_paused = true)]
    async fn virtual_stream_write_times_out_when_channel_full() {
        use tokio::io::AsyncWriteExt;

        let (tx, _rx) = bounded_async::<PrioritizedFrame>(1);
        let (mux, _streams) = Multiplexer::new(tx, true);

        // `open_stream` consumes the only channel slot with its OpenStream frame.
        let mut stream = mux
            .open_stream(Protocol::HTTP)
            .await
            .expect("open_stream uses the only slot");

        // Channel is now full and never drained: the write must fail fast.
        let result = stream.write(b"hello").await;

        assert!(
            result.is_err(),
            "write to a full frame_tx must time out rather than hang"
        );
    }

    /// Issue #129: a slow virtual-stream handler must not block a fast one.
    ///
    /// Two streams are opened through the real control path (stream A first,
    /// then B). The TCP dispatch loop spawns each handler, so the fast stream
    /// B completes before the slow stream A even though A was opened first.
    /// Under the old serial `stream_handler(s).await` loop, B would be stuck
    /// behind A's sleep and this assertion would fail (or time out).
    ///
    /// `start_paused` lets tokio auto-advance virtual time past A's sleep
    /// without waiting wall-clock.
    #[tokio::test(start_paused = true)]
    async fn slow_stream_does_not_block_concurrent_stream() {
        use crate::tunnel::client::spawn_stream_dispatch_loop;

        const STREAM_A: u32 = 2;
        const STREAM_B: u32 = 4;
        const SLOW_DELAY: Duration = Duration::from_secs(10);

        let (frame_tx, _frame_rx) = bounded_async::<PrioritizedFrame>(64);
        let (mux, new_stream_rx) = Multiplexer::new(frame_tx, false);

        let (order_tx, mut order_rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

        // Fire-and-forget dispatch: A sleeps before recording, B records now.
        spawn_stream_dispatch_loop(new_stream_rx, move |stream: VirtualStream| {
            let order_tx = order_tx.clone();
            let id = stream.id();
            async move {
                if id == STREAM_A {
                    tokio::time::sleep(SLOW_DELAY).await;
                }
                let _ = order_tx.send(id);
            }
        });

        // Push the slow stream first, then the fast stream, via process_frame.
        for stream_id in [STREAM_A, STREAM_B] {
            mux.process_frame(Frame::OpenStream(Box::new(OpenStreamFrame {
                stream_id,
                protocol: Protocol::TCP,
                headers: vec![],
                body_hint: None,
                priority: StreamPriority::default(),
            })))
            .await
            .expect("process_frame queues the new stream");
        }

        // B must complete before A despite being opened second. The bounded
        // timeout fails fast if the dispatch loop serializes handlers.
        let first = timeout(Duration::from_secs(30), order_rx.recv())
            .await
            .expect("first completion within timeout")
            .expect("order channel stays open");
        let second = timeout(Duration::from_secs(30), order_rx.recv())
            .await
            .expect("second completion within timeout")
            .expect("order channel stays open");

        assert_eq!(first, STREAM_B, "fast stream B must complete first");
        assert_eq!(second, STREAM_A, "slow stream A must complete last");
    }

    /// A server-side stream plus the receiver of every frame it sends.
    async fn stream_with_wire() -> (Multiplexer, VirtualStream, AsyncReceiver<PrioritizedFrame>) {
        let (frame_tx, frame_rx) = bounded_async::<PrioritizedFrame>(64);
        let (mux, new_stream_rx) = Multiplexer::new(frame_tx, false);
        mux.process_frame(Frame::OpenStream(Box::new(OpenStreamFrame {
            stream_id: 2,
            protocol: Protocol::GRPC,
            headers: vec![],
            body_hint: None,
            priority: StreamPriority::default(),
        })))
        .await
        .unwrap();
        let stream = new_stream_rx.recv().await.unwrap();
        (mux, stream, frame_rx)
    }

    fn sent_closes(wire: &AsyncReceiver<PrioritizedFrame>) -> Vec<CloseReason> {
        let mut out = vec![];
        while let Ok(Some((_, frame))) = wire.try_recv() {
            if let Frame::CloseStream { reason, .. } = frame {
                out.push(reason);
            }
        }
        out
    }

    #[tokio::test]
    async fn shutdown_that_waits_sends_exactly_one_close() {
        use tokio::io::AsyncWriteExt;
        let (mux, mut stream, wire) = stream_with_wire().await;
        // Fill the wire so the close has to wait.
        while let Ok(true) = mux
            .frame_tx
            .try_send((StreamPriority::default(), Frame::Heartbeat { timestamp: 0 }))
        {}
        let mut shutdown = Box::pin(stream.shutdown());
        assert!(futures::poll!(&mut shutdown).is_pending());
        while let Ok(Some(_)) = wire.try_recv() {}
        shutdown.await.unwrap();
        drop(stream);
        assert!(matches!(sent_closes(&wire)[..], [CloseReason::Normal]));
    }

    #[tokio::test]
    async fn dropped_stream_tells_the_peer_with_an_error() {
        let (_mux, stream, wire) = stream_with_wire().await;
        drop(stream);
        assert!(matches!(sent_closes(&wire)[..], [CloseReason::Error(_)]));
    }

    #[tokio::test]
    async fn normal_shutdown_sends_one_normal_close_and_no_error() {
        use tokio::io::AsyncWriteExt;
        let (_mux, mut stream, wire) = stream_with_wire().await;
        stream.shutdown().await.unwrap();
        drop(stream);
        assert!(matches!(sent_closes(&wire)[..], [CloseReason::Normal]));
    }

    #[tokio::test]
    async fn drop_after_peer_closed_without_our_close_is_an_error() {
        use tokio::io::AsyncReadExt;
        let (mux, mut stream, wire) = stream_with_wire().await;
        mux.process_frame(Frame::CloseStream {
            stream_id: 2,
            reason: CloseReason::Normal,
        })
        .await
        .unwrap();
        let mut buf = [0u8; 8];
        assert_eq!(
            stream.read(&mut buf).await.unwrap(),
            0,
            "normal close is EOF"
        );
        assert_eq!(stream.read(&mut buf).await.unwrap(), 0, "and stays EOF");
        // The peer only finished its direction; ours never closed, so it may be cut short.
        drop(stream);
        assert!(matches!(sent_closes(&wire)[..], [CloseReason::Error(_)]));
    }

    #[tokio::test]
    async fn error_close_from_peer_is_a_read_error() {
        use tokio::io::AsyncReadExt;
        let (mux, mut stream, _wire) = stream_with_wire().await;
        mux.process_frame(Frame::Data {
            stream_id: 2,
            data: Bytes::from_static(b"partial"),
            end_of_stream: false,
        })
        .await
        .unwrap();
        mux.process_frame(Frame::CloseStream {
            stream_id: 2,
            reason: CloseReason::Error("stream dropped".into()),
        })
        .await
        .unwrap();
        let mut got = vec![];
        let err = stream.read_to_end(&mut got).await.unwrap_err();
        assert_eq!(got, b"partial", "data before the close is delivered intact");
        assert_eq!(err.kind(), io::ErrorKind::ConnectionReset);
    }

    #[tokio::test]
    async fn channel_lost_without_close_is_a_read_error() {
        use tokio::io::AsyncReadExt;
        let (mux, mut stream, _wire) = stream_with_wire().await;
        mux.streams.clear(); // the sender goes away with no CloseStream
        let mut buf = [0u8; 8];
        assert!(stream.read(&mut buf).await.is_err());
    }

    #[tokio::test]
    async fn close_all_streams_fails_pending_and_new_streams() {
        use tokio::io::AsyncReadExt;

        let (frame_tx, _frame_rx) = bounded_async::<PrioritizedFrame>(64);
        let (mux, new_stream_rx) = Multiplexer::new(frame_tx, false);
        // An ingress handler's clone: keeps the streams map alive after the session ends.
        let held = mux.clone();
        mux.process_frame(Frame::OpenStream(Box::new(OpenStreamFrame {
            stream_id: 2,
            protocol: Protocol::GRPC,
            headers: vec![],
            body_hint: None,
            priority: StreamPriority::default(),
        })))
        .await
        .expect("process_frame queues the new stream");
        let mut stream = new_stream_rx.recv().await.expect("stream queued");

        mux.close_all_streams();

        let mut buf = [0u8; 16];
        let read = timeout(Duration::from_secs(5), stream.read(&mut buf))
            .await
            .expect("read must not wait on a dead session");
        assert!(read.is_err(), "a cut stream must error, not look complete");

        assert!(held.open_stream(Protocol::GRPC).await.is_err());
    }

    #[tokio::test]
    async fn client_echoes_server_heartbeat_nonce() {
        let (tx, rx) = bounded_async::<PrioritizedFrame>(8);
        let (client, _streams) = Multiplexer::new(tx, true);

        client
            .process_frame(Frame::Heartbeat { timestamp: 42 })
            .await
            .unwrap();

        let (_, frame) = rx.recv().await.unwrap();
        assert_eq!(frame, Frame::HeartbeatAck { timestamp: 42 });
    }

    #[tokio::test]
    async fn server_does_not_echo_heartbeats() {
        let (tx, rx) = bounded_async::<PrioritizedFrame>(8);
        let (server, _streams) = Multiplexer::new(tx, false);

        server
            .process_frame(Frame::Heartbeat { timestamp: 42 })
            .await
            .unwrap();

        assert!(rx.try_recv().unwrap().is_none());
    }

    #[tokio::test]
    async fn ping_completes_on_matching_ack_and_times_out_without_one() {
        let (tx, rx) = bounded_async::<PrioritizedFrame>(8);
        let (server, _streams) = Multiplexer::new(tx, false);

        let echo = server.clone();
        let responder = tokio::spawn(async move {
            let (_, frame) = rx.recv().await.unwrap();
            let Frame::Heartbeat { timestamp } = frame else {
                panic!("expected a heartbeat, got {frame:?}")
            };
            echo.process_frame(Frame::HeartbeatAck { timestamp })
                .await
                .unwrap();
            rx
        });
        assert!(server.ping(Duration::from_secs(5)).await.is_ok());

        let _rx = responder.await.unwrap();
        assert!(server.ping(Duration::from_millis(50)).await.is_err());
    }
}
