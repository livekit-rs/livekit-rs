//! Generic WebSocket message stream.

pub mod frame;

mod deflate;
pub use deflate::offers_permessage_deflate;
mod message;

pub use self::{frame::CloseFrame, message::Message};

use self::{
    frame::{
        Frame, FrameCodec,
        coding::{CloseCode, Control as OpCtl, Data as OpData, OpCode},
    },
    message::{IncompleteMessage, MessageType},
};
use crate::{
    error::{CapacityError, Error, ProtocolError, Result},
    protocol::frame::Utf8Bytes,
};
use log::*;
use std::{
    io::{self, Read, Write},
    mem::replace,
};

/// Indicates a Client or Server role of the websocket
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// This socket is a server
    Server,
    /// This socket is a client
    Client,
}

/// The configuration for WebSocket connection.
///
/// # Example
/// ```
/// # use tungstenite::protocol::WebSocketConfig;;
/// let conf = WebSocketConfig::default()
///     .read_buffer_size(256 * 1024)
///     .write_buffer_size(256 * 1024);
/// ```
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct WebSocketConfig {
    /// Read buffer capacity. This buffer is eagerly allocated and used for receiving
    /// messages.
    ///
    /// For high read load scenarios a larger buffer, e.g. 128 KiB, improves performance.
    ///
    /// For scenarios where you expect a lot of connections and don't need high read load
    /// performance a smaller buffer, e.g. 4 KiB, would be appropriate to lower total
    /// memory usage.
    ///
    /// The default value is 128 KiB.
    pub read_buffer_size: usize,
    /// The target minimum size of the write buffer to reach before writing the data
    /// to the underlying stream.
    /// The default value is 128 KiB.
    ///
    /// If set to `0` each message will be eagerly written to the underlying stream.
    /// It is often more optimal to allow them to buffer a little, hence the default value.
    ///
    /// Note: [`flush`](WebSocket::flush) will always fully write the buffer regardless.
    pub write_buffer_size: usize,
    /// The max size of the write buffer in bytes. Setting this can provide backpressure
    /// in the case the write buffer is filling up due to write errors.
    /// The default value is unlimited.
    ///
    /// Note: The write buffer only builds up past [`write_buffer_size`](Self::write_buffer_size)
    /// when writes to the underlying stream are failing. So the **write buffer can not
    /// fill up if you are not observing write errors even if not flushing**.
    ///
    /// Note: Should always be at least [`write_buffer_size + 1 message`](Self::write_buffer_size)
    /// and probably a little more depending on error handling strategy.
    pub max_write_buffer_size: usize,
    /// The maximum size of an incoming message. `None` means no size limit. The default value is 64 MiB
    /// which should be reasonably big for all normal use-cases but small enough to prevent
    /// memory eating by a malicious user.
    pub max_message_size: Option<usize>,
    /// The maximum size of a single incoming message frame. `None` means no size limit. The limit is for
    /// frame payload NOT including the frame header. The default value is 16 MiB which should
    /// be reasonably big for all normal use-cases but small enough to prevent memory eating
    /// by a malicious user.
    pub max_frame_size: Option<usize>,
    /// When set to `true`, the server will accept and handle unmasked frames
    /// from the client. According to the RFC 6455, the server must close the
    /// connection to the client in such cases, however it seems like there are
    /// some popular libraries that are sending unmasked frames, ignoring the RFC.
    /// By default this option is set to `false`, i.e. according to RFC 6455.
    pub accept_unmasked_frames: bool,
    /// RFC 7692 has already been negotiated, with both directions using
    /// no-context-takeover. Raw-socket callers own the HTTP negotiation.
    pub permessage_deflate: bool,
    /// Use Gorilla's reserved-bit checks for the LiveKit RTC consumer.
    /// Off by default. Negotiated RSV1 on continuation/control is ignored
    /// without changing the first frame's negotiated compression state.
    pub livekit_rtc_reserved_bits: bool,
    /// Optional independent decoded-message cap. None preserves Gorilla's
    /// wire-only SetReadLimit contract; no unchecked decoded preallocation.
    pub max_decompressed_message_size: Option<usize>,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            read_buffer_size: 128 * 1024,
            write_buffer_size: 128 * 1024,
            max_write_buffer_size: usize::MAX,
            max_message_size: Some(64 << 20),
            max_frame_size: Some(16 << 20),
            accept_unmasked_frames: false,
            permessage_deflate: false,
            livekit_rtc_reserved_bits: false,
            max_decompressed_message_size: None,
        }
    }
}

impl WebSocketConfig {
    /// Set the already-negotiated no-context-takeover DEFLATE extension.
    pub fn permessage_deflate(mut self, enabled: bool) -> Self {
        self.permessage_deflate = enabled;
        self
    }
    /// Set an independent decoded cap; it does not replace the wire read budget.
    pub fn max_decompressed_message_size(mut self, limit: Option<usize>) -> Self {
        self.max_decompressed_message_size = limit;
        self
    }

    /// Set [`Self::read_buffer_size`].
    pub fn read_buffer_size(mut self, read_buffer_size: usize) -> Self {
        self.read_buffer_size = read_buffer_size;
        self
    }

    /// Set [`Self::write_buffer_size`].
    pub fn write_buffer_size(mut self, write_buffer_size: usize) -> Self {
        self.write_buffer_size = write_buffer_size;
        self
    }

    /// Set [`Self::max_write_buffer_size`].
    pub fn max_write_buffer_size(mut self, max_write_buffer_size: usize) -> Self {
        self.max_write_buffer_size = max_write_buffer_size;
        self
    }

    /// Set [`Self::max_message_size`].
    pub fn max_message_size(mut self, max_message_size: Option<usize>) -> Self {
        self.max_message_size = max_message_size;
        self
    }

    /// Set [`Self::max_frame_size`].
    pub fn max_frame_size(mut self, max_frame_size: Option<usize>) -> Self {
        self.max_frame_size = max_frame_size;
        self
    }

    /// Set [`Self::accept_unmasked_frames`].
    pub fn accept_unmasked_frames(mut self, accept_unmasked_frames: bool) -> Self {
        self.accept_unmasked_frames = accept_unmasked_frames;
        self
    }

    /// Panic if values are invalid.
    pub(crate) fn assert_valid(&self) {
        assert!(
            self.max_write_buffer_size > self.write_buffer_size,
            "WebSocketConfig::max_write_buffer_size must be greater than write_buffer_size, \
            see WebSocketConfig docs`"
        );
    }
}

/// WebSocket input-output stream.
///
/// This is THE structure you want to create to be able to speak the WebSocket protocol.
/// It may be created by calling `connect`, `accept` or `client` functions.
///
/// Use [`WebSocket::read`], [`WebSocket::send`] to received and send messages.
#[derive(Debug)]
pub struct WebSocket<Stream> {
    /// The underlying socket.
    socket: Stream,
    /// The context for managing a WebSocket.
    context: WebSocketContext,
}

impl<Stream> WebSocket<Stream> {
    /// Convert a raw socket into a WebSocket without performing a handshake.
    ///
    /// Call this function if you're using Tungstenite as a part of a web framework
    /// or together with an existing one. If you need an initial handshake, use
    /// `connect()` or `accept()` functions of the crate to construct a websocket.
    ///
    /// # Panics
    /// Panics if config is invalid e.g. `max_write_buffer_size <= write_buffer_size`.
    pub fn from_raw_socket(stream: Stream, role: Role, config: Option<WebSocketConfig>) -> Self {
        WebSocket {
            socket: stream,
            context: WebSocketContext::new(role, config),
        }
    }

    /// Convert a raw socket into a WebSocket without performing a handshake.
    ///
    /// Call this function if you're using Tungstenite as a part of a web framework
    /// or together with an existing one. If you need an initial handshake, use
    /// `connect()` or `accept()` functions of the crate to construct a websocket.
    ///
    /// # Panics
    /// Panics if config is invalid e.g. `max_write_buffer_size <= write_buffer_size`.
    pub fn from_partially_read(
        stream: Stream,
        part: Vec<u8>,
        role: Role,
        config: Option<WebSocketConfig>,
    ) -> Self {
        WebSocket {
            socket: stream,
            context: WebSocketContext::from_partially_read(part, role, config),
        }
    }

    /// Consumes the `WebSocket` and returns the underlying stream.
    pub fn into_inner(self) -> Stream {
        self.socket
    }

    /// Returns a shared reference to the inner stream.
    pub fn get_ref(&self) -> &Stream {
        &self.socket
    }
    /// Returns a mutable reference to the inner stream.
    pub fn get_mut(&mut self) -> &mut Stream {
        &mut self.socket
    }

    /// Change the configuration.
    ///
    /// # Panics
    /// Panics if config is invalid e.g. `max_write_buffer_size <= write_buffer_size`.
    pub fn set_config(&mut self, set_func: impl FnOnce(&mut WebSocketConfig)) {
        self.context.set_config(set_func);
    }

    /// Read the configuration.
    pub fn get_config(&self) -> &WebSocketConfig {
        self.context.get_config()
    }

    /// Check if it is possible to read messages.
    ///
    /// Reading is impossible after receiving `Message::Close`. It is still possible after
    /// sending close frame since the peer still may send some data before confirming close.
    pub fn can_read(&self) -> bool {
        self.context.can_read()
    }

    /// Check if it is possible to write messages.
    ///
    /// Writing gets impossible immediately after sending or receiving `Message::Close`.
    pub fn can_write(&self) -> bool {
        self.context.can_write()
    }
}

impl<Stream: Read + Write> WebSocket<Stream> {
    /// Read a message from stream, if possible.
    ///
    /// This will also queue responses to ping and close messages. These responses
    /// will be written and flushed on the next call to [`read`](Self::read),
    /// [`write`](Self::write) or [`flush`](Self::flush).
    ///
    /// # Closing the connection
    /// When the remote endpoint decides to close the connection this will return
    /// the close message with an optional close frame.
    ///
    /// You should continue calling [`read`](Self::read), [`write`](Self::write) or
    /// [`flush`](Self::flush) to drive the reply to the close frame until [`Error::ConnectionClosed`]
    /// is returned. Once that happens it is safe to drop the underlying connection.
    pub fn read(&mut self) -> Result<Message> {
        self.context.read(&mut self.socket)
    }

    /// Writes and immediately flushes a message.
    /// Equivalent to calling [`write`](Self::write) then [`flush`](Self::flush).
    pub fn send(&mut self, message: Message) -> Result<()> {
        self.write(message)?;
        self.flush()
    }

    /// Write a message to the provided stream, if possible.
    ///
    /// A subsequent call should be made to [`flush`](Self::flush) to flush writes.
    ///
    /// In the event of stream write failure the message frame will be stored
    /// in the write buffer and will try again on the next call to [`write`](Self::write)
    /// or [`flush`](Self::flush).
    ///
    /// If the write buffer would exceed the configured [`WebSocketConfig::max_write_buffer_size`]
    /// [`Err(WriteBufferFull(msg_frame))`](Error::WriteBufferFull) is returned.
    ///
    /// This call will generally not flush. However, if there are queued automatic messages
    /// they will be written and eagerly flushed.
    ///
    /// For example, upon receiving ping messages tungstenite queues pong replies automatically.
    /// The next call to [`read`](Self::read), [`write`](Self::write) or [`flush`](Self::flush)
    /// will write & flush the pong reply. This means you should not respond to ping frames manually.
    ///
    /// You can however send pong frames manually in order to indicate a unidirectional heartbeat
    /// as described in [RFC 6455](https://tools.ietf.org/html/rfc6455#section-5.5.3). Note that
    /// if [`read`](Self::read) returns a ping, you should [`flush`](Self::flush) before passing
    /// a custom pong to [`write`](Self::write), otherwise the automatic queued response to the
    /// ping will not be sent as it will be replaced by your custom pong message.
    ///
    /// # Errors
    /// - If the WebSocket's write buffer is full, [`Error::WriteBufferFull`] will be returned
    ///   along with the equivalent passed message frame.
    /// - If the connection is closed and should be dropped, this will return [`Error::ConnectionClosed`].
    /// - If you try again after [`Error::ConnectionClosed`] was returned either from here or from
    ///   [`read`](Self::read), [`Error::AlreadyClosed`] will be returned. This indicates a program
    ///   error on your part.
    /// - [`Error::Io`] is returned if the underlying connection returns an error
    ///   (consider these fatal except for WouldBlock).
    /// - [`Error::Capacity`] if your message size is bigger than the configured max message size.
    pub fn write(&mut self, message: Message) -> Result<()> {
        self.context.write(&mut self.socket, message)
    }

    /// Flush writes.
    ///
    /// Ensures all messages previously passed to [`write`](Self::write) and automatic
    /// queued pong responses are written & flushed into the underlying stream.
    pub fn flush(&mut self) -> Result<()> {
        self.context.flush(&mut self.socket)
    }

    /// Close the connection.
    ///
    /// This function guarantees that the close frame will be queued.
    /// There is no need to call it again. Calling this function is
    /// the same as calling `write(Message::Close(..))`.
    ///
    /// After queuing the close frame you should continue calling [`read`](Self::read) or
    /// [`flush`](Self::flush) to drive the close handshake to completion.
    ///
    /// The websocket RFC defines that the underlying connection should be closed
    /// by the server. Tungstenite takes care of this asymmetry for you.
    ///
    /// When the close handshake is finished (we have both sent and received
    /// a close message), [`read`](Self::read) or [`flush`](Self::flush) will return
    /// [Error::ConnectionClosed] if this endpoint is the server.
    ///
    /// If this endpoint is a client, [Error::ConnectionClosed] will only be
    /// returned after the server has closed the underlying connection.
    ///
    /// It is thus safe to drop the underlying connection as soon as [Error::ConnectionClosed]
    /// is returned from [`read`](Self::read) or [`flush`](Self::flush).
    pub fn close(&mut self, code: Option<CloseFrame>) -> Result<()> {
        self.context.close(&mut self.socket, code)
    }

    /// Old name for [`read`](Self::read).
    #[deprecated(note = "Use `read`")]
    pub fn read_message(&mut self) -> Result<Message> {
        self.read()
    }

    /// Old name for [`send`](Self::send).
    #[deprecated(note = "Use `send`")]
    pub fn write_message(&mut self, message: Message) -> Result<()> {
        self.send(message)
    }

    /// Old name for [`flush`](Self::flush).
    #[deprecated(note = "Use `flush`")]
    pub fn write_pending(&mut self) -> Result<()> {
        self.flush()
    }
}

/// A context for managing WebSocket stream.
#[derive(Debug)]
pub struct WebSocketContext {
    /// Server or client?
    role: Role,
    /// encoder/decoder of frame.
    frame: FrameCodec,
    /// The state of processing, either "active" or "closing".
    state: WebSocketState,
    /// Receive: an incomplete message being processed.
    incomplete: Option<IncompleteMessage>,
    incoming_deflate: Option<deflate::Decoder>,
    wire_message_size: usize,
    /// Send in addition to regular messages E.g. "pong" or "close".
    additional_send: Option<Frame>,
    /// True indicates there is an additional message (like a pong)
    /// that failed to flush previously and we should try again.
    unflushed_additional: bool,
    /// The configuration for the websocket session.
    config: WebSocketConfig,
}

impl WebSocketContext {
    /// Create a WebSocket context that manages a post-handshake stream.
    ///
    /// # Panics
    /// Panics if config is invalid e.g. `max_write_buffer_size <= write_buffer_size`.
    pub fn new(role: Role, config: Option<WebSocketConfig>) -> Self {
        let conf = config.unwrap_or_default();
        Self::_new(role, FrameCodec::new(conf.read_buffer_size), conf)
    }

    /// Create a WebSocket context that manages a post-handshake stream.
    ///
    /// # Panics
    /// Panics if config is invalid e.g. `max_write_buffer_size <= write_buffer_size`.
    pub fn from_partially_read(part: Vec<u8>, role: Role, config: Option<WebSocketConfig>) -> Self {
        let conf = config.unwrap_or_default();
        Self::_new(
            role,
            FrameCodec::from_partially_read(part, conf.read_buffer_size),
            conf,
        )
    }

    fn _new(role: Role, mut frame: FrameCodec, config: WebSocketConfig) -> Self {
        config.assert_valid();
        frame.set_max_out_buffer_len(config.max_write_buffer_size);
        frame.set_out_buffer_write_len(config.write_buffer_size);
        Self {
            role,
            frame,
            state: WebSocketState::Active,
            incomplete: None,
            incoming_deflate: None,
            wire_message_size: 0,
            additional_send: None,
            unflushed_additional: false,
            config,
        }
    }

    /// Change the configuration.
    ///
    /// # Panics
    /// Panics if config is invalid e.g. `max_write_buffer_size <= write_buffer_size`.
    pub fn set_config(&mut self, set_func: impl FnOnce(&mut WebSocketConfig)) {
        set_func(&mut self.config);
        self.config.assert_valid();
        self.frame
            .set_max_out_buffer_len(self.config.max_write_buffer_size);
        self.frame
            .set_out_buffer_write_len(self.config.write_buffer_size);
    }

    /// Read the configuration.
    pub fn get_config(&self) -> &WebSocketConfig {
        &self.config
    }

    /// Check if it is possible to read messages.
    ///
    /// Reading is impossible after receiving `Message::Close`. It is still possible after
    /// sending close frame since the peer still may send some data before confirming close.
    pub fn can_read(&self) -> bool {
        self.state.can_read()
    }

    /// Check if it is possible to write messages.
    ///
    /// Writing gets impossible immediately after sending or receiving `Message::Close`.
    pub fn can_write(&self) -> bool {
        self.state.is_active()
    }

    /// Read a message from the provided stream, if possible.
    ///
    /// This function sends pong and close responses automatically.
    /// However, it never blocks on write.
    pub fn read<Stream>(&mut self, stream: &mut Stream) -> Result<Message>
    where
        Stream: Read + Write,
    {
        // Do not read from already closed connections.
        self.state.check_not_terminated()?;

        loop {
            if self.additional_send.is_some() || self.unflushed_additional {
                // Since we may get ping or close, we need to reply to the messages even during read.
                match self.flush(stream) {
                    Ok(_) => {}
                    Err(Error::Io(err)) if err.kind() == io::ErrorKind::WouldBlock => {
                        // If blocked continue reading, but try again later
                        self.unflushed_additional = true;
                    }
                    Err(err) => return Err(err),
                }
            } else if self.role == Role::Server && !self.state.can_read() {
                self.state = WebSocketState::Terminated;
                return Err(Error::ConnectionClosed);
            }

            // If we get here, either write blocks or we have nothing to write.
            // Thus if read blocks, just let it return WouldBlock.
            if let Some(message) = self.read_message_frame(stream)? {
                trace!("Received message {message}");
                return Ok(message);
            }
        }
    }

    /// Write a message to the provided stream.
    ///
    /// A subsequent call should be made to [`flush`](Self::flush) to flush writes.
    ///
    /// In the event of stream write failure the message frame will be stored
    /// in the write buffer and will try again on the next call to [`write`](Self::write)
    /// or [`flush`](Self::flush).
    ///
    /// If the write buffer would exceed the configured [`WebSocketConfig::max_write_buffer_size`]
    /// [`Err(WriteBufferFull(msg_frame))`](Error::WriteBufferFull) is returned.
    pub fn write<Stream>(&mut self, stream: &mut Stream, message: Message) -> Result<()>
    where
        Stream: Read + Write,
    {
        // When terminated, return AlreadyClosed.
        self.state.check_not_terminated()?;

        // Do not write after sending a close frame.
        if !self.state.is_active() {
            return Err(Error::Protocol(ProtocolError::SendAfterClosing));
        }

        let frame = match message {
            Message::Text(data) => self.message_frame(data.into(), OpData::Text)?,
            Message::Binary(data) => self.message_frame(data, OpData::Binary)?,
            Message::Ping(data) => Frame::ping(data),
            Message::Pong(data) => {
                self.set_additional(Frame::pong(data));
                // Note: user pongs can be user flushed so no need to flush here
                return self._write(stream, None).map(|_| ());
            }
            Message::Close(code) => return self.close(stream, code),
            Message::Frame(f) => f,
        };

        let should_flush = self._write(stream, Some(frame))?;
        if should_flush {
            self.flush(stream)?;
        }
        Ok(())
    }

    fn message_frame(&self, payload: bytes::Bytes, kind: OpData) -> Result<Frame> {
        let mut frame = if self.config.permessage_deflate {
            Frame::message(deflate::encode(&payload)?, OpCode::Data(kind), true)
        } else {
            Frame::message(payload, OpCode::Data(kind), true)
        };
        frame.header_mut().rsv1 = self.config.permessage_deflate;
        Ok(frame)
    }

    /// Flush writes.
    ///
    /// Ensures all messages previously passed to [`write`](Self::write) and automatically
    /// queued pong responses are written & flushed into the `stream`.
    #[inline]
    pub fn flush<Stream>(&mut self, stream: &mut Stream) -> Result<()>
    where
        Stream: Read + Write,
    {
        self._write(stream, None)?;
        self.frame.write_out_buffer(stream)?;
        stream.flush()?;
        self.unflushed_additional = false;
        Ok(())
    }

    /// Writes any data in the out_buffer, `additional_send` and given `data`.
    ///
    /// Does **not** flush.
    ///
    /// Returns true if the write contents indicate we should flush immediately.
    fn _write<Stream>(&mut self, stream: &mut Stream, data: Option<Frame>) -> Result<bool>
    where
        Stream: Read + Write,
    {
        if let Some(data) = data {
            self.buffer_frame(stream, data)?;
        }

        // Upon receipt of a Ping frame, an endpoint MUST send a Pong frame in
        // response, unless it already received a Close frame. It SHOULD
        // respond with Pong frame as soon as is practical. (RFC 6455)
        let should_flush = if let Some(msg) = self.additional_send.take() {
            trace!("Sending pong/close");
            match self.buffer_frame(stream, msg) {
                Err(Error::WriteBufferFull(msg)) => {
                    // if an system message would exceed the buffer put it back in
                    // `additional_send` for retry. Otherwise returning this error
                    // may not make sense to the user, e.g. calling `flush`.
                    if let Message::Frame(msg) = *msg {
                        self.set_additional(msg);
                        false
                    } else {
                        unreachable!()
                    }
                }
                Err(err) => return Err(err),
                Ok(_) => true,
            }
        } else {
            self.unflushed_additional
        };

        // If we're closing and there is nothing to send anymore, we should close the connection.
        if self.role == Role::Server && !self.state.can_read() {
            // The underlying TCP connection, in most normal cases, SHOULD be closed
            // first by the server, so that it holds the TIME_WAIT state and not the
            // client (as this would prevent it from re-opening the connection for 2
            // maximum segment lifetimes (2MSL), while there is no corresponding
            // server impact as a TIME_WAIT connection is immediately reopened upon
            // a new SYN with a higher seq number). (RFC 6455)
            self.frame.write_out_buffer(stream)?;
            self.state = WebSocketState::Terminated;
            Err(Error::ConnectionClosed)
        } else {
            Ok(should_flush)
        }
    }

    /// Close the connection.
    ///
    /// This function guarantees that the close frame will be queued.
    /// There is no need to call it again. Calling this function is
    /// the same as calling `send(Message::Close(..))`.
    pub fn close<Stream>(&mut self, stream: &mut Stream, code: Option<CloseFrame>) -> Result<()>
    where
        Stream: Read + Write,
    {
        if let WebSocketState::Active = self.state {
            self.state = WebSocketState::ClosedByUs;
            let frame = Frame::close(code);
            self._write(stream, Some(frame))?;
        }
        self.flush(stream)
    }

    /// Try to decode one message frame. May return None.
    fn read_message_frame(&mut self, stream: &mut impl Read) -> Result<Option<Message>> {
        let livekit_rtc_reserved_bits = self.config.livekit_rtc_reserved_bits;
        let header_budget = cfg!(feature = "livekit-server-header-read-budget");
        let server = self.role == Role::Server;
        let read = if server && (header_budget || livekit_rtc_reserved_bits) {
            let can_read = self.state.can_read();
            let incomplete = self.incomplete.as_ref().map(|_| self.wire_message_size);
            let message_limit = self.config.max_message_size;
            let accept_unmasked = self.config.accept_unmasked_frames;
            let compressed = self.config.permessage_deflate;
            self.frame.read_frame_with_header_check(
                stream,
                self.config.max_frame_size,
                true,
                accept_unmasked,
                livekit_rtc_reserved_bits,
                |bytes| {
                    if livekit_rtc_reserved_bits && can_read {
                        if let [first, second, ..] = bytes {
                            check_livekit_rtc_header_start(
                                *first, *second, incomplete.is_some(), compressed, accept_unmasked,
                            )?;
                            // Gorilla reads all eight length bytes and checks the
                            // signed range before waiting for the masking key.
                            if second & 0x7f == 127 && bytes.len() >= 10 && bytes[2] & 0x80 != 0 {
                                return Err(ProtocolError::LivekitRtcReadLengthOverflow.into());
                            }
                        }
                    }
                    Ok(())
                },
                |header, payload_len| {
                    if !header_budget {
                        return Ok(());
                    }
                    check_server_header_budget(
                        header,
                        payload_len,
                        can_read,
                        incomplete,
                        message_limit,
                        accept_unmasked,
                        compressed,
                        livekit_rtc_reserved_bits,
                    )
                },
            )
        } else {
            self.frame.read_frame(
                stream,
                self.config.max_frame_size,
                server,
                self.config.accept_unmasked_frames,
            )
        };
        let mut frame = match read.check_connection_reset(self.state)? {
            None => {
                // Connection closed by peer
                return match replace(&mut self.state, WebSocketState::Terminated) {
                    WebSocketState::ClosedByPeer | WebSocketState::CloseAcknowledged => {
                        Err(Error::ConnectionClosed)
                    }
                    _ => Err(Error::Protocol(ProtocolError::ResetWithoutClosingHandshake)),
                };
            }
            Some(frame) => frame,
        };

        if !self.state.can_read() {
            return Err(Error::Protocol(ProtocolError::ReceivedAfterClosing));
        }
        // MUST be 0 unless an extension is negotiated that defines meanings
        // for non-zero values.  If a nonzero value is received and none of
        // the negotiated extensions defines the meaning of such a nonzero
        // value, the receiving endpoint MUST _Fail the WebSocket
        // Connection_.
        check_reserved_bits(
            frame.header(),
            self.config.permessage_deflate,
            self.role == Role::Server && self.config.livekit_rtc_reserved_bits,
        )?;

        if self.role == Role::Client && frame.is_masked() {
            // A client MUST close a connection if it detects a masked frame. (RFC 6455)
            return Err(Error::Protocol(ProtocolError::MaskedFrameFromServer));
        }

        match frame.header().opcode {
            OpCode::Control(ctl) => {
                match ctl {
                    // All control frames MUST have a payload length of 125 bytes or less
                    // and MUST NOT be fragmented. (RFC 6455)
                    _ if !frame.header().is_final => {
                        Err(Error::Protocol(ProtocolError::FragmentedControlFrame))
                    }
                    _ if frame.payload().len() > 125 => {
                        Err(Error::Protocol(ProtocolError::ControlFrameTooBig))
                    }
                    OpCtl::Close => {
                        let close = if server && livekit_rtc_reserved_bits {
                            read_livekit_rtc_close(frame)?
                        } else {
                            frame.into_close()?
                        };
                        self.incoming_deflate = None;
                        self.incomplete = None;
                        self.wire_message_size = 0;
                        Ok(self.do_close(close).map(Message::Close))
                    }
                    OpCtl::Reserved(i) => {
                        Err(Error::Protocol(ProtocolError::UnknownControlFrameType(i)))
                    }
                    OpCtl::Ping => {
                        let data = frame.into_payload();
                        // No ping processing after we sent a close frame.
                        if self.state.is_active() {
                            self.set_additional(Frame::pong(data.clone()));
                        }
                        Ok(Some(Message::Ping(data)))
                    }
                    OpCtl::Pong => Ok(Some(Message::Pong(frame.into_payload()))),
                }
            }

            OpCode::Data(data) => {
                let fin = frame.header().is_final;
                match (data, self.incomplete.is_some()) {
                    (OpData::Continue, false) => {
                        return Err(ProtocolError::UnexpectedContinueFrame.into());
                    }
                    (OpData::Text | OpData::Binary, true) => {
                        return Err(ProtocolError::ExpectedFragment(data).into());
                    }
                    _ => {}
                }
                if data != OpData::Continue {
                    self.wire_message_size = 0;
                    if frame.header().rsv1 {
                        self.incoming_deflate = Some(deflate::Decoder::new());
                    }
                }
                self.wire_message_size =
                    self.wire_message_size.saturating_add(frame.payload().len());
                check_max_size(self.wire_message_size, self.config.max_message_size)?;
                let decoded_limit = if self.incoming_deflate.is_some() {
                    self.config.max_decompressed_message_size
                } else {
                    self.config.max_message_size
                };
                if let Some(decoder) = &mut self.incoming_deflate {
                    let decoded = match decoder.decode(frame.payload(), fin, decoded_limit) {
                        Ok(payload) => payload,
                        Err(error) => {
                            self.incoming_deflate = None;
                            self.incomplete = None;
                            self.wire_message_size = 0;
                            return Err(error);
                        }
                    };
                    let mut header = frame.header().clone();
                    header.rsv1 = false;
                    frame = Frame::from_payload(header, decoded);
                }
                if fin {
                    self.incoming_deflate = None;
                    self.wire_message_size = 0;
                }

                let payload = match (data, self.incomplete.as_mut()) {
                    (OpData::Continue, None) => Err(ProtocolError::UnexpectedContinueFrame),
                    (OpData::Continue, Some(incomplete)) => {
                        incomplete.extend(frame.into_payload(), decoded_limit)?;
                        Ok(None)
                    }
                    (_, Some(_)) => Err(ProtocolError::ExpectedFragment(data)),
                    (OpData::Text, _) => Ok(Some((frame.into_payload(), MessageType::Text))),
                    (OpData::Binary, _) => Ok(Some((frame.into_payload(), MessageType::Binary))),
                    (OpData::Reserved(i), _) => Err(ProtocolError::UnknownDataFrameType(i)),
                }?;

                match (payload, fin) {
                    (None, true) => Ok(Some(self.incomplete.take().unwrap().complete()?)),
                    (None, false) => Ok(None),
                    (Some((payload, t)), true) => {
                        check_max_size(payload.len(), decoded_limit)?;
                        match t {
                            MessageType::Text => Ok(Some(Message::Text(payload.try_into()?))),
                            MessageType::Binary => Ok(Some(Message::Binary(payload))),
                        }
                    }
                    (Some((payload, t)), false) => {
                        let mut incomplete = IncompleteMessage::new(t);
                        incomplete.extend(payload, decoded_limit)?;
                        self.incomplete = Some(incomplete);
                        Ok(None)
                    }
                }
            }
        } // match opcode
    }

    /// Received a close frame. Tells if we need to return a close frame to the user.
    #[allow(clippy::option_option)]
    fn do_close(&mut self, close: Option<CloseFrame>) -> Option<Option<CloseFrame>> {
        debug!("Received close frame: {close:?}");
        match self.state {
            WebSocketState::Active => {
                self.state = WebSocketState::ClosedByPeer;

                // Gorilla1.5.3's default server close handler acknowledges the
                // received code with an empty reason. Preserve input provenance,
                // client behavior and upstream invalid-code handling separately.
                #[cfg(feature = "livekit-server-close-reply-empty-reason")]
                let omit_reply_reason = self.role == Role::Server
                    && close.as_ref().map_or(true, |frame| frame.code.is_allowed());

                let close = close.map(|frame| {
                    if !frame.code.is_allowed() {
                        CloseFrame {
                            code: CloseCode::Protocol,
                            reason: Utf8Bytes::from_static("Protocol violation"),
                        }
                    } else {
                        frame
                    }
                });

                #[cfg(feature = "livekit-server-close-reply-empty-reason")]
                let reply = {
                    let mut reply_close = close.clone();
                    if omit_reply_reason {
                        if let Some(frame) = &mut reply_close {
                            frame.reason = Utf8Bytes::from_static("");
                        }
                    }
                    Frame::close(reply_close)
                };
                #[cfg(not(feature = "livekit-server-close-reply-empty-reason"))]
                let reply = Frame::close(close.clone());
                debug!("Replying to close with {reply:?}");
                self.set_additional(reply);

                Some(close)
            }
            WebSocketState::ClosedByPeer | WebSocketState::CloseAcknowledged => {
                // It is already closed, just ignore.
                None
            }
            WebSocketState::ClosedByUs => {
                // We received a reply.
                self.state = WebSocketState::CloseAcknowledged;
                Some(close)
            }
            WebSocketState::Terminated => unreachable!(),
        }
    }

    /// Write a single frame into the write-buffer.
    fn buffer_frame<Stream>(&mut self, stream: &mut Stream, mut frame: Frame) -> Result<()>
    where
        Stream: Read + Write,
    {
        match self.role {
            Role::Server => {}
            Role::Client => {
                // 5.  If the data is being sent by the client, the frame(s) MUST be
                // masked as defined in Section 5.3. (RFC 6455)
                frame.set_random_mask();
            }
        }

        trace!("Sending frame: {frame:?}");
        self.frame
            .buffer_frame(stream, frame)
            .check_connection_reset(self.state)
    }

    /// Replace `additional_send` if it is currently a `Pong` message.
    fn set_additional(&mut self, add: Frame) {
        let empty_or_pong = self
            .additional_send
            .as_ref()
            .map_or(true, |f| f.header().opcode == OpCode::Control(OpCtl::Pong));
        if empty_or_pong {
            self.additional_send.replace(add);
        }
    }
}

// Copyright (c) 2013 The Gorilla WebSocket Authors. All rights reserved.
// Source-derived BSD-2-Clause logic; see LICENSE-GORILLA (workspace notice also retained in LICENSES).
// Source: Gorilla1.5.3 conn.go advanceFrame steps2-5. Invalid protocol
// headers retain priority over data-budget failures. Controls consume no budget.
fn check_server_header_budget(
    header: &frame::FrameHeader,
    payload_len: u64,
    can_read: bool,
    incomplete: Option<usize>,
    message_limit: Option<usize>,
    accept_unmasked: bool,
    permessage_deflate: bool,
    livekit_rtc_reserved_bits: bool,
) -> Result<()> {
    if !can_read {
        return Err(Error::Protocol(ProtocolError::ReceivedAfterClosing));
    }
    check_reserved_bits(header, permessage_deflate, livekit_rtc_reserved_bits)?;
    match header.opcode {
        OpCode::Control(_) if !header.is_final => {
            return Err(Error::Protocol(ProtocolError::FragmentedControlFrame));
        }
        OpCode::Control(_) if payload_len > 125 => {
            return Err(Error::Protocol(ProtocolError::ControlFrameTooBig));
        }
        OpCode::Control(OpCtl::Reserved(i)) => {
            return Err(Error::Protocol(ProtocolError::UnknownControlFrameType(i)));
        }
        OpCode::Data(OpData::Continue) if incomplete.is_none() => {
            return Err(Error::Protocol(ProtocolError::UnexpectedContinueFrame));
        }
        OpCode::Data(data @ (OpData::Text | OpData::Binary)) if incomplete.is_some() => {
            return Err(Error::Protocol(ProtocolError::ExpectedFragment(data)));
        }
        OpCode::Data(OpData::Reserved(i)) => {
            return Err(Error::Protocol(ProtocolError::UnknownDataFrameType(i)));
        }
        _ => {}
    }
    if header.mask.is_none() && !accept_unmasked {
        return Err(Error::Protocol(ProtocolError::UnmaskedFrameFromClient));
    }
    if let OpCode::Data(_) = header.opcode {
        let preceding = incomplete.unwrap_or(0);
        if livekit_rtc_reserved_bits && payload_len > (i64::MAX as u64).saturating_sub(preceding as u64) {
            return Err(ProtocolError::LivekitRtcReadLengthOverflow.into());
        }
        let max_size = message_limit.unwrap_or(usize::MAX);
        let remaining = max_size.saturating_sub(preceding);
        if preceding > max_size || payload_len > remaining as u64 {
            return Err(Error::Capacity(CapacityError::MessageTooLong {
                size: preceding.saturating_add(usize::try_from(payload_len).unwrap_or(usize::MAX)),
                max_size,
            }));
        }
    }
    Ok(())
}

// Gorilla v1.5.3 advanceFrame validates the first two bytes before reading
// an extended length or mask key, and reports the violations in wire order.
fn check_livekit_rtc_header_start(
    first: u8,
    second: u8,
    incomplete: bool,
    permessage_deflate: bool,
    accept_unmasked: bool,
) -> Result<()> {
    let mut errors = Vec::new();
    if !permessage_deflate && first & 0x40 != 0 {
        errors.push("RSV1 set".to_owned());
    }
    if first & 0x20 != 0 {
        errors.push("RSV2 set".to_owned());
    }
    if first & 0x10 != 0 {
        errors.push("RSV3 set".to_owned());
    }
    match first & 0x0f {
        8..=10 => {
            if second & 0x7f > 125 {
                errors.push("len > 125 for control".to_owned());
            }
            if first & 0x80 == 0 {
                errors.push("FIN not set on control".to_owned());
            }
        }
        1 | 2 if incomplete => errors.push("data before FIN".to_owned()),
        0 if !incomplete => errors.push("continuation after FIN".to_owned()),
        0..=2 => {}
        opcode => errors.push(format!("bad opcode {opcode}")),
    }
    if !accept_unmasked && second & 0x80 == 0 {
        errors.push("bad MASK".to_owned());
    }
    if errors.is_empty() {
        return Ok(());
    }
    Err(livekit_rtc_violation(errors.join(", ")))
}

// Gorilla v1.5.3 conn.go advanceFrame accepts a missing status even for
// a one-byte Close and checks the code before the UTF-8 reason.
fn read_livekit_rtc_close(frame: Frame) -> Result<Option<CloseFrame>> {
    let [high, low, ..] = frame.payload().as_ref() else {
        return Ok(None);
    };
    let code = u16::from_be_bytes([*high, *low]);
    if !matches!(code, 1000..=1003 | 1007..=1013 | 3000..=4999) {
        return Err(livekit_rtc_violation(format!("bad close code {code}")));
    }
    frame
        .into_close()
        .map_err(|_| livekit_rtc_violation("invalid utf8 payload in close frame".to_owned()))
}

fn livekit_rtc_violation(reason: String) -> Error {
    ProtocolError::LivekitRtcProtocolViolation(std::sync::Arc::new(reason)).into()
}

// Gorilla v1.5.3 conn.go advanceFrame accepts negotiated RSV1 on all
// opcodes. Only the first data frame selects a decompression reader.
fn check_reserved_bits(
    header: &frame::FrameHeader,
    permessage_deflate: bool,
    livekit_rtc_reserved_bits: bool,
) -> Result<()> {
    let valid_rsv1 = permessage_deflate
        && (livekit_rtc_reserved_bits
            || matches!(header.opcode, OpCode::Data(OpData::Text | OpData::Binary)));
    let unexpected_rsv1 = header.rsv1 && !valid_rsv1;
    if !unexpected_rsv1 && !header.rsv2 && !header.rsv3 {
        return Ok(());
    }
    Err(Error::Protocol(ProtocolError::NonZeroReservedBits))
}

fn check_max_size(size: usize, max_size: Option<usize>) -> crate::Result<()> {
    if let Some(max_size) = max_size {
        if size > max_size {
            return Err(Error::Capacity(CapacityError::MessageTooLong {
                size,
                max_size,
            }));
        }
    }
    Ok(())
}

/// The current connection state.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum WebSocketState {
    /// The connection is active.
    Active,
    /// We initiated a close handshake.
    ClosedByUs,
    /// The peer initiated a close handshake.
    ClosedByPeer,
    /// The peer replied to our close handshake.
    CloseAcknowledged,
    /// The connection does not exist anymore.
    Terminated,
}

impl WebSocketState {
    /// Tell if we're allowed to process normal messages.
    fn is_active(self) -> bool {
        matches!(self, WebSocketState::Active)
    }

    /// Tell if we should process incoming data. Note that if we send a close frame
    /// but the remote hasn't confirmed, they might have sent data before they receive our
    /// close frame, so we should still pass those to client code, hence ClosedByUs is valid.
    fn can_read(self) -> bool {
        matches!(self, WebSocketState::Active | WebSocketState::ClosedByUs)
    }

    /// Check if the state is active, return error if not.
    fn check_not_terminated(self) -> Result<()> {
        match self {
            WebSocketState::Terminated => Err(Error::AlreadyClosed),
            _ => Ok(()),
        }
    }
}

/// Translate "Connection reset by peer" into `ConnectionClosed` if appropriate.
trait CheckConnectionReset {
    fn check_connection_reset(self, state: WebSocketState) -> Self;
}

impl<T> CheckConnectionReset for Result<T> {
    fn check_connection_reset(self, state: WebSocketState) -> Self {
        match self {
            Err(Error::Io(io_error)) => Err({
                if !state.can_read() && io_error.kind() == io::ErrorKind::ConnectionReset {
                    Error::ConnectionClosed
                } else {
                    Error::Io(io_error)
                }
            }),
            x => x,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Message, Role, WebSocket, WebSocketConfig};
    use crate::error::{CapacityError, Error};

    use std::{io, io::Cursor};

    struct WriteMoc<Stream>(Stream);

    impl<Stream> io::Write for WriteMoc<Stream> {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<Stream: io::Read> io::Read for WriteMoc<Stream> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.0.read(buf)
        }
    }

    #[test]
    fn receive_messages() {
        let incoming = Cursor::new(vec![
            0x89, 0x02, 0x01, 0x02, 0x8a, 0x01, 0x03, 0x01, 0x07, 0x48, 0x65, 0x6c, 0x6c, 0x6f,
            0x2c, 0x20, 0x80, 0x06, 0x57, 0x6f, 0x72, 0x6c, 0x64, 0x21, 0x82, 0x03, 0x01, 0x02,
            0x03,
        ]);
        let mut socket = WebSocket::from_raw_socket(WriteMoc(incoming), Role::Client, None);
        assert_eq!(socket.read().unwrap(), Message::Ping(vec![1, 2].into()));
        assert_eq!(socket.read().unwrap(), Message::Pong(vec![3].into()));
        assert_eq!(
            socket.read().unwrap(),
            Message::Text("Hello, World!".into())
        );
        assert_eq!(
            socket.read().unwrap(),
            Message::Binary(vec![0x01, 0x02, 0x03].into())
        );
    }

    #[test]
    fn size_limiting_text_fragmented() {
        let incoming = Cursor::new(vec![
            0x01, 0x07, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x80, 0x06, 0x57, 0x6f, 0x72,
            0x6c, 0x64, 0x21,
        ]);
        let limit = WebSocketConfig {
            max_message_size: Some(10),
            ..WebSocketConfig::default()
        };
        let mut socket = WebSocket::from_raw_socket(WriteMoc(incoming), Role::Client, Some(limit));

        assert!(matches!(
            socket.read(),
            Err(Error::Capacity(CapacityError::MessageTooLong {
                size: 13,
                max_size: 10
            }))
        ));
    }

    #[test]
    fn size_limiting_binary() {
        let incoming = Cursor::new(vec![0x82, 0x03, 0x01, 0x02, 0x03]);
        let limit = WebSocketConfig {
            max_message_size: Some(2),
            ..WebSocketConfig::default()
        };
        let mut socket = WebSocket::from_raw_socket(WriteMoc(incoming), Role::Client, Some(limit));

        assert!(matches!(
            socket.read(),
            Err(Error::Capacity(CapacityError::MessageTooLong {
                size: 3,
                max_size: 2
            }))
        ));
    }
}

#[cfg(test)]
mod deflate_frame_tests {
    use super::*;
    use std::io::{Cursor, Read, Write};
    #[derive(Default)]
    struct Wire {
        input: Cursor<Vec<u8>>,
        output: Vec<u8>,
    }
    impl Read for Wire {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            self.input.read(bytes)
        }
    }
    impl Write for Wire {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.output.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    fn config() -> WebSocketConfig {
        WebSocketConfig::default().permessage_deflate(true)
    }
    fn raw(kind: OpCode, fin: bool, compressed: bool, bytes: &[u8], mask: bool) -> Vec<u8> {
        let header = frame::FrameHeader {
            is_final: fin,
            rsv1: compressed,
            opcode: kind,
            mask: mask.then_some([1, 2, 3, 4]),
            ..Default::default()
        };
        let frame = Frame::from_payload(header, bytes::Bytes::copy_from_slice(bytes));
        let mut wire = Vec::new();
        frame.format(&mut wire).unwrap();
        wire
    }
    fn reader(input: Vec<u8>, role: Role, config: WebSocketConfig) -> WebSocket<Wire> {
        WebSocket::from_raw_socket(
            Wire {
                input: Cursor::new(input),
                ..Default::default()
            },
            role,
            Some(config),
        )
    }
    #[test]
    fn rfc7692_known_messages_decode_through_actual_websocket_frames() {
        // RFC 7692 §7.2.3.1/.3/.4/.5: fixed Huffman, uncompressed
        // DEFLATE block, BFINAL=1, and multiple blocks in one message.
        for payload in [
            vec![0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00],
            vec![
                0x00, 0x05, 0x00, 0xfa, 0xff, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x00,
            ],
            vec![0xf3, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00, 0x00],
            vec![
                0xf2, 0x48, 0x05, 0x00, 0x00, 0x00, 0xff, 0xff, 0xca, 0xc9, 0xc9, 0x07, 0x00,
            ],
        ] {
            for role in [Role::Client, Role::Server] {
                let input = raw(
                    OpCode::Data(OpData::Text),
                    true,
                    true,
                    &payload,
                    role == Role::Server,
                );
                assert_eq!(
                    reader(input, role, config()).read().unwrap(),
                    Message::text("Hello")
                );
            }
        }
    }
    #[test]
    fn fragmented_compression_keeps_ping_and_close_uncompressed_and_resets_context() {
        let compressed = [0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00];
        let mut input = raw(
            OpCode::Data(OpData::Text),
            false,
            true,
            &compressed[..1],
            true,
        );
        input.extend(raw(
            OpCode::Control(OpCtl::Ping),
            true,
            false,
            b"ping",
            true,
        ));
        input.extend(raw(
            OpCode::Data(OpData::Continue),
            false,
            false,
            &compressed[1..3],
            true,
        ));
        input.extend(raw(
            OpCode::Data(OpData::Continue),
            true,
            false,
            &compressed[3..],
            true,
        ));
        input.extend(raw(
            OpCode::Data(OpData::Text),
            true,
            true,
            &compressed,
            true,
        ));
        input.extend(raw(OpCode::Data(OpData::Text), true, false, b"plain", true));
        input.extend(raw(
            OpCode::Control(OpCtl::Close),
            true,
            false,
            &[3, 232],
            true,
        ));
        let mut socket = reader(
            input,
            Role::Server,
            config().max_message_size(Some(compressed.len())),
        );
        assert_eq!(
            socket.read().unwrap(),
            Message::Ping(b"ping".as_slice().into())
        );
        assert_eq!(socket.read().unwrap(), Message::text("Hello"));
        assert_eq!(socket.read().unwrap(), Message::text("Hello"));
        assert_eq!(socket.read().unwrap(), Message::text("plain"));
        assert!(
            matches!(socket.read().unwrap(), Message::Close(Some(close)) if close.code == CloseCode::Normal)
        );
        let _ = socket.flush();
        assert!(socket.context.incoming_deflate.is_none());
        assert!(socket.context.incomplete.is_none());
        let output = &socket.get_ref().output;
        assert!(!output.is_empty());
        assert_eq!(
            output[0] & 0x40,
            0,
            "automatic PONG must never be compressed"
        );
    }
    #[test]
    fn compressed_wire_budget_and_optional_decoded_cap_are_different_domains() {
        let plain = vec![b'x'; 8192];
        let compressed = deflate::encode(&plain).unwrap();
        assert!(compressed.len() < 128);
        let mut input = raw(
            OpCode::Data(OpData::Binary),
            false,
            true,
            &compressed[..1],
            true,
        );
        input.extend(raw(
            OpCode::Data(OpData::Continue),
            true,
            false,
            &compressed[1..],
            true,
        ));
        let cfg = config().max_message_size(Some(compressed.len()));
        assert_eq!(
            reader(input.clone(), Role::Server, cfg).read().unwrap(),
            Message::Binary(plain.into())
        );
        assert!(matches!(
            reader(
                input.clone(),
                Role::Server,
                cfg.max_decompressed_message_size(Some(64))
            )
            .read(),
            Err(Error::Capacity(CapacityError::DecompressedMessageTooLong {
                max_size: 64,
                ..
            }))
        ));
        assert!(matches!(
            reader(
                input,
                Role::Server,
                cfg.max_message_size(Some(compressed.len() - 1))
            )
            .read(),
            Err(Error::Capacity(CapacityError::MessageTooLong { .. }))
        ));
    }
    #[test]
    fn livekit_rtc_negotiated_continuation_rsv1_keeps_the_original_decoder() {
        let compressed = [0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00];
        let mut input = raw(
            OpCode::Data(OpData::Text),
            false,
            true,
            &compressed[..2],
            true,
        );
        input.extend(raw(
            OpCode::Data(OpData::Continue),
            true,
            true,
            &compressed[2..],
            true,
        ));
        let mut strict = reader(input.clone(), Role::Server, config());
        assert!(matches!(
            strict.read(),
            Err(Error::Protocol(ProtocolError::NonZeroReservedBits))
        ));
        let mut rtc_config = config();
        rtc_config.livekit_rtc_reserved_bits = true;
        let mut rtc = reader(input, Role::Server, rtc_config);
        assert_eq!(rtc.read().unwrap(), Message::text("Hello"));
    }

    #[test]
    fn livekit_rtc_close_payload_uses_gorilla_status_then_utf8() {
        let close = |payload: &[u8]| raw(OpCode::Control(OpCtl::Close), true, false, payload, true);
        let mut rtc = config();
        rtc.livekit_rtc_reserved_bits = true;
        let one_byte = close(&[3]);
        assert_eq!(
            reader(one_byte.clone(), Role::Server, rtc).read().unwrap(),
            Message::Close(None)
        );
        assert!(matches!(
            reader(one_byte, Role::Server, config()).read(),
            Err(Error::Protocol(ProtocolError::InvalidCloseSequence))
        ));

        for (payload, expected) in [
            ([3, 246, 255], "bad close code 1014"),
            ([3, 232, 255], "invalid utf8 payload in close frame"),
        ] {
            let error = reader(close(&payload), Role::Server, rtc).read().unwrap_err();
            match error {
                Error::Protocol(ProtocolError::LivekitRtcProtocolViolation(reason)) => {
                    assert_eq!(reason.as_str(), expected);
                }
                other => panic!("unexpected close error: {other}"),
            }
        }
    }

    #[test]
    fn malformed_deflate_reserved_bits_and_context_takeover_are_rejected() {
        let good = [0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00];
        let input = raw(OpCode::Data(OpData::Text), true, true, &good, true);
        assert!(matches!(
            reader(input, Role::Server, WebSocketConfig::default()).read(),
            Err(Error::Protocol(ProtocolError::NonZeroReservedBits))
        ));
        for opcode in [OpCode::Control(OpCtl::Ping), OpCode::Data(OpData::Continue)] {
            let input = raw(opcode, true, true, &good, true);
            assert!(matches!(
                reader(input, Role::Server, config()).read(),
                Err(Error::Protocol(ProtocolError::NonZeroReservedBits))
            ));
        }
        let input = raw(OpCode::Data(OpData::Binary), true, true, &[0x06], true);
        assert!(matches!(
            reader(input, Role::Server, config()).read(),
            Err(Error::Protocol(ProtocolError::InvalidCompression))
        ));
        let mut input = raw(OpCode::Data(OpData::Text), true, true, &good, true);
        // RFC 7692 §7.2.3.2's second message needs the first message's history.
        input.extend(raw(
            OpCode::Data(OpData::Text),
            true,
            true,
            &[0xf2, 0x00, 0x11, 0x00, 0x00],
            true,
        ));
        let mut socket = reader(input, Role::Server, config());
        assert_eq!(socket.read().unwrap(), Message::text("Hello"));
        assert!(matches!(
            socket.read(),
            Err(Error::Protocol(ProtocolError::InvalidCompression))
        ));
    }
    #[test]
    fn outgoing_text_and_binary_use_fresh_deflate_state_for_each_message() {
        let mut server = WebSocket::from_raw_socket(Wire::default(), Role::Server, Some(config()));
        let message = Message::text("same message with a fresh compression dictionary");
        server.send(message.clone()).unwrap();
        let first = server.get_ref().output.clone();
        assert_ne!(first[0] & 0x40, 0);
        server.send(message.clone()).unwrap();
        assert_eq!(&server.get_ref().output[first.len()..], &first);
        server
            .send(Message::Binary(vec![0, 1, 2, 255].into()))
            .unwrap();
        let mut client = reader(server.into_inner().output, Role::Client, config());
        assert_eq!(client.read().unwrap(), message);
        assert_eq!(client.read().unwrap(), message);
        assert_eq!(
            client.read().unwrap(),
            Message::Binary(vec![0, 1, 2, 255].into())
        );
    }
    #[test]
    fn extension_offer_parser_matches_gorilla_token_and_header_boundaries() {
        assert!(offers_permessage_deflate([
            "foo; x=\"a,b\", permessage-deflate; client_max_window_bits"
        ]));
        assert!(offers_permessage_deflate([
            "bad invalid",
            "permessage-deflate"
        ]));
        assert!(!offers_permessage_deflate(["PerMessage-Deflate"]));
        assert!(!offers_permessage_deflate([
            "foo invalid, permessage-deflate"
        ]));
        assert!(!offers_permessage_deflate(["permessage-deflate; =invalid"]));
        assert!(!offers_permessage_deflate([""]));
    }
}
