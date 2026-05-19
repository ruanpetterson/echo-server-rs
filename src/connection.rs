//! Per-connection read/write task.

use std::io::{self, ErrorKind, Read, Write};
use std::marker::PhantomData;
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};

use crate::protocol::Protocol;
use crate::reactor::Reactor;
use crate::task::TaskStatus;

/// Number of bytes attempted per socket read.
const READ_BUFFER_LEN: usize = 1024;

/// Handles reads and writes for one TCP connection.
pub(crate) struct Connection<P> {
    /// Non-blocking TCP stream.
    stream: TcpStream,
    /// Reused read buffer.
    read_buf: [u8; READ_BUFFER_LEN],
    /// Pending response bytes.
    write_buf: Option<WriteSource>,
    /// Number of pending response bytes already written.
    written: usize,
    /// Protocol marker for static specialization.
    protocol: PhantomData<P>,
}

/// Source of the currently pending write.
enum WriteSource {
    /// Static protocol response.
    Static(&'static [u8]),
    /// Bytes stored in the connection-owned write buffer.
    Buffered(Vec<u8>),
}

impl<P> Connection<P> {
    /// Creates a connection task from an accepted stream.
    pub(crate) fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            read_buf: [0; READ_BUFFER_LEN],
            write_buf: None,
            written: 0,
            protocol: PhantomData,
        }
    }

    /// Returns the connection file descriptor.
    pub(crate) fn fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }
}

impl<P> Connection<P>
where
    P: Protocol,
{
    /// Advances this connection once.
    pub(crate) fn handle(&mut self, reactor: &Reactor) -> io::Result<TaskStatus> {
        if self.write_buf.is_some() {
            return self.flush(reactor);
        }

        match self.stream.read(&mut self.read_buf) {
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                reactor.modify_read(self.fd())?;
                Ok(TaskStatus::Pending)
            }
            Ok(0) => Ok(TaskStatus::Complete),
            Err(error) => {
                if error.kind() != ErrorKind::ConnectionReset {
                    eprintln!("ERROR(handler): {error}");
                }

                Ok(TaskStatus::Complete)
            }
            Ok(read) => {
                let request = &self.read_buf[..read];

                self.write_buf = if let Some(response) = P::static_response(request) {
                    Some(WriteSource::Static(response))
                } else {
                    let mut buf = vec![];
                    P::prepare_response(request, &mut buf).then_some(WriteSource::Buffered(buf))
                };
                self.written = 0;

                if self.write_buf.is_some() {
                    self.flush(reactor)
                } else {
                    Ok(TaskStatus::Complete)
                }
            }
        }
    }

    /// Writes as much of the pending response as the socket accepts.
    fn flush(&mut self, reactor: &Reactor) -> io::Result<TaskStatus> {
        let Some(write_source) = self.write_buf.as_ref() else {
            return Ok(TaskStatus::Pending);
        };
        let response = match write_source {
            WriteSource::Static(response) => *response,
            WriteSource::Buffered(response) => response,
        };

        while self.written < response.len() {
            match self.stream.write(&response[self.written..]) {
                Ok(0) => {
                    reactor.modify_read_write(self.fd())?;
                    return Ok(TaskStatus::Pending);
                }
                Ok(w) => self.written += w,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    reactor.modify_read_write(self.fd())?;
                    return Ok(TaskStatus::Pending);
                }
                Err(_) => return Ok(TaskStatus::Complete),
            }
        }

        self.write_buf = None;
        self.written = 0;
        reactor.modify_read(self.fd())?;

        Ok(TaskStatus::Pending)
    }
}
