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
    /// Pending response bytes.
    write_buf: Vec<u8>,
    /// Number of pending response bytes already written.
    written: Option<usize>,
    /// Protocol marker for static specialization.
    protocol: PhantomData<P>,
}

impl<P> Connection<P> {
    /// Creates a connection task from an accepted stream.
    pub(crate) fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            write_buf: Vec::new(),
            written: None,
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
        if self.written.is_some() {
            return self.flush(reactor);
        }

        let mut read_buf = [0u8; READ_BUFFER_LEN];

        match self.stream.read(&mut read_buf) {
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                reactor.register_read(self.fd())?;
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
                self.write_buf.clear();
                self.written =
                    P::prepare_response(&read_buf[..read], &mut self.write_buf).then_some(0);

                if self.written.is_some() {
                    self.flush(reactor)
                } else {
                    Ok(TaskStatus::Complete)
                }
            }
        }
    }

    /// Writes as much of the pending response as the socket accepts.
    fn flush(&mut self, reactor: &Reactor) -> io::Result<TaskStatus> {
        let Some(written) = self.written.as_mut() else {
            return Ok(TaskStatus::Pending);
        };

        while *written < self.write_buf.len() {
            match self.stream.write(&self.write_buf[*written..]) {
                Ok(0) => {
                    reactor.register_read_write(self.fd())?;
                    return Ok(TaskStatus::Pending);
                }
                Ok(w) => *written += w,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    reactor.register_read_write(self.fd())?;
                    return Ok(TaskStatus::Pending);
                }
                Err(_) => return Ok(TaskStatus::Complete),
            }
        }

        self.write_buf.clear();
        self.written = None;
        reactor.register_read(self.fd())?;
        Ok(TaskStatus::Pending)
    }
}
