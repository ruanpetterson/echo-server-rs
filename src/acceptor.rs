//! Listener accept task.

use std::io::{self, ErrorKind};
use std::marker::PhantomData;
use std::net::TcpListener;
use std::os::fd::AsRawFd;

use crate::connection::Connection;
use crate::protocol::Protocol;
use crate::reactor::Reactor;
use crate::task::{ReadyQueue, Task, TaskStatus, TaskStorage};

/// Accepts client connections and schedules per-connection tasks.
pub(crate) struct Acceptor<P> {
    /// Non-blocking listening socket.
    listener: TcpListener,
    /// Protocol marker for accepted connections.
    protocol: PhantomData<P>,
}

impl<P> Acceptor<P> {
    /// Creates a new accept task.
    pub(crate) fn new(listener: TcpListener) -> Self {
        Self {
            listener,
            protocol: PhantomData,
        }
    }
}

impl<P> Acceptor<P>
where
    P: Protocol,
{
    /// Accepts all currently available clients.
    pub(crate) fn handle(
        &mut self,
        reactor: &Reactor,
        ready: &mut ReadyQueue,
        tasks: &mut TaskStorage<P>,
    ) -> io::Result<TaskStatus> {
        loop {
            let stream = match self.listener.accept() {
                Ok((stream, _)) => stream,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    reactor.modify_read(self.listener.as_raw_fd())?;
                    return Ok(TaskStatus::Pending);
                }
                Err(error) => {
                    _ = reactor.unregister(self.listener.as_raw_fd());
                    eprintln!("ERROR(acceptor): {error}");
                    return Ok(TaskStatus::Complete);
                }
            };

            stream.set_nonblocking(true)?;
            stream.set_nodelay(P::TCP_NODELAY)?;

            let connection = Connection::<P>::new(stream);
            let fd = connection.fd();

            reactor.add_read(fd)?;
            ready.push_back(fd);
            tasks.insert(fd, Task::from(connection));
        }
    }
}
