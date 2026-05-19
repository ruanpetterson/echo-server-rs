//! Server construction and event loop.

use std::io;
use std::marker::PhantomData;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::os::fd::AsRawFd;

use crate::acceptor::Acceptor;
use crate::protocol::Protocol;
use crate::reactor::{Reactor, Wait};
use crate::task::{ReadyQueue, Task, TaskEntry, TaskStorage};

/// Runtime configuration for the server.
pub struct ServerConfig {
    /// Address the listener binds to.
    pub addr: SocketAddrV4,
    /// Initial capacity for ready and stored tasks.
    pub task_capacity: usize,
    /// Number of epoll events read per wait.
    pub event_capacity: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 1337),
            task_capacity: 1024,
            event_capacity: 1024,
        }
    }
}

/// Single-threaded TCP server specialized for protocol `P`.
pub struct Server<P> {
    /// Reactor used to receive readiness events.
    reactor: Reactor,
    /// File descriptors ready to run.
    ready: ReadyQueue,
    /// Task storage keyed by file descriptor.
    tasks: TaskStorage<P>,
    /// Protocol marker for static specialization.
    protocol: PhantomData<P>,
}

impl<P> Server<P>
where
    P: Protocol,
{
    /// Binds the listening socket and creates a server runtime.
    pub fn bind(config: ServerConfig) -> io::Result<Self> {
        let listener = TcpListener::bind(config.addr)?;
        listener.set_nonblocking(true)?;

        let listener_fd = listener.as_raw_fd();
        let mut ready = ReadyQueue::with_capacity(config.task_capacity);
        let mut tasks = TaskStorage::with_capacity(config.task_capacity);
        let reactor = Reactor::new(config.event_capacity)?;

        ready.push_back(listener_fd);
        tasks.insert(listener_fd, Task::from(Acceptor::<P>::new(listener)));

        Ok(Self {
            reactor,
            ready,
            tasks,
            protocol: PhantomData,
        })
    }

    /// Runs the event loop until a shutdown signal or unrecoverable reactor error.
    pub fn run(mut self) -> io::Result<()> {
        loop {
            while let Some(fd) = self.ready.pop_front() {
                let Some((index, entry)) = self.tasks.take(fd) else {
                    continue;
                };

                self.drive_task(index, entry)?;
            }

            match self.reactor.wait(&mut self.ready)? {
                Wait::Ready => {}
                Wait::Shutdown => break,
            }
        }

        Ok(())
    }

    /// Advances a task once and stores it again if it remains active.
    fn drive_task(&mut self, index: usize, mut entry: TaskEntry<P>) -> io::Result<()> {
        if entry
            .task
            .handle(&self.reactor, &mut self.ready, &mut self.tasks)?
            .is_pending()
        {
            self.tasks.put(index, entry);
        } else {
            self.tasks.remove(index);
        }

        Ok(())
    }
}
