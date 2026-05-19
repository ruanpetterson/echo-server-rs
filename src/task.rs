//! Task queue and storage types.

use std::collections::VecDeque;
use std::io;
use std::os::fd::RawFd;

use crate::acceptor::Acceptor;
use crate::connection::Connection;
use crate::protocol::Protocol;
use crate::reactor::Reactor;

/// Queue of file descriptors ready to be processed.
pub(crate) type ReadyQueue = VecDeque<RawFd>;

/// A stored task and its associated file descriptor.
pub(crate) struct TaskEntry<P> {
    /// File descriptor used to look up this task.
    pub(crate) fd: RawFd,
    /// Task implementation.
    pub(crate) task: Task<P>,
}

/// Storage for scheduled tasks.
pub(crate) struct TaskStorage<P> {
    /// Sparse task slots.
    entries: Vec<Option<TaskEntry<P>>>,
}

impl<P> TaskStorage<P> {
    /// Creates task storage with an initial capacity.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }

    /// Inserts a new task for `fd`.
    pub(crate) fn insert(&mut self, fd: RawFd, task: Task<P>) {
        self.entries.push(Some(TaskEntry { fd, task }));
    }

    /// Takes the task registered for `fd`, leaving its slot empty.
    pub(crate) fn take(&mut self, fd: RawFd) -> Option<(usize, TaskEntry<P>)> {
        let index = self
            .entries
            .iter()
            .position(|entry| entry.as_ref().is_some_and(|entry| entry.fd == fd))?;

        let entry = self.entries.get_mut(index)?.take()?;
        Some((index, entry))
    }

    /// Replaces a previously taken task at `index`.
    pub(crate) fn put(&mut self, index: usize, entry: TaskEntry<P>) {
        self.entries[index] = Some(entry);
    }

    /// Removes a previously taken task slot.
    pub(crate) fn remove(&mut self, index: usize) {
        _ = self.entries.swap_remove(index);
    }
}

/// Result of advancing a task.
pub(crate) enum TaskStatus {
    /// Keep the task in storage.
    Pending,
    /// Drop the task from storage.
    Complete,
}

impl TaskStatus {
    /// Returns true when the task should stay scheduled.
    pub(crate) fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }
}

/// Tagged task storage for statically dispatched task variants.
pub(crate) enum Task<P> {
    /// Accept loop task bound to the listening socket.
    Acceptor(Acceptor<P>),
    /// Per-connection read/write task.
    Connection(Connection<P>),
}

impl<P> Task<P>
where
    P: Protocol,
{
    /// Advances the task once.
    pub(crate) fn handle(
        &mut self,
        reactor: &Reactor,
        ready: &mut ReadyQueue,
        tasks: &mut TaskStorage<P>,
    ) -> io::Result<TaskStatus> {
        match self {
            Self::Acceptor(acceptor) => acceptor.handle(reactor, ready, tasks),
            Self::Connection(connection) => connection.handle(reactor),
        }
    }
}

impl<P> From<Acceptor<P>> for Task<P> {
    fn from(acceptor: Acceptor<P>) -> Self {
        Self::Acceptor(acceptor)
    }
}

impl<P> From<Connection<P>> for Task<P> {
    fn from(connection: Connection<P>) -> Self {
        Self::Connection(connection)
    }
}
