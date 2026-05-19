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
        let mut entries = Vec::with_capacity(capacity);
        entries.resize_with(capacity, || None);

        Self { entries }
    }

    /// Inserts a new task for `fd`.
    pub(crate) fn insert(&mut self, fd: RawFd, task: Task<P>) {
        let index = fd as usize;

        if self.entries.len() <= index {
            self.entries.resize_with(index + 1, || None);
        }

        self.entries[index] = Some(TaskEntry { fd, task });
    }

    /// Takes the task registered for `fd`, leaving its slot empty.
    pub(crate) fn take(&mut self, fd: RawFd) -> Option<(usize, TaskEntry<P>)> {
        let index = fd as usize;
        let entry = self.entries.get_mut(index)?.take()?;

        Some((index, entry))
    }

    /// Replaces a previously taken task at `index`.
    pub(crate) fn put(&mut self, index: usize, entry: TaskEntry<P>) {
        self.entries[index] = Some(entry);
    }

    /// Removes a previously taken task slot.
    pub(crate) fn remove(&mut self, index: usize) {
        self.entries[index] = None;
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

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::os::fd::RawFd;
    use std::os::unix::io::{FromRawFd, IntoRawFd};

    use super::{Task, TaskStorage};
    use crate::acceptor::Acceptor;
    use crate::protocol::Protocol;

    struct TestProtocol;

    impl Protocol for TestProtocol {
        const TCP_NODELAY: bool = false;

        fn prepare_response(_request: &[u8], _response: &mut Vec<u8>) -> bool {
            false
        }
    }

    fn task() -> Task<TestProtocol> {
        let fd = File::open("/dev/null").unwrap().into_raw_fd();
        // SAFETY: the test never calls listener operations. It only needs an owned fd so dropping
        // the task closes a valid descriptor without requiring network sandbox permissions.
        let listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };

        Task::from(Acceptor::new(listener))
    }

    #[test]
    fn storage_takes_tasks_by_fd() {
        let mut storage = TaskStorage::with_capacity(4);

        storage.insert(3, task());
        storage.insert(5, task());

        let (_, entry) = storage.take(5).unwrap();
        assert_eq!(entry.fd, 5);
        assert!(storage.take(5).is_none());

        let (_, entry) = storage.take(3).unwrap();
        assert_eq!(entry.fd, 3);
    }

    #[test]
    fn storage_grows_to_high_fd() {
        let mut storage = TaskStorage::with_capacity(1);
        let high_fd: RawFd = 128;

        storage.insert(high_fd, task());

        let (_, entry) = storage.take(high_fd).unwrap();
        assert_eq!(entry.fd, high_fd);
    }

    #[test]
    fn storage_puts_and_removes_by_index() {
        let mut storage = TaskStorage::with_capacity(4);

        storage.insert(3, task());
        let (index, entry) = storage.take(3).unwrap();

        storage.put(index, entry);
        assert!(storage.take(3).is_some());

        storage.insert(3, task());
        let (index, _) = storage.take(3).unwrap();
        storage.remove(index);
        assert!(storage.take(3).is_none());
    }
}
