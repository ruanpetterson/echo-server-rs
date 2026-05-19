//! Epoll-backed readiness reactor used by the server event loop.

use std::io;
use std::mem::MaybeUninit;
use std::os::fd::RawFd;
use std::ptr;

use crate::task::ReadyQueue;

/// Result of waiting for reactor events.
pub(crate) enum Wait {
    /// One or more task file descriptors may have been queued.
    Ready,
    /// Shutdown was requested.
    Shutdown,
}

/// Coordinates readiness notifications for task-owned file descriptors.
pub(crate) struct Reactor {
    /// epoll instance used to wait for I/O readiness.
    epfd: RawFd,
    /// signalfd monitored to initiate graceful shutdown on `SIGINT`.
    shutdown_fd: RawFd,
    /// Reused epoll event buffer.
    events: Vec<libc::epoll_event>,
}

impl Reactor {
    /// Creates a reactor and registers the shutdown signal descriptor.
    pub(crate) fn new(event_capacity: usize) -> io::Result<Self> {
        // SAFETY: `epoll_create1` has no aliasing requirements; the provided flag is valid and
        // no pointers are involved.
        let epfd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };

        if epfd < 0 {
            return Err(io::Error::last_os_error());
        }

        let reactor = Self {
            epfd,
            shutdown_fd: shutdown_fd()?,
            events: vec![libc::epoll_event { events: 0, u64: 0 }; event_capacity.max(1)],
        };

        reactor.add_read(reactor.shutdown_fd)?;

        Ok(reactor)
    }

    /// Registers `fd` for readable events.
    pub(crate) fn add_read(&self, fd: RawFd) -> io::Result<()> {
        self.control(fd, libc::EPOLL_CTL_ADD, libc::EPOLLIN | libc::EPOLLONESHOT)
    }

    /// Updates `fd` to readable events.
    pub(crate) fn modify_read(&self, fd: RawFd) -> io::Result<()> {
        self.control(fd, libc::EPOLL_CTL_MOD, libc::EPOLLIN | libc::EPOLLONESHOT)
    }

    /// Updates `fd` to readable and writable events.
    pub(crate) fn modify_read_write(&self, fd: RawFd) -> io::Result<()> {
        self.control(
            fd,
            libc::EPOLL_CTL_MOD,
            libc::EPOLLIN | libc::EPOLLOUT | libc::EPOLLONESHOT,
        )
    }

    /// Removes `fd` from the epoll interest list.
    pub(crate) fn unregister(&self, fd: RawFd) -> io::Result<()> {
        // SAFETY: `self.epfd` and `fd` are raw file descriptors and `EPOLL_CTL_DEL` ignores the
        // event pointer, so a null pointer is permitted here.
        if unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_DEL, fd, ptr::null_mut()) } < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// Waits for readiness events and enqueues their file descriptors.
    pub(crate) fn wait(&mut self, ready: &mut ReadyQueue) -> io::Result<Wait> {
        // SAFETY: `events` is a writable buffer and its pointer remains valid for the syscall.
        let count = unsafe {
            libc::epoll_wait(
                self.epfd,
                self.events.as_mut_ptr(),
                self.events.len() as i32,
                -1,
            )
        };

        if count < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                return Ok(Wait::Ready);
            }

            return Err(error);
        }

        for event in self.events.iter().take(count as usize) {
            let fd = event.u64 as RawFd;

            if fd == self.shutdown_fd {
                return Ok(Wait::Shutdown);
            }

            ready.push_back(fd);
        }

        Ok(Wait::Ready)
    }

    /// Applies an epoll control operation with explicit readiness flags.
    fn control(&self, fd: RawFd, operation: i32, flags: i32) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: flags as u32,
            u64: fd as u64,
        };

        // SAFETY: `self.epfd` and `fd` are live descriptors, and `event` remains valid across
        // the syscall.
        if unsafe { libc::epoll_ctl(self.epfd, operation, fd, &mut event) } < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        // SAFETY: these descriptors are owned by the reactor and closed once on drop.
        unsafe {
            libc::close(self.shutdown_fd);
            libc::close(self.epfd);
        }
    }
}

/// Creates a non-blocking `signalfd` that becomes readable when `SIGINT` is delivered.
fn shutdown_fd() -> io::Result<RawFd> {
    // SAFETY: zero initialization is a valid starting state for `sigset_t`, which is then fully
    // initialized through libc signal-set functions before use.
    let mut mask = unsafe { MaybeUninit::<libc::sigset_t>::zeroed().assume_init() };

    // SAFETY: `mask` is a valid mutable `sigset_t` pointer.
    if unsafe { libc::sigemptyset(&mut mask) } < 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: `mask` is initialized and `SIGINT` is a valid signal number to add.
    if unsafe { libc::sigaddset(&mut mask, libc::SIGINT) } < 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: `mask` points to a valid signal set and the old-mask pointer may be null when the
    // previous mask does not need to be captured.
    if unsafe { libc::sigprocmask(libc::SIG_BLOCK, &mask, ptr::null_mut()) } < 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: `mask` remains valid for the duration of the call, and the provided flags are valid
    // for creating a non-blocking close-on-exec signalfd.
    let fd = unsafe { libc::signalfd(-1, &mask, libc::SFD_NONBLOCK | libc::SFD_CLOEXEC) };

    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(fd)
}
