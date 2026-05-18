//! Epoll-backed readiness reactor used by the server event loop.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::mem::MaybeUninit;
use std::ops::ControlFlow;
use std::os::fd::RawFd;
use std::ptr;
use std::rc::Rc;

use crate::HandleImpl;

/// Coordinates readiness notifications for task-owned file descriptors.
pub struct Reactor {
    /// epoll instance used to wait for I/O readiness.
    epfd: RawFd,
    /// signalfd monitored to initiate graceful shutdown on `SIGINT`.
    shutdown_fd: RawFd,
    /// Queue of task file descriptors that should be run by the main loop.
    tasks_ready: Rc<RefCell<VecDeque<RawFd>>>,
    /// Backing storage for all scheduled tasks.
    tasks_storage: Rc<RefCell<Vec<Option<(RawFd, HandleImpl)>>>>,
}

impl Reactor {
    /// Creates a reactor, wires in shutdown handling, and registers the shutdown fd.
    pub fn new(
        tasks_ready: Rc<RefCell<VecDeque<RawFd>>>,
        tasks_storage: Rc<RefCell<Vec<Option<(RawFd, HandleImpl)>>>>,
    ) -> io::Result<Self> {
        // SAFETY: `epoll_create1` has no aliasing requirements; the provided flag is valid and
        // no pointers are involved.
        let epfd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };

        if epfd < 0 {
            return Err(io::Error::last_os_error());
        }

        let reactor = Self {
            epfd,
            shutdown_fd: shutdown_fd()?,
            tasks_ready,
            tasks_storage,
        };

        reactor.register(reactor.shutdown_fd)?;

        Ok(reactor)
    }

    /// Registers `fd` for readable events, updating the existing registration if needed.
    pub fn register(&self, fd: RawFd) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: fd as u64,
        };

        // SAFETY: `self.epfd` and `fd` are owned file descriptors, and `event` is a valid
        // mutable pointer for the duration of the syscall.
        if unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_ADD, fd, &mut event) } < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EEXIST) {
                return Err(error);
            }

            // SAFETY: same as above; this updates an existing registration using the same valid
            // event pointer.
            if unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_MOD, fd, &mut event) } < 0 {
                return Err(io::Error::last_os_error());
            }
        }

        Ok(())
    }

    /// Registers `fd` with explicit epoll `flags`, updating an existing registration if needed.
    pub fn register_with(&self, fd: RawFd, flags: i32) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: flags as u32,
            u64: fd as u64,
        };

        // SAFETY: `self.epfd` and `fd` are live descriptors, and `event` remains valid across
        // the syscall.
        if unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_ADD, fd, &mut event) } < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EEXIST) {
                return Err(error);
            }

            // SAFETY: same as above; this mutates an existing epoll registration in place.
            if unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_MOD, fd, &mut event) } < 0 {
                return Err(io::Error::last_os_error());
            }
        }

        Ok(())
    }

    /// Removes `fd` from the epoll interest list.
    pub fn unregister(&self, fd: RawFd) -> io::Result<()> {
        // SAFETY: `self.epfd` and `fd` are raw file descriptors and `EPOLL_CTL_DEL` ignores the
        // event pointer, so a null pointer is permitted here.
        if unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_DEL, fd, ptr::null_mut()) } < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// Waits for readiness events and enqueues the corresponding task file descriptors.
    pub fn wait(&self) -> ControlFlow<(), ()> {
        let epfd = self.epfd;

        let mut events = [libc::epoll_event { events: 0, u64: 0 }; 1024];

        // SAFETY: `events` is a properly initialized writable buffer and its pointer remains
        // valid for the duration of the blocking syscall.
        let n = unsafe { libc::epoll_wait(epfd, events.as_mut_ptr(), events.len() as i32, -1) };

        if n < 0 {
            // return Err(io::Error::last_os_error());
            return ControlFlow::Break(());
        }

        for event in events.iter().take(n as usize) {
            let fd = event.u64 as RawFd;

            if fd == self.shutdown_fd {
                // SAFETY: `event.u64` holds the signalfd that triggered shutdown and has not been
                // closed yet, so closing it once here is valid.
                unsafe {
                    libc::close(event.u64 as _);
                }

                for task in self.tasks_storage.borrow_mut().drain(..) {
                    let Some((fd, _task)) = task else {
                        continue;
                    };

                    _ = self.unregister(fd);
                }

                _ = self.unregister(fd);

                // SAFETY: `epfd` is this reactor's epoll descriptor and is closed exactly once
                // during the shutdown path.
                unsafe {
                    libc::close(epfd as _);
                }

                return ControlFlow::Break(());
            }

            _ = self.unregister(fd);
            self.tasks_ready.borrow_mut().push_back(fd);
        }

        ControlFlow::Continue(())
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
