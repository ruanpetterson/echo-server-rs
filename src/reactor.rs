use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::mem::MaybeUninit;
use std::ops::ControlFlow;
use std::os::fd::RawFd;
use std::ptr;
use std::rc::Rc;

use crate::HandleImpl;

pub struct Reactor {
    epfd: RawFd,
    shutdown_fd: RawFd,
    tasks_ready: Rc<RefCell<VecDeque<RawFd>>>,
    tasks_storage: Rc<RefCell<Vec<Option<(RawFd, HandleImpl)>>>>,
}

impl Reactor {
    pub fn new(
        tasks_ready: Rc<RefCell<VecDeque<RawFd>>>,
        tasks_storage: Rc<RefCell<Vec<Option<(RawFd, HandleImpl)>>>>,
    ) -> io::Result<Self> {
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

    pub fn register(&self, fd: RawFd) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: fd as u64,
        };

        if unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_ADD, fd, &mut event) } < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EEXIST) {
                return Err(error);
            }

            if unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_MOD, fd, &mut event) } < 0 {
                return Err(io::Error::last_os_error());
            }
        }

        Ok(())
    }

    pub fn register_with(&self, fd: RawFd, flags: i32) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: flags as u32,
            u64: fd as u64,
        };

        if unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_ADD, fd, &mut event) } < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EEXIST) {
                return Err(error);
            }

            if unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_MOD, fd, &mut event) } < 0 {
                return Err(io::Error::last_os_error());
            }
        }

        Ok(())
    }

    pub fn unregister(&self, fd: RawFd) -> io::Result<()> {
        if unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_DEL, fd, ptr::null_mut()) } < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    pub fn wait(&self) -> ControlFlow<(), ()> {
        let epfd = self.epfd;

        let mut events = [libc::epoll_event { events: 0, u64: 0 }; 1024];

        let n = unsafe { libc::epoll_wait(epfd, events.as_mut_ptr(), events.len() as i32, -1) };

        if n < 0 {
            // return Err(io::Error::last_os_error());
            return ControlFlow::Break(());
        }

        for event in events.iter().take(n as usize) {
            let fd = event.u64 as RawFd;

            if fd == self.shutdown_fd {
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
fn shutdown_fd() -> io::Result<RawFd> {
    let mut mask = unsafe { MaybeUninit::<libc::sigset_t>::zeroed().assume_init() };

    if unsafe { libc::sigemptyset(&mut mask) } < 0 {
        return Err(io::Error::last_os_error());
    }

    if unsafe { libc::sigaddset(&mut mask, libc::SIGINT) } < 0 {
        return Err(io::Error::last_os_error());
    }

    if unsafe { libc::sigprocmask(libc::SIG_BLOCK, &mask, ptr::null_mut()) } < 0 {
        return Err(io::Error::last_os_error());
    }

    let fd = unsafe { libc::signalfd(-1, &mask, libc::SFD_NONBLOCK | libc::SFD_CLOEXEC) };

    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(fd)
}
