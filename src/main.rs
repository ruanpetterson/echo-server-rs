//! Single-threaded event-driven TCP server built around an epoll-backed reactor.
//!
//! The binary can operate as either a small echo server or a fixed-response HTTP
//! server, depending on the `IS_ECHO_SERVER` toggle.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::undocumented_unsafe_blocks)]

use std::cell::RefCell;
use std::collections::VecDeque;
use std::error::Error;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::ops::ControlFlow;
use std::os::fd::{AsRawFd, RawFd};
use std::rc::Rc;

/// Linux-specific reactor implementation used by the server loop.
mod reactor;

// FIXME: echo server is broken, but it's works for small inputs.
/// Toggles the binary between echo mode and fixed-response HTTP mode.
const IS_ECHO_SERVER: bool = true;

/// Request-line prefix accepted by the HTTP mode branch.
const HTTP_REQUEST: &[u8] = b"GET / HTTP/1.1";
/// Full HTTP response written back when the request matches `HTTP_REQUEST`.
const HTTP_RESPONSE: &[u8] = b"HTTP/1.1 200\r\nContent-Type: text/plain\r\nContent-Length: 15\r\nConnection: keep-alive\r\n\r\nHello, world!\r\n";

/// Starts the listener, schedules the initial accept task, and runs the reactor loop.
fn main() -> Result<(), Box<dyn Error>> {
    let tasks_ready = Rc::new(RefCell::new(VecDeque::with_capacity(1024)));
    let tasks_storage = Rc::new(RefCell::new(Vec::with_capacity(1024)));

    let reactor = Rc::new(reactor::Reactor::new(
        tasks_ready.clone(),
        tasks_storage.clone(),
    )?);

    let addr = {
        let ip = Ipv4Addr::new(0, 0, 0, 0);
        SocketAddrV4::new(ip, 1337)
    };

    let listener = TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let fd = listener.as_raw_fd();

    let accept_fn = Acceptor {
        reactor: reactor.clone(),
        listener,
        tasks_ready: tasks_ready.clone(),
        tasks_storage: tasks_storage.clone(),
    };

    tasks_ready.borrow_mut().push_back(fd);

    tasks_storage
        .borrow_mut()
        .push(Some((fd, HandleImpl::from(accept_fn))));

    let mut fds = std::iter::from_fn(|| {
        let mut tasks = tasks_ready.borrow_mut();
        tasks.pop_front()
    });

    loop {
        for fd in fds.by_ref() {
            let (index, fd, mut task) = {
                let mut tasks_storage = tasks_storage.borrow_mut();

                let Some(index) = tasks_storage
                    .iter()
                    .filter_map(Option::as_ref)
                    .position(|(fd_, _)| *fd_ == fd)
                else {
                    continue;
                };

                // SAFETY: `index` comes from `position` on this exact vector borrow, so it is
                // in-bounds for the duration of this access.
                let Some((fd, task)) = unsafe { tasks_storage.get_unchecked_mut(index) }.take()
                else {
                    continue;
                };

                (index, fd, task)
            };

            let task_still_pending = task.handle().is_continue();
            let mut tasks_storage = tasks_storage.borrow_mut();

            if task_still_pending {
                // SAFETY: `index` still refers to the same slot that was taken above, and the
                // vector has not been resized or reordered before reinserting into that slot.
                _ = unsafe { tasks_storage.get_unchecked_mut(index) }.insert((fd, task));
            } else {
                _ = tasks_storage.swap_remove(index);
            }
        }

        if reactor.wait().is_break() {
            break;
        }
    }

    Ok(())
}

/// Common interface for scheduled tasks driven by ready file descriptors.
trait Handle {
    /// Advances the task once and reports whether it should remain scheduled.
    fn handle(&mut self) -> ControlFlow<(), ()>;
}

/// Accepts new client connections and schedules a `Handler` for each accepted stream.
struct Acceptor {
    /// Reactor used to register interest in the listener file descriptor.
    reactor: Rc<reactor::Reactor>,
    /// Non-blocking listening socket bound by `main`.
    listener: TcpListener,
    /// Queue of file descriptors ready to be processed by the main loop.
    tasks_ready: Rc<RefCell<VecDeque<RawFd>>>,
    /// Storage backing the task queue keyed by file descriptor.
    tasks_storage: Rc<RefCell<Vec<Option<(RawFd, HandleImpl)>>>>,
}

impl Handle for Acceptor {
    fn handle(&mut self) -> ControlFlow<(), ()> {
        loop {
            let stream = match self.listener.accept() {
                Ok((stream, _)) => stream,
                Err(e) if let ErrorKind::WouldBlock = e.kind() => {
                    _ = self.reactor.register(self.listener.as_raw_fd());
                    break;
                }
                Err(e) => {
                    _ = self.reactor.unregister(self.listener.as_raw_fd());
                    eprintln!("ERROR(acceptor): {e}");
                    return ControlFlow::Break(());
                }
            };

            stream.set_nonblocking(true).unwrap();
            stream.set_nodelay(!IS_ECHO_SERVER).unwrap();

            let handle_fn = Handler {
                stream,
                reactor: self.reactor.clone(),
                buf: vec![],
                written: None,
            };

            self.tasks_ready
                .borrow_mut()
                .push_back(handle_fn.stream.as_raw_fd());

            self.tasks_storage.borrow_mut().push(Some((
                handle_fn.stream.as_raw_fd(),
                HandleImpl::from(handle_fn),
            )));
        }

        ControlFlow::Continue(())
    }
}

/// Reads from and writes to a single client connection.
struct Handler {
    /// Reactor used to adjust readiness interests for the stream.
    reactor: Rc<reactor::Reactor>,
    /// Non-blocking TCP stream owned by this handler.
    stream: TcpStream,
    /// Buffered payload for echo-mode writes.
    buf: Vec<u8>,
    /// Number of bytes already written for the current response, if any.
    written: Option<usize>,
}

impl Handle for Handler {
    fn handle(&mut self) -> ControlFlow<(), ()> {
        let mut buf = [0u8; 1024];

        let write = |written_opt: &mut Option<usize>,
                     buf: &[u8],
                     reactor: &Rc<reactor::Reactor>,
                     mut stream: &TcpStream| {
            if let Some(written) = written_opt.as_mut() {
                let buf = if IS_ECHO_SERVER { buf } else { HTTP_RESPONSE };
                let buf = &buf[*written..];
                // let buf = unsafe { HTTP_RESPONSE.get_unchecked(*written..) };
                return match stream.write(buf) {
                    Ok(n) => {
                        *written += n;

                        if *written == HTTP_RESPONSE.len() {
                            _ = reactor.register(stream.as_raw_fd());
                            _ = written_opt.take();
                            return ControlFlow::Continue(());
                        }

                        _ = reactor
                            .register_with(stream.as_raw_fd(), libc::EPOLLIN | libc::EPOLLOUT);
                        ControlFlow::Continue(())
                    }
                    Err(e) if let ErrorKind::WouldBlock = e.kind() => {
                        _ = reactor
                            .register_with(stream.as_raw_fd(), libc::EPOLLIN | libc::EPOLLOUT);
                        ControlFlow::Continue(())
                    }
                    Err(_) => ControlFlow::Break(()),
                };
            };

            ControlFlow::Continue(())
        };

        if self.written.is_some() {
            return write(&mut self.written, &self.buf, &self.reactor, &self.stream);
        }

        match self.stream.read(&mut buf) {
            Err(e) if let ErrorKind::WouldBlock = e.kind() => {
                _ = self.reactor.register(self.stream.as_raw_fd());
                ControlFlow::Continue(())
            }
            Ok(0) => {
                ControlFlow::Break(())
            }
            Err(e) => {
                if !matches!(e.kind(), ErrorKind::ConnectionReset) {
                    eprintln!("ERROR(handler): {e}");
                }

                ControlFlow::Break(())
            }
            Ok(n) => {
                if IS_ECHO_SERVER {
                    _ = self.written.insert(0);
                    self.buf.clear();
                    self.buf.extend_from_slice(&buf);
                    return write(&mut self.written, &self.buf, &self.reactor, &self.stream);
                }

                let Some(path) = &buf[..n].split(|&c| c == b'\n').next() else {
                    return ControlFlow::Break(());
                };

                if matches!(path.trim_ascii(), HTTP_REQUEST) {
                    _ = self.written.insert(0);
                    return write(&mut self.written, &self.buf, &self.reactor, &self.stream);
                }

                ControlFlow::Break(())
            }
        }
    }
}

/// Tagged task storage used by the scheduler to keep heterogeneous handlers in one vector.
enum HandleImpl {
    /// Accept loop task bound to the listening socket.
    Acceptor(Acceptor),
    /// Per-connection read/write task.
    Handler(Handler),
}

impl From<Acceptor> for HandleImpl {
    fn from(acceptor: Acceptor) -> Self {
        HandleImpl::Acceptor(acceptor)
    }
}

impl From<Handler> for HandleImpl {
    fn from(handler: Handler) -> Self {
        HandleImpl::Handler(handler)
    }
}

impl Handle for HandleImpl {
    fn handle(&mut self) -> ControlFlow<(), ()> {
        match self {
            HandleImpl::Acceptor(acceptor) => acceptor.handle(),
            HandleImpl::Handler(handler) => handler.handle(),
        }
    }
}
