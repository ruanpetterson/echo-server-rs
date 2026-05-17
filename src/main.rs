use std::cell::RefCell;
use std::collections::VecDeque;
use std::error::Error;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::ops::ControlFlow;
use std::os::fd::{AsRawFd, RawFd};
use std::rc::Rc;

use rustc_hash::FxHashMap as HashMap;

mod reactor;

const IS_ECHO_SERVER: bool = false;
const HTTP_REQUEST: &[u8] = b"GET / HTTP/1.1";
const HTTP_RESPONSE: &[u8] = b"HTTP/1.1 200\r\nContent-Type: text/plain\r\nContent-Length: 15\r\nConnection: close\r\n\r\nHello, world!\r\n";

fn main() -> Result<(), Box<dyn Error>> {
    let tasks_ready = Rc::new(RefCell::new(VecDeque::with_capacity(8192)));
    let tasks_storage = Rc::new(RefCell::new(HashMap::default()));

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
        .insert(fd, HandleImpl::from(accept_fn));

    let mut fds = std::iter::from_fn(|| {
        let mut tasks = tasks_ready.borrow_mut();
        tasks.pop_front()
    });

    loop {
        for fd in fds.by_ref() {
            let Some(mut task) = tasks_storage.borrow_mut().remove(&fd) else {
                continue;
            };

            if task.handle().is_continue() {
                tasks_storage.borrow_mut().insert(fd, task);
            }
        }

        if reactor.wait().is_break() {
            break;
        }
    }

    Ok(())
}

trait Handle {
    fn handle(&mut self) -> ControlFlow<(), ()>;
}

struct Acceptor {
    reactor: Rc<reactor::Reactor>,
    listener: TcpListener,
    tasks_ready: Rc<RefCell<VecDeque<RawFd>>>,
    tasks_storage: Rc<RefCell<HashMap<RawFd, HandleImpl>>>,
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

            let handle_fn = Handler {
                stream,
                reactor: self.reactor.clone(),
            };

            self.tasks_ready
                .borrow_mut()
                .push_back(handle_fn.stream.as_raw_fd());

            self.tasks_storage
                .borrow_mut()
                .insert(handle_fn.stream.as_raw_fd(), HandleImpl::from(handle_fn));
        }

        ControlFlow::Continue(())
    }
}

impl Drop for Acceptor {
    fn drop(&mut self) {
        _ = self.reactor.unregister(self.listener.as_raw_fd());
    }
}

struct Handler {
    reactor: Rc<reactor::Reactor>,
    stream: TcpStream,
}

impl Handle for Handler {
    fn handle(&mut self) -> ControlFlow<(), ()> {
        let mut buf = [0u8; 1024];

        loop {
            match self.stream.read(&mut buf) {
                Err(e) if let ErrorKind::WouldBlock = e.kind() => {
                    _ = self.reactor.register(self.stream.as_raw_fd());
                    return ControlFlow::Continue(());
                }
                Ok(0) => {
                    return ControlFlow::Break(());
                }
                Err(e) => {
                    eprintln!("ERROR(handler): {e}");
                    return ControlFlow::Break(());
                }
                Ok(n) => {
                    if IS_ECHO_SERVER {
                        _ = self.stream.write_all(&buf[..n]);
                        continue;
                    }

                    let Some(path) = &buf[..n].split(|&c| c == b'\n').next() else {
                        return ControlFlow::Break(());
                    };

                    if matches!(path.trim_ascii(), HTTP_REQUEST) {
                        _ = self.stream.write_all(HTTP_RESPONSE);
                        return ControlFlow::Break(());
                    }
                }
            }
        }
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        _ = self.reactor.unregister(self.stream.as_raw_fd());
    }
}

enum HandleImpl {
    Acceptor(Acceptor),
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
