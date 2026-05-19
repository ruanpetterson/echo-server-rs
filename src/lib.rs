//! Single-threaded event-driven TCP server built around an epoll-backed reactor.

#![warn(missing_docs)]
#![warn(clippy::undocumented_unsafe_blocks)]

mod acceptor;
mod connection;
mod reactor;
mod server;
mod task;

pub mod protocol;

pub use server::{Server, ServerConfig};
