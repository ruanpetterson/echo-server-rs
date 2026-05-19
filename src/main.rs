//! Binary entrypoint for the HTTP server.

use echo_server::Server;
use echo_server::ServerConfig;
use echo_server::protocol::HttpHelloWorld;

/// Instantiates the default HTTP server and runs it until shutdown.
fn main() -> std::io::Result<()> {
    let config = ServerConfig::default();
    Server::<HttpHelloWorld>::bind(config)?.run()
}
