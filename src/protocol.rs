//! Compile-time protocol implementations.

/// Protocol behavior used by a statically specialized server.
pub trait Protocol {
    /// Whether accepted sockets should enable `TCP_NODELAY`.
    const TCP_NODELAY: bool;

    /// Builds a response for `request` into `response`.
    fn prepare_response(request: &[u8], response: &mut Vec<u8>) -> bool;

    /// Returns a static response for protocols that do not need per-request response allocation.
    fn static_response(_request: &[u8]) -> Option<&'static [u8]> {
        None
    }
}

/// Echo protocol that writes back exactly the bytes read from the socket.
pub struct Echo;
impl Protocol for Echo {
    const TCP_NODELAY: bool = false;

    fn prepare_response(request: &[u8], response: &mut Vec<u8>) -> bool {
        response.extend_from_slice(request);
        true
    }
}

/// Fixed-response HTTP protocol for `GET / HTTP/1.1`.
pub struct HttpHelloWorld;
impl Protocol for HttpHelloWorld {
    const TCP_NODELAY: bool = true;

    fn prepare_response(request: &[u8], response: &mut Vec<u8>) -> bool {
        let Some(static_response) = Self::static_response(request) else {
            return false;
        };

        response.extend_from_slice(static_response);
        true
    }

    fn static_response(request: &[u8]) -> Option<&'static [u8]> {
        // N.B. using question mark bloat the codegen.
        #[allow(clippy::question_mark)]
        let Some(request_line) = request.split(|&byte| byte == b'\n').next() else {
            return None;
        };

        if request_line.trim_ascii() != HTTP_REQUEST_LINE {
            return None;
        }

        Some(HTTP_RESPONSE)
    }
}

/// Request line accepted by the HTTP protocol.
const HTTP_REQUEST_LINE: &[u8] = b"GET / HTTP/1.1";

/// Full HTTP response emitted by the HTTP protocol.
const HTTP_RESPONSE: &[u8] = b"HTTP/1.1 200\r\nContent-Type: text/plain\r\nContent-Length: 15\r\nConnection: keep-alive\r\n\r\nHello, world!\r\n";

#[cfg(test)]
mod tests {
    use super::{Echo, HttpHelloWorld, Protocol};

    #[test]
    fn echo_returns_exact_request_bytes() {
        let mut response = Vec::new();

        assert!(Echo::prepare_response(b"abc\0def", &mut response));
        assert_eq!(response, b"abc\0def");
    }

    #[test]
    fn http_accepts_root_get_request() {
        let mut response = Vec::new();

        assert!(HttpHelloWorld::prepare_response(
            b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n",
            &mut response,
        ));
        assert!(response.starts_with(b"HTTP/1.1 200\r\n"));
    }

    #[test]
    fn http_rejects_other_request_lines() {
        let mut response = Vec::new();

        assert!(!HttpHelloWorld::prepare_response(
            b"GET /nope HTTP/1.1\r\n\r\n",
            &mut response,
        ));
        assert!(response.is_empty());
    }
}
