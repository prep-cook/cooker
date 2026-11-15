//! The end of a Kimi Chat stream: the old path completed a body that ended without `[DONE]`
//! by translating a synthetic one.

use bytes::Bytes;
use futures_util::StreamExt;

use crate::types::Body;

/// A Chat stream with the old path's synthetic `[DONE]` after a clean end at an event boundary.
/// A body that ends inside an event gets none, so the ingress fails it; an upstream `[DONE]` has
/// ended the stream before the producer reads further, and a read error ends the body.
pub(crate) fn completed(body: Body) -> Body {
    let state = (body, Some(Boundary::default()));
    futures_util::stream::unfold(state, |(mut body, mut boundary)| async move {
        let tracker = boundary.as_mut()?;
        match body.next().await {
            Some(Ok(chunk)) => {
                tracker.feed(&chunk);
                Some((Ok(chunk), (body, boundary)))
            }
            Some(Err(error)) => Some((Err(error), (body, None))),
            None if tracker.at_event_end() => Some((Ok(Bytes::from_static(b"data: [DONE]\n\n")), (body, None))),
            None => None,
        }
    })
    .boxed()
}

/// The line ends that close a body read so far. An SSE event ends at a blank line: two line
/// ends in a row, each `\n`, `\r` or `\r\n`. An empty body sent no event to end.
#[derive(Default)]
struct Boundary {
    /// Line ends at the end of the body, up to two.
    ends: u8,
    /// The body ends with `\r`, which a `\n` completes rather than adds to.
    cr: bool,
}

impl Boundary {
    fn feed(&mut self, chunk: &[u8]) {
        let tail = match chunk.iter().rposition(|b| !matches!(b, b'\n' | b'\r')) {
            Some(last) => {
                *self = Self { ends: 0, cr: false };
                &chunk[last + 1..]
            }
            None => chunk,
        };
        for &b in tail {
            if b == b'\n' && self.cr {
                self.cr = false;
            } else {
                self.ends = (self.ends + 1).min(2);
                self.cr = b == b'\r';
            }
        }
    }

    fn at_event_end(&self) -> bool {
        self.ends >= 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn ends_with_done(chunks: &[&'static str]) -> bool {
        let chunks: Vec<_> = chunks.iter().map(|c| Ok(Bytes::from_static(c.as_bytes()))).collect();
        let sent = chunks.len();
        let out: Vec<_> = completed(futures_util::stream::iter(chunks).boxed()).collect().await;
        out.len() > sent
    }

    #[tokio::test]
    async fn done_follows_only_a_blank_line() {
        for (chunks, done) in [
            (&["data: a\n\n"][..], true),
            (&["data: a\n", "\n"], true),
            (&["data: a\r\n\r\n"], true),
            (&["data: a\r", "\n\r", "\n"], true),
            (&["data: a\r\r"], true),
            (&["data: a"], false),
            (&["data: a\n"], false),
            (&["data: a\r\n"], false),
            (&["data: a\n\n", "data: b"], false),
            (&[], false),
        ] {
            assert_eq!(ends_with_done(chunks).await, done, "{chunks:?}");
        }
    }
}
