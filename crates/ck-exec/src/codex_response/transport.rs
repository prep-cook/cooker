//! Codex response stream delivery and bootstrap buffering.
use super::*;
/// Upstream events → processed client events. Stops after a terminal event or the first
/// error; a clean EOF before any terminal event is Go's request-scoped 408.
pub(crate) fn processed(upstream: ExecStream, processor: Processor, emitted: usize) -> ExecStream {
    struct State {
        upstream: ExecStream,
        processor: Processor,
        emitted: usize,
        done: bool,
        /// A failure to deliver after the lines that preceded it in its event.
        failed: Option<ExecError>,
    }
    futures_util::stream::unfold(
        State {
            upstream,
            processor,
            emitted,
            done: false,
            failed: None,
        },
        |mut st| async move {
            if let Some(error) = st.failed.take() {
                st.done = true;
                return Some((Err(error), st));
            }
            if st.done {
                return None;
            }
            let item = match st.upstream.next().await {
                Some(Ok(event)) => match st.processor.event(&event) {
                    Step::Event(out) => {
                        st.emitted += 1;
                        Ok(out)
                    }
                    Step::Terminal(out) => {
                        st.done = true;
                        Ok(out)
                    }
                    Step::Fail {
                        error,
                        before: Some(before),
                        ..
                    } => {
                        st.emitted += 1;
                        st.failed = Some(error);
                        Ok(before)
                    }
                    Step::Fail { error, .. } => {
                        st.done = true;
                        Err(error)
                    }
                },
                Some(Err(error)) => {
                    st.done = true;
                    // Go records the scan error, then how the stream ended.
                    st.processor.record(&error);
                    st.processor.record(&ended(st.emitted));
                    Err(error)
                }
                None => {
                    st.done = true;
                    let end = ended(st.emitted);
                    st.processor.record(&end);
                    // Go ends silently when nothing at all was emitted.
                    if st.emitted == 0 {
                        return None;
                    }
                    Err(end)
                }
            };
            Some((item, st))
        },
    )
    .boxed()
}

/// How a stream that ended without a terminal event is reported: Go's 502 "upstream
/// stream closed before first payload" when nothing was emitted, else the 408.
fn ended(emitted: usize) -> ExecError {
    if emitted == 0 {
        request_scoped(502, "upstream stream closed before first payload")
    } else {
        request_scoped(408, INCOMPLETE_MESSAGE)
    }
}

/// Result of holding back the frames that precede generation.
pub(crate) enum Bootstrap {
    /// Failed before anything was shown: the attempt may fail over.
    Reject(ExecError),
    /// The stream to deliver: held frames first, then the rest.
    Stream(ExecStream),
}

/// Codex stream bootstrap buffering (`codex.stream-bootstrap-buffering`, M5-0025): hold
/// handshake frames (created, rate limits, empty `*.added`, heartbeats) up to 48 frames,
/// 1 MiB and the optional timeout, so early failures and disconnects can fail over before
/// headers are committed. Lines count as frames, as in Go.
pub(crate) async fn bootstrap(
    mut upstream: ExecStream,
    mut processor: Processor,
    timeout: Option<Duration>,
    started: Instant,
) -> Bootstrap {
    let mut held: VecDeque<Result<Bytes, ExecError>> = VecDeque::new();
    let mut gate = Gate::new(timeout, started);
    loop {
        let event = match upstream.next().await {
            Some(Ok(event)) => event,
            Some(Err(error)) => {
                processor.record(&error);
                return Bootstrap::Reject(error);
            }
            None if held.is_empty() => {
                // "upstream stream closed before first payload": an empty stream.
                processor.record(&ended(0));
                return Bootstrap::Stream(futures_util::stream::empty().boxed());
            }
            None => {
                // Only handshake frames were buffered; no client output needs replay.
                let error = ExecError::local(408, FailureScope::Transport, INCOMPLETE_MESSAGE);
                processor.record(&error);
                return Bootstrap::Reject(error);
            }
        };
        match processor.event_with(&event, Some(&mut gate)) {
            Step::Fail { error, body, before } => {
                // Only a released stream must deliver its failure in-band.
                if !gate.released && !gate.timed_out() {
                    return Bootstrap::Reject(if body.as_deref().is_some_and(is_overload) {
                        bootstrap_overload(body.as_deref().unwrap_or_default())
                    } else {
                        error
                    });
                }
                held.extend(before.map(Ok));
                held.push_back(Err(error));
                return Bootstrap::Stream(futures_util::stream::iter(held).boxed());
            }
            Step::Terminal(out) => {
                held.push_back(Ok(out));
                return Bootstrap::Stream(futures_util::stream::iter(held).boxed());
            }
            Step::Event(out) => {
                held.push_back(Ok(out));
                if gate.released {
                    let emitted = held.len();
                    let rest = processed(upstream, processor, emitted);
                    return Bootstrap::Stream(futures_util::stream::iter(held).chain(rest).boxed());
                }
            }
        }
    }
}
