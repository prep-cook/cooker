# Execution contract (S1 Phase D)

This page fixes the interfaces Phase D units build against: stream ownership, the executor trait, the request handle, the response producer, the server exchange and stream handle, the usage tracker input, and the error and commitment rules. It complements `docs/INTERNAL-STREAM.md` §2.1 (responsibilities), §3.6 (commitment), §3.7 (destination policy) and §6.6–6.7 (resume). A change to a signature below is a contract change: update this page in the same PR.

## 1. Where each part lives

| Part | File | Implemented by |
| --- | --- | --- |
| `Execute`, `Attempt`, `Client`, `Request`, `Streams`, `Upstream`, `Body`, `Counted` | `crates/ck-exec/src/types.rs` | each provider unit |
| `Producer`, `Started` | `crates/ck-exec/src/pipeline.rs` | D1 |
| `Protocol`, `Framing`, `Output`, `deliver`, `Failed` | `crates/ck-server/src/http/egress.rs` | D2 |
| `Exchange`, `Owner`, `StreamHandle`, `StreamId`, `Uncommitted` | `crates/ck-server/src/inference/streams.rs` | D2 (exchange), D3 (registry, resume) |
| `CanonicalUsage`, `ResponseFacts` (tracker input) | `crates/ck-server/src/observability/usage_snapshot.rs` | D3 |
| `Writer::allocator`, `Store::create_dependent`, `Writer::now` | `crates/ck-stream/src/publication/writer.rs` | done in D0 |

Dependency edges `ck-exec -> ck-stream` and `ck-server -> ck-stream` are allowed in `crates/AGENTS.md`. The Anthropic, Responses and Gemini codecs are production modules of `ck-translate` (`ck_translate::{anthropic, responses, gemini}`); the old Gemini pair module is `gemini_gemini` until D9 deletes it.

Two tests prove the contract: `crates/ck-exec/tests/execution_contract.rs` type-checks an executor and a `Send` server task, and `crates/ck-server/tests/execution_flow.rs` runs one exchange end to end with the Chat codecs (two attempts, egress while the producer runs, resume after both streams finished).

## 2. Streams and quotas

An exchange uses one request stream and one response stream per attempt. Every response stream depends on the request stream (`Store::create_dependent`), and the request stream and its dependents charge one exchange quota (`docs/INTERNAL-STREAM.md` §6.4): their request, response, scratch and replay frames together against `max_frames`, and their metadata together against `max_metadata_bytes`, 8 MiB per exchange by default.

| Stream | Producer (`Writer`) | Holds | Charged with |
| --- | --- | --- | --- |
| Request | `Exchange::request` | the client request records (`request.start` .. `request.end`) and the request document | request ingress, every attempt's encoded upload (request egress scratch), request-view rebuilds on resume |
| Response, one per attempt | the attempt's producer task | that attempt's canonical response | response ingress, response egress output and state, resume re-encoding |

- **Cross-attempt encoding.** Request egress encodes only a document of the stream it allocates on (`Document::check_owner`). Every attempt therefore encodes into the request stream's scratch (`Streams::request`), never into its response stream, and the response ingress publishes into the attempt's fresh response stream (`Streams::response`), whose emitter starts from an empty log.
- **Allocation handles.** `Writer::allocator` returns a second handle on a stream that never publishes records. It allocates scratch and metadata under the exchange quota, also after the terminal record, until the stream is invalidated; a failure after the terminal fails only that delivery (§9). Codecs that read a stream need a writer of its generation (`internal::decode`, `Document::check_owner`), so response egress decodes and encodes with the response stream's allocator while the producer owns the producer writer, and resume uses the same handle after the producer ended. Dropping an allocator publishes nothing; its `finish` is the control path (§7).
- **Dependency.** Each attempt creates its response stream with `Store::create_dependent(&exchange.request)`. While a dependent is cached, the request stream neither expires nor is evicted on its own, so a committed response always has its request records; eviction takes the request stream and all its dependents as one unit, at the position of its most recently read member and never while any member has an outstanding reader lease, and the request expires on the first maintenance tick after its last dependent left. The link is charged before admission may evict another stream and is taken under the cache lock; it fails with `Gone` once the request stream is invalidated. A dependent, or a request of another store, cannot be a dependency (`InvalidRecord`), so dependencies form no chain or cycle.
- **Request view on resume.** The live egress reads the view of the client fence. A resume, served by a later HTTP request, rebuilds the view from the request stream's records with `codec::chat::request::read_request(&mut StreamHandle::request, reader)`, the protocol-neutral canonical rebuild, and allocates it on the request stream.

## 3. Executor

Every provider module implements `Execute` once. The selector in `crates/ck-exec/src/lib.rs` matches `Credential::provider` and calls the provider's implementation directly: no trait objects, no second registry.

- `Attempt::identity` is the codec identity of the attempt's response stream (direction `Response`, this attempt's number). The server creates it; executors build their response ingress with it, and the producer publishes a local count with it.
- `capability(&Attempt) -> Self::Capability` returns the destination capability for the attempt's credential and upstream model. It does no network work and no credential preparation. Configured limits such as Anthropic `default_max_tokens` enter through `Attempt::cfg` and `Attempt::resolved_model`.
- `execute` and `compact` take `(&Attempt, &Request, Streams)` and return `Result<Upstream<Self::Ingress>, ExecError>` as a `Send` future. The executor encodes the upload with its request egress into `Streams::request`, creates its response ingress on `Streams::response` with `Attempt::store`, sends, and returns when the upstream answered.
- `Request::validated` is this attempt's fence (§4). An executor encodes only through `RequestEgress`; no executor reads client body bytes, except the one path G5 allows.
- `Upstream` carries the success status, upstream headers, the upstream body as a byte stream, and the response ingress. The body is passed on chunk by chunk; nothing collects it.
- `count_tokens` returns `Counted`: `Counted::Upstream` when the upstream counts, or `Counted::Local { input_tokens }` for a tokenizer count. A local count re-fences the request view with `operation: Generate` (`RequestView` fields are public) and counts what request egress encodes for it; the provider's capability accepts `CountTokens` exactly when that `Generate` projection is valid. The producer publishes the canonical `count_tokens` result for a local count. A count is never generation usage.
- `compact` runs only a native compact. The capability refuses compact where the destination has none; a summary is never substituted.

API-key upstream requests send `User-Agent: cooker`, including requests with caller or configured user-agent headers. OAuth client identities and Claude cloak headers are unchanged (Q13, 2026-11-16).

xAI `compaction_trigger` input follows ordinary generation routing and is preserved for xAI to process. The gateway returns xAI's response through the response codec; it neither redirects the trigger to `/responses/compact` nor fabricates compaction events. Explicit native compact requests still use `/responses/compact` (Q17, 2026-11-16).

`Client::usage` carries request-side facts only (§8, G4).

**G5, the client's own bytes for the Claude cloak.** `Client::original` borrows the client's request body as the server received it; the server fills it on every attempt from the body it already holds, without a copy. Only the Claude executor reads it, and only for an Anthropic Messages request that the OAuth cloak or the Claude Code identity shapes: a credential the cloak may serve (an OAuth token, a fingerprint profile or a cloak setting), or a request the Claude Code detection confirms from its headers and body. That request is prepared from these bytes exactly as the old route prepares them, and the result goes upstream, because the detection reads the client's member order and `cch` signs the body the cloak built from them. This is one of two exceptions to the no-raw-passthrough rule in `crates/AGENTS.md` (owner ruling 2026-11-16, Q14 option (a)); live-session messages are the other (Settled 55). The fence (§4) still validates the request first, so the capability refuses what it refuses for any other client. Every other request, including a foreign client's request on a cloak credential, is encoded from the fenced view.

A generation the cloak may shape whose upstream answers in one JSON body stays on the old arm: restoring its tool aliases and recording its session continuity read the complete body, which §3 forbids collecting. Only the fenced document shows the client's `stream` member, so for it `destination` answers the capability key while `refence` and `start` refuse with `Destination`. A streamed generation and every count run on the contract path.

**G6, routing headers.** `Attempt::headers` is the client's request headers, which the server fills on every attempt. The selector reads them only to decide whether an attempt stays on a provider's old arm (`destination` answers `None`, or `refence` and `start` refuse with `Destination`), before any byte of the attempt is sent. Request execution reads `Client::headers`. Claude no longer routes by them: G5 gives its executor the client's own body.

`Request<'a>`, `Attempt<'a>` and `Store` are `Sync`, so a server task may hold them across an await.

## 4. Fences and failover

1. **Client fence (G8).** Request ingress `finish` closes the client fence against `codec::ClientCapability`, whose key is `codec::CLIENT`, and publishes `request.end`. The fence checks the complete document and its media but no destination, so a refused fence publishes a failure for the client's own request, never for one candidate's. No request egress encodes a request validated against this key: `ValidatedRequest::for_destination` refuses it. A failure is answered before candidate selection with no upstream request: 400 for `Json`, `Utf8`, `DuplicateKey`, `Truncated`, `Schema` and `Unsupported`, 413 for `Limit` and `EventTooLarge`.
2. **Re-fence per attempt.** Every attempt, the first included, builds its `Request::validated` with `RequestFence::default().end(client_fence.view(), &capability)` for its own destination. Only this re-fence authorizes encoding. A re-fence failure is an attempt failure with scope `Model`: nothing was sent.
3. **Q2.** Every candidate is attempted through step 2, whatever its capability key; failover is not restricted to the client fence's key (owner ruling 2026-11-16).

D1 adds these selector methods:

```rust
impl Executors {
    /// The attempt's capability key; `None` while its provider is not converted, in which
    /// case D2 takes the old execution path before fencing.
    pub fn destination(&self, attempt: &Attempt<'_>) -> Option<&'static str>;
    /// Closes a fence against the attempt's destination. The server does not call it: the
    /// client fence is step 1 (G8). D9 deletes it.
    pub fn fence<'a, I: RequestIngress>(&self, attempt: &Attempt<'_>, ingress: &'a mut I, request: &mut Writer)
        -> codec::Result<ValidatedRequest<'a>>;
    /// Step 2: re-fences the client view for this attempt's destination.
    pub fn refence<'a>(&self, attempt: &Attempt<'_>, view: RequestView<'a>) -> codec::Result<ValidatedRequest<'a>>;
    /// Holds the request stream from `response`, runs the operation in the view and starts
    /// the producer on `response`.
    pub async fn start(&self, attempt: &Attempt<'_>, request: &Request<'_>, upload: &mut Writer, response: Writer)
        -> Result<Started, ExecError>;
}
```

## 5. Producer

`pipeline::Started` is what an attempt yields once the upstream answered: status, headers, the response stream's allocator (`Started::stream`) and the `Producer` task. The producer owns the response stream's producer writer, the upstream body and the response ingress. It drives `upstream bytes -> ResponseIngress -> Writer` on its own task, schedules `flush` at `flush_deadline`, finishes the writer with `Terminal::Done` after the ingress completes, and stops when `Writer::cancelled` resolves. For document streams, a slow or absent client never slows it; live sessions use transport flow control instead (Settled 55).

The producer's output, `Result<(), ExecError>`, is the attempt's routing outcome. `Err` carries the scope the scheduler and cooldown use.

## 6. Server exchange and stream handle

An `Exchange` is one client request: its `Owner`, its egress `Output` and the request stream's producer writer. `Owner` is the creating `Caller` plus, for an ephemeral client key, its grant's call scope (`sess_...`); a resume must match both. `Output` is the client protocol, operation and `Framing` (`Json`, `Sse`, or Gemini's `JsonArray`), fixed when the exchange starts.

`http::egress::deliver` drives one attempt's response stream through the client codec. At commitment the exchange finishes the request stream with `Terminal::Done` and becomes a `StreamHandle`, registered under a fresh `StreamId` of 128 random bits; then the response goes out with `X-Ck-Stream-Id`, the zero cursor in `X-Ck-Stream-Cursor`, the caller's trace, routing and upstream headers, and the first bytes. An attempt whose stream publishes `error` or a terminal before any deliverable byte returns `Failed` with the exchange and an `Uncommitted` reason; the `error` record never reaches the egress. The live body is the resume body from the zero cursor, so SSE events carry `id: v1.e.<n>` from the start. The handle keeps the owner, the output, the response stream's allocator and the request stream's allocator. `StreamId` has no `Debug` or `Display` and travels only in `X-Ck-Stream-Id`.

Resume looks up the handle, authorizes the caller against `StreamHandle::owner` before any cursor lookup, attaches a reader with `Store::reader(response.id(), ..)`, rebuilds the request view (§2) and re-encodes with `StreamHandle::output`. A resume request cannot change the output, select a provider or start an attempt.

## 7. Cancel, discard and producer loss

- **Explicit cancel.** `finish(Terminal::Cancelled)` on the response stream's allocator. The producer's `Writer::cancelled` resolves, it stops reading upstream and its writes fail with `Closed`. Shutdown uses `Terminal::Shutdown` the same way.
- **Discard.** `finish(Terminal::Gone)` on a failed attempt's response writer or allocator invalidates the stream at once and releases its dependency on the request stream. An exchange that ends without commitment discards its request stream the same way.
- **Producer lost or producer defect.** A dropped producer writer (panic or abort), or a gateway defect after the producer started (`Storage(InvalidPosition, InvalidRecord, IncompleteRecord)`, an ingress bound to another writer), ends the stream with `Terminal::ProducerLost`; the client sees `stream.producer_lost`. A defect's producer outcome is 500 with scope `Request`, which never fails over; a panic stays 502 with scope `Transport`. Task abort is never a cancel.

## 8. Usage

The tracker's input is `CanonicalUsage` and `ResponseFacts`, read from the attempt's response stream by a runtime-owned reader, independently of client delivery, so a resumed or second reader never accounts again. `CanonicalUsage` is the latest canonical `x_ck.usage` snapshot: a later snapshot replaces the earlier one, and `None` is an absent count, not zero. `CanonicalUsage` also carries billing dimensions besides the token counts, each in its own unit: the service tier a source reports inside its usage, and the dimensions `usage_snapshot::DIMENSIONS` lists for the source protocol, read from the snapshot's `native` value, so provider fields stay out of the canonical snapshot. The legacy parsers read the same rows from the upstream usage object. The usage record writes them under `dimensions` and leaves out a dimension the upstream did not report. `ResponseFacts` carries the `response.start` model and the last chunk `service_tier`, which takes precedence over the usage tier. The Go breakdown is derived from the counts. D3 deletes `parse_body`, `StreamUsage` and the `Tracker::body`/`event` byte parsers.

**G4, request-side facts.** `Client::usage` is the attempt's `UsageSink`. An executor reports through it only what the canonical stream does not carry: `request` (the body sent upstream, for the reasoning effort), `upstream_model`, `claude_cache`, `round_trip_started`, `first_byte`, and upstream request and response capture through its `CaptureSink`. It never calls a `response_*` method; response usage is the canonical snapshot above.

## 9. Errors and commitment

Commitment is the first deliverable byte the client's `ResponseEgress` emits. Before it the client receives nothing: no status, success header or keepalive. From it on, failover is disabled even if the socket write fails.

`codec::Error::Storage` wraps every `ck_stream::Error`; boundaries map the inner error, not the variant.

| Failure | Raised as | Before commitment | After commitment |
| --- | --- | --- | --- |
| Client syntax or schema (`Json`, `Utf8`, `DuplicateKey`, `Truncated`, `Schema`) or unsupported semantics (`Unsupported`) at request ingress or the client fence | `codec::Error` | 400 in the client's error shape; no upstream request | not reachable |
| Client size (`Limit`, `EventTooLarge`) at request ingress | `codec::Error` | 413; no upstream request | not reachable |
| Re-fence refused for an attempt | `codec::Error` | attempt failure, scope `Model`; nothing sent | not reachable |
| `Storage(StorageExhausted)` or `Storage(Io)` while the producer runs | `stream.storage_exhausted`, 503, scope `resource` | resource error to the client; no failover | the stream's terminal, delivered as `error` |
| `Storage(StorageExhausted)` or `Storage(Io)` in an egress or resume allocation after the producer's terminal | a failure of that one delivery; the stream keeps its terminal | 503 `stream.storage_exhausted`; no failover | that delivery ends its HTTP response with the protocol's error terminal, or with 503 if it has sent nothing yet; other readers and later resumes are unaffected |
| `Storage(Gone)`: expiry or eviction of a stream the exchange uses | `stream.gone`, 410, scope `resource` | resource error to the client; no failover | attached readers receive `error`; resume answers 410 |
| `Storage(Closed)` | the stream already has a terminal | follow that terminal's row | follow that terminal's row |
| `Storage(InvalidPosition, InvalidRecord, IncompleteRecord)` | gateway defect | 500, scope `Request`; no failover, also once the producer started | delivered as `error`; after the producer started, `stream.producer_lost` (§7) |
| Upstream refused or unreachable | `Err(ExecError)` from `Execute` | attempt failure: `ExecError::scope` decides failover and cooldown; the attempt's response stream is discarded (§7) | not reachable: nothing of this attempt was delivered |
| Upstream body error, invalid upstream data, or EOF before the completion boundary | producer publishes `error` (scope `upstream`) and returns `Err(ExecError)` | attempt failure as above | delivered as `error` in the client's protocol; the scope still updates cooldown |
| Producer panic or dropped writer | `stream.producer_lost`, 502, scope `upstream` | attempt failure | delivered as `error` |
| Explicit client cancel | `Terminal::Cancelled` | nothing is delivered | delivered as the protocol's cancellation where it has one |
| Shutdown | `Terminal::Shutdown` | the listener's shutdown response; no failover | delivered as the shutdown terminal |
| Client disconnect on a document stream | nothing | the reader lease is released; the producer continues | same |
| Client disconnect on a live session or Responses WebSocket turn | Terminal::ClientDisconnected (cancelled, reason client_disconnect) | nothing is delivered | not reachable: the client is gone |

Only an attempt failure before commitment may fail over. The client sees the official API's error shape for its protocol in every row.

## 10. Codec clock

Codec collection delays and `flush_deadline` read the store clock through `Writer::now`, an `Instant`. Under `Store::open` it is the monotonic time; under `Store::with_clock` it is the injected clock's time since the store was created, so tests advance a `ManualClock` instead of sleeping.

## 11. Module scaffold

D0 declares every module that D1–D3 and the provider units create, so later units do not edit a shared `mod.rs` or `lib.rs`: `ck-exec` `types`, `pipeline`, `google_wire` (P4), `xai_media` (P6); `ck-server` `http::egress`, `inference::streams`, `management::api::streams_stats`; `ck-translate` `gemini::envelope` (P7). New test targets need no declaration.

## 12. Phase E boundary

Phase E extends this contract; it does not change it. It reuses `Attempt`, `Client`, `Body`, the producer and its allocator, `Exchange`, `StreamHandle` and §2–§9 unchanged, except that live-session messages are forwarded as received under transport flow control and only their metadata projection goes through the producer (Settled 55).

- One-shot media operations (`codec::Operation::Media`: image generate and edit, speech, video create, edit, extend, retrieve and content) and `Operation::AlphaSearch` keep the `Execute` shape and run through `Execute::execute`. `Capability::admits` names the operations a destination validates; the default admits only generate, count and compact, and `RequestFence::end` refuses any other operation with `Unsupported` before `validate`, so no text destination accepts a media or search request by omission. A media executor overrides `admits` in the unit that implements it. `Operation::name` is the `request.start` operation string.
- Duplex sessions are their own traits in `ck-exec/src/session.rs`, separate from `Execute`. A client disconnect closes a live session and cancels an in-flight Responses WebSocket turn; neither reattaches (Settled 55, 56).
  - Responses WebSocket: each turn is an ordinary attempt under this contract (capability, fence, ingress, producer) with the expanded history, so it fails over like any attempt. `session::Turns` runs a turn on an upstream socket and returns a `CancelTurn`, which sends the transport's cancel command or closes the socket on disconnect. A provider without an upstream socket runs the turn through `Execute::execute`, and dropping the body cancels it.
  - Live sessions (Realtime, Live, WebRTC sideband): `session::Live::open` dials the upstream on the attempt's credential, the only point where a credential is chosen (E-Q10), and returns a `FrameSink` and the upstream `Frames`. `session::Frame` keeps the opcode and payload as received. `codec::live::Projector` admits each client message before it is forwarded (E-Q14) and publishes only the metadata projection and the final counters; it produces no client bytes.

## 13. Open for an owner

- **Q7, Interactions and legacy completions.** No codec exists, so no module is declared for them.
- **G5, buffered cloaked generations.** They stay on the old arm until an owner decides between allowing the Claude relay to collect a buffered body for tool-alias restore and continuity, and sending every cloaked generation upstream as a stream, which changes its `stream` member.
