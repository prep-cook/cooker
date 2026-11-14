# Execution contract (S1 Phase D)

This page fixes the interfaces Phase D units build against: the executor trait, the request handle, the response producer, the server exchange and stream handle, the usage tracker input, and the error and commitment rules. It complements `docs/INTERNAL-STREAM.md` §2.1 (responsibilities), §3.6 (commitment), §3.7 (destination policy) and §6.6–6.7 (resume). A change to a signature below is a contract change: update this page in the same PR.

## 1. Where each part lives

| Part | File | Implemented by |
| --- | --- | --- |
| `Execute`, `Attempt`, `Client`, `Request`, `Upstream`, `Body` | `crates/ck-exec/src/types.rs` | each provider unit |
| `Producer`, `Started` | `crates/ck-exec/src/pipeline.rs` | D1 |
| `Protocol`, `Output` | `crates/ck-server/src/http/egress.rs` | D2 |
| `Exchange`, `StreamHandle`, `StreamId` | `crates/ck-server/src/inference/streams.rs` | D2 (exchange), D3 (registry, resume) |
| `CanonicalUsage` (tracker input) | `crates/ck-server/src/observability/usage_snapshot.rs` | D3 |
| Codec clock, `Writer::now` | `crates/ck-stream/src/publication/writer.rs` | done in D0 |

Dependency edges `ck-exec -> ck-stream` and `ck-server -> ck-stream` are allowed in `crates/AGENTS.md`. The Anthropic, Responses and Gemini codecs are production modules of `ck-translate` (`ck_translate::{anthropic, responses, gemini}`); the old Gemini pair module is `gemini_gemini` until D9 deletes it.

## 2. Executor

Every provider module implements `Execute` once. The selector in `crates/ck-exec/src/lib.rs` matches `Credential::provider` and calls the provider's implementation directly: no trait objects, no second registry.

- `capability(&Attempt) -> Self::Capability` returns the destination capability for the attempt's credential and upstream model. It does no network work and no credential preparation. Configured limits such as Anthropic `default_max_tokens` enter through `Attempt::cfg` and `Attempt::resolved_model`.
- `execute`, `count_tokens` and `compact` each take `(&Attempt, &Request, &mut Writer)` and return `Result<Upstream<Self::Ingress>, ExecError>` as a `Send` future. `Writer` is the attempt's own response stream; request egress writes the upstream body into its scratch.
- `Request::validated` is the fence. An executor calls `ValidatedRequest::for_destination` with its capability key before encoding and encodes only through `RequestEgress`. No executor reads the client body bytes.
- `Upstream` carries the success status, upstream headers, the upstream body as a byte stream, and the response ingress bound to the attempt's writer. The body is passed on chunk by chunk; nothing collects it.
- A local operation, such as a tokenizer count, returns an `Upstream` whose body holds the provider-shaped result, so one producer path decodes every operation.
- `compact` runs only a native compact. The capability refuses compact where the destination has none; a summary is never substituted.

`Request<'a>` is `Send + Sync`, so a server task may hold it across an await. `crates/ck-exec/tests/execution_contract.rs` checks this and fails to compile if a codec type loses `Sync`.

## 3. Producer

`pipeline::Started` is what an attempt yields once the upstream answered: status, headers and the `Producer` task. The producer owns the writer, the upstream body and the response ingress. It drives `upstream bytes -> ResponseIngress -> Writer` on its own task, schedules `flush` at `flush_deadline`, and stops when `Writer::cancelled` resolves. A slow or absent client never slows it.

The producer's output, `Result<(), ExecError>`, is the attempt's routing outcome. `Err` carries the scope the scheduler and cooldown use.

D1 adds two selector methods with these shapes:

```rust
impl Executors {
    /// Closes the request fence against the attempt's destination.
    pub fn fence<'a, I: RequestIngress>(&self, attempt: &Attempt<'_>, ingress: &'a mut I, owner: &mut Writer)
        -> codec::Result<ValidatedRequest<'a>>;
    /// Runs the operation in the request view and starts the producer.
    pub async fn start(&self, attempt: &Attempt<'_>, request: &Request<'_>, writer: Writer)
        -> Result<Started, ExecError>;
}
```

## 4. Server exchange and stream handle

An `Exchange` is one client request: its owner (`Caller`) and its egress `Output` (protocol, operation, body mode). Each routing attempt writes its own ck-stream. The attempt whose egress commits becomes the exchange's `StreamHandle`, registered under a fresh `StreamId` of 128 random bits. `StreamId` has no `Debug` or `Display` and travels only in `X-Ck-Stream-Id`.

Resume looks up the handle, authorizes the caller against `StreamHandle::owner` before any cursor lookup, and re-encodes with `StreamHandle::output`. A resume request cannot change the output, select a provider or start an attempt.

## 5. Usage

The tracker's input is `CanonicalUsage`: one attempt's latest canonical `x_ck.usage` snapshot. A runtime-owned reader on the attempt's stream supplies it, independently of client delivery, so a resumed or second reader never accounts again. A later snapshot replaces the earlier one; `None` is an absent count, not zero. D3 deletes `parse_body`, `StreamUsage` and the `Tracker::body`/`event` byte parsers.

## 6. Errors and commitment

Commitment is the first deliverable byte the client's `ResponseEgress` emits. Before it the client receives nothing: no status, success header or keepalive. From it on, failover is disabled even if the socket write fails.

| Failure | Raised as | Before commitment | After commitment |
| --- | --- | --- | --- |
| Client syntax or schema (`Json`, `Utf8`, `DuplicateKey`, `Truncated`, `Schema`) or unsupported semantics (`Unsupported`) at request ingress | `codec::Error` | 400 in the client's error shape; no upstream request | not reachable |
| Client size (`Limit`, `EventTooLarge`) at request ingress | `codec::Error` | 413; no upstream request | not reachable |
| Storage refusal (`Storage`) anywhere | `stream.storage_exhausted`, 503, scope `resource` | error to the client; no failover | the stream's `error` terminal |
| Upstream refused or unreachable | `Err(ExecError)` from `Execute` | attempt failure: `ExecError::scope` decides failover and cooldown; a fresh attempt gets a fresh stream | not reachable: nothing of this attempt was delivered |
| Upstream body error, invalid upstream data, or EOF before the completion boundary | producer publishes `error` (scope `upstream`) and returns `Err(ExecError)` | attempt failure as above; the attempt's stream is never delivered | delivered as `error` in the client's protocol; the scope still updates cooldown |
| Producer panic or dropped writer | `stream.producer_lost`, 502, scope `upstream` | attempt failure | delivered as `error` |
| Expiry or eviction | `stream.gone`, 410, scope `resource` | not reachable | attached readers receive `error`; resume answers 410 |
| Client disconnect | nothing | the reader lease is released; the producer continues | same |

Only an attempt failure before commitment may fail over. The client sees the official API's error shape for its protocol in every row.

## 7. Codec clock

Codec collection delays and `flush_deadline` read the store clock through `Writer::now`, an `Instant`. Under `Store::open` it is the monotonic time; under `Store::with_clock` it is the injected clock's time since the store was created, so tests advance a `ManualClock` instead of sleeping.

## 8. Module scaffold

D0 declares every module that D1–D3 and the provider units create, so later units do not edit a shared `mod.rs` or `lib.rs`: `ck-exec` `types`, `pipeline`, `google_wire` (P4), `xai_media` (P6); `ck-server` `http::egress`, `inference::streams`, `management::api::streams_stats`; `ck-translate` `gemini::envelope` (P7). New test targets need no declaration.

## 9. Open for D1–D3

- **Resumed egress and the request view.** `RequestIngress::finish` closes the fence once and returns a `ValidatedRequest` that borrows the ingress. A resume served by a later HTTP request has no `RequestView` unless the exchange keeps that borrow alive or the codec offers a view accessor. The codec request files are under the R1 freeze; settle this in D3 together with the Responses echo (P2).
- **Failover across capabilities (Q2).** `ValidatedRequest::for_destination` rejects a destination other than the fenced one. Until an owner rules, D2 must not silently drop candidates of another capability.
- **Uncommitted attempt streams.** A failed attempt's stream ends with `error` and is never read. D3 releases it rather than leaving it to LRU eviction.
- **Resume identity.** `StreamHandle::owner` is the `Caller`; an ephemeral-session scope (§6.7) is not represented yet.
- **Usage fields outside the snapshot.** `Detail::response_service_tier` and the Go breakdown have no canonical source yet.
- **Claude compact.** `claude.rs:246-250` answers `responses/compact` with a generated summary, which §2 forbids. P1 needs an owner ruling or must refuse it through the capability.
- **Interactions and legacy completions (Q7).** No codec exists, so no module is declared for them.
