use ck_stream::{ScratchLease, Writer};

mod usage;

use usage::tool_model;

use super::request::present;

use crate::{
    boundary,
    codec::{
        self, BodyMode, Error, ResponseIngress, Result,
        chat::response::sse_provenance,
        document::{Document, Value},
        encode::{self, Encoder},
        event::{Direction, Emitter, Identity, Kind, Scope},
        json,
        media::{self, Publisher},
        source::Source,
        sse,
        table::Table,
    },
    video::Ledger,
};

/// Which upstream dialect a response body speaks. `OpenAi` and `Xai` share the Images JSON
/// (buffered) and `image_generation.*` / `image_edit.*` events (OpenAI SSE only); `Codex` is the
/// Responses event stream of the forced `image_generation` tool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Wire {
    OpenAi,
    Xai,
    Codex,
}

impl Wire {
    fn protocol(self) -> &'static str {
        match self {
            Self::OpenAi => "openai-images",
            Self::Xai => "xai-images",
            Self::Codex => "codex",
        }
    }
}

/// What a deferred xAI image answer is to its job (X16): the create's acknowledgement naming the
/// job, a poll the upstream answered 202 while the job runs, or a poll's 200, which holds the
/// images or the job's `error`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Deferred {
    Accepted,
    Pending,
    Polled,
}

/// A deferred answer's job: the job a poll asked for, and the table its usage is charged against
/// once (E-Q13).
struct Job {
    phase: Deferred,
    id: Option<String>,
    ledger: Box<dyn Ledger>,
}

/// Members of an Images response that map to canonical records.
const TOP: &[&str] = &[
    "created",
    "data",
    "usage",
    "background",
    "output_format",
    "quality",
    "size",
];
const ITEM: &[&str] = &[
    "b64_json",
    "url",
    "revised_prompt",
    "type",
    "partial_image_index",
    "created_at",
    "usage",
    "size",
    "quality",
    "background",
    "output_format",
];
const CODEX_ITEM: &[&str] = &[
    "id",
    "type",
    "status",
    "action",
    "result",
    "revised_prompt",
    "output_format",
    "size",
    "background",
    "quality",
    "partial_image_b64",
    "partial_image_index",
];

/// The usage-bearing and creation members a Codex `response.completed` is read for.
const IMAGE_CALL: &str = "image_generation_call";

/// One SSE record the canonical records of an image keep in `X.source.sse`.
type Sse<'a> = (&'a Source, &'a sse::Event);

/// One image occurrence read from a document: where its fields are and how it is published.
struct Image<'a> {
    doc: &'a Document,
    /// The object holding `b64_json`/`url`/`revised_prompt`.
    item: usize,
    /// The object holding values shared by every image of the response.
    top: usize,
    b64: Option<usize>,
    url: Option<usize>,
    partial: bool,
    /// The creation time this image's source reports, when it reports one per image.
    created: Option<(&'a Document, usize)>,
    /// The record the image came from.
    sse: Option<Sse<'a>>,
    /// Encoded SSE fields retained with a delayed Codex result.
    provenance: Option<&'a ScratchLease>,
    /// Native objects whose unknown members are encoded only when the image is published.
    original: Option<&'a [(usize, &'a str, &'a [&'a str])]>,
    /// Known names of `item`; its other members are kept in `X.original`.
    known: &'a [&'a str],
}

impl Image<'_> {
    fn pick(&self, name: &str) -> Result<Option<usize>> {
        match present(self.doc, self.item, name)? {
            Some(node) => Ok(Some(node)),
            None if self.top != self.item => present(self.doc, self.top, name),
            None => Ok(None),
        }
    }

    /// Writes `,"name":value` of the settings the response reports for this image.
    fn settings(&self, e: &mut Encoder<'_, '_>) -> Result<()> {
        for name in SETTINGS {
            if let Some(node) = self.pick(name)? {
                e.bytes(b",")?;
                e.string(name)?;
                e.bytes(b":")?;
                self.doc.write(node, e)?;
            }
        }
        Ok(())
    }
}

/// Settings of an image the response reports, kept with its media or, for a URL result, on its
/// `image.result`.
const SETTINGS: [&str; 4] = ["output_format", "size", "quality", "background"];

/// A non-empty string member: an empty `b64_json` or `url` is no image.
fn nonempty(doc: &Document, node: usize) -> Result<usize> {
    boundary::string(doc, node)?;
    if doc.chars(node)?.next_scalar()?.is_none() {
        return Err(Error::Schema);
    }
    Ok(node)
}

/// A member that is present and, when a string, not empty.
fn filled(doc: &Document, parent: usize, name: &str) -> Result<Option<usize>> {
    Ok(match present(doc, parent, name)? {
        Some(node)
            if matches!(doc.node(node)?.value, Value::String(_)) && doc.chars(node)?.next_scalar()?.is_none() =>
        {
            None
        }
        member => member,
    })
}

struct State {
    wire: Wire,
    emitter: Emitter,
    started: bool,
    images: u64,
    partials: u64,
    usage: bool,
    completed: bool,
    terminal: bool,
    /// Codex `output_item.done` documents and provenance, held until `response.completed`
    /// brings the usage that precedes their media.
    calls: Table<(Document, ScratchLease)>,
    /// Set for a deferred xAI answer.
    job: Option<Job>,
}

/// One image response: buffered JSON, or SSE events. `finish` means complete framing.
pub struct Ingress {
    owner: u64,
    input: Option<Source>,
    sse: Option<sse::Scanner>,
    json: Option<json::Scanner>,
    state: State,
    limit: u64,
    closed: bool,
}

impl Ingress {
    /// `limit` bounds one buffered body or one SSE event, which hold the image as base64 in mapped
    /// storage until their media is published. `Xai` has no event stream.
    pub fn new(owner: &mut Writer, identity: Identity, wire: Wire, mode: BodyMode, limit: u64) -> Result<Self> {
        if identity.direction != Direction::Response || (wire == Wire::Xai && mode == BodyMode::Streaming) {
            return Err(Error::Schema);
        }
        if wire == Wire::Codex && mode == BodyMode::Buffered {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            owner: owner.id(),
            input: Some(Source::new(owner)?),
            sse: if mode == BodyMode::Streaming {
                Some(sse::Scanner::new(owner, limit)?)
            } else {
                None
            },
            json: if mode == BodyMode::Buffered {
                Some(json::Scanner::new(owner, limit)?)
            } else {
                None
            },
            state: State {
                wire,
                emitter: Emitter::new(identity),
                started: false,
                images: 0,
                partials: 0,
                usage: false,
                completed: false,
                terminal: false,
                calls: Table::new(owner)?,
                job: None,
            },
            limit,
            closed: false,
        })
    }

    /// A deferred xAI answer (buffered JSON): `job` is the job a poll asked for, and none for the
    /// create, whose answer names it. The answer becomes one `image.job` operation, or for a
    /// finished poll the images with the job's usage, which `ledger` charges once.
    pub fn deferred(
        owner: &mut Writer,
        identity: Identity,
        phase: Deferred,
        job: Option<&str>,
        limit: u64,
        ledger: Box<dyn Ledger>,
    ) -> Result<Self> {
        match (phase, job) {
            (Deferred::Accepted, None) => {}
            (Deferred::Pending | Deferred::Polled, Some(id)) if !id.is_empty() => {}
            _ => return Err(Error::Schema),
        }
        let mut ingress = Self::new(owner, identity, Wire::Xai, BodyMode::Buffered, limit)?;
        ingress.state.job = Some(Job {
            phase,
            id: job.map(str::to_owned),
            ledger,
        });
        Ok(ingress)
    }

    fn report(&mut self, owner: &mut Writer, result: Result<()>) -> Result<()> {
        if let Err(error) = &result {
            self.closed = true;
            if !matches!(error, Error::Storage(_)) && !self.state.terminal {
                self.state.terminal = true;
                codec::upstream_failure(&mut self.state.emitter, owner, 502, "upstream")?;
            }
        }
        result
    }
}

impl ResponseIngress for Ingress {
    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<()> {
        boundary::enter(self.owner, owner, self.closed)?;
        let result = (|| {
            let source = self.input.as_mut().ok_or(Error::Closed)?;
            if bytes.len() as u64 > self.limit.saturating_sub(source.len()) && self.json.is_some() {
                return Err(Error::EventTooLarge);
            }
            source.push_bytes(owner, bytes)?;
            if let Some(scanner) = &mut self.json {
                return scanner.advance(source);
            }
            let scanner = self.sse.as_mut().ok_or(Error::Closed)?;
            let state = &mut self.state;
            scanner.advance(source, |event| state.event(owner, source, event))?;
            source.release_before(scanner.consumed())
        })();
        self.report(owner, result)
    }

    fn finish(&mut self, owner: &mut Writer) -> Result<()> {
        boundary::enter(self.owner, owner, self.closed)?;
        let result = (|| {
            if let Some(mut scanner) = self.json.take() {
                let source = self.input.take().ok_or(Error::Closed)?;
                let pending = self
                    .state
                    .job
                    .as_ref()
                    .is_some_and(|job| job.phase == Deferred::Pending);
                // xAI may answer a running job's poll with 202 and no body; the polled job is known.
                let doc = if pending && source.is_empty() {
                    boundary::mapped(owner, |e| e.bytes(b"{}"))?
                } else {
                    scanner.finish(&source)?;
                    Document::from_scanned(owner, source, scanner)?
                };
                self.state.buffered(owner, &doc)?;
            } else {
                let scanner = self.sse.as_mut().ok_or(Error::Closed)?;
                let source = self.input.as_ref().ok_or(Error::Closed)?;
                let state = &mut self.state;
                scanner.finish(source, |event| state.event(owner, source, event))?;
                let ended = match state.wire {
                    Wire::Codex => state.completed,
                    _ => state.images != 0,
                };
                if !ended {
                    return Err(Error::Truncated);
                }
            }
            self.state.terminal = true;
            self.state
                .emitter
                .emit(owner, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))?;
            Ok(())
        })();
        let result = self.report(owner, result);
        self.closed = true;
        result
    }

    fn fail(&mut self, owner: &mut Writer, status: u16, scope: &'static str) -> Result<()> {
        boundary::enter(self.owner, owner, self.closed)?;
        self.closed = true;
        self.state.terminal = true;
        codec::upstream_failure(&mut self.state.emitter, owner, status, scope)
    }
}

impl State {
    fn start(
        &mut self,
        owner: &mut Writer,
        created: Option<(&Document, usize)>,
        extras: Option<&Document>,
    ) -> Result<()> {
        if self.started {
            return Ok(());
        }
        self.started = true;
        let protocol = self.wire.protocol();
        self.emitter.emit(
            owner,
            Kind::ResponseStart,
            Scope::default(),
            |e| {
                e.bytes(b",\"id\":\"images\",\"model\":\"\",\"created\":")?;
                match created {
                    Some((doc, node)) => {
                        doc.unsigned(node)?;
                        doc.write(node, e)
                    }
                    None => e.bytes(b"0"),
                }
            },
            |e| {
                e.bytes(b",\"synthetic\":[\"id\",\"model\"")?;
                if created.is_none() {
                    e.bytes(b",\"created\"")?;
                }
                e.bytes(b"]")?;
                if let Some(doc) = extras {
                    boundary::original(doc, protocol, &[(doc.root(), "", TOP)], e)?;
                }
                Ok(())
            },
        )?;
        Ok(())
    }

    fn event(&mut self, owner: &mut Writer, source: &Source, event: &sse::Event) -> Result<()> {
        if self.terminal {
            return Err(Error::Closed);
        }
        if !event.has_data(source)? {
            return Ok(());
        }
        let known = match event.name(source)? {
            Some(name) => self.known_name(source, name)?,
            None => false,
        };
        let lease = encode::scratch(owner, |e| event.write_data(source, |b| e.bytes(b)))?;
        let mut data = Source::new(owner)?;
        data.push_scratch(lease)?;
        let length = data.len();
        let doc = match Document::parse(owner, data, length) {
            Ok(doc) => doc,
            // Data of an event this codec does not read need not be JSON (§3.7).
            Err(Error::Json | Error::Truncated) if !known => return self.opaque(owner, source, event, None),
            Err(error) => return Err(error),
        };
        let root = doc.root();
        let kind = if matches!(doc.node(root)?.value, Value::Object) {
            if present(&doc, root, "error")?.is_some() {
                return Err(Error::Schema);
            }
            match doc.member(root, "type")? {
                Some(kind) => Some(kind),
                None if known => return Err(Error::Schema),
                None => None,
            }
        } else {
            None
        };
        let Some(kind) = kind else {
            return self.opaque(owner, source, event, Some(&doc));
        };
        boundary::string(&doc, kind)?;
        let is = |name: &str| doc.string_is(kind, name);
        let sse = Some((source, event));
        match self.wire {
            Wire::OpenAi => {
                let partial = is("image_generation.partial_image")? || is("image_edit.partial_image")?;
                if partial || is("image_generation.completed")? || is("image_edit.completed")? {
                    self.openai_event(owner, &doc, partial, sse)
                } else if is("error")? {
                    Err(Error::Schema)
                } else {
                    self.opaque(owner, source, event, Some(&doc))
                }
            }
            Wire::Codex => self.codex_event(owner, doc, sse),
            Wire::Xai => Err(Error::Unsupported),
        }
    }

    /// Whether the SSE event name is one the stream's dialect defines: its data must then be a
    /// complete JSON event.
    fn known_name(&self, source: &Source, name: crate::codec::source::Span) -> Result<bool> {
        match self.wire {
            Wire::OpenAi => {
                for known in [
                    &b"image_generation.partial_image"[..],
                    b"image_generation.completed",
                    b"image_edit.partial_image",
                    b"image_edit.completed",
                    b"error",
                ] {
                    if source.equals(name, known)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            _ => {
                let mut head = [0u8; 9];
                let mut len = 0;
                source.visit(name, |bytes| {
                    let take = bytes.len().min(head.len() - len);
                    head[len..len + take].copy_from_slice(&bytes[..take]);
                    len += take;
                    Ok(())
                })?;
                Ok(&head[..len] == b"response." || source.equals(name, b"error")?)
            }
        }
    }

    /// An event this codec has no mapping for: `{name?, format, value}` with its SSE fields.
    fn opaque(
        &mut self,
        owner: &mut Writer,
        source: &Source,
        event: &sse::Event,
        doc: Option<&Document>,
    ) -> Result<()> {
        self.start(owner, None, None)?;
        let protocol = self.wire.protocol();
        let name = event.name(source)?;
        self.emitter.emit(
            owner,
            Kind::Opaque,
            Scope::default(),
            |e| {
                if let Some(name) = name {
                    e.bytes(b",\"name\":")?;
                    e.quoted_json(|e| e.span(source, name))?;
                }
                match doc {
                    Some(doc) => {
                        e.bytes(b",\"format\":\"json\",\"value\":")?;
                        doc.write(doc.root(), e)
                    }
                    None => {
                        e.bytes(b",\"format\":\"text\",\"value\":")?;
                        e.quoted_json(|e| event.write_data(source, |b| e.bytes(b)))
                    }
                }
            },
            |e| sse_provenance(protocol, source, event, e),
        )?;
        Ok(())
    }

    fn openai_event(&mut self, owner: &mut Writer, doc: &Document, partial: bool, sse: Option<Sse<'_>>) -> Result<()> {
        let root = doc.root();
        let created = present(doc, root, "created_at")?.map(|node| (doc, node));
        self.start(owner, created, None)?;
        if let Some(usage) = present(doc, root, "usage")? {
            self.usage(owner, doc, usage)?;
        }
        self.image(
            owner,
            &Image {
                doc,
                item: root,
                top: root,
                b64: present(doc, root, "b64_json")?,
                url: present(doc, root, "url")?,
                partial,
                created,
                sse,
                provenance: None,
                original: None,
                known: ITEM,
            },
        )
    }

    /// A buffered Images response: `created`, `data[]`, one `usage` and shared image settings.
    fn buffered(&mut self, owner: &mut Writer, doc: &Document) -> Result<()> {
        let root = doc.root();
        if !matches!(doc.node(root)?.value, Value::Object) {
            return Err(Error::Schema);
        }
        let failed = present(doc, root, "error")?.is_some();
        match self.job.as_ref().map(|job| job.phase) {
            Some(Deferred::Accepted) => return self.job(owner, doc, "queued"),
            Some(Deferred::Pending) => return self.job(owner, doc, "running"),
            Some(Deferred::Polled) if failed => return self.job(owner, doc, "failed"),
            _ if failed => return Err(Error::Schema),
            _ => {}
        }
        let data = doc.required(root, "data")?;
        if !matches!(doc.node(data)?.value, Value::Array) || doc.children(data)?.next().is_none() {
            return Err(Error::Schema);
        }
        let created = present(doc, root, "created")?.map(|node| (doc, node));
        self.start(owner, created, Some(doc))?;
        if let Some(usage) = present(doc, root, "usage")? {
            self.usage(owner, doc, usage)?;
        }
        for item in doc.children(data)? {
            if !matches!(doc.node(item)?.value, Value::Object) {
                return Err(Error::Schema);
            }
            let (b64, url) = (filled(doc, item, "b64_json")?, filled(doc, item, "url")?);
            // An item with neither is no image; a response left with none fails at completion.
            if b64.is_none() && url.is_none() {
                continue;
            }
            self.image(
                owner,
                &Image {
                    doc,
                    item,
                    top: root,
                    b64,
                    url,
                    partial: false,
                    created: None,
                    sse: None,
                    provenance: None,
                    original: None,
                    known: ITEM,
                },
            )?;
        }
        if self.images == 0 {
            return Err(Error::Schema);
        }
        Ok(())
    }

    /// A deferred job's answer as one `image.job` operation, as a video job is one `video.job`:
    /// `job_id` (the answer's `request_id`, else the polled job, marked `id_implied`), `status`
    /// (`queued` once created, `running` while a poll answers 202, `failed` for an `error` body)
    /// and a failed job's `error` object. Every other member stays in `X.original`.
    fn job(&mut self, owner: &mut Writer, doc: &Document, status: &str) -> Result<()> {
        let root = doc.root();
        let job = self.job.as_mut().ok_or(Error::Schema)?;
        let wire = present(doc, root, "request_id")?;
        if let Some(node) = wire {
            nonempty(doc, node)?;
        }
        let error = if status == "failed" {
            let node = doc.required(root, "error")?;
            if !matches!(doc.node(node)?.value, Value::Object) {
                return Err(Error::Schema);
            }
            Some(node)
        } else {
            None
        };
        let id = match (wire, &job.id) {
            (Some(node), _) => {
                let mut id = String::new();
                let mut chars = doc.chars(node)?;
                while let Some(c) = chars.next_scalar()? {
                    id.push(c);
                }
                id
            }
            // The create's answer must name its job.
            (None, None) => return Err(Error::Schema),
            (None, Some(id)) => id.clone(),
        };
        if job.phase == Deferred::Accepted {
            job.ledger.accepted(&id);
        }
        let known: &[&str] = if error.is_some() {
            &["request_id", "error"]
        } else {
            &["request_id"]
        };
        self.start(owner, None, None)?;
        let protocol = self.wire.protocol();
        self.emitter.emit(
            owner,
            Kind::Operation,
            Scope::default(),
            |e| {
                e.bytes(b",\"kind\":\"image.job\",\"value\":{\"job_id\":")?;
                e.string(&id)?;
                e.bytes(b",\"status\":")?;
                e.string(status)?;
                if let Some(error) = error {
                    e.bytes(b",\"error\":")?;
                    doc.write(error, e)?;
                }
                e.bytes(b"}")
            },
            |e| {
                if wire.is_none() {
                    e.bytes(b",\"id_implied\":true")?;
                }
                boundary::original(doc, protocol, &[(root, "", known)], e)
            },
        )?;
        Ok(())
    }

    fn codex_event(&mut self, owner: &mut Writer, doc: Document, sse: Option<Sse<'_>>) -> Result<()> {
        let root = doc.root();
        let kind = doc.required(root, "type")?;
        let is = |name: &str| doc.string_is(kind, name);
        if is("response.created")? {
            let response = present(&doc, root, "response")?;
            let created = response.map(|r| present(&doc, r, "created_at")).transpose()?.flatten();
            self.start(owner, created.map(|node| (&doc, node)), None)
        } else if is("response.image_generation_call.partial_image")? {
            self.start(owner, None, None)?;
            self.image(
                owner,
                &Image {
                    doc: &doc,
                    item: root,
                    top: root,
                    b64: present(&doc, root, "partial_image_b64")?,
                    url: None,
                    partial: true,
                    created: None,
                    sse,
                    provenance: None,
                    original: None,
                    known: CODEX_ITEM,
                },
            )
        } else if is("response.output_item.done")? {
            let item = doc.required(root, "item")?;
            if boundary::is(&doc, item, "type", IMAGE_CALL)? && has_result(&doc, item)? {
                let provenance = encode::scratch(owner, |e| {
                    if let Some((source, event)) = sse {
                        sse_provenance(self.wire.protocol(), source, event, e)?;
                    }
                    Ok(())
                })?;
                self.calls.push((doc, provenance))?;
            }
            Ok(())
        } else if is("response.completed")? {
            self.codex_completed(owner, &doc, sse)
        } else if is("response.failed")? || is("response.incomplete")? || is("error")? {
            Err(Error::Schema)
        } else if doc.string_is(kind, "response.in_progress")?
            || doc.string_is(kind, "response.queued")?
            || doc.string_is(kind, "response.output_item.added")?
            || doc.string_is(kind, "response.image_generation_call.in_progress")?
            || doc.string_is(kind, "response.image_generation_call.generating")?
            || doc.string_is(kind, "response.image_generation_call.completed")?
        {
            Ok(())
        } else if let Some((source, event)) = sse {
            self.opaque(owner, source, event, Some(&doc))
        } else {
            Err(Error::Schema)
        }
    }

    fn codex_completed(&mut self, owner: &mut Writer, doc: &Document, sse: Option<Sse<'_>>) -> Result<()> {
        let root = doc.root();
        let response = doc.required(root, "response")?;
        let created = present(doc, response, "created_at")?.map(|node| (doc, node));
        self.start(owner, created, None)?;
        if let Some(usage) = present(doc, response, "usage")? {
            self.usage(owner, doc, usage)?;
        }
        if let Some(tool) = present(doc, response, "tool_usage")?
            && let Some(usage) = present(doc, tool, "image_gen")?
        {
            let model = tool_model(doc, response)?;
            self.tool_usage(owner, doc, usage, model)?;
        }
        let calls = std::mem::replace(&mut self.calls, Table::new(owner)?);
        if !calls.is_empty() {
            for (call, provenance) in calls.iter() {
                let item = call.required(call.root(), "item")?;
                self.image(
                    owner,
                    &Image {
                        doc: call,
                        item,
                        top: item,
                        b64: present(call, item, "result")?,
                        url: None,
                        partial: false,
                        created,
                        sse: None,
                        provenance: Some(provenance),
                        original: Some(&[(call.root(), "", &["type", "item"]), (item, "/item", CODEX_ITEM)]),
                        known: CODEX_ITEM,
                    },
                )?;
            }
        } else if let Some(output) = present(doc, response, "output")? {
            let mut sse = sse;
            for (index, item) in doc.children(output)?.enumerate() {
                if boundary::is(doc, item, "type", IMAGE_CALL)? && has_result(doc, item)? {
                    let mut pointer = [0u8; 48];
                    let prefix = pointer_prefix(&mut pointer, "/response/output", index)?;
                    self.image(
                        owner,
                        &Image {
                            doc,
                            item,
                            top: item,
                            b64: present(doc, item, "result")?,
                            url: None,
                            partial: false,
                            created,
                            // One completion record supplies all these images; keep its fields once.
                            sse: sse.take(),
                            provenance: None,
                            original: Some(&[(item, prefix, CODEX_ITEM)]),
                            known: CODEX_ITEM,
                        },
                    )?;
                }
            }
        }
        if self.images == 0 {
            return Err(Error::Schema);
        }
        self.completed = true;
        Ok(())
    }

    /// Publishes one image: its media lifetime when it carries base64, and for a finished image
    /// the `image.result` operation naming the media or the upstream URL. A partial preview never
    /// counts as a result.
    fn image(&mut self, owner: &mut Writer, image: &Image<'_>) -> Result<()> {
        let doc = image.doc;
        let id = if image.partial {
            media::local_id(b'p', self.partials)?
        } else {
            media::local_id(b'm', self.images)?
        };
        let index = self.images;
        let wire_index = match present(doc, image.item, "partial_image_index")? {
            Some(node) if image.partial => doc.unsigned(node)?,
            _ => self.partials,
        };
        let protocol = self.wire.protocol();
        let mut pointer = [0u8; 32];
        let prefix = if image.item == doc.root() {
            ""
        } else {
            pointer_prefix(&mut pointer, "/data", index as usize)?
        };
        match (image.b64, image.url) {
            (Some(_), Some(_)) | (None, None) => return Err(Error::Schema),
            (None, Some(url)) => {
                nonempty(doc, url)?;
                if image.partial {
                    return Err(Error::Schema);
                }
            }
            (Some(b64), None) => {
                nonempty(doc, b64)?;
                let Value::String(text) = doc.node(b64)?.value else {
                    return Err(Error::Schema);
                };
                let scope = Scope {
                    media_id: Some(id),
                    ..Scope::default()
                };
                let mut publisher = Publisher::start_with(
                    &mut self.emitter,
                    owner,
                    scope,
                    |e| {
                        e.bytes(b",\"kind\":\"image\",\"encoding\":\"base64\"")?;
                        if let Some(mime) = mime(image)? {
                            e.bytes(b",\"mime\":")?;
                            mime.write(e)?;
                        }
                        e.bytes(b",\"attributes\":{\"index\":")?;
                        e.unsigned(index)?;
                        if image.partial {
                            e.bytes(b",\"partial_image_index\":")?;
                            e.unsigned(wire_index)?;
                        }
                        image.settings(e)?;
                        e.bytes(b"}")
                    },
                    |e| {
                        // A preview has no `image.result`: its media carries the source record and
                        // the members this codec does not map.
                        if !image.partial {
                            return Ok(());
                        }
                        if let Some((source, event)) = image.sse {
                            sse_provenance(protocol, source, event, e)?;
                        }
                        boundary::original(doc, protocol, &[(image.item, prefix, image.known)], e)
                    },
                )?;
                publisher.collect(&mut self.emitter, owner, |emit| media::decode_base64(doc, text, emit))?;
                publisher.end(&mut self.emitter, owner)?;
            }
        }
        if image.partial {
            self.partials += 1;
            return Ok(());
        }
        self.emitter.emit(
            owner,
            Kind::Operation,
            Scope::default(),
            |e| {
                e.bytes(b",\"kind\":\"image.result\",\"value\":{\"index\":")?;
                e.unsigned(index)?;
                match image.url {
                    Some(url) => {
                        e.bytes(b",\"url\":")?;
                        doc.write(url, e)?;
                        // A URL has no media to carry its type and settings.
                        if let Some(mime) = mime(image)? {
                            e.bytes(b",\"mime\":")?;
                            mime.write(e)?;
                        }
                        let mut settings = false;
                        for name in SETTINGS {
                            if image.pick(name)?.is_some() {
                                settings = true;
                            }
                        }
                        if settings {
                            e.bytes(b",\"attributes\":{\"index\":")?;
                            e.unsigned(index)?;
                            image.settings(e)?;
                            e.bytes(b"}")?;
                        }
                    }
                    None => {
                        e.bytes(b",\"media_id\":")?;
                        e.string(id.as_str())?;
                    }
                }
                if let Some(revised) = present(doc, image.item, "revised_prompt")? {
                    boundary::string(doc, revised)?;
                    e.bytes(b",\"revised_prompt\":")?;
                    doc.write(revised, e)?;
                }
                if let Some((created, node)) = image.created {
                    created.unsigned(node)?;
                    e.bytes(b",\"created_at\":")?;
                    created.write(node, e)?;
                }
                e.bytes(b"}")
            },
            |e| {
                if let Some((source, event)) = image.sse {
                    sse_provenance(protocol, source, event, e)?;
                }
                if let Some(provenance) = image.provenance {
                    let mut offset = 0;
                    while offset < provenance.len() {
                        let bytes = provenance.chunk(offset)?;
                        e.bytes(bytes)?;
                        offset += bytes.len();
                    }
                }
                let fallback = [(image.item, prefix, image.known)];
                boundary::original(doc, protocol, image.original.unwrap_or(&fallback), e)
            },
        )?;
        self.images += 1;
        Ok(())
    }
}

/// The media type of an image: the one the source reports, else the one its format names. An
/// image whose source names neither has none.
enum Mime<'a> {
    Reported(&'a Document, usize),
    Format(&'static str),
}

impl Mime<'_> {
    fn write(&self, e: &mut Encoder<'_, '_>) -> Result<()> {
        match self {
            Self::Reported(doc, node) => doc.write(*node, e),
            Self::Format(mime) => e.string(mime),
        }
    }
}

fn mime<'a>(image: &Image<'a>) -> Result<Option<Mime<'a>>> {
    if let Some(node) = image.pick("mime_type")? {
        boundary::string(image.doc, node)?;
        return Ok(Some(Mime::Reported(image.doc, node)));
    }
    let Some(format) = image.pick("output_format")? else {
        return Ok(None);
    };
    for (name, mime) in [
        ("png", "image/png"),
        ("jpeg", "image/jpeg"),
        ("jpg", "image/jpeg"),
        ("webp", "image/webp"),
    ] {
        if image.doc.string_is(format, name)? {
            return Ok(Some(Mime::Format(mime)));
        }
    }
    Ok(None)
}

fn has_result(doc: &Document, item: usize) -> Result<bool> {
    let Some(result) = present(doc, item, "result")? else {
        return Ok(false);
    };
    boundary::string(doc, result)?;
    Ok(doc.chars(result)?.next_scalar()?.is_some())
}

fn pointer_prefix<'a>(out: &'a mut [u8], parent: &str, index: usize) -> Result<&'a str> {
    use std::io::Write;
    let mut cursor = std::io::Cursor::new(&mut out[..]);
    write!(cursor, "{parent}/{index}").map_err(|_| Error::Limit)?;
    let len = cursor.position() as usize;
    std::str::from_utf8(&out[..len]).map_err(|_| Error::Schema)
}
