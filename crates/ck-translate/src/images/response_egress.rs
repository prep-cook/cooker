use ck_stream::{Lease, ScratchLease, Writer};

use crate::{
    boundary,
    codec::{
        Error, ResponseEgress, Result,
        document::{Document, Text, Value},
        encode::{self, Encoder},
        event::{Identity, Kind},
        internal::Binding,
        media::{self, write_encoded_chunks},
        table::Table,
    },
};

const PROTOCOL: &str = "openai-images";

/// Whether a member kept from the source belongs in the client's response: one of OpenAI Images,
/// or of xAI's Images API, which extends it on the same endpoints (`mime_type`, `file_output`,
/// `storage_error`).
fn restored(doc: &Document, entry: usize) -> Result<bool> {
    Ok(boundary::is(doc, entry, "protocol", PROTOCOL)? || boundary::is(doc, entry, "protocol", "xai-images")?)
}

/// The client's Images response form.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Format {
    /// One JSON document, written when the stream is done.
    Json,
    /// `image_generation.*` events.
    GenerationEvents,
    /// `image_edit.*` events.
    EditEvents,
}

/// What a result is delivered as: the `response_format` the client asked for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Delivery {
    Base64,
    /// An upstream URL, or a `data:` URL for base64 media.
    Url,
}

/// Something the egress had no place for, named without its content (§3.7): the hook behind
/// `Egress::on_drop` gets one of these per dropped record, for the caller's debug log.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Dropped {
    pub subject: &'static str,
    pub reason: &'static str,
}

type DropHook = Box<dyn FnMut(Dropped) + Send>;

struct Open {
    id: crate::codec::event::LocalId,
    start: Document,
    chunks: Table<Text>,
    ended: bool,
    partial: Option<u64>,
}

struct Result_ {
    media: Option<Open>,
    operation: Document,
}

/// Image results from the canonical stream. Chunks stay in mapped storage between their data
/// records and the one body or event that carries them.
pub struct Egress {
    binding: Binding,
    format: Format,
    delivery: Delivery,
    holder: Document,
    start: Option<Document>,
    usage: Option<Document>,
    tool_usage: Option<Document>,
    open: Option<Open>,
    results: Table<Result_>,
    /// A deferred job's `image.job` operation, which answers in place of results.
    job: Option<Document>,
    delivered: u64,
    done: bool,
    closed: bool,
    on_drop: Option<DropHook>,
}

impl Egress {
    pub fn new(owner: &mut Writer, identity: Option<Identity>, format: Format, delivery: Delivery) -> Result<Self> {
        Ok(Self {
            binding: Binding::new(owner, identity),
            format,
            delivery,
            holder: Document::streaming(owner)?,
            start: None,
            usage: None,
            tool_usage: None,
            open: None,
            results: Table::new(owner)?,
            job: None,
            delivered: 0,
            done: false,
            closed: false,
            on_drop: None,
        })
    }

    /// The client's HTTP status: 202 while a deferred job runs (xAI's own), else 200.
    pub fn status(&self) -> u16 {
        match &self.job {
            Some(job) if job_status(job, "running").unwrap_or(false) => 202,
            _ => 200,
        }
    }

    /// Whether [`Egress::status`] is final: a deferred job's record, a result or the end was read.
    /// Nothing before them decides between 202 and 200.
    pub fn decided(&self) -> bool {
        self.job.is_some() || self.delivered != 0 || self.done
    }

    /// Reports each record this egress has no place for: SSE fields of the source, events and
    /// members of another protocol. The report names the subject and reason, never the content.
    pub fn on_drop(mut self, hook: impl FnMut(Dropped) + Send + 'static) -> Self {
        self.on_drop = Some(Box::new(hook));
        self
    }

    fn drop(&mut self, subject: &'static str, reason: &'static str) {
        if let Some(hook) = &mut self.on_drop {
            hook(Dropped { subject, reason });
        }
    }

    /// Reports what a record carries that no part of the client's response can hold. Runs once per
    /// record, outside the encoders, which run twice.
    fn scan(&mut self, doc: &Document) -> Result<()> {
        let x = doc.required(doc.root(), "x_ck")?;
        if let Some(source) = doc.member(x, "source")?
            && let Some(sse) = doc.member(source, "sse")?
        {
            // The event name is the grammar's own; `id`, `retry` and comments have no place.
            for field in doc.children(sse)? {
                let comment = matches!(doc.node(doc.required(field, "comment")?)?.value, Value::Bool(true));
                if comment || !doc.string_is(doc.required(field, "name")?, "event")? {
                    self.drop("SSE id, retry and comment fields", "no carrier in the Images response");
                    break;
                }
            }
        }
        if let Some(original) = doc.member(x, "original")? {
            for entry in doc.children(original)? {
                if !restored(doc, entry)? {
                    self.drop(
                        "members of another protocol",
                        "only a client of the source protocol gets them",
                    );
                }
            }
        }
        Ok(())
    }

    fn record(
        &mut self,
        owner: &mut Writer,
        input: Lease,
        mut output: impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let (kind, doc) = self.binding.decode(owner, input)?;
        let root = doc.root();
        let x = doc.required(root, "x_ck")?;
        if matches!(kind, Kind::Error | Kind::Cancelled) {
            return Err(Error::Truncated);
        }
        if self.start.is_none() && kind != Kind::ResponseStart {
            return Err(Error::Schema);
        }
        match kind {
            Kind::ResponseStart if self.start.is_none() => {
                self.scan(&doc)?;
                self.start = Some(doc);
            }
            Kind::Usage => {
                // The Images `usage` is the image tool's when the source reports one apart from
                // the model's (a Codex tool call); otherwise the response's own.
                if doc.member(root, "scope")?.is_some() {
                    self.tool_usage.get_or_insert(doc);
                } else if self.usage.is_none() {
                    self.usage = Some(doc);
                }
            }
            Kind::MediaStart if self.open.is_none() => {
                if !boundary::is(&doc, root, "kind", "image")? || !boundary::is(&doc, root, "encoding", "base64")? {
                    return Err(Error::Schema);
                }
                self.scan(&doc)?;
                let attributes = doc.required(root, "attributes")?;
                let partial = doc
                    .member(attributes, "partial_image_index")?
                    .map(|node| doc.unsigned(node))
                    .transpose()?;
                self.open = Some(Open {
                    id: media::media_id(&doc, x)?,
                    start: doc,
                    chunks: Table::new(owner)?,
                    ended: false,
                    partial,
                });
            }
            Kind::Data => {
                let open = self.open.as_mut().filter(|open| !open.ended).ok_or(Error::Schema)?;
                if media::media_id(&doc, x)? != open.id {
                    return Err(Error::Schema);
                }
                let Value::String(text) = doc.node(doc.required(root, "bytes")?)?.value else {
                    return Err(Error::Schema);
                };
                open.chunks.push(self.holder.retain_text(doc, text)?)?;
            }
            Kind::MediaEnd => {
                let open = self.open.as_mut().filter(|open| !open.ended).ok_or(Error::Schema)?;
                if media::media_id(&doc, x)? != open.id || !boundary::is(&doc, root, "status", "completed")? {
                    return Err(Error::Truncated);
                }
                open.ended = true;
                if open.partial.is_some() {
                    let open = self.open.take().ok_or(Error::Schema)?;
                    // Only a streaming client sees previews.
                    if self.format != Format::Json {
                        let body = self.event(owner, Some(&open), None)?;
                        output(body)?;
                    }
                }
            }
            Kind::Operation if boundary::is(&doc, root, "kind", "image.job")? => {
                self.scan(&doc)?;
                self.job = Some(doc);
            }
            Kind::Operation => {
                if !boundary::is(&doc, root, "kind", "image.result")? {
                    return Ok(());
                }
                self.scan(&doc)?;
                let value = doc.required(root, "value")?;
                let media = if doc.member(value, "media_id")?.is_some() {
                    let open = self.open.take().filter(|open| open.ended).ok_or(Error::Schema)?;
                    if !doc.string_is(doc.required(value, "media_id")?, open.id.as_str())? {
                        return Err(Error::Schema);
                    }
                    Some(open)
                } else {
                    None
                };
                self.delivered += 1;
                if self.format == Format::Json {
                    self.results.push(Result_ { media, operation: doc })?;
                } else {
                    let body = self.event(owner, media.as_ref(), Some(&doc))?;
                    output(body)?;
                }
            }
            Kind::Done if self.open.is_none() && self.job.is_some() => {
                self.done = true;
                output(self.job_body(owner)?)?;
            }
            Kind::Done if self.open.is_none() && self.delivered != 0 => {
                self.done = true;
                if self.format == Format::Json {
                    output(self.buffered(owner)?)?;
                }
            }
            Kind::Opaque => self.drop(
                "an event without an Images form",
                "the Images grammar has no place for it",
            ),
            Kind::Heartbeat => {}
            _ => return Err(Error::Schema),
        }
        Ok(())
    }

    /// The creation time of a result: its own, else the response's when the source reported one.
    fn created<'a>(&'a self, operation: Option<&'a Document>) -> Result<Option<(&'a Document, usize)>> {
        if let Some(op) = operation
            && let Some(created) = op.member(op.required(op.root(), "value")?, "created_at")?
        {
            return Ok(Some((op, created)));
        }
        let Some(start) = &self.start else { return Ok(None) };
        let created = start.required(start.root(), "created")?;
        let synthetic = start
            .member(start.required(start.root(), "x_ck")?, "synthetic")?
            .map(|list| start.children(list))
            .transpose()?
            .into_iter()
            .flatten()
            .try_fold(false, |found, item| {
                Ok::<_, Error>(found || start.string_is(item, "created")?)
            })?;
        Ok((!synthetic).then_some((start, created)))
    }

    /// `"created":` or `"created_at":` and its value; a source without a creation time has none
    /// to report, so the epoch marks it.
    fn write_created(&self, e: &mut Encoder<'_, '_>, name: &str, operation: Option<&Document>) -> Result<()> {
        e.string(name)?;
        e.bytes(b":")?;
        match self.created(operation)? {
            Some((doc, created)) => doc.write(created, e),
            None => e.bytes(b"0"),
        }
    }

    /// The image's own `data[i]` or event member: URL or base64, optional revised prompt.
    fn result(
        &self,
        e: &mut Encoder<'_, '_>,
        first: &mut bool,
        open: Option<&Open>,
        operation: Option<&Document>,
    ) -> Result<()> {
        let value = match operation {
            Some(op) => Some((op, op.required(op.root(), "value")?)),
            None => None,
        };
        if let Some((op, value)) = value
            && let Some(revised) = op.member(value, "revised_prompt")?
        {
            sep(e, first)?;
            e.bytes(b"\"revised_prompt\":")?;
            op.write(revised, e)?;
        }
        match (open, value) {
            (Some(open), _) => match self.delivery {
                Delivery::Base64 => {
                    sep(e, first)?;
                    e.bytes(b"\"b64_json\":\"")?;
                    write_encoded_chunks(&self.holder, open.chunks.iter().copied(), e)?;
                    e.bytes(b"\"")
                }
                Delivery::Url => {
                    sep(e, first)?;
                    // A source that names no media type gets a data URL without one.
                    e.bytes(b"\"url\":\"data:")?;
                    if let Some(mime) = open.start.member(open.start.root(), "mime")? {
                        open.start.write_contents(mime, e)?;
                    }
                    e.bytes(b";base64,")?;
                    write_encoded_chunks(&self.holder, open.chunks.iter().copied(), e)?;
                    e.bytes(b"\"")
                }
            },
            (None, Some((op, value))) => {
                // A URL result stays a URL whatever the delivery, as Go's handler writes it:
                // nothing here fetches it to make base64.
                sep(e, first)?;
                e.bytes(b"\"url\":")?;
                op.write(op.required(value, "url")?, e)
            }
            (None, None) => Err(Error::Schema),
        }
    }

    /// The settings the source reported for a result: on its media, or for a URL result on its
    /// operation.
    fn attributes<'a>(
        open: Option<&'a Open>,
        operation: Option<&'a Document>,
    ) -> Result<Option<(&'a Document, usize)>> {
        if let Some(open) = open {
            return Ok(Some((
                &open.start,
                open.start.required(open.start.root(), "attributes")?,
            )));
        }
        let Some(op) = operation else { return Ok(None) };
        let value = op.required(op.root(), "value")?;
        Ok(op.member(value, "attributes")?.map(|attributes| (op, attributes)))
    }

    fn settings(
        &self,
        open: Option<&Open>,
        operation: Option<&Document>,
        e: &mut Encoder<'_, '_>,
        names: &[&str],
    ) -> Result<()> {
        let Some((doc, attributes)) = Self::attributes(open, operation)? else {
            return Ok(());
        };
        for name in names {
            if let Some(node) = doc.member(attributes, name)? {
                e.bytes(b",")?;
                e.string(name)?;
                e.bytes(b":")?;
                doc.write(node, e)?;
            }
        }
        Ok(())
    }

    /// The first result that reports any of the response-level settings.
    fn first_settings(&self, names: &[&str]) -> Result<Option<&Result_>> {
        for result in self.results.iter() {
            if let Some((doc, attributes)) = Self::attributes(result.media.as_ref(), Some(&result.operation))? {
                for name in names {
                    if doc.member(attributes, name)?.is_some() {
                        return Ok(Some(result));
                    }
                }
            }
        }
        Ok(None)
    }

    fn usage_member(&self, e: &mut Encoder<'_, '_>) -> Result<()> {
        if let Some(usage) = self.tool_usage.as_ref().or(self.usage.as_ref()) {
            let native = usage.required(
                usage.required(usage.required(usage.root(), "usage")?, "native")?,
                "value",
            )?;
            e.bytes(b",\"usage\":")?;
            usage.write(native, e)?;
        }
        Ok(())
    }

    /// The canonical carrier determines placement: response.start holds response extras;
    /// image.result and preview media.start hold image extras, regardless of native pointer shape.
    fn extras(&self, doc: &Document, e: &mut Encoder<'_, '_>, first: &mut bool) -> Result<()> {
        let x = doc.required(doc.root(), "x_ck")?;
        let Some(original) = doc.member(x, "original")? else {
            return Ok(());
        };
        for entry in doc.children(original)? {
            if !restored(doc, entry)? {
                continue;
            }
            let pointer = doc.required(entry, "pointer")?;
            let name = crate::codec::provenance::leaf(doc.chars(pointer)?)?;
            sep(e, first)?;
            crate::codec::provenance::write_segment(name, e)?;
            e.bytes(b":")?;
            doc.write(doc.required(entry, "value")?, e)?;
        }
        Ok(())
    }

    fn event(&mut self, owner: &mut Writer, open: Option<&Open>, operation: Option<&Document>) -> Result<ScratchLease> {
        let name = match (self.format, operation.is_some()) {
            (Format::GenerationEvents, false) => "image_generation.partial_image",
            (Format::GenerationEvents, true) => "image_generation.completed",
            (_, false) => "image_edit.partial_image",
            (_, true) => "image_edit.completed",
        };
        let this = &*self;
        encode::scratch(owner, |e| {
            e.bytes(b"event: ")?;
            e.bytes(name.as_bytes())?;
            e.bytes(b"\ndata: {\"type\":")?;
            e.string(name)?;
            if let Some(open) = open.filter(|open| open.partial.is_some()) {
                e.bytes(b",\"partial_image_index\":")?;
                e.unsigned(open.partial.unwrap_or(0))?;
            }
            e.bytes(b",")?;
            this.write_created(e, "created_at", operation)?;
            let mut first = false;
            this.result(e, &mut first, open, operation)?;
            this.settings(open, operation, e, &SETTINGS)?;
            if let Some(op) = operation {
                this.usage_member(e)?;
                this.extras(op, e, &mut first)?;
            } else if let Some(open) = open {
                this.extras(&open.start, e, &mut first)?;
            }
            e.bytes(b"}\n\n")
        })
    }

    /// A deferred job's answer as xAI wrote it: `request_id` unless the poll implied it, a failed
    /// job's `error`, then the other members of xAI's body.
    fn job_body(&self, owner: &mut Writer) -> Result<ScratchLease> {
        let job = self.job.as_ref().ok_or(Error::Schema)?;
        let value = job.required(job.root(), "value")?;
        let x = job.required(job.root(), "x_ck")?;
        let implied = job.member(x, "id_implied")?.is_some();
        encode::scratch(owner, |e| {
            e.bytes(b"{")?;
            let mut first = true;
            if !implied {
                sep(e, &mut first)?;
                e.bytes(b"\"request_id\":")?;
                job.write(job.required(value, "job_id")?, e)?;
            }
            if let Some(error) = job.member(value, "error")? {
                sep(e, &mut first)?;
                e.bytes(b"\"error\":")?;
                job.write(error, e)?;
            }
            self.extras(job, e, &mut first)?;
            e.bytes(b"}")
        })
    }

    fn buffered(&mut self, owner: &mut Writer) -> Result<ScratchLease> {
        let this = &*self;
        let settings = this.first_settings(&SETTINGS)?;
        encode::scratch(owner, |e| {
            e.bytes(b"{")?;
            this.write_created(e, "created", this.results.first().map(|r| &r.operation))?;
            if let Some(result) = settings {
                this.settings(result.media.as_ref(), Some(&result.operation), e, &SETTINGS)?;
            }
            this.usage_member(e)?;
            if let Some(start) = &this.start {
                this.extras(start, e, &mut false)?;
            }
            e.bytes(b",\"data\":[")?;
            for (i, result) in this.results.iter().enumerate() {
                if i != 0 {
                    e.bytes(b",")?;
                }
                e.bytes(b"{")?;
                let mut first = true;
                this.result(e, &mut first, result.media.as_ref(), Some(&result.operation))?;
                this.extras(&result.operation, e, &mut first)?;
                e.bytes(b"}")?;
            }
            e.bytes(b"]}")
        })
    }
}

/// Settings of an image the response reports.
const SETTINGS: [&str; 4] = ["size", "quality", "background", "output_format"];

/// Whether an `image.job` operation has `status`.
fn job_status(job: &Document, status: &str) -> Result<bool> {
    boundary::is(job, job.required(job.root(), "value")?, "status", status)
}

fn sep(e: &mut Encoder<'_, '_>, first: &mut bool) -> Result<()> {
    if !std::mem::replace(first, false) {
        e.bytes(b",")?;
    }
    Ok(())
}

impl ResponseEgress for Egress {
    fn advance(
        &mut self,
        owner: &mut Writer,
        input: Lease,
        output: impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if self.closed {
            return Err(Error::Closed);
        }
        let result = self.record(owner, input, output);
        if result.is_err() {
            self.closed = true;
        }
        result
    }

    fn finish(&mut self) -> Result<()> {
        if self.done {
            Ok(())
        } else {
            self.closed = true;
            Err(Error::Truncated)
        }
    }
}
