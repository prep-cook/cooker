use ck_stream::{ScratchLease, Writer};

use crate::{
    boundary,
    codec::{
        Error, Result,
        document::{Document, Value},
        encode::{self, Encoder},
        json::Scanner,
        provenance,
        source::Source,
    },
};

const OPTIONS: &[&str] = &[
    "voice",
    "language",
    "speed",
    "instructions",
    "response_format",
    "output_format",
    "stream_format",
];
/// Members of xAI's `output_format` object (https://docs.x.ai/developers/rest-api-reference,
/// text to speech, "Output Formats"). Anything else is refused, not forwarded.
const OUTPUT_FORMAT: &[&str] = &["codec", "sample_rate", "bit_rate"];
const KNOWN: &[&str] = &[
    "model",
    "input",
    "voice",
    "language",
    "speed",
    "instructions",
    "response_format",
    "output_format",
    "stream_format",
];

/// Client-boundary failure. Storage exhaustion remains a resource failure, not a
/// malformed request or permission to retry with a larger body limit.
#[derive(Debug)]
pub struct RequestFailure {
    pub error: Error,
}

impl RequestFailure {
    pub fn status(&self) -> u16 {
        match self.error {
            Error::EventTooLarge => 413,
            Error::Storage(_) | Error::Limit => 503,
            _ => 400,
        }
    }
}
impl From<Error> for RequestFailure {
    fn from(error: Error) -> Self {
        Self { error }
    }
}
impl std::fmt::Display for RequestFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}
impl std::error::Error for RequestFailure {}

/// Request payload and parser metadata are charged to the owning mapped stream.
/// A failed decoder stays failed; only `finish` returns an encodable document.
pub struct Decoder {
    source: Source,
    scanner: Scanner,
    limit: u64,
    failed: bool,
}

impl Decoder {
    pub fn new(owner: &mut Writer, limit: u64) -> Result<Self, RequestFailure> {
        Ok(Self {
            source: Source::new(owner)?,
            scanner: Scanner::new(owner, limit)?,
            limit,
            failed: false,
        })
    }

    pub fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<(), RequestFailure> {
        self.source.check_owner(owner)?;
        if self.failed {
            return Err(Error::Closed.into());
        }
        let result = (|| {
            if bytes.len() as u64 > self.limit.saturating_sub(self.source.len()) {
                return Err(Error::EventTooLarge);
            }
            self.source.push_bytes(owner, bytes)?;
            self.scanner.advance(&self.source)
        })();
        self.failed = result.is_err();
        result.map_err(Into::into)
    }

    pub fn finish(mut self, owner: &mut Writer) -> Result<Document, RequestFailure> {
        self.source.check_owner(owner)?;
        if self.failed {
            return Err(Error::Closed.into());
        }
        self.scanner.finish(&self.source)?;
        let native = Document::from_scanned(owner, self.source, self.scanner)?;
        let root = native.root();
        object(&native, root)?;
        for name in ["model", "input", "voice"] {
            nonempty(&native, native.required(root, name)?)?;
        }
        validate_options(&native, root)?;
        Ok(boundary::mapped(owner, |e| {
            e.bytes(b"{\"model\":")?;
            native.write(native.required(root, "model")?, e)?;
            e.bytes(b",\"text\":")?;
            native.write(native.required(root, "input")?, e)?;
            e.bytes(b",\"x_ck\":{\"v\":1,\"operation\":\"speech\",\"original_protocol\":\"openai-speech\",\"provider_parameters\":{\"audio\":{")?;
            let mut first = true;
            for name in OPTIONS {
                if let Some(node) = native.member(root, name)? {
                    if !first {
                        e.bytes(b",")?;
                    }
                    first = false;
                    e.string(name)?;
                    e.bytes(b":")?;
                    native.write(node, e)?;
                }
            }
            e.bytes(b"}}")?;
            boundary::original(&native, "openai-speech", &[(root, "", KNOWN)], e)?;
            e.bytes(b"}}")
        })?)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Destination {
    OpenAi,
    Xai,
}

/// Validate current canonical values before producing any upstream bytes. xAI
/// instructions, an `output_format` sent to OpenAI, a request carrying both
/// `response_format` and `output_format`, and unknown `output_format` members are
/// refused, rather than silently discarded (L16). Format names match exactly and
/// zero rates are refused. Original voice names remain in the document
/// independently of the xAI projection.
pub fn encode(owner: &mut Writer, doc: &Document, destination: Destination) -> Result<ScratchLease, RequestFailure> {
    doc.check_owner(owner)?;
    let (root, x, audio) = canonical(doc, destination)?;
    Ok(encode::scratch(owner, |e| {
        e.bytes(b"{")?;
        if destination == Destination::OpenAi {
            e.bytes(b"\"model\":")?;
            doc.write(doc.required(root, "model")?, e)?;
            e.bytes(b",\"input\":")?;
            doc.write(doc.required(root, "text")?, e)?;
            for name in OPTIONS {
                boundary::member(doc, audio, name, e)?;
            }
            originals(doc, x, e)?;
            return e.bytes(b"}");
        }
        // xAI members in key order, as Go's `json.Marshal` writes the `/tts` body map.
        let mut first = true;
        let mut key = |e: &mut Encoder<'_, '_>, name: &str| {
            if !std::mem::take(&mut first) {
                e.bytes(b",")?;
            }
            e.string(name)?;
            e.bytes(b":")
        };
        if let Some(language) = doc.member(audio, "language")? {
            key(e, "language")?;
            doc.write(language, e)?;
        }
        if let Some(output) = doc.member(audio, "output_format")? {
            key(e, "output_format")?;
            doc.write(output, e)?;
        } else if let Some(format) = doc.member(audio, "response_format")?
            && !doc.string_is(format, "mp3")?
        {
            key(e, "output_format")?;
            e.bytes(b"{\"codec\":")?;
            doc.write(format, e)?;
            e.bytes(b",\"sample_rate\":24000}")?;
        }
        if let Some(speed) = doc.member(audio, "speed")? {
            key(e, "speed")?;
            doc.write(speed, e)?;
        }
        key(e, "text")?;
        doc.write(doc.required(root, "text")?, e)?;
        key(e, "voice_id")?;
        voice(doc, doc.required(audio, "voice")?, e)?;
        e.bytes(b"}")
    })?)
}

/// The checks [`encode`] runs before writing, without writing: a destination's capability runs
/// them at the fence, so a request it cannot express is refused before any attempt sends.
pub fn check(doc: &Document, destination: Destination) -> Result<()> {
    canonical(doc, destination).map(drop)
}

/// The root, `x_ck` and audio parameter nodes of a request `destination` can express.
fn canonical(doc: &Document, destination: Destination) -> Result<(usize, usize, usize)> {
    let root = doc.root();
    object(doc, root)?;
    for name in ["model", "text"] {
        nonempty(doc, doc.required(root, name)?)?;
    }
    let x = doc.required(root, "x_ck")?;
    if doc.unsigned(doc.required(x, "v")?)? != 1 || !boundary::is(doc, x, "operation", "speech")? {
        return Err(Error::Schema);
    }
    let parameters = doc.required(x, "provider_parameters")?;
    object(doc, parameters)?;
    for field in doc.children(parameters)? {
        if !doc.name_is(doc.node(field)?.name.ok_or(Error::Schema)?, "audio")? {
            return Err(Error::Unsupported);
        }
    }
    let audio = doc.required(parameters, "audio")?;
    object(doc, audio)?;
    nonempty(doc, doc.required(audio, "voice")?)?;
    validate_options(doc, audio)?;
    for field in doc.children(audio)? {
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        if !OPTIONS
            .iter()
            .try_fold(false, |found, key| Ok::<_, Error>(found || doc.name_is(name, key)?))?
        {
            return Err(Error::Unsupported);
        }
    }
    // OpenAI's speech API has no `output_format`; sample rate and codec cannot be expressed there (L16).
    if destination == Destination::OpenAi && doc.member(audio, "output_format")?.is_some() {
        return Err(Error::Unsupported);
    }
    if destination == Destination::Xai {
        if doc.member(audio, "instructions")?.is_some() {
            return Err(Error::Unsupported);
        }
        let format = if let Some(output) = doc.member(audio, "output_format")? {
            doc.member(output, "codec")?
        } else {
            doc.member(audio, "response_format")?
        };
        if let Some(format) = format
            && !["mp3", "wav", "pcm"].iter().try_fold(false, |found, value| {
                Ok::<_, Error>(found || doc.string_is(format, value)?)
            })?
        {
            return Err(Error::Unsupported);
        }
    }
    Ok((root, x, audio))
}

fn object(doc: &Document, node: usize) -> Result<()> {
    if matches!(doc.node(node)?.value, Value::Object) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}

fn nonempty(doc: &Document, node: usize) -> Result<()> {
    boundary::string(doc, node)?;
    if doc.chars(node)?.next_scalar()?.is_none() {
        Err(Error::Schema)
    } else {
        Ok(())
    }
}

fn validate_options(doc: &Document, root: usize) -> Result<()> {
    for name in ["language", "instructions", "response_format", "stream_format"] {
        if let Some(node) = doc.member(root, name)? {
            boundary::string(doc, node)?;
        }
    }
    if let Some(output) = doc.member(root, "output_format")? {
        // One field names the format: a request carrying both would have one of them
        // silently ignored on every destination.
        if doc.member(root, "response_format")?.is_some() {
            return Err(Error::Unsupported);
        }
        object(doc, output)?;
        for field in doc.children(output)? {
            let name = doc.node(field)?.name.ok_or(Error::Schema)?;
            if !OUTPUT_FORMAT
                .iter()
                .try_fold(false, |found, key| Ok::<_, Error>(found || doc.name_is(name, key)?))?
            {
                return Err(Error::Unsupported);
            }
        }
        if let Some(codec) = doc.member(output, "codec")? {
            boundary::string(doc, codec)?;
        }
        // A zero rate is refused rather than replaced by a default the client did not ask for.
        for name in ["sample_rate", "bit_rate"] {
            if let Some(rate) = doc.member(output, name)?
                && doc.unsigned(rate)? == 0
            {
                return Err(Error::Schema);
            }
        }
    }
    if let Some(node) = doc.member(root, "speed")?
        && !matches!(doc.node(node)?.value, Value::Number(_) | Value::Unsigned(_))
    {
        return Err(Error::Schema);
    }
    if let Some(node) = doc.member(root, "stream_format")?
        && !doc.string_is(node, "audio")?
    {
        return Err(Error::Unsupported);
    }
    Ok(())
}

fn voice(doc: &Document, node: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    for (source, target) in [
        ("alloy", "ara"),
        ("ash", "orion"),
        ("ballad", "luna"),
        ("coral", "celeste"),
        ("echo", "rex"),
        ("fable", "sal"),
        ("onyx", "leo"),
        ("nova", "eve"),
        ("sage", "iris"),
        ("shimmer", "aurora"),
        ("verse", "lumen"),
    ] {
        if doc.string_is(node, source)? {
            return e.string(target);
        }
    }
    doc.write(node, e)
}

fn originals(doc: &Document, x: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let Some(original) = doc.member(x, "original")? else {
        return Ok(());
    };
    for entry in doc.children(original)? {
        if !boundary::is(doc, entry, "protocol", "openai-speech")? {
            continue;
        }
        let name = provenance::leaf(doc.chars(doc.required(entry, "pointer")?)?)?;
        if KNOWN
            .iter()
            .try_fold(false, |found, key| Ok::<_, Error>(found || name.is(key)?))?
        {
            continue;
        }
        e.bytes(b",")?;
        provenance::write_segment(name, e)?;
        e.bytes(b":")?;
        doc.write(doc.required(entry, "value")?, e)?;
    }
    Ok(())
}
