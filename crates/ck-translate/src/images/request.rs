use ck_stream::{ScratchLease, Writer};

use super::request_multipart::form_body;
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

pub(super) const PROTOCOL: &str = "openai-images";
/// The image model of a Codex tool call when the request names none (`codexDefaultImageToolModel`).
const DEFAULT_TOOL_MODEL: &str = "gpt-image-2";

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Control {
    Text,
    Unsigned,
    Bool,
    Refs,
}

/// Every option the codec maps, in the order encoders write them. `model`, `prompt` and `n` are
/// canonical members of the document; these live in `x_ck.provider_parameters.images`.
pub(super) const CONTROLS: &[(&str, Control)] = &[
    ("size", Control::Text),
    ("quality", Control::Text),
    ("style", Control::Text),
    ("background", Control::Text),
    ("output_format", Control::Text),
    ("moderation", Control::Text),
    ("response_format", Control::Text),
    ("user", Control::Text),
    ("input_fidelity", Control::Text),
    ("aspect_ratio", Control::Text),
    ("resolution", Control::Text),
    ("output_compression", Control::Unsigned),
    ("partial_images", Control::Unsigned),
    ("stream", Control::Bool),
    ("images", Control::Refs),
    ("image", Control::Refs),
    ("mask", Control::Refs),
];
pub(super) const KNOWN: &[&str] = &[
    "model",
    "prompt",
    "n",
    "size",
    "quality",
    "style",
    "background",
    "output_format",
    "moderation",
    "response_format",
    "user",
    "input_fidelity",
    "aspect_ratio",
    "resolution",
    "output_compression",
    "partial_images",
    "stream",
    "images",
    "image",
    "mask",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    Generate,
    Edit,
}

impl Mode {
    pub(super) fn operation(self) -> &'static str {
        match self {
            Self::Generate => "image_generate",
            Self::Edit => "image_edit",
        }
    }
}

/// Client-boundary failure. Storage exhaustion remains a resource failure, not a malformed
/// request or permission to retry with a larger body limit.
#[derive(Debug)]
pub struct RequestFailure {
    pub error: Error,
}

impl RequestFailure {
    pub fn status(&self) -> u16 {
        match self.error {
            Error::EventTooLarge | Error::Limit => 413,
            Error::Storage(_) => 503,
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

/// One JSON request body, held in mapped storage. A failed decoder stays failed; only `finish`
/// returns an encodable document.
pub struct Decoder {
    source: Source,
    scanner: Scanner,
    mode: Mode,
    limit: u64,
    failed: bool,
}

impl Decoder {
    pub fn new(owner: &mut Writer, mode: Mode, limit: u64) -> Result<Self, RequestFailure> {
        Ok(Self {
            source: Source::new(owner)?,
            scanner: Scanner::new(owner, limit)?,
            mode,
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
        validate_wire(&native, root, self.mode)?;
        Ok(boundary::mapped(owner, |e| {
            e.bytes(b"{")?;
            if let Some(model) = present(&native, root, "model")? {
                e.bytes(b"\"model\":")?;
                native.write(model, e)?;
                e.bytes(b",")?;
            }
            e.bytes(b"\"text\":")?;
            native.write(native.required(root, "prompt")?, e)?;
            if let Some(n) = present(&native, root, "n")? {
                e.bytes(b",\"n\":")?;
                native.write(n, e)?;
            }
            e.bytes(b",\"x_ck\":{\"v\":1,\"operation\":")?;
            e.string(self.mode.operation())?;
            e.bytes(b",\"original_protocol\":\"openai-images\",\"provider_parameters\":{\"images\":{")?;
            let mut first = true;
            for (name, _) in CONTROLS {
                if let Some(node) = present(&native, root, name)? {
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
            boundary::original(&native, PROTOCOL, &[(root, "", KNOWN)], e)?;
            e.bytes(b"}}")
        })?)
    }
}

/// The member of `parent` unless it is absent or null: the references mark these members
/// nullable, and a null is the same as no member.
pub(super) fn present(doc: &Document, parent: usize, name: &str) -> Result<Option<usize>> {
    Ok(match doc.member(parent, name)? {
        Some(node) if !matches!(doc.node(node)?.value, Value::Null) => Some(node),
        _ => None,
    })
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

/// The address of an image reference: its `url`, or `image_url` as OpenAI names it.
pub(super) fn address(doc: &Document, node: usize) -> Result<Option<usize>> {
    Ok(match present(doc, node, "image_url")? {
        Some(url) => Some(url),
        None => present(doc, node, "url")?,
    })
}

/// An image or mask reference: an object naming exactly one of an address (a data URL included)
/// and a file id. OpenAI Images (`ImageRefParam`) and xAI (`image`, `images`) both define it so;
/// xAI calls the address `url` and also accepts `image_url`.
fn reference(doc: &Document, node: usize) -> Result<()> {
    object(doc, node)?;
    let url = present(doc, node, "url")?;
    let image_url = present(doc, node, "image_url")?;
    let file = present(doc, node, "file_id")?;
    for value in [url, image_url, file].into_iter().flatten() {
        nonempty(doc, value)?;
    }
    match (url.is_some() && image_url.is_some(), url.or(image_url), file) {
        (false, Some(_), None) | (false, None, Some(_)) => Ok(()),
        _ => Err(Error::Schema),
    }
}

fn validate_wire(doc: &Document, root: usize, mode: Mode) -> Result<()> {
    object(doc, root)?;
    nonempty(doc, doc.required(root, "prompt")?)?;
    if let Some(model) = present(doc, root, "model")? {
        nonempty(doc, model)?;
    }
    if let Some(n) = present(doc, root, "n")?
        && doc.unsigned(n)? == 0
    {
        return Err(Error::Schema);
    }
    validate_controls(doc, root, mode)
}

/// Types of every mapped control, shared by wire and canonical documents.
fn validate_controls(doc: &Document, parent: usize, mode: Mode) -> Result<()> {
    for (name, kind) in CONTROLS {
        let Some(node) = present(doc, parent, name)? else {
            continue;
        };
        match kind {
            Control::Text => boundary::string(doc, node)?,
            Control::Unsigned => {
                doc.unsigned(node)?;
            }
            Control::Bool => {
                if !matches!(doc.node(node)?.value, Value::Bool(_)) {
                    return Err(Error::Schema);
                }
            }
            Control::Refs => match doc.node(node)?.value {
                Value::Array if *name == "images" => {
                    let mut any = false;
                    for item in doc.children(node)? {
                        reference(doc, item)?;
                        any = true;
                    }
                    if !any {
                        return Err(Error::Schema);
                    }
                }
                Value::Object if *name != "images" => reference(doc, node)?,
                _ => return Err(Error::Schema),
            },
        }
    }
    let images = present(doc, parent, "images")?.is_some();
    let image = present(doc, parent, "image")?.is_some();
    let masked = present(doc, parent, "mask")?.is_some();
    // xAI defines `image` and `images` as mutually exclusive.
    if images && image {
        return Err(Error::Schema);
    }
    match mode {
        Mode::Edit if !images && !image => Err(Error::Schema),
        Mode::Generate if images || image || masked => Err(Error::Schema),
        _ => Ok(()),
    }
}

/// Where an encoded request goes. `OpenAiForm` and `Codex` carry what the executor owns: the
/// multipart boundary (random, at least 32 characters, sent in `Content-Type`) and the Responses
/// model the image tool runs under.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Destination<'a> {
    OpenAi,
    OpenAiForm(&'a str),
    Xai,
    Codex(&'a str),
}

impl Destination<'_> {
    fn allows(self, name: &str, mode: Mode) -> bool {
        match self {
            Self::OpenAi | Self::OpenAiForm(_) => !matches!(name, "aspect_ratio" | "resolution"),
            // `stream` only selects how the client is answered; xAI has no event stream.
            Self::Xai => matches!(
                name,
                "response_format" | "aspect_ratio" | "resolution" | "images" | "image" | "user" | "stream"
            ),
            Self::Codex(_) => match name {
                "size" | "quality" | "background" | "output_format" | "moderation" | "output_compression"
                | "partial_images" | "response_format" | "stream" | "images" | "image" | "mask" => true,
                "input_fidelity" => mode == Mode::Edit,
                _ => false,
            },
        }
    }
}

pub(super) struct Canonical {
    pub(super) mode: Mode,
    pub(super) root: usize,
    pub(super) controls: usize,
    pub(super) x: usize,
}

fn canonical(doc: &Document) -> Result<Canonical> {
    let root = doc.root();
    object(doc, root)?;
    nonempty(doc, doc.required(root, "text")?)?;
    if let Some(model) = present(doc, root, "model")? {
        nonempty(doc, model)?;
    }
    let x = doc.required(root, "x_ck")?;
    if doc.unsigned(doc.required(x, "v")?)? != 1 {
        return Err(Error::Schema);
    }
    let mode = if boundary::is(doc, x, "operation", "image_generate")? {
        Mode::Generate
    } else if boundary::is(doc, x, "operation", "image_edit")? {
        Mode::Edit
    } else {
        return Err(Error::Schema);
    };
    let parameters = doc.required(x, "provider_parameters")?;
    object(doc, parameters)?;
    for field in doc.children(parameters)? {
        if !doc.name_is(doc.node(field)?.name.ok_or(Error::Schema)?, "images")? {
            return Err(Error::Unsupported);
        }
    }
    let controls = doc.required(parameters, "images")?;
    object(doc, controls)?;
    for field in doc.children(controls)? {
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        if !CONTROLS.iter().try_fold(false, |found, (key, _)| {
            Ok::<_, Error>(found || doc.name_is(name, key)?)
        })? {
            return Err(Error::Unsupported);
        }
    }
    if let Some(n) = present(doc, root, "n")?
        && doc.unsigned(n)? == 0
    {
        return Err(Error::Schema);
    }
    validate_controls(doc, controls, mode)?;
    Ok(Canonical {
        mode,
        root,
        controls,
        x,
    })
}

/// The checks [`encode`] runs before writing, without writing: a destination's capability runs
/// them at the fence, so a request it cannot express is refused before any attempt sends.
pub fn check(doc: &Document, destination: Destination<'_>) -> Result<()> {
    expressible(doc, &canonical(doc)?, destination)
}

fn expressible(doc: &Document, c: &Canonical, destination: Destination<'_>) -> Result<()> {
    for (key, _) in CONTROLS {
        if present(doc, c.controls, key)?.is_some() && !destination.allows(key, c.mode) {
            return Err(Error::Unsupported);
        }
    }
    // The image tool makes one image per call.
    if present(doc, c.root, "n")?.is_some_and(|n| doc.unsigned(n).is_ok_and(|n| n > 1))
        && matches!(destination, Destination::Codex(_))
    {
        return Err(Error::Unsupported);
    }
    Ok(())
}

/// Writes one upstream body from a canonical image request. Every control the destination cannot
/// express is refused here, before bytes exist, rather than dropped (L16); members of another
/// protocol's source stay out of the body (§3.7).
pub fn encode(
    owner: &mut Writer,
    doc: &Document,
    destination: Destination<'_>,
) -> Result<ScratchLease, RequestFailure> {
    doc.check_owner(owner)?;
    let c = canonical(doc)?;
    expressible(doc, &c, destination)?;
    let owner_protocol = boundary::is(doc, c.x, "original_protocol", PROTOCOL)?;
    Ok(match destination {
        Destination::OpenAi => encode::scratch(owner, |e| json_body(doc, &c, owner_protocol, e))?,
        Destination::OpenAiForm(boundary) => {
            encode::scratch(owner, |e| form_body(doc, &c, boundary, owner_protocol, e))?
        }
        Destination::Xai => encode::scratch(owner, |e| xai_body(doc, &c, e))?,
        Destination::Codex(main) => encode::scratch(owner, |e| codex_body(doc, &c, main, e))?,
    })
}

/// Writes `,"key":value` when `parent` has a non-null member `key`.
fn member(doc: &Document, parent: usize, key: &str, e: &mut Encoder<'_, '_>) -> Result<()> {
    if let Some(node) = present(doc, parent, key)? {
        e.bytes(b",")?;
        e.string(key)?;
        e.bytes(b":")?;
        doc.write(node, e)?;
    }
    Ok(())
}

fn head(doc: &Document, c: &Canonical, prompt_name: &str, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{")?;
    if let Some(model) = present(doc, c.root, "model")? {
        e.bytes(b"\"model\":")?;
        doc.write(model, e)?;
        e.bytes(b",")?;
    }
    e.string(prompt_name)?;
    e.bytes(b":")?;
    doc.write(doc.required(c.root, "text")?, e)?;
    member(doc, c.root, "n", e)
}

fn json_body(doc: &Document, c: &Canonical, same_protocol: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    head(doc, c, "prompt", e)?;
    for (name, kind) in CONTROLS {
        let Some(node) = present(doc, c.controls, name)? else {
            continue;
        };
        e.bytes(b",")?;
        // OpenAI Images has one input list, `images`; xAI's single `image` is a list of one.
        e.string(if *name == "image" { "images" } else { name })?;
        e.bytes(b":")?;
        if *kind != Control::Refs {
            doc.write(node, e)?;
        } else if *name == "image" {
            e.bytes(b"[")?;
            openai_ref(doc, node, e)?;
            e.bytes(b"]")?;
        } else if *name == "images" {
            e.bytes(b"[")?;
            for (i, item) in doc.children(node)?.enumerate() {
                if i != 0 {
                    e.bytes(b",")?;
                }
                openai_ref(doc, item, e)?;
            }
            e.bytes(b"]")?;
        } else {
            openai_ref(doc, node, e)?;
        }
    }
    if same_protocol {
        originals(doc, c.x, e)?;
    }
    e.bytes(b"}")
}

/// A reference as OpenAI Images spells it: `image_url` or `file_id`; a JSON body carries no
/// `filename`, and a null member is the absent one.
fn openai_ref(doc: &Document, node: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{")?;
    let mut first = true;
    for child in doc.children(node)? {
        if matches!(doc.node(child)?.value, Value::Null) {
            continue;
        }
        let name = doc.node(child)?.name.ok_or(Error::Schema)?;
        if doc.name_is(name, "filename")? {
            continue;
        }
        if !first {
            e.bytes(b",")?;
        }
        first = false;
        if doc.name_is(name, "url")? {
            e.string("image_url")?;
        } else {
            doc.write_name(name, e)?;
        }
        e.bytes(b":")?;
        doc.write(child, e)?;
    }
    e.bytes(b"}")
}

/// Restores the members this codec does not map, for a destination of the source's own protocol.
fn originals(doc: &Document, x: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let Some(original) = doc.member(x, "original")? else {
        return Ok(());
    };
    for entry in doc.children(original)? {
        if !boundary::is(doc, entry, "protocol", PROTOCOL)? {
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

/// xAI names a reference's address `url` (and also accepts `image_url`).
fn xai_ref(doc: &Document, node: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    if let Some(url) = address(doc, node)? {
        e.bytes(b"{\"url\":")?;
        doc.write(url, e)?;
    } else {
        e.bytes(b"{\"file_id\":")?;
        doc.write(doc.required(node, "file_id")?, e)?;
    }
    e.bytes(b"}")
}

fn xai_body(doc: &Document, c: &Canonical, e: &mut Encoder<'_, '_>) -> Result<()> {
    head(doc, c, "prompt", e)?;
    for name in ["response_format", "aspect_ratio", "resolution", "user"] {
        member(doc, c.controls, name, e)?;
    }
    if let Some(image) = present(doc, c.controls, "image")? {
        e.bytes(b",\"image\":")?;
        xai_ref(doc, image, e)?;
    }
    if let Some(images) = present(doc, c.controls, "images")? {
        // A list of one is xAI's `image`; `images` is for several references.
        let mut items = doc.children(images)?;
        let (first, second) = (items.next(), items.next());
        match (first, second) {
            (Some(only), None) => {
                e.bytes(b",\"image\":")?;
                xai_ref(doc, only, e)?;
            }
            _ => {
                e.bytes(b",\"images\":[")?;
                for (i, item) in doc.children(images)?.enumerate() {
                    if i != 0 {
                        e.bytes(b",")?;
                    }
                    xai_ref(doc, item, e)?;
                }
                e.bytes(b"]")?;
            }
        }
    }
    e.bytes(b"}")
}

/// The Responses request that forces the `image_generation` tool (`codexBuildImagesResponsesRequest`).
/// `model` and the tool controls are the tool's; the conversation runs on `main`.
fn codex_body(doc: &Document, c: &Canonical, main: &str, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{\"instructions\":\"\",\"stream\":true,\"reasoning\":{\"effort\":\"medium\",\"summary\":\"auto\"},\"parallel_tool_calls\":true,\"include\":[\"reasoning.encrypted_content\"],\"model\":")?;
    e.string(main)?;
    e.bytes(b",\"store\":false,\"tool_choice\":{\"type\":\"image_generation\"},\"tools\":[{\"type\":\"image_generation\",\"action\":")?;
    e.string(if c.mode == Mode::Edit { "edit" } else { "generate" })?;
    e.bytes(b",\"model\":")?;
    match present(doc, c.root, "model")? {
        Some(model) => doc.write(model, e)?,
        None => e.string(DEFAULT_TOOL_MODEL)?,
    }
    for name in [
        "size",
        "quality",
        "background",
        "output_format",
        "moderation",
        "output_compression",
        "partial_images",
        "input_fidelity",
    ] {
        member(doc, c.controls, name, e)?;
    }
    if let Some(mask) = present(doc, c.controls, "mask")? {
        e.bytes(b",\"input_image_mask\":")?;
        codex_ref(doc, mask, false, e)?;
    }
    e.bytes(b"}],\"input\":[{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":")?;
    doc.write(doc.required(c.root, "text")?, e)?;
    e.bytes(b"}")?;
    if let Some(image) = present(doc, c.controls, "image")? {
        e.bytes(b",")?;
        codex_ref(doc, image, true, e)?;
    }
    if let Some(images) = present(doc, c.controls, "images")? {
        for item in doc.children(images)? {
            e.bytes(b",")?;
            codex_ref(doc, item, true, e)?;
        }
    }
    e.bytes(b"]}]}")
}

fn codex_ref(doc: &Document, node: usize, input: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{")?;
    if input {
        e.bytes(b"\"type\":\"input_image\",")?;
    }
    if let Some(url) = address(doc, node)? {
        e.bytes(b"\"image_url\":")?;
        doc.write(url, e)?;
    } else {
        e.bytes(b"\"file_id\":")?;
        doc.write(doc.required(node, "file_id")?, e)?;
    }
    e.bytes(b"}")
}
