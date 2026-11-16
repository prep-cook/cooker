//! Usage snapshots of an image response: the model's own, and the image tool's apart from it.
//! The payload is `{usage: UsageSnapshot}` (§3.5); the provider's own fields, such as xAI's
//! `cost_in_usd_ticks`, stay in `native`.

use ck_stream::Writer;

use super::State;
use crate::{
    boundary,
    codec::{
        Error, Result,
        document::{Document, Value},
        encode::Encoder,
        event::{Emitter, Kind, Scope},
    },
};

/// Canonical counts: `(snapshot member, path in the native usage)`.
const COUNTS: &[(&str, &[&str])] = &[
    ("input_tokens", &["input_tokens"]),
    ("output_tokens", &["output_tokens"]),
    ("total_tokens", &["total_tokens"]),
    ("cache_read_tokens", &["input_tokens_details", "cached_tokens"]),
    ("reasoning_tokens", &["output_tokens_details", "reasoning_tokens"]),
];
/// Modality breakdowns, written under `details`.
const DETAILS: &[(&str, &[&str])] = &[
    ("input_text_tokens", &["input_tokens_details", "text_tokens"]),
    ("input_image_tokens", &["input_tokens_details", "image_tokens"]),
    ("output_text_tokens", &["output_tokens_details", "text_tokens"]),
    ("output_image_tokens", &["output_tokens_details", "image_tokens"]),
];

impl State {
    /// The response's own usage. One snapshot per response: a repeated source copy, such as each
    /// streamed completion carrying the same `usage`, is read once and never added again.
    pub(super) fn usage(&mut self, owner: &mut Writer, doc: &Document, node: usize) -> Result<()> {
        if self.usage || matches!(doc.node(node)?.value, Value::Null) {
            return Ok(());
        }
        self.usage = true;
        let protocol = self.wire.protocol();
        let Some(job) = self.job.as_mut() else {
            return snapshot(&mut self.emitter, protocol, owner, doc, node, None, None);
        };
        // A deferred job's usage is the job's (E-Q13): a poll's report is published only when it
        // reports a count and the job table charges the job with it; any other publishes nothing.
        let id = job.id.clone().ok_or(Error::Schema)?;
        if !reports(doc, node)? {
            return Ok(());
        }
        let emitter = &mut self.emitter;
        job.ledger.charge(&id, &mut || {
            snapshot(emitter, protocol, owner, doc, node, None, Some(&id))
        })?;
        Ok(())
    }

    /// The image tool's usage, apart from the model's, under the `tool:<model>` scope.
    pub(super) fn tool_usage(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        node: usize,
        model: Option<usize>,
    ) -> Result<()> {
        if matches!(doc.node(node)?.value, Value::Null) {
            return Ok(());
        }
        snapshot(
            &mut self.emitter,
            self.wire.protocol(),
            owner,
            doc,
            node,
            Some(model),
            None,
        )
    }
}

/// One snapshot: the response's own, the image tool's (`tool`) or a deferred job's (`job`).
fn snapshot(
    emitter: &mut Emitter,
    protocol: &'static str,
    owner: &mut Writer,
    doc: &Document,
    node: usize,
    tool: Option<Option<usize>>,
    job: Option<&str>,
) -> Result<()> {
    if !matches!(doc.node(node)?.value, Value::Object) {
        return Err(Error::Schema);
    }
    emitter.emit(
        owner,
        Kind::Usage,
        Scope::default(),
        |e| {
            if let Some(model) = tool {
                e.bytes(b",\"scope\":\"tool:")?;
                match model {
                    Some(model) => doc.write_contents(model, e)?,
                    None => e.bytes(b"image_generation")?,
                }
                e.bytes(b"\"")?;
            }
            if let Some(job) = job {
                e.bytes(b",\"scope\":\"job:")?;
                e.escaped(job)?;
                e.bytes(b"\"")?;
            }
            e.bytes(b",\"usage\":{\"native\":{\"protocol\":")?;
            e.string(protocol)?;
            e.bytes(b",\"value\":")?;
            doc.write(node, e)?;
            e.bytes(b"},\"final\":true")?;
            for (name, path) in COUNTS {
                if let Some(count) = count(doc, node, path)? {
                    e.bytes(b",")?;
                    e.string(name)?;
                    e.bytes(b":")?;
                    doc.write(count, e)?;
                }
            }
            details(doc, node, e)?;
            e.bytes(b"}")
        },
        |_| Ok(()),
    )?;
    Ok(())
}

fn details(doc: &Document, node: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let mut first = true;
    for (name, path) in DETAILS {
        if let Some(count) = count(doc, node, path)? {
            e.bytes(if first { b",\"details\":{" } else { b"," })?;
            first = false;
            e.string(name)?;
            e.bytes(b":")?;
            doc.write(count, e)?;
        }
    }
    if !first {
        e.bytes(b"}")?;
    }
    Ok(())
}

/// Whether a usage object reports a billed value: xAI's `cost_in_usd_ticks` or a canonical count.
/// An empty usage reports nothing, so it does not spend a job's charge.
fn reports(doc: &Document, node: usize) -> Result<bool> {
    if !matches!(doc.node(node)?.value, Value::Object) {
        return Err(Error::Schema);
    }
    for (_, path) in COUNTS {
        if count(doc, node, path)?.is_some() {
            return Ok(true);
        }
    }
    Ok(count(doc, node, &["cost_in_usd_ticks"])?.is_some())
}

/// The unsigned count at `path`, when the native usage reports one: null and absent counts stay
/// out of the snapshot, and the native value keeps them.
fn count(doc: &Document, mut node: usize, path: &[&str]) -> Result<Option<usize>> {
    for name in path {
        if !matches!(doc.node(node)?.value, Value::Object) {
            return Ok(None);
        }
        match doc.member(node, name)? {
            Some(child) => node = child,
            None => return Ok(None),
        }
    }
    Ok(match doc.node(node)?.value {
        Value::Unsigned(_) | Value::Number(_) if doc.unsigned(node).is_ok() => Some(node),
        _ => None,
    })
}

/// The image tool's model: the `model` of the response's `image_generation` tool, when it names one.
pub(super) fn tool_model(doc: &Document, response: usize) -> Result<Option<usize>> {
    let Some(tools) = doc.member(response, "tools")? else {
        return Ok(None);
    };
    for tool in doc.children(tools)? {
        if boundary::is(doc, tool, "type", "image_generation")?
            && let Some(model) = doc.member(tool, "model")?
        {
            boundary::string(doc, model)?;
            return Ok(Some(model));
        }
    }
    Ok(None)
}
