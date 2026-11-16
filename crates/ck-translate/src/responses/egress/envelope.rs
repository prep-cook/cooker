use super::*;

impl<F: FnMut(&'static str, u64)> Egress<F> {
    /// Ends the response as failed with a sanitized error, as the body in buffered mode and in
    /// a `response.failed` event when streaming. A failure the upstream reported itself (an
    /// `error` record with a `type`) first gets the official `error` event, which the client SDK
    /// raises, with the same sanitized values.
    pub(super) fn failed(
        &mut self,
        owner: &mut Writer,
        kind: Kind,
        record: Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        // A stream that fails before its start still opens with `response.created`.
        self.owed |= self.sequence == 0;
        self.created(owner, output)?;
        self.terminal = true;
        if kind == Kind::Error && record.member(record.root(), "type")?.is_some() {
            let payload =
                b",\"error\":{\"type\":\"server_error\",\"code\":\"server_error\",\"message\":\"Response failed\"}";
            self.emit(owner, "error", |e| e.bytes(payload), output)?;
        }
        let doc = &mut self.document;
        let root = doc.root();
        doc.set(root, "status", Value::Text("failed"))?;
        let error = doc.set(root, "error", Value::Object)?;
        doc.set(error, "code", Value::Text("server_error"))?;
        doc.set(error, "message", Value::Text("Response failed"))?;
        order_array(doc, doc.required(root, "output")?, "output_index", owner)?;
        let response = encode::scratch(owner, |e| write_response(doc, e))?;
        if self.mode == BodyMode::Buffered {
            return output(response);
        }
        self.emit(
            owner,
            "response.failed",
            |e| {
                e.bytes(b",\"response\":")?;
                write_lease(&response, e)
            },
            output,
        )
    }

    /// Repeats the request's `tools`, `tool_choice` and `parallel_tool_calls` in a generation's
    /// response, as the Responses API does; a source without them, such as a Chat upstream,
    /// otherwise leaves the API defaults. A Responses source's own values replace these by merge.
    /// Call it before the first record: `Closed` once any output was written.
    pub fn echo(&mut self, owner: &mut Writer, request: codec::RequestView<'_>) -> Result<()> {
        crate::boundary::enter(self.owner, owner, self.terminal || self.owed || self.sequence > 0)?;
        let echoed = super::super::mapped(owner, |e| super::super::request::write_echo(request.document, e))?;
        let doc = &mut self.document;
        let node = doc.absorb(echoed)?;
        super::super::merge(doc, doc.root(), node)
    }

    /// Restores the source's model, creation time and status; `response.created` is owed until
    /// `created` publishes it. Fields the source did not supply keep the response's own values,
    /// and the exchange id stays.
    pub(super) fn response_start(&mut self, incoming: Document) -> Result<()> {
        let doc = &mut self.document;
        let target = doc.root();
        let node = doc.absorb(incoming)?;
        for (from, to) in [("model", "model"), ("created", "created_at"), ("status", "status")] {
            if let Some(value) = doc.remove(node, from)?
                && !codec::identity::synthetic(doc, node, from)?
            {
                let target = doc.set(target, to, Value::Null)?;
                doc.transplant(target, value)?;
            }
        }
        self.owed = true;
        Ok(())
    }

    /// Absorbs a canonical record. A chunk's root `model` replaces the response's; its `id` does
    /// not replace the exchange id.
    pub(super) fn absorb(&mut self, kind: Kind, incoming: Document) -> Result<usize> {
        let node = self.document.absorb(incoming)?;
        if kind != Kind::Chunk {
            return Ok(node);
        }
        let doc = &mut self.document;
        let root = doc.root();
        if let Some(model) = doc.member(node, "model")? {
            let target = doc.set(root, "model", Value::Null)?;
            doc.transplant(target, model)?;
        }
        Ok(node)
    }

    /// Publishes the owed `response.created` with the response as it stands. Every event other
    /// than a heartbeat, usage, start, opaque record or chunk calls it before its own output; a
    /// chunk with output follows the start of its item, and the finish chunk writes nothing.
    pub(super) fn created(
        &mut self,
        owner: &mut Writer,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if !self.owed {
            return Ok(());
        }
        self.owed = false;
        let response = encode::scratch(owner, |e| write_response(&self.document, e))?;
        self.emit(
            owner,
            "response.created",
            |e| {
                e.bytes(b",\"response\":")?;
                write_lease(&response, e)
            },
            output,
        )
    }

    /// Closes the response: buffered mode writes the body, streaming the terminal event.
    pub(super) fn done(
        &mut self,
        owner: &mut Writer,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if self.images.iter().any(|image| !image.closed) || self.audio.iter().any(|audio| !audio.closed) {
            return Err(Error::Truncated);
        }
        self.created(owner, output)?;
        self.terminal = true;
        let doc = &mut self.document;
        let root = doc.root();
        if is(doc, root, "status", "in_progress")? {
            doc.set(root, "status", Value::Text("completed"))?;
        }
        order_array(doc, doc.required(root, "output")?, "output_index", owner)?;
        let response = encode::scratch(owner, |e| write_response(doc, e))?;
        if self.mode == BodyMode::Buffered {
            return output(response);
        }
        let name = if is(doc, root, "status", "incomplete")? {
            "response.incomplete"
        } else {
            "response.completed"
        };
        self.emit(
            owner,
            name,
            |e| {
                e.bytes(b",\"response\":")?;
                write_lease(&response, e)
            },
            output,
        )
    }

    /// Restores native response fields, or writes or drops an unknown event.
    pub(super) fn opaque(
        &mut self,
        owner: &mut Writer,
        incoming: Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let root = incoming.root();
        if is(&incoming, root, "name", "responses.response")? {
            let progress = match incoming
                .member(root, "x_ck")?
                .map(|x| incoming.member(x, "source"))
                .transpose()?
            {
                Some(Some(source)) => is(&incoming, source, "event", "response.in_progress")?,
                _ => false,
            };
            let doc = &mut self.document;
            let node = doc.absorb(incoming)?;
            let value = doc.required(node, "value")?;
            super::super::opaque::preserve_carrier_collision(doc, value)?;
            // The snapshot's own id never replaces the exchange id.
            doc.remove(value, "id")?;
            super::super::merge(doc, doc.root(), value)?;
            // Every snapshot carries the native identity, so the owed `response.created` follows
            // its merge rather than a synthesized start.
            self.created(owner, output)?;
            if !progress || self.mode == BodyMode::Buffered {
                return Ok(());
            }
            let doc = &self.document;
            let response = encode::scratch(owner, |e| write_response(doc, e))?;
            return self.emit(
                owner,
                "response.in_progress",
                |e| {
                    e.bytes(b",\"response\":")?;
                    write_lease(&response, e)
                },
                output,
            );
        }
        if is(&incoming, root, "name", "responses.audio")? {
            let node = self.document.absorb(incoming)?;
            let at = self.audio_index(node)?.ok_or(Error::Schema)?;
            let doc = &mut self.document;
            let value = doc.required(node, "value")?;
            return super::super::merge(doc, self.audio[at].audio, value);
        }
        // Only a Responses source's own unknown event reaches the client, as that native event; a
        // buffered body has no place for events, and extension fields of mapped events have none
        // in the events this egress writes (§3.7).
        let native = match incoming.member(incoming.required(root, "x_ck")?, "source")? {
            Some(source) => is(&incoming, source, "protocol", "responses")?,
            None => false,
        };
        if self.mode == BodyMode::Buffered || !native || is(&incoming, root, "name", "responses.fields")? {
            (self.diagnostic)("opaque", 0);
            return Ok(());
        }
        let sequence = self.sequence;
        if matches!(incoming.node(incoming.required(root, "value")?)?.value, Value::Object) {
            self.sequence = sequence.checked_add(1).ok_or(Error::Limit)?;
        }
        output(encode::scratch(owner, |e| {
            super::super::opaque::write(&incoming, sequence, e)
        })?)
    }

    /// Stores the usage object: the native value when the source was Responses, otherwise the
    /// Responses projection of the canonical snapshot.
    pub(super) fn usage(&mut self, node: usize) -> Result<()> {
        let doc = &mut self.document;
        let usage = doc.required(node, "usage")?;
        let value = super::super::usage::project(doc, usage)?;
        let target = doc.set(doc.root(), "usage", Value::Null)?;
        doc.transplant(target, value)
    }

    /// Appends one compact output item. Another protocol's item, such as an Anthropic
    /// compaction block, reaches a Responses client only inside the cooker envelope (D2 plan
    /// §2.4.1, unit D2-6a); until that exists it is refused, never written as a native item.
    pub(super) fn compact(&mut self, node: usize) -> Result<()> {
        let doc = &mut self.document;
        let output_node = doc.required(doc.root(), "output")?;
        let item = doc.required(node, "item")?;
        if is(doc, item, "type", "opaque")? {
            return Err(Error::Unsupported);
        }
        doc.detach(node, item)?;
        doc.append(output_node, item)?;
        Ok(())
    }
}
