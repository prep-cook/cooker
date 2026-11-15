use super::*;

impl State {
    pub(super) fn event(&mut self, owner: &mut Writer, source: &Source, event: &sse::Event) -> Result<()> {
        if self.terminal {
            return Err(Error::Closed);
        }
        if !event.has_data(source)? {
            if !event.fields().is_empty() {
                self.ensure_start(owner, None)?;
                self.emitter.emit(
                    owner,
                    Kind::Heartbeat,
                    Scope::default(),
                    |_| Ok(()),
                    |e| sse_provenance("chat", source, event, e),
                )?;
            }
            return Ok(());
        }
        let name = event.name(source)?;
        let known = name.is_none() || name.is_some_and(|span| source.equals(span, b"message").unwrap_or(false));
        let error_event = name.is_some_and(|span| source.equals(span, b"error").unwrap_or(false));
        let lease = encode::scratch(owner, |e| event.write_data(source, |b| e.bytes(b)))?;
        let mut data = Source::new(owner)?;
        data.push_scratch(lease)?;
        if known && data.equals(data.whole(), b"[DONE]")? {
            return self.done(owner, Some((source, event)));
        }
        if !known && !error_event {
            self.ensure_start(owner, None)?;
            let length = data.len();
            match Document::parse(owner, data, length) {
                Ok(doc) => {
                    self.emitter.emit(
                        owner,
                        Kind::Opaque,
                        Scope::default(),
                        |e| {
                            e.bytes(b",\"format\":\"json\",\"value\":")?;
                            doc.write(doc.root(), e)
                        },
                        |e| sse_provenance("chat", source, event, e),
                    )?;
                }
                Err(Error::Json | Error::Truncated) => {
                    self.emitter.emit(
                        owner,
                        Kind::Opaque,
                        Scope::default(),
                        |e| {
                            e.bytes(b",\"format\":\"text\",\"value\":")?;
                            e.quoted_json(|e| event.write_data(source, |b| e.bytes(b)))
                        },
                        |e| sse_provenance("chat", source, event, e),
                    )?;
                }
                Err(error) => return Err(error),
            }
            return Ok(());
        }
        let length = data.len();
        let mut doc = Document::parse(owner, data, length)?;
        if error_event || doc.member(doc.root(), "error")?.is_some() {
            return Err(Error::Schema);
        }
        self.document(owner, &mut doc, false, Some((source, event)))
    }
}
