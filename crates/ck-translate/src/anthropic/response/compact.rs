use super::*;

impl State {
    /// Publishes Anthropic's answer to an on-demand compaction: `stop_reason: "compaction"` and
    /// exactly one `compaction` block. The canonical compact event carries the block unchanged
    /// as an opaque Anthropic item (owner ruling Q3), since Anthropic refuses an edited one.
    pub(super) fn compaction(&mut self, owner: &mut Writer, doc: &Document, root: usize) -> Result<()> {
        if !is(doc, root, "stop_reason", "compaction")? {
            return Err(Error::Schema);
        }
        let content = doc.required(root, "content")?;
        if !matches!(doc.node(content)?.value, Value::Array) {
            return Err(Error::Schema);
        }
        let mut blocks = doc.children(content)?;
        let (Some(block), None) = (blocks.next(), blocks.next()) else {
            return Err(Error::Schema);
        };
        if !is(doc, block, "type", "compaction")? {
            return Err(Error::Schema);
        }
        string(doc, doc.required(block, "content")?)?;
        string(doc, doc.required(block, "signature")?)?;
        self.emitter.emit(
            owner,
            Kind::Compact,
            Scope::default(),
            |e| {
                e.bytes(b",\"item\":{\"type\":\"opaque\",\"protocol\":\"anthropic\",\"value\":")?;
                doc.write(block, e)?;
                e.bytes(b"}")
            },
            |_| Ok(()),
        )?;
        self.update_usage(owner, doc, root)?;
        self.finished = true;
        Ok(())
    }
}
