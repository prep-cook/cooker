use std::ops::Range;

use crate::{ByteSpan, Error, Lease, Position};

/// Codec-supplied pointer and value locations in a `request.field` record.
///
/// The pointer span contains JSON string contents without quotes, still escaped
/// as stored. The value span contains its complete encoded JSON value. This view
/// checks storage bounds only, not JSON, pointer syntax, capability or request.end.
#[derive(Clone, Copy)]
pub struct RequestFieldView<'a> {
    pointer: ByteSpan<'a>,
    value: ByteSpan<'a>,
}

impl<'a> RequestFieldView<'a> {
    /// Check both spans within one published lease. Codecs supply parsed locations.
    pub fn new(lease: &'a Lease, pointer: Range<Position>, value: Range<Position>) -> Result<Self, Error> {
        Ok(Self {
            pointer: ByteSpan::new(lease, pointer)?,
            value: ByteSpan::new(lease, value)?,
        })
    }

    pub fn pointer(self) -> ByteSpan<'a> {
        self.pointer
    }

    pub fn value(self) -> ByteSpan<'a> {
        self.value
    }
}
