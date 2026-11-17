use std::ops::Range;

use crate::{Error, Lease, Position};

/// An end-exclusive byte span in one published record, borrowing its lease.
///
/// Spans retain no payload or owned lease. Bytes remain encoded as stored; codecs
/// supply parsed locations and validate syntax. Each new read checks invalidation.
#[derive(Clone, Copy)]
pub struct ByteSpan<'a> {
    lease: &'a Lease,
    start: u64,
    end: u64,
}

impl<'a> ByteSpan<'a> {
    /// Check canonical coordinates, generation, order and record containment.
    /// Empty spans are allowed at any position through the record's exclusive end.
    /// Returns `InvalidPosition` for invalid bounds and `Gone` after invalidation.
    pub fn new(lease: &'a Lease, range: Range<Position>) -> Result<Self, Error> {
        let record = lease.record();
        let generation = record.start.generation;
        let start = range.start.byte(generation)?;
        let end = range.end.byte(generation)?;
        if start > end || start < record.start.byte(generation)? || end > record.end.byte(generation)? {
            return Err(Error::InvalidPosition);
        }
        let span = Self { lease, start, end };
        span.check_live()?;
        Ok(span)
    }

    pub fn start(self) -> Position {
        Position::at(self.lease.record().start.generation, self.start)
    }

    pub fn end(self) -> Position {
        Position::at(self.lease.record().start.generation, self.end)
    }

    pub fn len(self) -> u64 {
        self.end - self.start
    }

    pub fn is_empty(self) -> bool {
        self.start == self.end
    }

    /// Borrow from `position` to the earlier of the frame boundary and span end.
    /// The position must be inside this span. For an empty span, only its start
    /// is accepted and the result is empty, after checking invalidation.
    /// Existing borrows remain immutable if the stream is invalidated later.
    pub fn chunk(self, position: Position) -> Result<&'a [u8], Error> {
        let byte = position.byte(self.lease.record().start.generation)?;
        if self.is_empty() && byte == self.start {
            self.check_live()?;
            return Ok(&[]);
        }
        if byte < self.start || byte >= self.end {
            return Err(Error::InvalidPosition);
        }
        let chunk = self.lease.chunk(position)?;
        let len = (self.end - byte).min(chunk.len() as u64) as usize;
        Ok(&chunk[..len])
    }

    /// Visit only this span, in frame order. An empty live span calls no visitor.
    /// Invalidation stops subsequent reads with `Gone`; earlier visits may have run.
    pub fn visit(self, mut visitor: impl FnMut(&[u8])) -> Result<(), Error> {
        if self.is_empty() {
            return self.check_live();
        }
        let mut position = self.start();
        let mut byte = self.start;
        while byte < self.end {
            let chunk = self.chunk(position)?;
            visitor(chunk);
            byte += chunk.len() as u64;
            position = Position::at(position.generation, byte);
        }
        Ok(())
    }

    fn check_live(self) -> Result<(), Error> {
        // Lease::chunk checks liveness even for an empty view. At the exclusive
        // end, probe the nonempty record's start without exposing any probe bytes.
        let record = self.lease.record();
        let position = if self.start == record.end.byte(record.start.generation)? {
            record.start
        } else {
            self.start()
        };
        self.lease.chunk(position).map(|_| ())
    }
}

/// Codec-supplied event-name and data locations, excluding SSE delimiters.
/// `None` names the default event; this view does not parse or validate SSE/JSON.
#[derive(Clone, Copy)]
pub struct EventView<'a> {
    name: Option<ByteSpan<'a>>,
    data: ByteSpan<'a>,
}

impl<'a> EventView<'a> {
    /// Check each supplied span within the same lease, without validating syntax.
    pub fn new(lease: &'a Lease, name: Option<Range<Position>>, data: Range<Position>) -> Result<Self, Error> {
        Ok(Self {
            name: name.map(|range| ByteSpan::new(lease, range)).transpose()?,
            data: ByteSpan::new(lease, data)?,
        })
    }

    pub fn name(self) -> Option<ByteSpan<'a>> {
        self.name
    }

    pub fn data(self) -> ByteSpan<'a> {
        self.data
    }
}
