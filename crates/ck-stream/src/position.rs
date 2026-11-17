use crate::FRAME_BYTES;

/// A logical frame coordinate, scoped to one stream generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Position {
    pub generation: u64,
    pub frame: u64,
    pub offset: u32,
}

impl Position {
    pub(crate) fn at(generation: u64, byte: u64) -> Self {
        Self {
            generation,
            frame: byte / FRAME_BYTES as u64,
            offset: (byte % FRAME_BYTES as u64) as u32,
        }
    }

    pub(crate) fn byte(self, generation: u64) -> Result<u64, crate::Error> {
        if self.generation != generation || self.offset as usize >= FRAME_BYTES {
            return Err(crate::Error::InvalidPosition);
        }
        self.frame
            .checked_mul(FRAME_BYTES as u64)
            .and_then(|n| n.checked_add(u64::from(self.offset)))
            .ok_or(crate::Error::InvalidPosition)
    }
}

/// Immutable complete-record boundaries; padding is outside this range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Record {
    pub sequence: u64,
    pub start: Position,
    pub end: Position,
    pub kind: u32,
    pub correlation: u32,
}

impl Record {
    pub fn len(self) -> u64 {
        (self.end.frame - self.start.frame) * FRAME_BYTES as u64 + u64::from(self.end.offset)
            - u64::from(self.start.offset)
    }

    pub fn is_empty(self) -> bool {
        self.start == self.end
    }
}
