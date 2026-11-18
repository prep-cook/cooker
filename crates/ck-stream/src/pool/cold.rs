use std::ops::Range;

/// Free frames whose memory decay returned, as a stack of frame ranges. Decay appends
/// sorted frames and allocation takes from the top, so a burst's frames stay a few
/// ranges rather than one entry per frame.
#[derive(Default)]
pub(super) struct ColdList {
    runs: Vec<Range<u32>>,
}

impl ColdList {
    pub(super) fn push(&mut self, frame: u32) {
        self.push_run(frame..frame + 1);
    }

    /// Push `run`, merging it with the top range when they touch.
    pub(super) fn push_run(&mut self, run: Range<u32>) {
        if run.start >= run.end {
            return;
        }
        match self.runs.last_mut() {
            Some(top) if top.end == run.start => top.end = run.end,
            Some(top) if run.end == top.start => top.start = run.start,
            _ => self.runs.push(run),
        }
    }

    /// Take up to `most` consecutive frames from the top, ending at the top frame.
    pub(super) fn take(&mut self, most: u32) -> Option<Range<u32>> {
        let top = self.runs.last_mut()?;
        let start = top.end - most.min(top.end - top.start);
        let taken = start..top.end;
        top.end = start;
        if top.start == top.end {
            self.runs.pop();
        }
        Some(taken)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.runs.iter().map(|run| run.len()).sum()
    }
}
