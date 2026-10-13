//! The inclusive byte window both planes clip a chunk against, and the
//! chunk-number windows a read is fetched in.
//!
//! Content chunks are 1 MiB and block chunks are 8 MiB. The slice arithmetic
//! is the same, so a window-edge change lands once; so is the arithmetic that
//! cuts a read into the bounded statements a plane fetches it with (R4).

use std::ops::Range;

use bytes::Bytes;

/// The slice of a chunk that falls inside the inclusive window `[start, end]`,
/// and how many bytes that slice contributes.
///
/// `chunk_index` is the chunk's sequence and `chunk_bytes` is the plane's
/// fixed chunk size. `None` when the chunk does not overlap the window.
pub(crate) fn clip_chunk(
    chunk_len: usize,
    chunk_index: u64,
    chunk_bytes: u64,
    start: u64,
    end: u64,
) -> Option<(Range<usize>, u64)> {
    let chunk_start = chunk_index * chunk_bytes;
    let chunk_end = chunk_start + chunk_len as u64;
    let from = start.max(chunk_start);
    let to = (end + 1).min(chunk_end);
    if from >= to {
        return None;
    }
    let range = (from - chunk_start) as usize..(to - chunk_start) as usize;
    Some((range, to - from))
}

/// The chunk-number windows the rows `[first, last]` are fetched in, in order,
/// at most `per_window` rows each.
///
/// A plane reads a body one of these per statement, so the bytes one read holds
/// and the connection it borrows are bounded by a window rather than by the
/// body (R4). An empty answer for `first > last` is the caller's empty read.
pub(crate) fn chunk_windows(first: i32, last: i32, per_window: i32) -> Vec<(i32, i32)> {
    let per_window = per_window.max(1);
    let mut windows = Vec::new();
    let mut next = first;
    while next <= last {
        let stop = next.saturating_add(per_window - 1).min(last);
        windows.push((next, stop));
        next = stop.saturating_add(1);
    }
    windows
}

/// One read of a chunk window. `contiguous` requires `chunk_no` to arrive as
/// `first, first+1, …` with no gaps; the block plane only checks that the
/// bytes add up to the window.
pub(crate) struct ChunkWindow {
    next: i32,
    last: i32,
    served: u64,
    expect: u64,
    start: u64,
    end: u64,
    chunk_bytes: u64,
    contiguous: bool,
}

impl ChunkWindow {
    pub(crate) fn new(start: u64, end: u64, chunk_bytes: u64, contiguous: bool) -> Self {
        Self {
            next: (start / chunk_bytes) as i32,
            last: (end / chunk_bytes) as i32,
            served: 0,
            expect: end - start + 1,
            start,
            end,
            chunk_bytes,
            contiguous,
        }
    }

    /// The bytes of this chunk that fall in the window. `Err` is a gap in a
    /// contiguous run, carrying the bytes already served. `Ok(None)` is a chunk
    /// the window does not touch.
    pub(crate) fn push(&mut self, index: i32, chunk: Vec<u8>) -> Result<Option<Bytes>, ChunkFault> {
        if self.contiguous && index != self.next {
            return Err(ChunkFault::Gap {
                served: self.served,
                expect: self.expect,
            });
        }
        if self.contiguous {
            self.next = index + 1;
        }
        let Some((range, n)) = clip_chunk(
            chunk.len(),
            index as u64,
            self.chunk_bytes,
            self.start,
            self.end,
        ) else {
            return Ok(None);
        };
        self.served += n;
        Ok(Some(Bytes::from(chunk).slice(range)))
    }

    /// The run covered `[start, end]` exactly.
    pub(crate) fn finish(&self) -> Result<(), ChunkFault> {
        if self.contiguous && self.next != self.last + 1 || self.served != self.expect {
            return Err(ChunkFault::Short {
                served: self.served,
                expect: self.expect,
            });
        }
        Ok(())
    }
}

/// Why a window stopped short of `[start, end]`.
#[derive(Debug)]
pub(crate) enum ChunkFault {
    /// A contiguous run skipped `chunk_no`.
    Gap { served: u64, expect: u64 },
    /// The rows ended before the window was full, or a block's bytes did not add up.
    Short { served: u64, expect: u64 },
}

impl ChunkFault {
    /// Bytes served, then bytes the window required.
    pub(crate) fn counts(self) -> (u64, u64) {
        match self {
            Self::Gap { served, expect } | Self::Short { served, expect } => (served, expect),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{chunk_windows, clip_chunk};

    #[test]
    fn a_window_clips_the_first_chunk_and_skips_a_miss() {
        let (range, served) = clip_chunk(4, 0, 4, 1, 2).expect("the first chunk overlaps");
        assert_eq!(range, 1..3);
        assert_eq!(served, 2);
        assert!(clip_chunk(4, 1, 4, 1, 2).is_none());
    }

    #[test]
    fn a_contiguous_window_rejects_a_gap_and_a_short_run() {
        let mut window = super::ChunkWindow::new(0, 3, 4, true);
        let first = window.push(0, vec![1, 2, 3, 4]).expect("the first chunk");
        assert_eq!(first.expect("bytes").as_ref(), &[1, 2, 3, 4]);
        assert!(
            window.push(2, vec![0, 0, 0, 0]).is_err(),
            "seq 1 is missing"
        );
        let mut short = super::ChunkWindow::new(0, 7, 4, true);
        short.push(0, vec![1, 2, 3, 4]).expect("the only chunk");
        assert!(short.finish().is_err(), "the window still owes four bytes");
    }

    /// The window plan covers `[first, last]` exactly, in order, and no window
    /// is wider than the bound: the arithmetic a read's resident bytes follow.
    #[test]
    fn the_window_plan_is_bounded_and_covers_the_run() {
        assert_eq!(chunk_windows(0, 9, 4), vec![(0, 3), (4, 7), (8, 9)]);
        assert_eq!(chunk_windows(3, 3, 4), vec![(3, 3)]);
        assert_eq!(
            chunk_windows(0, 0, 4),
            vec![(0, 0)],
            "a one-chunk read is one window"
        );
        assert!(
            chunk_windows(1, 0, 4).is_empty(),
            "an empty run fetches nothing"
        );
        // 100 GiB in 1 MiB chunks, eight chunks a window: every window is
        // bounded and the plan still covers the whole run.
        let plan = chunk_windows(0, 102_399, 8);
        assert_eq!(plan.len(), 12_800, "ceil(102400 / 8) windows");
        assert!(
            plan.iter().all(|(first, last)| last - first < 8),
            "no window exceeds the bound"
        );
        assert_eq!(plan.first().copied(), Some((0, 7)));
        assert_eq!(plan.last().copied(), Some((102_392, 102_399)));
    }
}
