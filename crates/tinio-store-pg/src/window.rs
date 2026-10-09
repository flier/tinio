//! The inclusive byte window both planes clip a chunk against.
//!
//! Content chunks are 1 MiB and block chunks are 8 MiB. The slice arithmetic
//! is the same, so a window-edge change lands once.

use std::ops::Range;

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

#[cfg(test)]
mod tests {
    use super::clip_chunk;

    #[test]
    fn a_window_clips_the_first_chunk_and_skips_a_miss() {
        let (range, served) = clip_chunk(4, 0, 4, 1, 2).expect("the first chunk overlaps");
        assert_eq!(range, 1..3);
        assert_eq!(served, 2);
        assert!(clip_chunk(4, 1, 4, 1, 2).is_none());
    }
}
