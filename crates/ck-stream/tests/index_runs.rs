use ck_stream::{Config, Error, FRAME_BYTES, Lease, ManualClock, Read, Reader, Record, Store, Terminal, Writer};
use std::{sync::Arc, task::Poll};

fn store(frames: usize, metadata: usize) -> (tempfile::TempDir, Store, Arc<ManualClock>) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path().join("pool"));
    config.pool_bytes = Some(frames * FRAME_BYTES);
    config.max_event_bytes = 2 * FRAME_BYTES;
    config.max_metadata_bytes = metadata;
    config.header_slots = 4;
    config.workers = 1;
    let clock = Arc::new(ManualClock::default());
    let store = Store::with_clock(config, clock.clone()).unwrap();
    (dir, store, clock)
}

fn append(writer: &mut Writer, bytes: &[u8], kind: u32, correlation: u32) -> Record {
    let mut reservation = writer.begin_record(bytes.len(), kind, correlation).unwrap();
    reservation.write(bytes).unwrap();
    reservation.commit().unwrap()
}

fn lease(reader: &mut Reader) -> Lease {
    match reader.try_next() {
        Poll::Ready(Some(Read::Record(lease))) => lease,
        _ => panic!("expected record"),
    }
}

fn assert_record(reader: &mut Reader, expected: Record, byte: u8) {
    let lease = lease(reader);
    assert_eq!(lease.record(), expected);
    let mut length = 0;
    lease
        .visit(|bytes| {
            assert!(bytes.iter().all(|value| *value == byte));
            length += bytes.len();
        })
        .unwrap();
    assert_eq!(length as u64, expected.len());
}

fn terminal(reader: &mut Reader, expected: Terminal) {
    assert!(matches!(reader.try_next(), Poll::Ready(Some(Read::Terminal(actual))) if actual == expected));
    assert!(matches!(reader.try_next(), Poll::Ready(None)));
}

#[test]
fn mixed_runs_preserve_each_boundary_kind_correlation_and_resume_sequence() {
    let (_dir, store, _) = store(8, 4096);
    let mut writer = store.create().unwrap();
    let mut expected = Vec::new();
    for (len, count, kind, correlation) in [
        (7, 5, 1, 2),
        (13, 3, 1, 2),
        (13, 4, 2, 2),
        (13, 6, 2, 3),
        (7, 2, 1, 2),
        (FRAME_BYTES + 1, 2, 4, 5),
    ] {
        for _ in 0..count {
            let byte = expected.len() as u8;
            let record = append(&mut writer, &vec![byte; len], kind, correlation);
            assert_eq!(record.sequence, expected.len() as u64);
            expected.push((record, byte));
        }
    }
    writer.finish(Terminal::Done);
    for start in 0..=expected.len() {
        let mut reader = store.reader(writer.id(), start as u64).unwrap();
        for &(record, byte) in &expected[start..] {
            assert_record(&mut reader, record, byte);
        }
        terminal(&mut reader, Terminal::Done);
    }
    assert!(matches!(
        store.reader(writer.id(), expected.len() as u64 + 1),
        Err(Error::InvalidPosition)
    ));
}

#[test]
fn appends_continue_runs_across_pages_frames_and_other_runs() {
    let (_dir, store, _) = store(8, 4096);
    let mut writer = store.create().unwrap();
    let mut expected = Vec::new();
    // Equal appends fill the first frame exactly and, after one shorter reserved record,
    // straddle every later frame boundary; each run stays exact.
    let per_frame = FRAME_BYTES / 256;
    for step in 0..per_frame * 15 / 2 {
        let byte = expected.len() as u8;
        let record = if step == per_frame + 4 {
            append(&mut writer, &[byte; 255], 0, 0)
        } else {
            writer.append(&[byte; 256]).unwrap()
        };
        assert_eq!(record.sequence, expected.len() as u64);
        expected.push((record, byte));
    }
    let end = expected[per_frame - 1].0.end;
    assert_eq!((end.frame, end.offset), (1, 0));
    assert!(
        expected
            .iter()
            .any(|(record, _)| record.start.frame + 1 == record.end.frame && record.end.offset > 0)
    );
    writer.finish(Terminal::Done);
    let mut reader = store.reader(writer.id(), 0).unwrap();
    for &(record, byte) in &expected {
        assert_record(&mut reader, record, byte);
    }
    terminal(&mut reader, Terminal::Done);
}

#[test]
fn repeated_records_fit_a_small_unchanged_metadata_budget() {
    // 100 records of up to 257 bytes fit the eight frames; separate index entries would not
    // fit the 512-byte budget.
    for len in [1, 7, 64, 257] {
        let (_dir, store, _) = store(8, 512);
        let mut writer = store.create().unwrap();
        let mut reader = store.reader(writer.id(), 0).unwrap();
        let payload = vec![42; len];
        for sequence in 0..100 {
            let record = append(&mut writer, &payload, 17, 23);
            assert_eq!(record.sequence, sequence);
        }
        writer.finish(Terminal::Done);
        for sequence in 0..100 {
            let read = lease(&mut reader);
            assert_eq!(read.record().sequence, sequence);
            assert_eq!(read.record().len(), len as u64);
            let mut length = 0;
            read.visit(|bytes| {
                assert!(bytes.iter().all(|byte| *byte == 42));
                length += bytes.len();
            })
            .unwrap();
            assert_eq!(length, len);
        }
        terminal(&mut reader, Terminal::Done);
    }
}

#[test]
fn partial_release_cuts_a_run_without_reusing_sequences_or_leased_frames() {
    let (_dir, store, _) = store(8, 2048);
    let mut writer = store.create().unwrap();
    let records: Vec<_> = (0..12)
        .map(|byte| append(&mut writer, &vec![byte; FRAME_BYTES / 4], 1, 2))
        .collect();
    let mut lagging = store.reader(writer.id(), 0).unwrap();
    let pinned = lease(&mut lagging);
    let borrowed = pinned.chunk(pinned.record().start).unwrap();
    let mut middle = store.reader(writer.id(), 7).unwrap();
    let mut cut = records[7].start;
    cut.offset += 1;
    writer.release_before(cut).unwrap();
    assert!(matches!(store.reader(writer.id(), 6), Err(Error::Gone)));
    terminal(&mut lagging, Terminal::Gone);
    assert_eq!(store.used_frames(), 3);
    assert!(borrowed.iter().all(|byte| *byte == 0));
    drop(pinned);
    assert_eq!(store.used_frames(), 2);
    writer.release_before(cut).unwrap();
    for (sequence, &record) in records.iter().enumerate().skip(7) {
        assert_record(&mut middle, record, sequence as u8);
    }
    let next = append(&mut writer, &vec![12; FRAME_BYTES / 4], 1, 2);
    assert_eq!(next.sequence, 12);
    assert_record(&mut middle, next, 12);
    writer.release_before(next.end).unwrap();
    assert!(matches!(store.reader(writer.id(), 12), Err(Error::Gone)));
    let next = append(&mut writer, &vec![13; FRAME_BYTES / 4], 1, 2);
    assert_eq!(next.sequence, 13);
    assert_record(&mut middle, next, 13);
}

#[test]
fn abandoned_reservations_leave_runs_exact() {
    let (_dir, store, _) = store(8, 2048);
    let mut writer = store.create().unwrap();
    let mut expected: Vec<_> = (b'a'..=b'd')
        .map(|byte| (append(&mut writer, &[byte; 3], 1, 2), byte))
        .collect();
    let mut reader = store.reader(writer.id(), 4).unwrap();
    let committed = writer.committed();
    {
        let mut abandoned = writer.begin_record(3, 1, 2).unwrap();
        abandoned.write(b"xx").unwrap();
        assert!(matches!(reader.try_next(), Poll::Pending));
        assert!(matches!(abandoned.commit(), Err(Error::IncompleteRecord)));
    }
    assert_eq!(writer.committed(), committed);
    let next = append(&mut writer, b"eee", 1, 2);
    assert_eq!((next.sequence, next.start), (4, committed));
    assert_record(&mut reader, next, b'e');
    expected.push((next, b'e'));
    writer.finish(Terminal::Done);
    let mut inside = expected[2].0.start;
    inside.offset += 1;
    writer.release_before(inside).unwrap();
    assert!(matches!(store.reader(writer.id(), 1), Err(Error::Gone)));
    let mut replay = store.reader(writer.id(), 2).unwrap();
    for &(record, byte) in &expected[2..] {
        assert_record(&mut replay, record, byte);
    }
    terminal(&mut replay, Terminal::Done);
}

#[test]
fn variable_records_exhaust_metadata_visibly_and_keep_the_complete_prefix() {
    let (_dir, store, _) = store(8, 512);
    let mut writer = store.create().unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let mut expected = Vec::new();
    for sequence in 0..128 {
        match writer.begin_record(1 + sequence % 3, sequence as u32 % 2, sequence as u32) {
            Ok(mut reservation) => {
                reservation.write(&[9; 3][..1 + sequence % 3]).unwrap();
                expected.push(reservation.commit().unwrap());
            }
            Err(Error::StorageExhausted) => break,
            Err(error) => panic!("unexpected error: {error}"),
        }
    }
    assert!(!expected.is_empty());
    assert!(expected.len() < 128);
    assert_eq!(store.used_frames(), 1);
    for record in expected {
        assert_record(&mut reader, record, 9);
    }
    terminal(&mut reader, Terminal::StorageExhausted);
}

#[test]
fn invalidation_hides_run_backlog_but_keeps_borrowed_bytes_pinned() {
    let (_dir, store, _) = store(8, 512);
    let mut writer = store.create().unwrap();
    for _ in 0..100 {
        writer.append(b"retained").unwrap();
    }
    let mut reader = store.reader(writer.id(), 50).unwrap();
    let pinned = lease(&mut reader);
    let borrowed = pinned.chunk(pinned.record().start).unwrap();
    writer.finish(Terminal::Gone);
    terminal(&mut reader, Terminal::Gone);
    assert_eq!(borrowed, b"retained");
    assert!(matches!(pinned.chunk(pinned.record().start), Err(Error::Gone)));
    assert!(matches!(store.reader(writer.id(), 51), Err(Error::Gone)));
    assert_eq!(store.used_frames(), 1);
    drop(pinned);
    assert_eq!(store.used_frames(), 0);
}
