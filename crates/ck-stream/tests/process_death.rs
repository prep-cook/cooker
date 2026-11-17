use ck_stream::{Config, FRAME_BYTES, Read, Store};
use std::{
    io::{BufRead, BufReader, Read as _, Write},
    os::unix::process::ExitStatusExt,
    process::{Child, Command, Stdio},
    task::Poll,
};

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn killed_producer_leaves_no_persistent_pool_file() {
    const DIRECTORY: &str = "CK_STREAM_DEATH_TEST_DIRECTORY";
    const READY: &str = "CK_STREAM_DEATH_TEST_READY";
    if let Some(directory) = std::env::var_os(DIRECTORY) {
        let mut config = Config::new(directory.into());
        config.pool_bytes = Some(4 * FRAME_BYTES);
        config.max_event_bytes = FRAME_BYTES;
        config.header_slots = 8;
        config.workers = 1;
        let store = Store::open(config).unwrap();
        let mut writer = store.create().unwrap();
        writer.append(b"retained private response").unwrap();
        let mut reader = store.reader(writer.id(), 0).unwrap();
        let Poll::Ready(Some(Read::Record(lease))) = reader.try_next() else {
            panic!("missing published record");
        };
        assert_eq!(lease.chunk(lease.record().start).unwrap(), b"retained private response");
        println!("{READY}");
        std::io::stdout().flush().unwrap();
        let _ = std::io::stdin().read(&mut [0]);
        // The parent kills this process while writer, reader, lease and mapping remain live.
        std::process::exit(1);
    }

    let directory = tempfile::tempdir().unwrap();
    let pool = directory.path().join("pool");
    let mut child = ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "killed_producer_leaves_no_persistent_pool_file",
                "--nocapture",
            ])
            .env(DIRECTORY, &pool)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let output = BufReader::new(child.0.stdout.take().unwrap());
    assert!(
        output.lines().any(|line| line.unwrap().contains(READY)),
        "child did not publish"
    );
    assert_eq!(std::fs::read_dir(&pool).unwrap().count(), 0);
    child.0.kill().unwrap();
    assert_eq!(child.0.wait().unwrap().signal(), Some(9));
    assert_eq!(
        std::fs::read_dir(&pool).unwrap().count(),
        0,
        "pool file survived SIGKILL"
    );
}
