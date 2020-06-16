use libaiofut::{AIOBatchScheduler, AIONotifier, AIOScheduler};
use std::os::unix::io::AsRawFd;
use futures::executor::LocalPool;
use futures::task::{LocalSpawn, LocalSpawnExt};
use futures::future::FutureExt;

#[test]
fn simple1() {
    let notifier = AIONotifier::new(10, None, None, None).unwrap();
    let mut scheduler = AIOBatchScheduler::new(&notifier, None);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open("test")
        .unwrap();
    let fd = file.as_raw_fd();
    let ws = vec![(0, "hello"), (5, "world"), (2, "xxxx")];
    let ws = ws
        .into_iter()
        .map(|(off, s)| scheduler.write(fd, off, s.as_bytes().into(), None))
        .collect::<Vec<_>>();
    scheduler.submit();
    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    for w in ws.into_iter() {
        let h = spawner.spawn_local_with_handle(w).unwrap().map(|r| {
            println!("wrote {} bytes", r.unwrap().0);
        });
        spawner.spawn_local(h).unwrap();
    }
    pool.run();
}
