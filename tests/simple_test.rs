use libaiofut::{AIOBatchScheduler, AIONotifier, AIOScheduler};
use std::os::unix::io::AsRawFd;

#[test]
fn simple1() {
    let notifier = AIONotifier::new(10, None, None, None).unwrap();
    let mut scheduler = AIOBatchScheduler::new(&notifier, None);
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open("test")
        .unwrap();
    let fd = file.as_raw_fd();
    let ws = vec![
        (0, "hello"),
        (5, "world"),
        (2, "xxxx")
    ];
    let ws = ws.into_iter().map(|(off, s)| scheduler.write(fd, off, s.as_bytes().into(), None)).collect::<Vec<_>>();
    scheduler.submit();
    for w in ws.into_iter() {
        println!("{}", futures::executor::block_on(w).unwrap().0);
    }
}
