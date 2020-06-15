mod abi;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::{hash_map, HashMap, VecDeque};
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicPtr, Ordering},
    Arc,
};

pub enum Error {
    MaxEventsTooLarge,
    LowKernelRes,
    NotSupported,
    OtherError,
}

// NOTE: I assume it aio_context_t is thread-safe, no?
struct AIOContext(abi::IOContextPtr);
unsafe impl Sync for AIOContext {}
unsafe impl Send for AIOContext {}

/// Keep the necessary data for an AIO operation. Memory-safe when moved.
struct AIO {
    // hold the buffer used by iocb
    data: Option<Box<[u8]>>,
    iocb: AtomicPtr<abi::IOCb>,
    id: u64
}

impl AIO {
    fn new(
        id: u64,
        fd: RawFd,
        off: u64,
        data: Box<[u8]>,
        priority: u16,
        flags: u32,
        opcode: abi::IOCmd,
    ) -> Self {
        let mut iocb = Box::new(abi::IOCb::default());
        iocb.aio_fildes = fd as u32;
        iocb.aio_lio_opcode = opcode as u16;
        iocb.aio_reqprio = priority;
        iocb.aio_buf = data.as_ptr() as u64;
        iocb.aio_nbytes = data.len() as u64;
        iocb.aio_offset = off;
        iocb.aio_flags = flags;
        iocb.aio_data = id;
        let iocb = AtomicPtr::new(Box::into_raw(iocb));
        let data = Some(data);
        AIO { iocb, id, data }
    }
}

impl Drop for AIO {
    fn drop(&mut self) {
        unsafe {
            drop(Box::from_raw(self.iocb.load(Ordering::Acquire)));
        }
    }
}

struct AIOFuture<'a> {
    notifier: &'a AIONotifier,
    aio_id: u64
}

impl<'a> std::future::Future for AIOFuture<'a> {
    type Output = AIOResult;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Self::Output> {
        if let Some(ret) = self.notifier.poll(self.aio_id, cx.waker()) {
            std::task::Poll::Ready(ret)
        } else {
            std::task::Poll::Pending
        }
    }
}

impl std::ops::Deref for AIOContext {
    type Target = abi::IOContextPtr;
    fn deref(&self) -> &abi::IOContextPtr {
        &self.0
    }
}

const LIBAIO_EAGAIN: libc::c_int = -libc::EAGAIN;
const LIBAIO_ENOMEM: libc::c_int = -libc::ENOMEM;
const LIBAIO_ENOSYS: libc::c_int = -libc::ENOSYS;

impl AIOContext {
    fn new(maxevents: usize) -> Result<Self, Error> {
        let mut ctx = std::ptr::null_mut();
        unsafe {
            match abi::io_setup(maxevents as libc::c_int, &mut ctx) {
                0 => Ok(()),
                LIBAIO_EAGAIN => Err(Error::MaxEventsTooLarge),
                LIBAIO_ENOMEM => Err(Error::LowKernelRes),
                LIBAIO_ENOSYS => Err(Error::NotSupported),
                _ => Err(Error::OtherError),
            }
            .and(Ok(AIOContext(ctx)))
        }
    }
}

/// The result of an AIO operation: the number of bytes written on success,
/// and the errno on failure.
pub type AIOResult = Result<(usize, Box<[u8]>), i32>;

/// Schedules the submission of AIO operations.
#[async_trait(?Send)]
pub trait AIOScheduler {
    async fn read(&mut self, fd: RawFd, offset: u64, length: usize, priority: Option<u16>) -> AIOResult;
    async fn write(&mut self, fd: RawFd, offset: u64, data: Box<[u8]>, priority: Option<u16>) -> AIOResult;
}

enum AIOState {
    FutureInit(AIO),
    FuturePending(AIO, std::task::Waker),
    FutureDone(AIOResult),
    FutureDropped
}

struct AIONotifierShared {
    waiting: Mutex<HashMap<u64, AIOState>>,
    io_ctx: AIOContext,
    stopped: AtomicBool,
}

/// Listens for finished AIO operations and wake up the futures.
pub struct AIONotifier {
    shared: Arc<AIONotifierShared>,
    listener: Option<std::thread::JoinHandle<()>>,
}

impl AIONotifier {
    pub fn new(
        maxevents: usize,
        min_nops: Option<u16>,
        max_nops: Option<u16>,
        timeout: Option<u32>,
    ) -> Result<Self, Error> {
        let min_nops = min_nops.unwrap_or(128);
        let max_nops = max_nops.unwrap_or(4096);

        let shared = Arc::new(AIONotifierShared {
            io_ctx: AIOContext::new(maxevents)?,
            waiting: Mutex::new(HashMap::new()),
            stopped: AtomicBool::new(false),
        });

        let s = shared.clone();
        let listener = Some(std::thread::spawn(move || {
            while !s.stopped.load(Ordering::Acquire) {
                let mut timespec = timeout.and_then(|nsec: u32| {
                    Some(libc::timespec {
                        tv_sec: 0,
                        tv_nsec: nsec as i64,
                    })
                });
                let mut events = vec![abi::IOEvent::default(); max_nops as usize];
                let ret = unsafe {
                    abi::io_getevents(
                        *s.io_ctx,
                        min_nops as i64,
                        max_nops as i64,
                        events.as_mut_ptr(),
                        timespec
                            .as_mut()
                            .and_then(|ts: &mut libc::timespec| Some(ts as *mut libc::timespec))
                            .unwrap_or(std::ptr::null_mut()),
                    )
                };
                if ret == 0 {
                    continue;
                }
                assert!(ret > 0);
                let mut w = s.waiting.lock();
                for ev in events[..ret as usize].iter() {
                    let id = ev.data as u64;
                    match w.entry(id) {
                        hash_map::Entry::Occupied(e) => {
                            let v = e.remove();
                            match v {
                                AIOState::FutureInit(mut aio) => {
                                    let v = AIOState::FutureDone(
                                        if ev.res >= 0 { Ok((ev.res as usize, aio.data.take().unwrap()))
                                        } else { Err(-ev.res as i32) });
                                    w.insert(id, v);
                                },
                                AIOState::FuturePending(mut aio, waker) => {
                                    let v = AIOState::FutureDone(
                                        if ev.res >= 0 { Ok((ev.res as usize, aio.data.take().unwrap()))
                                        } else { Err(-ev.res as i32) });
                                    w.insert(id, v);
                                    waker.wake();
                                },
                                s => { w.insert(id, s); }
                            }
                        },
                        _ => unreachable!()
                    }
                }
            }
        }));
        Ok(AIONotifier { shared, listener })
    }

    fn register_notify(&self, id: u64, state: AIOState) {
        let mut waiting = self.shared.waiting.lock();
        assert!(waiting.insert(id, state).is_none());
    }

    fn remove_notify(&self, id: u64) {
        let mut waiting = self.shared.waiting.lock();
        waiting.remove(&id);
    }

    fn poll(&self, id: u64, waker: &std::task::Waker) -> Option<AIOResult> {
        let mut waiting = self.shared.waiting.lock();
        match waiting.entry(id) {
            hash_map::Entry::Occupied(e) => {
                let v = e.remove();
                match v {
                    AIOState::FutureInit(aio) => {
                        waiting.insert(id, AIOState::FuturePending(aio, waker.clone()));
                        None
                    },
                    AIOState::FutureDone(res) => {
                        Some(res)
                    },
                    s => {
                        waiting.insert(id, s);
                        None
                    }
                }
            },
            _ => unreachable!()
        }
    }
}

impl Drop for AIONotifier {
    fn drop(&mut self) {
        self.shared.stopped.store(true, Ordering::Release);
        self.listener.take().unwrap().join().unwrap();
    }
}

pub struct AIOBatchScheduler<'a> {
    queued: Vec<*mut abi::IOCb>,
    max_nbatched: usize,
    notifier: &'a AIONotifier,
    last_id: u64,
}

impl<'a> AIOBatchScheduler<'a> {
    pub fn new(notifier: &'a AIONotifier, max_nbatched: Option<usize>) -> Self {
        let max_nbatched = max_nbatched.unwrap_or(1024);
        AIOBatchScheduler {
            queued: Vec::new(),
            max_nbatched,
            notifier,
            last_id: 0
        }
    }

    fn next_id(&mut self) -> u64 {
        let id = self.last_id;
        self.last_id.wrapping_add(1);
        self.last_id
    }

    fn schedule(&mut self, aio: AIO) -> AIOFuture {
        let fut = AIOFuture {
            notifier: self.notifier,
            aio_id: aio.id
        };
        let iocb = aio.iocb.load(Ordering::Acquire);
        self.notifier.register_notify(aio.id, AIOState::FutureInit(aio));
        self.queued.push(iocb);
        fut
    }
}

#[async_trait(?Send)]
impl<'a> AIOScheduler for AIOBatchScheduler<'a> {
    async fn read(&mut self, fd: RawFd, offset: u64, length: usize, priority: Option<u16>) -> AIOResult {
        let priority = priority.unwrap_or(0);
        let mut data = Vec::new();
        data.resize(length, 0);
        let data = data.into_boxed_slice();
        let aio = AIO::new(self.next_id(), fd, offset, data, priority, 0, abi::IOCmd::PWrite);
        self.schedule(aio).await
    }

    async fn write(&mut self, fd: RawFd, offset: u64, data: Box<[u8]>, priority: Option<u16>) -> AIOResult {
        let priority = priority.unwrap_or(0);
        let aio = AIO::new(self.next_id(), fd, offset, data, priority, 0, abi::IOCmd::PWrite);
        self.schedule(aio).await
    }
}
