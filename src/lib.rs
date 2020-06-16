mod abi;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::{hash_map, HashMap};
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicPtr, Ordering},
    Arc,
};

const LIBAIO_EAGAIN: libc::c_int = -libc::EAGAIN;
const LIBAIO_ENOMEM: libc::c_int = -libc::ENOMEM;
const LIBAIO_ENOSYS: libc::c_int = -libc::ENOSYS;

#[derive(Debug)]
pub enum Error {
    MaxEventsTooLarge,
    LowKernelRes,
    NotSupported,
    OtherError,
}

// NOTE: I assume it io_context_t is thread-safe, no?
struct AIOContext(abi::IOContextPtr);
unsafe impl Sync for AIOContext {}
unsafe impl Send for AIOContext {}

impl std::ops::Deref for AIOContext {
    type Target = abi::IOContextPtr;
    fn deref(&self) -> &abi::IOContextPtr {
        &self.0
    }
}

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

/// Represent the necessary data for an AIO operation. Memory-safe when moved.
struct AIO {
    // hold the buffer used by iocb
    data: Option<Box<[u8]>>,
    iocb: AtomicPtr<abi::IOCb>,
    id: u64,
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

/// The result of an AIO operation: the number of bytes written on success,
/// and the errno on failure.
pub type AIOResult = Result<(usize, Box<[u8]>), i32>;

/// Represents a scheduled asynchronous I/O operation.
pub struct AIOFuture {
    notifier: Arc<AIONotifierShared>,
    aio_id: u64,
}

impl std::future::Future for AIOFuture {
    type Output = AIOResult;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Self::Output> {
        if let Some(ret) = self.notifier.poll(self.aio_id, cx.waker()) {
            std::task::Poll::Ready(ret)
        } else {
            std::task::Poll::Pending
        }
    }
}

impl Drop for AIOFuture {
    fn drop(&mut self) {
        self.notifier.dropped(self.aio_id)
    }
}

/// The interface for an AIO scheduler.
pub trait AIOScheduler {
    fn read(
        &mut self,
        fd: RawFd,
        offset: u64,
        length: usize,
        priority: Option<u16>,
    ) -> AIOFuture;
    fn write(
        &mut self,
        fd: RawFd,
        offset: u64,
        data: Box<[u8]>,
        priority: Option<u16>,
    ) -> AIOFuture;
}

enum AIOState {
    FutureInit(AIO, bool),
    FuturePending(AIO, std::task::Waker, bool),
    FutureDone(AIOResult),
}

struct AIONotifierShared {
    waiting: Mutex<HashMap<u64, AIOState>>,
    io_ctx: AIOContext,
    stopped: AtomicBool,
}

impl AIONotifierShared {
    fn register_notify(&self, id: u64, state: AIOState) {
        let mut waiting = self.waiting.lock();
        assert!(waiting.insert(id, state).is_none());
    }

    fn dropped(&self, id: u64) {
        let mut waiting = self.waiting.lock();
        match waiting.entry(id) {
            hash_map::Entry::Occupied(mut e) => match e.get_mut() {
                AIOState::FutureInit(_, dropped) => *dropped = true,
                AIOState::FuturePending(_, _, dropped) => *dropped = true,
                AIOState::FutureDone(_) => {
                    e.remove();
                }
            },
            _ => (),
        }
    }

    fn poll(&self, id: u64, waker: &std::task::Waker) -> Option<AIOResult> {
        let mut waiting = self.waiting.lock();
        match waiting.entry(id) {
            hash_map::Entry::Occupied(e) => {
                let v = e.remove();
                match v {
                    AIOState::FutureInit(aio, _) => {
                        waiting.insert(id, AIOState::FuturePending(aio, waker.clone(), false));
                        None
                    }
                    AIOState::FutureDone(res) => Some(res),
                    s => {
                        waiting.insert(id, s);
                        None
                    }
                }
            }
            _ => unreachable!(),
        }
    }
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
            let mut timespec = timeout
                .and_then(|nsec: u32| {
                    Some(libc::timespec {
                        tv_sec: 0,
                        tv_nsec: nsec as i64,
                    })
                })
                .unwrap_or(libc::timespec {
                    tv_sec: 1,
                    tv_nsec: 0,
                });
            while !s.stopped.load(Ordering::Acquire) {
                let mut events = vec![abi::IOEvent::default(); max_nops as usize];
                let ret = unsafe {
                    abi::io_getevents(
                        *s.io_ctx,
                        min_nops as i64,
                        max_nops as i64,
                        events.as_mut_ptr(),
                        &mut timespec as *mut libc::timespec,
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
                        hash_map::Entry::Occupied(e) => match e.remove() {
                            AIOState::FutureInit(mut aio, dropped) => {
                                if !dropped {
                                    w.insert(
                                        id,
                                        AIOState::FutureDone(if ev.res >= 0 {
                                            Ok((ev.res as usize, aio.data.take().unwrap()))
                                        } else {
                                            Err(-ev.res as i32)
                                        }),
                                    );
                                }
                            }
                            AIOState::FuturePending(mut aio, waker, dropped) => {
                                if !dropped {
                                    w.insert(
                                        id,
                                        AIOState::FutureDone(if ev.res >= 0 {
                                            Ok((ev.res as usize, aio.data.take().unwrap()))
                                        } else {
                                            Err(-ev.res as i32)
                                        }),
                                    );
                                    waker.wake();
                                }
                            }
                            s => {
                                w.insert(id, s);
                            }
                        },
                        _ => unreachable!(),
                    }
                }
            }
        }));
        Ok(AIONotifier { shared, listener })
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
            last_id: 0,
        }
    }

    pub fn submit(&mut self) -> bool {
        if self.queued.is_empty() {
            return false;
        }
        let pending = &mut self.queued;
        let nmax = std::cmp::min(self.max_nbatched as usize, pending.len());
        let ret = unsafe {
            abi::io_submit(
                *self.notifier.shared.io_ctx,
                pending.len() as i64,
                (&mut pending[..nmax]).as_mut_ptr(),
            )
        };
        if (ret < 0 && ret == LIBAIO_EAGAIN) || ret == 0 {
            return false;
        }
        assert!(ret > 0);
        let nacc = ret as usize;
        self.queued = pending.split_off(nacc);
        ret as usize == nmax
    }

    fn next_id(&mut self) -> u64 {
        let id = self.last_id;
        self.last_id = self.last_id.wrapping_add(1);
        id
    }

    fn schedule(&mut self, aio: AIO) -> AIOFuture {
        let fut = AIOFuture {
            notifier: self.notifier.shared.clone(),
            aio_id: aio.id,
        };
        let iocb = aio.iocb.load(Ordering::Acquire);
        self.notifier.shared
            .register_notify(aio.id, AIOState::FutureInit(aio, false));
        self.queued.push(iocb);
        fut
    }
}

//#[async_trait(?Send)]
impl<'a> AIOScheduler for AIOBatchScheduler<'a> {
    fn read(
        &mut self,
        fd: RawFd,
        offset: u64,
        length: usize,
        priority: Option<u16>,
    ) -> AIOFuture {
        let priority = priority.unwrap_or(0);
        let mut data = Vec::new();
        data.resize(length, 0);
        let data = data.into_boxed_slice();
        let aio = AIO::new(
            self.next_id(),
            fd,
            offset,
            data,
            priority,
            0,
            abi::IOCmd::PWrite,
        );
        self.schedule(aio)
    }

    fn write(
        &mut self,
        fd: RawFd,
        offset: u64,
        data: Box<[u8]>,
        priority: Option<u16>,
    ) -> AIOFuture {
        let priority = priority.unwrap_or(0);
        let aio = AIO::new(
            self.next_id(),
            fd,
            offset,
            data,
            priority,
            0,
            abi::IOCmd::PWrite,
        );
        self.schedule(aio)
    }
}
