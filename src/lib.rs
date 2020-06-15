mod abi;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::{hash_map, HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicBool, Ordering},
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

#[async_trait]
/// Schedules the submission of AIO operations.
pub trait AIOScheduler {
    async fn read(offset: usize, length: usize) -> Result<usize, ()>;
    async fn write(offset: usize, data: Box<[u8]>) -> Result<usize, ()>;
}

struct AIONotifierShared {
    waiting: Mutex<HashMap<u64, std::task::Waker>>,
    io_ctx: AIOContext,
    stopped: AtomicBool,
}

/// Listens for finished AIO operations and wake up the futures.
pub struct AIONotifier {
    shared: Arc<AIONotifierShared>,
    listener: std::thread::JoinHandle<()>,
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
        let listener = std::thread::spawn(move || {
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
                for id in events[..ret as usize].iter().map(|e: &abi::IOEvent| e.data) {
                    w.remove(&id).unwrap().wake()
                }
            }
        });
        Ok(AIONotifier { shared, listener })
    }
}

pub struct AIOBatchScheduler {
    queued: VecDeque<abi::IOCb>,
    max_nbatched: usize,
}

#[async_trait]
impl AIOScheduler for AIOBatchScheduler {
    async fn read(offset: usize, length: usize) -> Result<usize, ()> {
        Ok(0)
    }

    async fn write(offset: usize, data: Box<[u8]>) -> Result<usize, ()> {
        Ok(0)
    }
}
