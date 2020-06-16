mod abi;
use async_trait::async_trait;
use parking_lot::{Condvar, Mutex};
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
pub struct AIO {
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
    notifier: Arc<AIONotifier>,
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
pub trait AIOSchedulerIn {
    fn schedule(&self, aio: AIO, notifier: &Arc<AIONotifier>) -> AIOFuture;
    fn next_id(&mut self) -> u64;
}

pub trait AIOSchedulerOut {
    fn queue_out(&self) -> &crossbeam_channel::Receiver<AtomicPtr<abi::IOCb>>;
    fn is_empty(&self) -> bool;
    fn submit(&mut self, notifier: &AIONotifier) -> usize;
}

enum AIOState {
    FutureInit(AIO, bool),
    FuturePending(AIO, std::task::Waker, bool),
    FutureDone(AIOResult),
}

pub struct AIONotifier {
    waiting: Mutex<HashMap<u64, AIOState>>,
    io_ctx: AIOContext,
    //stopped: AtomicBool,
}

impl AIONotifier {
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
                    AIOState::FuturePending(aio, waker, dropped) => {
                        waiting.insert(id, AIOState::FuturePending(aio, waker, dropped));
                        None
                    }
                    AIOState::FutureDone(res) => Some(res),
                }
            }
            _ => unreachable!(),
        }
    }

    fn finish(&self, id: u64, res: i64) {
        let mut w = self.waiting.lock();
        match w.entry(id) {
            hash_map::Entry::Occupied(e) => match e.remove() {
                AIOState::FutureInit(mut aio, dropped) => {
                    if !dropped {
                        w.insert(
                            id,
                            AIOState::FutureDone(if res >= 0 {
                                Ok((res as usize, aio.data.take().unwrap()))
                            } else {
                                Err(-res as i32)
                            }),
                        );
                    }
                }
                AIOState::FuturePending(mut aio, waker, dropped) => {
                    if !dropped {
                        w.insert(
                            id,
                            AIOState::FutureDone(if res >= 0 {
                                Ok((res as usize, aio.data.take().unwrap()))
                            } else {
                                Err(-res as i32)
                            }),
                        );
                        waker.wake();
                    }
                }
                AIOState::FutureDone(ret) => {
                    w.insert(id, AIOState::FutureDone(ret));
                }
            },
            _ => unreachable!(),
        }
    }
}

/// Listens for finished AIO operations and wake up the futures.
pub struct AIOManager<S: AIOSchedulerIn> {
    notifier: Arc<AIONotifier>,
    scheduler_in: S,
    listener: Option<std::thread::JoinHandle<()>>,
    exit_s: crossbeam_channel::Sender<()>,
}

impl<S: AIOSchedulerIn> AIOManager<S> {
    pub fn new<T: AIOSchedulerOut + Send + 'static>(
        scheduler: (S, T),
        maxevents: usize,
        max_nops: Option<u16>,
        timeout: Option<u32>,
    ) -> Result<Self, Error> {
        let (scheduler_in, mut scheduler_out) = scheduler;
        let max_nops = max_nops.unwrap_or(4096);
        let (exit_s, exit_r) = crossbeam_channel::bounded(0);

        let notifier = Arc::new(AIONotifier {
            io_ctx: AIOContext::new(maxevents)?,
            waiting: Mutex::new(HashMap::new()),
            //stopped: AtomicBool::new(false),
        });

        let n = notifier.clone();
        let listener = Some(std::thread::spawn(move || {
            let mut timespec = timeout
                .and_then(|nsec: u32| {
                    Some(libc::timespec {
                        tv_sec: 0,
                        tv_nsec: nsec as i64,
                    })
                })
                .unwrap_or(libc::timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                });
            let mut ongoing = 0;
            loop {
                // try to quiesce
                if ongoing == 0 && scheduler_out.is_empty() {
                    let mut sel = crossbeam_channel::Select::new();
                    sel.recv(&exit_r);
                    sel.recv(&scheduler_out.queue_out());
                    if sel.ready() == 0 {
                        exit_r.recv().unwrap();
                        break;
                    }
                }
                // submit as many aios as possible
                loop {
                    let nacc = scheduler_out.submit(&n);
                    ongoing += nacc;
                    if nacc == 0 {
                        break;
                    }
                }
                // no need to wait if there is no progress
                if ongoing == 0 {
                    continue;
                }
                // then block on any finishing aios
                let mut events = vec![abi::IOEvent::default(); max_nops as usize];
                let ret = unsafe {
                    abi::io_getevents(
                        *n.io_ctx,
                        1,
                        max_nops as i64,
                        events.as_mut_ptr(),
                        &mut timespec as *mut libc::timespec,
                    )
                };
                // TODO: AIO fatal error handling
                // avoid empty slice
                if ret == 0 {
                    continue;
                }
                assert!(ret > 0);
                ongoing -= ret as usize;
                for ev in events[..ret as usize].iter() {
                    n.finish(ev.data as u64, ev.res);
                }
            }
        }));
        Ok(AIOManager {
            notifier,
            listener,
            scheduler_in,
            exit_s,
        })
    }

    pub fn read(
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
            self.scheduler_in.next_id(),
            fd,
            offset,
            data,
            priority,
            0,
            abi::IOCmd::PWrite,
        );
        self.scheduler_in.schedule(aio, &self.notifier)
    }

    pub fn write(
        &mut self,
        fd: RawFd,
        offset: u64,
        data: Box<[u8]>,
        priority: Option<u16>,
    ) -> AIOFuture {
        let priority = priority.unwrap_or(0);
        let aio = AIO::new(
            self.scheduler_in.next_id(),
            fd,
            offset,
            data,
            priority,
            0,
            abi::IOCmd::PWrite,
        );
        self.scheduler_in.schedule(aio, &self.notifier)
    }
}

impl<S: AIOSchedulerIn> Drop for AIOManager<S> {
    fn drop(&mut self) {
        self.exit_s.send(()).unwrap();
        self.listener.take().unwrap().join().unwrap();
    }
}

pub struct AIOBatchSchedulerIn {
    queue_in: crossbeam_channel::Sender<AtomicPtr<abi::IOCb>>,
    last_id: u64,
}

pub struct AIOBatchSchedulerOut {
    queue_out: crossbeam_channel::Receiver<AtomicPtr<abi::IOCb>>,
    max_nbatched: usize,
    leftover: Vec<AtomicPtr<abi::IOCb>>,
}

impl AIOSchedulerIn for AIOBatchSchedulerIn {
    fn schedule(&self, aio: AIO, notifier: &Arc<AIONotifier>) -> AIOFuture {
        let fut = AIOFuture {
            notifier: notifier.clone(),
            aio_id: aio.id,
        };
        let iocb = aio.iocb.load(Ordering::Acquire);
        notifier.register_notify(aio.id, AIOState::FutureInit(aio, false));
        self.queue_in.send(AtomicPtr::new(iocb)).unwrap();
        fut
    }

    fn next_id(&mut self) -> u64 {
        let id = self.last_id;
        self.last_id = id.wrapping_add(1);
        id
    }
}

impl AIOSchedulerOut for AIOBatchSchedulerOut {
    fn queue_out(&self) -> &crossbeam_channel::Receiver<AtomicPtr<abi::IOCb>> {
        &self.queue_out
    }
    fn is_empty(&self) -> bool {
        self.leftover.len() == 0
    }
    fn submit(&mut self, notifier: &AIONotifier) -> usize {
        let mut quota = self.max_nbatched;
        let mut pending = self
            .leftover
            .iter()
            .map(|p| p.load(Ordering::Acquire))
            .collect::<Vec<_>>();
        if pending.len() < quota {
            quota -= pending.len();
            while let Ok(iocb) = self.queue_out.try_recv() {
                pending.push(iocb.load(Ordering::Acquire));
                quota -= 1;
                if quota == 0 {
                    break;
                }
            }
        }
        if pending.len() == 0 {
            return 0;
        }
        let mut ret = unsafe {
            abi::io_submit(
                *notifier.io_ctx,
                pending.len() as i64,
                (&mut pending).as_mut_ptr(),
            )
        };
        if ret < 0 && ret == LIBAIO_EAGAIN {
            ret = 0
        }
        let nacc = ret as usize;
        self.leftover = (&pending[nacc..])
            .iter()
            .map(|p| AtomicPtr::new(*p))
            .collect::<Vec<_>>();
        nacc
    }
}

pub fn get_batch_scheduler(
    max_nbatched: Option<usize>,
) -> (AIOBatchSchedulerIn, AIOBatchSchedulerOut) {
    let max_nbatched = max_nbatched.unwrap_or(128);
    let (queue_in, queue_out) = crossbeam_channel::unbounded();
    let bin = AIOBatchSchedulerIn {
        queue_in,
        last_id: 0,
    };
    let bout = AIOBatchSchedulerOut {
        queue_out,
        max_nbatched,
        leftover: Vec::new(),
    };
    (bin, bout)
}
