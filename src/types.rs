use std::fmt::Debug;

use std::hint;
use std::ops::{Deref, DerefMut, RangeBounds};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::{Arc, TryLockResult, Weak};
use std::time::Duration;

use async_channel::{
    bounded, unbounded, Receiver, RecvError, SendError, Sender, TryRecvError, TrySendError,
};

use log::info;

use range_lock::{VecRangeLock, VecRangeLockGuard};

use tokio::time::sleep;

pub type TArcMx<T> = Arc<tokio::sync::Mutex<T>>;
pub type TArcRW<T> = Arc<tokio::sync::RwLock<T>>;
pub type ArcMx<T> = Arc<parking_lot::Mutex<T>>;
pub type ArcRW<T> = Arc<parking_lot::RwLock<T>>;

// Channel like to go's channel
#[derive(Clone)]
pub struct Channel<T> {
    rx: Option<Receiver<T>>,
    tx: Option<Sender<T>>,
}

impl<T> Channel<T> {
    /// create a *Channel* with n cap
    pub fn new(n: usize) -> Self {
        let (tx, rx) = bounded(n);
        Channel {
            rx: Some(rx),
            tx: Some(tx),
        }
    }

    /// try to send message T without blocking
    pub fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        if let Some(tx) = &self.tx {
            return tx.try_send(msg);
        }
        Ok(())
    }

    /// try to receive a message without blocking
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(rx) = &self.rx {
            return rx.try_recv();
        }
        Err(TryRecvError::Empty)
    }

    /// async receive a message with blocking
    pub async fn recv(&self) -> Result<T, async_channel::RecvError> {
        let rx = self.rx.as_ref().unwrap();
        rx.recv().await
    }

    /// async send a message with blocking
    pub async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        let tx = self.tx.as_ref().unwrap();
        tx.send(msg).await
    }

    /// returns Sender
    pub fn tx(&self) -> Sender<T> {
        self.tx.as_ref().unwrap().clone()
    }

    pub fn rx(&self) -> Receiver<T> {
        self.rx.as_ref().unwrap().clone()
    }

    /// consume tx and return it if exist
    pub fn take_tx(&mut self) -> Option<Sender<T>> {
        self.tx.take()
    }

    /// close *Channel*, Sender will be consumed
    pub fn close(&self) {
        if let Some(tx) = &self.tx {
            tx.close();
        }
    }

    pub fn is_close(&self) -> bool {
        if let Some(tx) = &self.tx {
            return tx.is_closed();
        }
        true
    }
}

#[derive(Clone)]
pub struct UnChannel<T> {
    rx: Option<Receiver<T>>,
    tx: Option<Sender<T>>,
}

impl<T> UnChannel<T> {
    pub fn new() -> UnChannel<T> {
        let (tx, rx) = unbounded();
        UnChannel {
            rx: Some(rx),
            tx: Some(tx),
        }
    }

    /// returns Sender
    pub fn tx(&self) -> Option<&Sender<T>> {
        self.tx.as_ref()
    }

    // /// async receive a message with blocking
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        let rx = self.rx.as_ref().unwrap();
        rx.recv().await
    }

    /// close *Channel*, Sender will be consumed
    pub fn close(&self) {
        if let Some(tx) = &self.tx {
            tx.close();
        }
    }
}

/// Holds the two things we need to close a routine and wait for it to finish: a chan
/// to tell the routine to shut down, and a wait_group with which to wait for it to finish shutting
/// down.
#[derive(Clone)]
pub struct Closer {
    name: String,
    closed: Channel<()>,
    wait: Arc<AtomicIsize>,
    disable_log: bool,
}

impl Drop for Closer {
    fn drop(&mut self) {
        assert!(self.wait.load(Ordering::Relaxed) >= 0, "Sanity check!");
        if !self.disable_log {
            info!(
                "Worker-{}-{} exited",
                self.name,
                self.wait.load(Ordering::Relaxed)
            );
        }
    }
}

impl Closer {
    /// create a Closer with *initial* cap Workers
    pub fn new(name: String) -> Self {
        let close = Closer {
            name,
            closed: Channel::new(1),
            disable_log: false,
            wait: Arc::from(AtomicIsize::new(0)),
        };
        close
    }

    pub fn new_without_log(name: String) -> Self {
        let mut closer = Self::new(name);
        closer.disable_log = true;
        closer
    }

    /// Incr delta to the WaitGroup.
    pub fn add_running(&self, delta: isize) {
        let old = self.wait.fetch_add(delta, Ordering::Relaxed);
        assert!(old >= 0, "Sanity check");
    }

    /// Spawn a worker
    pub fn spawn(&self) -> Self {
        info!(
            "spawn a new closer: {}.{}.Worker",
            self.name,
            self.wait.load(Ordering::Relaxed)
        );
        self.add_running(1);
        self.clone()
    }

    /// Decr delta to the WaitGroup(Note: must be call for every worker avoid leak).
    pub fn done(&self) {
        let old = self.wait.fetch_sub(1, Ordering::Relaxed);
        assert!(old >= 0, "Sanity check");
    }

    /// Signals the `has_been_closed` signal.
    pub fn signal(&self) {
        self.closed.close();
    }

    /// Gets signaled when signal() is called.
    pub fn has_been_closed(&self) -> Channel<()> {
        self.closed.clone()
    }

    /// Waiting until done
    pub async fn wait(&self) {
        loop {
            if self.wait.load(Ordering::Relaxed) <= 0 {
                break;
            }
            match self.has_been_closed().try_recv() {
                Err(err) if err.is_closed() => return,
                Err(_) => {}
                Ok(()) => return,
            }
            tokio::time::sleep(Duration::from_micros(1)).await;
        }
    }

    /// Send a close signal and waiting util done
    pub async fn signal_and_wait(&self) {
        self.signal();
        loop {
            if self.wait.load(Ordering::Relaxed) <= 0 {
                break;
            }
            sleep(Duration::from_nanos(1000)).await;
        }
    }
}

#[derive(Debug, Clone)]
pub struct XWeak<T>(Weak<T>);

#[derive(Debug)]
pub struct XArc<T>(Arc<T>);

impl<T> Deref for XArc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<T> Clone for XArc<T> {
    fn clone(&self) -> Self {
        XArc(self.0.clone())
    }
}

impl<T> XArc<T> {
    pub fn new(x: T) -> XArc<T> {
        XArc(Arc::new(x))
    }

    pub fn to_ref(&self) -> &T {
        self.0.as_ref()
    }

    pub fn to_inner(self) -> Option<T> {
        Arc::into_inner(self.0)
    }
}

impl<T> XWeak<T> {
    pub fn new() -> Self {
        Self { 0: Weak::new() }
    }

    pub fn upgrade(&self) -> Option<XArc<T>> {
        self.0.upgrade().map(XArc)
    }

    pub fn from(xarc: &XArc<T>) -> Self {
        Self {
            0: Arc::downgrade(&xarc.0),
        }
    }
}

#[derive(Clone)]
pub struct XVec<T>(pub Arc<VecRangeLock<T>>);

impl<T> XVec<T> {
    pub fn new(v: Vec<T>) -> Self {
        XVec(Arc::new(VecRangeLock::new(v)))
    }

    #[inline]
    pub fn lock_all(&self) {
        let right = self.0.data_len();
        self.lock(0, right)
    }

    #[inline]
    pub fn lock(&self, left: usize, right: usize) {
        loop {
            let range = left..right;
            if self.0.try_lock(range).is_ok() {
                break;
            } else {
                hint::spin_loop();
            }
        }
    }

    #[inline]
    pub fn try_lock(&self, range: impl RangeBounds<usize>) -> TryLockResult<VecRangeLockGuard<T>> {
        self.0.try_lock(range)
    }
}

impl<T> Deref for XVec<T> {
    type Target = VecRangeLock<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{atomic::AtomicUsize, Arc};

    use atomic::Ordering;
    use crossbeam_epoch::Owned;

    use crate::types::Closer;

    #[test]
    fn it_closer() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let closer = Closer::new("test".to_owned());
            let count = Arc::new(AtomicUsize::new(100));
            for i in 0..count.load(Ordering::Relaxed) {
                let c = closer.spawn();
                let n = count.clone();
                tokio::spawn(async move {
                    n.fetch_sub(1, Ordering::Relaxed);
                    c.done();
                });
            }
            closer.signal_and_wait().await;
            assert_eq!(count.load(Ordering::Relaxed), 0);
        });
    }

    #[tokio::test]
    async fn lck() {
        use crossbeam_epoch::{self as epoch, Atomic, Shared};
        use std::sync::atomic::Ordering::SeqCst;

        let a = Atomic::new(1234);
        let guard = &epoch::pin();
        // let p = a.swap(Shared::null(), SeqCst, guard);
        // println!("{:?}", unsafe { p.as_ref().unwrap()});
        let p = a.swap(Owned::new(200), SeqCst, guard);
        let p = a.swap(Owned::new(200), SeqCst, guard);

        println!("{:?}", unsafe { p.as_ref().unwrap() });
    }
}
