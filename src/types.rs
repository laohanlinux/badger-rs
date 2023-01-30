use parking_lot::*;
use std::fmt::Debug;
use std::mem::ManuallyDrop;
use std::ops::{Deref, RangeBounds};
use std::sync::atomic::{AtomicI32, AtomicIsize, AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, TryLockResult, Weak};
use std::time::Duration;
use std::{hint, mem, thread};

use async_channel::{bounded, Receiver, RecvError, SendError, Sender, TryRecvError, TrySendError};

use range_lock::{VecRangeLock, VecRangeLockGuard};
use tokio::time::sleep;

// Channel like to go's channel
#[derive(Clone)]
pub(crate) struct Channel<T> {
    rx: Option<Receiver<T>>,
    tx: Option<Sender<T>>,
}

impl<T> Channel<T> {
    // create a *Channel* with n cap
    pub(crate) fn new(n: usize) -> Self {
        let (tx, rx) = bounded(n);
        Channel {
            rx: Some(rx),
            tx: Some(tx),
        }
    }

    // try to send message T without blocking
    pub(crate) fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        if let Some(tx) = &self.tx {
            return tx.try_send(msg);
        }
        Ok(())
    }

    // try to receive a message without blocking
    pub(crate) fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(rx) = &self.rx {
            return rx.try_recv();
        }
        Err(TryRecvError::Empty)
    }

    // async receive a message with blocking
    pub(crate) async fn recv(&self) -> Result<T, async_channel::RecvError> {
        let rx = self.rx.as_ref().unwrap();
        rx.recv().await
    }

    // async send a message with blocking
    pub(crate) async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        let tx = self.tx.as_ref().unwrap();
        tx.send(msg).await
    }

    // returns Sender
    pub(crate) fn tx(&self) -> Sender<T> {
        self.tx.as_ref().unwrap().clone()
    }

    // consume tx and return it if exist
    pub(crate) fn take_tx(&mut self) -> Option<Sender<T>> {
        self.tx.take()
    }

    // close *Channel*, Sender will be consumed
    pub(crate) fn close(&self) {
        if let Some(tx) = &self.tx {
            tx.close();
        }
    }
}

/// Holds the two things we need to close a routine and wait for it to finish: a chan
/// to tell the routine to shut down, and a wait_group with which to wait for it to finish shutting
/// down.
#[derive(Clone)]
pub(crate) struct Closer {
    closed: Channel<()>,
    wait: Arc<AtomicIsize>,
}

impl Closer {
    // create a Closer with *initial* cap Workers
    pub(crate) fn new(initial: isize) -> Self {
        assert!(initial >= 0, "Sanity check");
        let mut close = Closer {
            closed: Channel::new(1),
            wait: Arc::from(AtomicIsize::new(initial)),
        };
        close
    }

    // Incr delta to the WaitGroup.
    pub(crate) fn add_running(&self, delta: isize) {
        let old = self.wait.fetch_add(delta, Ordering::Relaxed);
        assert!(old >= 0, "Sanity check");
    }

    // Decr delta to the WaitGroup.
    pub(crate) fn done(&self) {
        let old = self.wait.fetch_sub(1, Ordering::Relaxed);
        assert!(old >= 0, "Sanity check");
    }

    // Signals the `has_been_closed` signal.
    pub(crate) fn signal(&self) {
        self.closed.close();
    }

    // Gets signaled when signal() is called.
    pub(crate) fn has_been_closed(&self) -> Channel<()> {
        self.closed.clone()
    }

    // Waiting until done
    pub(crate) async fn wait(&self) {
        loop {
            if self.wait.load(Ordering::Relaxed) <= 0 {
                break;
            }
            hint::spin_loop();
            sleep(Duration::from_millis(10)).await;
        }
    }

    // Send a close signal and waiting util done
    pub(crate) async fn signal_and_wait(&self) {
        self.signal();
        self.wait().await;
    }
}

#[derive(Debug, Clone)]
pub struct XWeak<T> {
    pub(crate) x: Weak<T>,
}

#[derive(Debug)]
pub struct XArc<T> {
    pub(crate) x: Arc<T>,
}

impl<T> Clone for XArc<T> {
    fn clone(&self) -> Self {
        XArc { x: self.x.clone() }
    }
}

impl<T> XArc<T> {
    pub fn new(x: T) -> XArc<T> {
        XArc { x: Arc::new(x) }
    }

    pub fn to_ref(&self) -> &T {
        self.x.as_ref()
    }
}

impl<T> XWeak<T> {
    pub fn new() -> Self {
        Self { x: Weak::new() }
    }

    pub fn upgrade(&self) -> Option<XArc<T>> {
        self.x.upgrade().map(|x| XArc { x })
    }

    pub fn from(xarc: &XArc<T>) -> Self {
        Self {
            x: Arc::downgrade(&xarc.x),
        }
    }
}

#[derive(Clone)]
pub struct XVec<T>(pub Arc<VecRangeLock<T>>);

impl<T> XVec<T> {
    pub fn new(v: Vec<T>) -> Self {
        XVec(Arc::new(VecRangeLock::new(v)))
    }

    pub fn lock_all(&self) {
        let right = self.0.data_len();
        self.lock(0, right)
    }

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

#[test]
fn it_closer() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let closer = Closer::new(1);
        let c = closer.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(200)).await;
            println!("Hello Word1");
            c.done();
        });
        closer.signal_and_wait().await;
        println!("Hello Word");
    });
}

#[test]
fn lck() {}
