use parking_lot::*;
use std::fmt::Debug;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_channel::{bounded, Receiver, RecvError, SendError, Sender, TryRecvError, TrySendError};
use tokio::time::sleep;

#[derive(Clone)]
pub(crate) struct Channel<T> {
    rx: Option<Receiver<T>>,
    tx: Option<Sender<T>>,
}

impl<T> Channel<T> {
    pub(crate) fn new(n: usize) -> Self {
        let (tx, rx) = bounded(n);
        Channel {
            rx: Some(rx),
            tx: Some(tx),
        }
    }
    pub(crate) fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        if let Some(tx) = &self.tx {
            return tx.try_send(msg);
        }
        Ok(())
    }

    pub(crate) fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(rx) = &self.rx {
            return rx.try_recv();
        }
        Err(TryRecvError::Empty)
    }

    pub(crate) async fn recv(&self) -> Result<T, async_channel::RecvError> {
        let rx = self.rx.as_ref().unwrap();
        rx.recv().await
    }

    pub(crate) async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        let tx = self.tx.as_ref().unwrap();
        tx.send(msg).await
    }

    pub(crate) fn tx(&self) -> Sender<T> {
        self.tx.as_ref().unwrap().clone()
    }

    pub(crate) fn take_tx(&mut self) -> Option<Sender<T>> {
        self.tx.take()
    }

    pub(crate) fn close(&self) {
        if self.tx.is_none() {
            return;
        }
        self.tx.as_ref().unwrap().close();
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
    pub(crate) fn new(initial: isize) -> Self {
        let mut close = Closer {
            closed: Channel::new(1),
            wait: Arc::from(AtomicIsize::new(initial)),
        };
        close
    }

    pub(crate) fn add_running(&self, delta: isize) {
        self.wait.fetch_add(delta, Ordering::Relaxed);
    }

    pub(crate) fn signal(&self) {
        self.closed.close();
    }

    // todo
    pub(crate) fn has_been_closed(&self) -> Channel<()> {
        self.closed.clone()
    }

    pub(crate) fn done(&self) {
        self.wait.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) async fn wait(&self) {
        loop {
            if self.wait.load(Ordering::Relaxed) <= 0 {
                break;
            }
            println!("wait");
            sleep(Duration::from_millis(10)).await;
        }
    }

    pub(crate) async fn signal_and_wait(&self) {
        self.signal();
        self.wait().await;
    }
}

#[derive(Debug, Clone)]
pub struct XWeak<T> {
    x: Weak<T>,
}

pub struct XArc<T> {
    x: Arc<T>,
}

impl<T> XArc<T> {
    fn new(x: T) -> XArc<T> {
        XArc { x: Arc::new(x) }
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

#[test]
fn it_closer() {
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let mut closer = Closer::new(1);
        let c = closer.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(20000)).await;
            println!("Hello Word1");
            c.done();
        });
        closer.signal_and_wait().await;
        println!("Hello Word");
    });
}
