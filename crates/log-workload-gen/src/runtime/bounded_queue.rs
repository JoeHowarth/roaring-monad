use crate::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct QueueDepth {
    current: Arc<AtomicUsize>,
    max: Arc<AtomicUsize>,
}

impl QueueDepth {
    pub fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    pub fn max(&self) -> usize {
        self.max.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct BoundedSender<T> {
    inner: mpsc::Sender<T>,
    depth: QueueDepth,
}

pub struct BoundedReceiver<T> {
    inner: mpsc::Receiver<T>,
    depth: QueueDepth,
}

pub fn bounded<T>(
    capacity: usize,
) -> Result<(BoundedSender<T>, BoundedReceiver<T>, QueueDepth), Error> {
    if capacity == 0 {
        return Err(Error::ConfigInvalid(
            "bounded queue capacity must be >= 1".to_string(),
        ));
    }

    let (tx, rx) = mpsc::channel(capacity);
    let depth = QueueDepth {
        current: Arc::new(AtomicUsize::new(0)),
        max: Arc::new(AtomicUsize::new(0)),
    };
    Ok((
        BoundedSender {
            inner: tx,
            depth: depth.clone(),
        },
        BoundedReceiver {
            inner: rx,
            depth: depth.clone(),
        },
        depth,
    ))
}

impl<T> BoundedSender<T> {
    pub async fn send(&self, item: T) -> Result<(), mpsc::error::SendError<T>> {
        self.inner.send(item).await?;
        let now = self.depth.current.fetch_add(1, Ordering::Relaxed) + 1;
        let mut prev = self.depth.max.load(Ordering::Relaxed);
        while now > prev {
            match self.depth.max.compare_exchange_weak(
                prev,
                now,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => prev = observed,
            }
        }
        Ok(())
    }
}

impl<T> BoundedReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        let item = self.inner.recv().await;
        if item.is_some() {
            self.depth.current.fetch_sub(1, Ordering::Relaxed);
        }
        item
    }
}
