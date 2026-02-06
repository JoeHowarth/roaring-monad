use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    pub epoch: u64,
}

#[derive(Debug)]
pub struct LeaseManager {
    epoch: AtomicU64,
    active: AtomicBool,
}

impl Default for LeaseManager {
    fn default() -> Self {
        Self::new(1)
    }
}

impl LeaseManager {
    pub fn new(initial_epoch: u64) -> Self {
        Self {
            epoch: AtomicU64::new(initial_epoch.max(1)),
            active: AtomicBool::new(true),
        }
    }

    pub async fn current(&self) -> Option<Lease> {
        if !self.active.load(Ordering::Relaxed) {
            return None;
        }
        Some(Lease {
            epoch: self.epoch.load(Ordering::Relaxed),
        })
    }

    pub fn lose(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    pub fn renew(&self) -> Lease {
        self.active.store(true, Ordering::Relaxed);
        let next = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
        Lease { epoch: next }
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::*;

    #[test]
    fn lease_lifecycle() {
        block_on(async {
            let mgr = LeaseManager::new(2);
            assert_eq!(mgr.current().await.expect("lease").epoch, 2);
            mgr.lose();
            assert!(mgr.current().await.is_none());
            let lease = mgr.renew();
            assert!(lease.epoch >= 3);
            assert_eq!(mgr.current().await.expect("lease").epoch, lease.epoch);
        });
    }
}
