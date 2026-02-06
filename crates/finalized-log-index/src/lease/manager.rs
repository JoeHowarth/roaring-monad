#[derive(Debug, Clone, Copy)]
pub struct Lease {
    pub epoch: u64,
}

#[derive(Debug, Default)]
pub struct LeaseManager;

impl LeaseManager {
    pub async fn current(&self) -> Option<Lease> {
        Some(Lease { epoch: 1 })
    }
}
