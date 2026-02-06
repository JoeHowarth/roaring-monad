#[derive(Debug, Clone)]
pub struct RecoveryPlan {
    pub warm_streams: usize,
}

pub fn startup_plan(warm_streams: usize) -> RecoveryPlan {
    RecoveryPlan { warm_streams }
}
