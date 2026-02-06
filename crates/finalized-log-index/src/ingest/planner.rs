#[derive(Debug, Clone, Copy)]
pub struct StreamAppendStats {
    pub addressed: usize,
    pub topics: usize,
}
