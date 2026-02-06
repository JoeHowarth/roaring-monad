#[derive(Debug, Clone)]
pub struct ChunkRef {
    pub chunk_seq: u64,
    pub min_local: u32,
    pub max_local: u32,
    pub count: u32,
}

#[derive(Debug, Clone, Default)]
pub struct Manifest {
    pub version: u64,
    pub last_chunk_seq: u64,
    pub chunk_refs: Vec<ChunkRef>,
    pub approx_count: u64,
}
