use roaring::RoaringBitmap;

#[derive(Debug, Clone)]
pub struct ChunkBlob {
    pub min_local: u32,
    pub max_local: u32,
    pub count: u32,
    pub crc32: u32,
    pub bitmap: RoaringBitmap,
}
