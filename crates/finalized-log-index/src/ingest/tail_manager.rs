use roaring::RoaringBitmap;

use crate::codec::manifest::{decode_tail, encode_tail};
use crate::domain::keys::tail_key;
use crate::error::Result;
use crate::store::traits::{FenceToken, MetaStore, PutCond};

#[derive(Debug, Default)]
pub struct TailManager;

impl TailManager {
    pub async fn load<M: MetaStore>(&self, store: &M, stream_id: &str) -> Result<RoaringBitmap> {
        let key = tail_key(stream_id);
        match store.get(&key).await? {
            Some(rec) => decode_tail(&rec.value),
            None => Ok(RoaringBitmap::new()),
        }
    }

    pub async fn store<M: MetaStore>(
        &self,
        store: &M,
        stream_id: &str,
        tail: &RoaringBitmap,
        epoch: u64,
    ) -> Result<()> {
        let key = tail_key(stream_id);
        let _ = store
            .put(&key, encode_tail(tail)?, PutCond::Any, FenceToken(epoch))
            .await?;
        Ok(())
    }

    pub fn append_all(&self, tail: &mut RoaringBitmap, values: &[u32]) {
        for v in values {
            tail.insert(*v);
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::*;
    use crate::store::meta::InMemoryMetaStore;

    #[test]
    fn tail_roundtrip() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            let m = TailManager;
            let sid = "addr/aa/00000000";
            let mut tail = RoaringBitmap::new();
            tail.insert(11);
            tail.insert(12);
            m.store(&store, sid, &tail, 0).await.expect("store");
            let got = m.load(&store, sid).await.expect("load");
            assert!(got.contains(11));
            assert!(got.contains(12));
        });
    }
}
