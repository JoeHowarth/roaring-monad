use crate::core::directory_resolver::ResolvedPrimaryLocation;
use crate::core::ids::TxId;
use crate::core::refs::BlockRef;
use crate::error::Result;
use crate::query::runner::{MaterializerCaches, QueryMaterializer};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::txs::filter::TxFilter;
use crate::txs::types::{DirByBlock, Tx};

pub struct TxMaterializer<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
    caches: MaterializerCaches<DirByBlock>,
}

impl<'a, M: MetaStore, B: BlobStore> TxMaterializer<'a, M, B> {
    pub fn new(tables: &'a Tables<M, B>) -> Self {
        Self {
            tables,
            caches: MaterializerCaches::default(),
        }
    }
}

impl<M: MetaStore, B: BlobStore> QueryMaterializer for TxMaterializer<'_, M, B> {
    type Id = TxId;
    type Item = Tx;
    type Filter = TxFilter;
    type Output = Tx;

    async fn resolve_id(&mut self, _id: Self::Id) -> Result<Option<ResolvedPrimaryLocation>> {
        let _ = (&self.tables, &self.caches);
        todo!("tx primary-id resolution is not implemented")
    }

    async fn load_run(
        &mut self,
        _run: &[(Self::Id, ResolvedPrimaryLocation)],
    ) -> Result<Vec<(Self::Id, Self::Item)>> {
        let _ = (&self.tables, &self.caches);
        todo!("tx run materialization is not implemented")
    }

    async fn block_ref_for(&mut self, _item: &Self::Item) -> Result<BlockRef> {
        let _ = (&self.tables, &self.caches);
        todo!("tx block-ref lookup is not implemented")
    }

    fn exact_match(&self, _item: &Self::Item, _filter: &Self::Filter) -> bool {
        let _ = (&self.tables, &self.caches);
        todo!("tx exact-match filtering is not implemented")
    }

    fn into_output(_item: Self::Item) -> Self::Output {
        todo!("tx output materialization is not implemented")
    }
}
