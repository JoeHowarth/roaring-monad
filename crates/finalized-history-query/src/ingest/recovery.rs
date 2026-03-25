use crate::error::Result;
use crate::family::FamilyStates;
use crate::ingest::authority::WriteContinuity;
use crate::ingest::open_pages::repair_sealed_open_bitmap_pages;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

/// Apply any writer-side recovery required before ingest continues from the
/// published head. Continuous leases skip recovery entirely; fresh acquisition
/// and reacquisition repair ownership-transition state idempotently.
pub async fn preflight_recovery<M: MetaStore, B: BlobStore>(
    continuity: WriteContinuity,
    tables: &Tables<M, B>,
    family_states: &FamilyStates,
) -> Result<()> {
    match continuity {
        WriteContinuity::Continuous => {}
        WriteContinuity::Fresh => {
            repair_sealed_open_bitmap_pages(
                tables,
                family_states.logs.next_log_id.get(),
                family_states.txs.next_tx_id.get(),
                family_states.traces.next_trace_id.get(),
            )
            .await?
        }
        WriteContinuity::Reacquired => {
            repair_sealed_open_bitmap_pages(
                tables,
                family_states.logs.next_log_id.get(),
                family_states.txs.next_tx_id.get(),
                family_states.traces.next_trace_id.get(),
            )
            .await?
        }
    }
    Ok(())
}
