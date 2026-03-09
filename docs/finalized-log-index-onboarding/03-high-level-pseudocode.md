# Finalized Log Index High-Level Pseudocode

This is intentionally not real code. It is a high-level, Python-like sketch of the architecture for understanding the main control flow.

The goal is to show:

- what the service stores
- how ingest updates the index
- how query plans and executes a lookup

## Core Terms

```python
global_log_id = monotonically increasing 64-bit ID assigned to each ingested log
shard = high bits of a global_log_id or block number
local_value = low bits stored inside a shard-local bitmap
index_kind = one of: "addr", "topic0_log", "topic1", "topic2", "topic3"
stream_id = f"{index_kind}/{hex_encoded_index_value}/{shard_hex}"
```

Examples:

```python
stream_id("addr", address_20_bytes, shard)
stream_id("topic0_log", topic0_32_bytes, shard)
```

## Main Data Shapes

```python
class Log:
    address: bytes20
    topics: list[bytes32]
    data: bytes
    block_num: int
    tx_idx: int
    log_idx: int
    block_hash: bytes32


class Block:
    block_num: int
    block_hash: bytes32
    parent_hash: bytes32
    logs: list[Log]


class MetaState:
    indexed_finalized_head: int
    next_log_id: int
    writer_epoch: int


class BlockMeta:
    block_hash: bytes32
    parent_hash: bytes32
    first_log_id: int
    count: int


class LogLocator:
    blob_key: str
    byte_offset: int
    byte_len: int


class Manifest:
    last_chunk_seq: int
    approx_count: int
    last_seal_unix_sec: int
    chunk_refs: list[ChunkRef]


class ChunkRef:
    chunk_seq: int
    min_local: int
    max_local: int
    count: int


class QueryFilter:
    from_block: int | None
    to_block: int | None
    block_hash: bytes32 | None
    address: Clause | None
    topic0: Clause | None
    topic1: Clause | None
    topic2: Clause | None
    topic3: Clause | None


class QueryOptions:
    max_results: int | None
```

## Storage Model

```python
meta_store:
    "meta/state" -> MetaState
    "block_meta/<block_num>" -> BlockMeta
    "block_hash_to_num/<block_hash>" -> block_num
    "log_locator_pages/<page_start_log_id>" -> {slot -> LogLocator}
    "manifests/<stream_id>" -> Manifest
    "tails/<stream_id>" -> roaring_bitmap_of_local_ids

blob_store:
    "log_packs/<first_log_id>" -> packed_log_bytes
    "chunks/<stream_id>/<chunk_seq>" -> roaring_bitmap_chunk
```

## Top-Level Service

```python
class FinalizedIndexService:
    def __init__(self, config, meta_store, blob_store, writer_epoch):
        self.config = config
        self.meta_store = meta_store
        self.blob_store = blob_store
        self.writer_epoch = writer_epoch
        self.ingest = IngestEngine(config, meta_store, blob_store)
        self.query = QueryEngine(config)
        self.runtime_state = RuntimeState()

    def ingest_finalized_block(self, block):
        reject_if_degraded_or_throttled()
        result = self.ingest.ingest_finalized_block(block, self.writer_epoch)
        update_runtime_state_from_result(result)
        return result

    def query_finalized(self, filter, options):
        reject_if_degraded()
        result = self.query.query_finalized(
            self.meta_store,
            self.blob_store,
            filter,
            options,
        )
        update_runtime_state_from_result(result)
        return result
```

## Ingest Flow

```python
class IngestEngine:
    def ingest_finalized_block(self, block, epoch):
        state, state_version = load_meta_state()

        expected_block_num = state.indexed_finalized_head + 1
        if block.block_num != expected_block_num:
            raise InvalidSequence

        expected_parent = zero_hash if state.indexed_finalized_head == 0 else \
            load_block_meta(state.indexed_finalized_head).block_hash
        if block.block_num > 0 and block.parent_hash != expected_parent:
            raise InvalidParent

        first_log_id = state.next_log_id

        # Step 1: write full logs to a packed blob
        if block.logs is not empty:
            packed_blob, spans = encode_log_pack(block.logs)
            pack_key = f"log_packs/{first_log_id}"
            blob_store.put(pack_key, packed_blob)

            # Step 2: write locator pages so log_id -> byte span can be resolved later
            page_updates = group_spans_into_locator_pages(first_log_id, pack_key, spans)
            for page_start, page_entries in page_updates:
                old_page = meta_store.get(f"log_locator_pages/{page_start}") or {}
                merged_page = merge(old_page, page_entries)
                meta_store.put_any(
                    f"log_locator_pages/{page_start}",
                    merged_page,
                    fence=epoch,
                )

        # Step 3: persist block-level metadata
        block_meta = BlockMeta(
            block_hash=block.block_hash,
            parent_hash=block.parent_hash,
            first_log_id=first_log_id,
            count=len(block.logs),
        )
        meta_store.put_any(f"block_meta/{block.block_num}", block_meta, fence=epoch)
        meta_store.put_any(f"block_hash_to_num/{block.block_hash}", block.block_num, fence=epoch)

        # Step 4: derive index stream appends
        appends = collect_stream_appends(block, first_log_id, epoch)

        # Step 5: apply appends to each stream
        for stream_id, local_values in appends.items():
            apply_stream_appends(stream_id, local_values, epoch)

        # Step 6: advance finalized head only after prior writes succeed
        next_state = MetaState(
            indexed_finalized_head=block.block_num,
            next_log_id=first_log_id + len(block.logs),
            writer_epoch=epoch,
        )
        store_state(next_state, state_version, epoch)

        return {
            "indexed_finalized_head": block.block_num,
            "written_logs": len(block.logs),
        }
```

## Deriving Stream Appends

```python
def collect_stream_appends(block, first_log_id, epoch):
    out = {}  # stream_id -> set(local_ids)
    for i, log in enumerate(block.logs):
        global_log_id = first_log_id + i
        log_shard = high40_bits(global_log_id)
        local_log_id = low24_bits(global_log_id)

        # Always index address at log level
        out[f"addr/{log.address}/{log_shard}"].add(local_log_id)

        if log.topics has at least one item:
            topic0 = log.topics[0]

            # Always index topic0 at log level
            out[f"topic0_log/{topic0}/{log_shard}"].add(local_log_id)

        # Index topic1/topic2/topic3 at log level
        for topic_index in [1, 2, 3]:
            if log has that topic:
                topic = log.topics[topic_index]
                out[f"topic{topic_index}/{topic}/{log_shard}"].add(local_log_id)

    return convert_sets_to_sorted_lists(out)
```

## Tail and Chunk Lifecycle

```python
def apply_stream_appends(stream_id, values, epoch):
    manifest, manifest_version = load_manifest(stream_id)
    tail = load_tail(stream_id)

    for value in values:
        tail.add(value)

    if should_seal(tail, manifest.last_seal_unix_sec, now()):
        chunk_seq = manifest.last_chunk_seq + 1
        chunk_blob = encode_chunk_bitmap(tail)

        blob_store.put(f"chunks/{stream_id}/{chunk_seq}", chunk_blob)

        manifest.chunk_refs.append(
            ChunkRef(
                chunk_seq=chunk_seq,
                min_local=min(tail),
                max_local=max(tail),
                count=len(tail),
            )
        )
        manifest.last_chunk_seq = chunk_seq
        manifest.approx_count += len(tail)
        manifest.last_seal_unix_sec = now()

        store_manifest(stream_id, manifest, manifest_version, epoch)
        tail = empty_bitmap()

    store_tail(stream_id, tail, epoch)


def should_seal(tail, last_seal_time, now_time):
    if tail is empty:
        return False
    if len(tail) >= config.target_entries_per_chunk:
        return True
    if encoded_size_of_tail(tail) >= config.target_chunk_bytes:
        return True
    if now_time - last_seal_time >= config.maintenance_seal_seconds:
        return True
    return False
```

## Topic0 Indexing

```python
topic0_log:
    one bitmap entry per matching log
```

Interpretation:

- `topic0_log` is the exact topic0 filter used by normal indexed queries
- `topic0` now shares the same manifest/tail/chunk lifecycle as the other topic streams

## Query Flow

```python
class QueryEngine:
    def query_finalized(self, meta_store, blob_store, filter, options):
        if filter_has_too_many_or_terms(filter) and config.policy == "error":
            raise QueryTooBroad

        state = meta_store.get("meta/state")
        if state is None:
            return []

        finalized_head = state.indexed_finalized_head

        if filter.block_hash is not None:
            return query_in_block_hash_mode(meta_store, blob_store, filter, options)

        clipped_from, clipped_to = clip_requested_block_range(filter, finalized_head)
        if clipped_from > clipped_to:
            return []

        from_meta = meta_store.get(f"block_meta/{clipped_from}")
        to_meta = meta_store.get(f"block_meta/{clipped_to}")
        if from_meta is None or to_meta is None:
            return []

        from_log_id = from_meta.first_log_id
        to_log_id_inclusive = to_meta.first_log_id + to_meta.count - 1
        if from_log_id > to_log_id_inclusive:
            return []

        force_block_scan = filter_is_too_broad_and_policy_is_block_scan(filter)
        clause_order = [] if force_block_scan else build_clause_order(
            meta_store,
            filter,
            from_log_id,
            to_log_id_inclusive,
        )

        plan = QueryPlan(
            filter=filter,
            options=options,
            clipped_from_block=clipped_from,
            clipped_to_block=clipped_to,
            from_log_id=from_log_id,
            to_log_id_inclusive=to_log_id_inclusive,
            clause_order=clause_order,
            force_block_scan=force_block_scan,
        )

        return execute_plan(meta_store, blob_store, plan)
```

## Planner

```python
def build_clause_order(meta_store, filter, from_log_id, to_log_id):
    estimates = []

    for clause_kind in ["address", "topic1", "topic2", "topic3", "topic0_log"]:
        values = extract_values_for_clause(filter, clause_kind)
        if values is empty:
            continue

        estimated_matches = 0
        for value in values:
            for shard in shards_covered_by(from_log_id, to_log_id):
                stream_id = build_stream_id(clause_kind, value, shard)
                estimated_matches += estimate_stream_overlap(
                    meta_store,
                    stream_id,
                    from_log_id,
                    to_log_id,
                    shard,
                )

        estimates.append((clause_kind, estimated_matches))

    sort estimates by estimated_matches ascending
    return [clause_kind for clause_kind, _ in estimates]


def estimate_stream_overlap(meta_store, stream_id, from_log_id, to_log_id, shard):
    count = 0

    manifest = meta_store.get(f"manifests/{stream_id}")
    if manifest exists:
        if local_from == 0 and local_to == MAX_LOCAL_ID:
            count += manifest.approx_count
        else:
            chunk_refs = overlapping_chunk_refs(manifest.chunk_refs, local_from, local_to)
            for chunk_ref in chunk_refs:
                count += chunk_ref.count

    tail = meta_store.get(f"tails/{stream_id}")
    if tail exists:
        for local_value in tail:
            if local_value is inside requested local range:
                count += 1

    return count
```

The planner does not load every chunk body. It uses metadata to estimate which clause is likely to be most selective.

```python
def overlapping_chunk_refs(chunk_refs, local_from, local_to):
    if local_from == 0 and local_to == MAX_LOCAL_ID:
        return chunk_refs

    start = first index where chunk_ref.max_local >= local_from
    end = first index where chunk_ref.min_local > local_to
    return chunk_refs[start:end]
```

## Executor

```python
def execute_plan(meta_store, blob_store, plan):
    if plan.force_block_scan:
        return execute_block_scan(meta_store, blob_store, plan)

    clause_sets = []

    for clause_kind in plan.clause_order:
        values = extract_values_for_clause(plan.filter, clause_kind)
        candidate_ids = fetch_union_log_level(
            meta_store,
            blob_store,
            clause_kind,
            values,
            plan.from_log_id,
            plan.to_log_id_inclusive,
        )
        clause_sets.append(candidate_ids)

    candidates = intersect_all_sets(clause_sets)
    out = []
    for log_id in candidates:
        log = load_log_by_id(meta_store, blob_store, log_id)
        if log is None:
            continue

        if log.block_num < plan.clipped_from_block:
            continue
        if log.block_num > plan.clipped_to_block:
            continue
        if not exact_match(log, plan.filter):
            continue

        out.append(log)
        if reached_limit(out, plan.options.max_results):
            break

    return sort_logs_by_block_tx_log_position(out)
```

## Loading Stream Entries

```python
def fetch_union_log_level(meta_store, blob_store, clause_kind, values, from_log_id, to_log_id):
    out = set()

    for value in values:
        for shard in shards_covered_by(from_log_id, to_log_id):
            stream_id = build_stream_id(clause_kind, value, shard)
            local_from, local_to = local_range_for_this_shard(from_log_id, to_log_id, shard)
            local_entries = load_stream_entries(
                meta_store,
                blob_store,
                stream_id,
                local_from,
                local_to,
            )

            for local_value in local_entries:
                out.add(make_global_id(shard, local_value))

    return out


def load_stream_entries(meta_store, blob_store, stream_id, local_from, local_to):
    out = set()

    manifest = meta_store.get(f"manifests/{stream_id}")
    if manifest exists:
        chunk_refs = overlapping_chunk_refs(manifest.chunk_refs, local_from, local_to)

        for chunk_ref in chunk_refs:
            chunk = blob_store.get(f"chunks/{stream_id}/{chunk_ref.chunk_seq}")
            for local_value in decode_bitmap(chunk):
                if local_from == 0 and local_to == MAX_LOCAL_ID:
                    out.add(local_value)
                elif local_from <= local_value <= local_to:
                    out.add(local_value)

    tail = meta_store.get(f"tails/{stream_id}")
    if tail exists:
        for local_value in decode_bitmap(tail):
            if local_from == 0 and local_to == MAX_LOCAL_ID:
                out.add(local_value)
            elif local_from <= local_value <= local_to:
                out.add(local_value)

    return out
```

## Loading a Full Log

```python
def load_log_by_id(meta_store, blob_store, log_id):
    page_start = locator_page_start(log_id)
    slot = log_id - page_start

    locator_page = meta_store.get(f"log_locator_pages/{page_start}") or {}
    locator = locator_page.get(slot)

    # Legacy fallback path, not the main path
    if locator is None:
        locator = meta_store.get(f"log_locators/{log_id}")
    if locator is None:
        return None

    blob = blob_store.get(locator.blob_key)
    if blob is None:
        return None

    encoded_log = blob[locator.byte_offset : locator.byte_offset + locator.byte_len]
    return decode_log(encoded_log)
```

## Block-Hash Mode

```python
def query_in_block_hash_mode(meta_store, blob_store, filter, options):
    block_num = meta_store.get(f"block_hash_to_num/{filter.block_hash}")
    if block_num is None:
        raise NotFound

    block_meta = meta_store.get(f"block_meta/{block_num}")
    if block_meta is None or block_meta.block_hash != filter.block_hash:
        raise NotFound

    one_block_plan = QueryPlan(
        filter=filter forced to [block_num, block_num],
        options=options,
        clipped_from_block=block_num,
        clipped_to_block=block_num,
        from_log_id=block_meta.first_log_id,
        to_log_id_inclusive=block_meta.first_log_id + block_meta.count - 1,
        clause_order=[],
        force_block_scan=False,
    )
    return execute_plan(meta_store, blob_store, one_block_plan)
```

## End-to-End Summary

```python
# Ingest summary
receive finalized block
-> validate sequence/finality
-> write packed logs
-> write log locator pages
-> write block metadata
-> update stream tails/chunks/manifests
-> advance finalized head

# Query summary
receive filter
-> resolve block range
-> estimate selective clauses
-> load matching stream entries
-> intersect candidate log IDs
-> fetch full logs by locator
-> run exact predicate checks
-> return ordered results
```

## How to Read This Alongside the Real Code

Use this pseudocode next to:

- `crates/finalized-log-index/src/ingest/engine.rs`
- `crates/finalized-log-index/src/query/engine.rs`
- `crates/finalized-log-index/src/query/planner.rs`
- `crates/finalized-log-index/src/query/executor.rs`

The real Rust code adds:

- async concurrency
- CAS and fencing behavior
- caches for stream state
- degradation/throttling logic
- error handling and codec validation
