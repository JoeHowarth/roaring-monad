use std::collections::{BTreeMap, BTreeSet};

use roaring::RoaringBitmap;

use crate::core::layout::LOCAL_ID_BITS;
use crate::streams::{BitmapBlob, decode_bitmap_blob};

pub fn hex_digit(v: u8) -> char {
    match v {
        0..=9 => (b'0' + v) as char,
        10..=15 => (b'a' + (v - 10)) as char,
        _ => '0',
    }
}

pub fn sharded_stream_id(index_kind: &str, value: &[u8], shard: u64) -> String {
    let shard_hex_width = ((64 - LOCAL_ID_BITS) as usize).div_ceil(4);
    let mut out =
        String::with_capacity(index_kind.len() + 1 + value.len() * 2 + 1 + shard_hex_width);
    out.push_str(index_kind);
    out.push('/');
    for byte in value {
        out.push(hex_digit((byte >> 4) & 0xf));
        out.push(hex_digit(byte & 0xf));
    }
    out.push('/');
    out.push_str(&format!("{:0width$x}", shard, width = shard_hex_width));
    out
}

pub fn parse_stream_shard(stream_id: &str) -> Option<u64> {
    let (_, shard_hex) = stream_id.rsplit_once('/')?;
    u64::from_str_radix(shard_hex, 16).ok()
}

pub fn page_start_local(local_id: u32, page_span: u32) -> u32 {
    (local_id / page_span) * page_span
}

pub fn group_stream_values_into_pages(
    values: impl IntoIterator<Item = (String, u32)>,
    page_span: u32,
) -> BTreeMap<String, BTreeMap<u32, RoaringBitmap>> {
    let mut grouped = BTreeMap::<String, BTreeSet<u32>>::new();
    for (stream, value) in values {
        grouped.entry(stream).or_default().insert(value);
    }

    let mut pages = BTreeMap::<String, BTreeMap<u32, RoaringBitmap>>::new();
    for (stream, values) in grouped {
        let stream_pages = pages.entry(stream).or_default();
        for value in values {
            let start = page_start_local(value, page_span);
            stream_pages.entry(start).or_default().insert(value);
        }
    }
    pages
}

pub fn overlaps(min_local: u32, max_local: u32, local_from: u32, local_to: u32) -> bool {
    min_local <= local_to && max_local >= local_from
}

pub fn merge_bitmap_bytes_into(
    bytes: &[u8],
    out: &mut RoaringBitmap,
    local_from: u32,
    local_to: u32,
    full_range: bool,
) -> crate::Result<bool> {
    let bitmap_blob = decode_bitmap_blob(bytes)?;
    if !overlaps(
        bitmap_blob.min_local,
        bitmap_blob.max_local,
        local_from,
        local_to,
    ) {
        return Ok(false);
    }
    if full_range || (bitmap_blob.min_local >= local_from && bitmap_blob.max_local <= local_to) {
        *out |= &bitmap_blob.bitmap;
        return Ok(true);
    }
    for value in bitmap_blob.bitmap {
        if value >= local_from && value <= local_to {
            out.insert(value);
        }
    }
    Ok(true)
}

pub fn compacted_bitmap_blob(bitmap: RoaringBitmap, page_start: u32) -> Option<(u32, BitmapBlob)> {
    if bitmap.is_empty() {
        return None;
    }
    let count = bitmap.len() as u32;
    let min_local = bitmap.min().unwrap_or(page_start);
    let max_local = bitmap.max().unwrap_or(page_start);
    Some((
        count,
        BitmapBlob {
            min_local,
            max_local,
            count,
            crc32: 0,
            bitmap,
        },
    ))
}
