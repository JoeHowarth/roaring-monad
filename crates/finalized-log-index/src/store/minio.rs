use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Region};
use bytes::Bytes;
use tokio::time::{Duration, sleep};

use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, Page};

#[derive(Clone)]
pub struct MinioBlobStore {
    client: Client,
    bucket: String,
    object_prefix: String,
    max_retries: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
}

impl MinioBlobStore {
    pub async fn new(
        endpoint: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        bucket: &str,
        object_prefix: &str,
    ) -> Result<Self> {
        let creds = Credentials::new(access_key, secret_key, None, None, "static");
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .credentials_provider(creds)
            .region(Region::new(region.to_string()))
            .load()
            .await;

        let conf = S3ConfigBuilder::from(&shared)
            .endpoint_url(endpoint)
            .force_path_style(true)
            .build();
        let client = Client::from_conf(conf);

        // Ensure bucket exists (idempotent best-effort).
        let head = client.head_bucket().bucket(bucket).send().await;
        if head.is_err() {
            let _ = client.create_bucket().bucket(bucket).send().await;
        }

        Ok(Self {
            client,
            bucket: bucket.to_string(),
            object_prefix: normalize_prefix(object_prefix),
            max_retries: 4,
            base_delay_ms: 25,
            max_delay_ms: 1000,
        })
    }

    pub fn with_retry_policy(
        mut self,
        max_retries: u32,
        base_delay_ms: u64,
        max_delay_ms: u64,
    ) -> Self {
        self.max_retries = max_retries;
        self.base_delay_ms = base_delay_ms;
        self.max_delay_ms = max_delay_ms;
        self
    }

    fn object_key(&self, key: &[u8]) -> String {
        let group = extract_group(key);
        format!("{}{}/{}", self.object_prefix, group, hex(key))
    }

    fn object_list_prefix(&self, prefix: &[u8]) -> String {
        if prefix.is_empty() {
            return self.object_prefix.clone();
        }
        format!("{}{}", self.object_prefix, extract_group(prefix))
    }
}

#[async_trait::async_trait]
impl BlobStore for MinioBlobStore {
    async fn put_blob(&self, key: &[u8], value: Bytes) -> Result<()> {
        let object_key = self.object_key(key);
        let payload = value.to_vec();
        self.with_retry("put_blob", || async {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&object_key)
                .body(payload.clone().into())
                .send()
                .await
                .map_err(|e| Error::Backend(format!("minio put_blob: {e}")))?;
            Ok(())
        })
        .await
    }

    async fn get_blob(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let object_key = self.object_key(key);
        self.with_retry("get_blob", || async {
            let res = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&object_key)
                .send()
                .await;

            let out = match res {
                Ok(resp) => {
                    let aggregated = resp
                        .body
                        .collect()
                        .await
                        .map_err(|e| Error::Backend(format!("minio read body: {e}")))?;
                    Some(aggregated.into_bytes())
                }
                Err(err) => {
                    let msg = err.to_string();
                    if msg.contains("NoSuchKey") || msg.contains("not found") {
                        None
                    } else {
                        return Err(Error::Backend(format!("minio get_blob: {err}")));
                    }
                }
            };

            Ok(out)
        })
        .await
    }

    async fn delete_blob(&self, key: &[u8]) -> Result<()> {
        let _ = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(self.object_key(key))
            .send()
            .await;
        Ok(())
    }

    async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        let mut req = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(self.object_list_prefix(prefix))
            .max_keys(limit as i32);

        if let Some(c) = cursor
            && !c.is_empty()
        {
            let token = String::from_utf8_lossy(&c).to_string();
            req = req.continuation_token(token);
        }

        let resp = self
            .with_retry("list_prefix", || async {
                req.clone()
                    .send()
                    .await
                    .map_err(|e| Error::Backend(format!("minio list_prefix: {e}")))
            })
            .await?;

        let mut keys = Vec::new();
        for obj in resp.contents() {
            let Some(k) = obj.key() else {
                continue;
            };
            if let Some(raw) = decode_object_key(k, &self.object_prefix)
                && raw.starts_with(prefix)
            {
                keys.push(raw);
            }
        }

        Ok(Page {
            keys,
            next_cursor: resp
                .next_continuation_token()
                .map(|t| t.as_bytes().to_vec()),
        })
    }
}

impl MinioBlobStore {
    async fn with_retry<T, F, Fut>(&self, _op: &str, mut f: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: core::future::Future<Output = Result<T>>,
    {
        let mut attempt: u32 = 0;
        loop {
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if attempt >= self.max_retries || !is_retryable_backend_error(&e) {
                        return Err(e);
                    }
                    let backoff =
                        compute_backoff_ms(attempt, self.base_delay_ms, self.max_delay_ms);
                    sleep(Duration::from_millis(backoff)).await;
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    }
}

fn compute_backoff_ms(attempt: u32, base_ms: u64, max_ms: u64) -> u64 {
    let factor = 1u64 << core::cmp::min(attempt, 8);
    core::cmp::min(base_ms.saturating_mul(factor), max_ms)
}

fn is_retryable_backend_error(err: &Error) -> bool {
    let Error::Backend(msg) = err else {
        return false;
    };
    let s = msg.to_ascii_lowercase();
    s.contains("timeout")
        || s.contains("temporar")
        || s.contains("connection")
        || s.contains("reset")
        || s.contains("refused")
        || s.contains("unavailable")
        || s.contains("throttl")
        || s.contains("503")
        || s.contains("500")
}

fn normalize_prefix(p: &str) -> String {
    if p.is_empty() {
        String::new()
    } else if p.ends_with('/') {
        p.to_string()
    } else {
        format!("{p}/")
    }
}

fn decode_object_key(path: &str, configured_prefix: &str) -> Option<Vec<u8>> {
    let full = if configured_prefix.is_empty() {
        path
    } else {
        path.strip_prefix(configured_prefix)?
    };
    let mut parts = full.split('/');
    let _group = parts.next()?;
    let hex_key = parts.next()?;
    unhex(hex_key).ok()
}

fn extract_group(key: &[u8]) -> String {
    let mut out = String::new();
    for &b in key {
        if b == b'/' {
            break;
        }
        if b.is_ascii_alphanumeric() || b == b'_' || b == b'-' {
            out.push(char::from(b));
        } else {
            break;
        }
    }
    if out.is_empty() {
        "misc".to_string()
    } else {
        out
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(nibble((b >> 4) & 0xf));
        out.push(nibble(b & 0xf));
    }
    out
}

fn unhex(s: &str) -> core::result::Result<Vec<u8>, ()> {
    if !s.len().is_multiple_of(2) {
        return Err(());
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0usize;
    while i < bytes.len() {
        let h = from_nibble(bytes[i]).ok_or(())?;
        let l = from_nibble(bytes[i + 1]).ok_or(())?;
        out.push((h << 4) | l);
        i += 2;
    }
    Ok(out)
}

fn nibble(v: u8) -> char {
    match v {
        0..=9 => (b'0' + v) as char,
        10..=15 => (b'a' + (v - 10)) as char,
        _ => '0',
    }
}

fn from_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(10 + b - b'a'),
        b'A'..=b'F' => Some(10 + b - b'A'),
        _ => None,
    }
}
