use std::cmp::max;
use std::path::Path;
use std::sync::Arc;

use super::{BlockMeta, FileObject, SsTable};
use crate::key::{KeyVec, TS_MIN};
use crate::table::bloom::Bloom;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};
use anyhow::Result;
use bytes::BufMut;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hash: Vec<u32>,
    max_ts: u64,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hash: Vec::new(),
            max_ts: TS_MIN,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        // 重新建立一个block
        if !self.builder.add(key, value) {
            self.finish_block();
            assert!(self.builder.add(key, value));
            self.first_key = key.to_key_vec();
        }
        self.last_key = key.to_key_vec();
        self.key_hash.push(farmhash::fingerprint32(key.key_ref()));
        self.max_ts = max(self.max_ts, key.ts());
    }

    fn finish_block(&mut self) {
        // 对于空的block不需要写入
        assert!(!self.first_key.is_empty());

        let new_builder = BlockBuilder::new(self.block_size);
        let old_builder = std::mem::replace(&mut self.builder, new_builder);
        let offset = self.data.len();
        let block_data = old_builder.build().encode();
        // checksum
        let hash = crc32fast::hash(&block_data);
        self.data.extend(block_data);
        self.data.put_u32(hash);
        self.meta.push(BlockMeta {
            offset,
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        })
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_block();
        assert!(!self.meta.is_empty());

        // | data block | ... | data block |   metadata   | meta block offset (u32) |
        let block_meta_offset = self.data.len();
        let mut buf = self.data;
        BlockMeta::encode_block_meta(&self.meta, &mut buf, self.max_ts);
        buf.put_u32(block_meta_offset as u32);
        //  add bloom
        let bloom = Bloom::build_from_key_hashes(
            &self.key_hash,
            Bloom::bloom_bits_per_key(self.key_hash.len(), 0.01),
        );
        let bloom_offset = buf.len();
        bloom.encode(&mut buf);
        buf.put_u32(bloom_offset as u32);
        let file = FileObject::create(path.as_ref(), buf)?;

        Ok(SsTable {
            file,
            block_meta_offset,
            id,
            block_cache,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            bloom: Some(bloom),
            max_ts: self.max_ts,
        })
    }
    pub fn is_empty(&self) -> bool {
        self.key_hash.is_empty()
    }
    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
