#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use anyhow::{anyhow, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::vec;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}
const SIZE_U64: usize = std::mem::size_of::<u64>();
const SIZE_U32: usize = std::mem::size_of::<u32>();
const SIZE_U16: usize = std::mem::size_of::<u16>();

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
        max_ts: u64,
    ) {
        // meta_len
        let mut add_size = SIZE_U32;
        let origin_size = buf.len();
        for meta in block_meta {
            // block offset
            add_size += SIZE_U32;
            // first key len
            add_size += SIZE_U16;
            // first key
            add_size += meta.first_key.raw_len();
            // last key len
            add_size += SIZE_U16;
            // last key
            add_size += meta.last_key.raw_len();
        }
        // checksum
        add_size += SIZE_U32;
        // max_ts
        add_size += SIZE_U64;

        // 预分配空间
        buf.reserve(add_size);

        buf.put_u32(block_meta.len() as u32);
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            let first_key_len = meta.first_key.raw_len();
            buf.put_u16(first_key_len as u16);
            buf.extend(meta.first_key.key_ref());
            buf.put_u64(meta.first_key.ts());
            let last_key_len = meta.last_key.raw_len();
            buf.put_u16(last_key_len as u16);
            buf.extend(meta.last_key.key_ref());
            buf.put_u64(meta.last_key.ts());
        }
        buf.put_u64(max_ts);
        // checksum
        let check_sum = crc32fast::hash(&buf[origin_size..]);
        buf.put_u32(check_sum);
        assert_eq!(buf.len() - origin_size, add_size);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: &[u8]) -> (Vec<BlockMeta>, u64) {
        let mut meta_vec = Vec::new();
        // checksum
        let check_sum = (&buf[(buf.remaining() - SIZE_U32)..buf.remaining()]).get_u32();
        assert_eq!(
            check_sum,
            crc32fast::hash(&buf[..(buf.remaining() - SIZE_U32)]),
            "Block meta data corruption!!!"
        );
        let num_meta = buf.get_u32();
        for _ in 0..num_meta {
            let block_offset = buf.get_u32();
            let first_key_len = buf.get_u16();
            let first_key_bytes = buf.copy_to_bytes(first_key_len as usize - SIZE_U64);
            let ts = buf.get_u64();
            let first_key = KeyBytes::from_bytes_with_ts(first_key_bytes, ts);
            let last_key_len = buf.get_u16();
            let last_key_bytes = buf.copy_to_bytes(last_key_len as usize - SIZE_U64);
            let ts = buf.get_u64();
            let last_key = KeyBytes::from_bytes_with_ts(last_key_bytes, ts);
            meta_vec.push(BlockMeta {
                offset: block_offset as usize,
                first_key,
                last_key,
            })
        }
        let max_ts = buf.get_u64();
        (meta_vec, max_ts)
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        // | bloom filter | bloom filter offset |
        let bloom_offset_data = file.read(file.size() - SIZE_U32 as u64, SIZE_U32 as u64)?;
        let bloom_offset = (&bloom_offset_data[..]).get_u32() as u64;
        let bloom_data = file.read(bloom_offset, file.size() - SIZE_U32 as u64 - bloom_offset)?;
        let bloom = Bloom::decode(&bloom_data[..])?;
        // | data block | ... | data block |   metadata   | meta block offset (u32) |
        let block_meta_offset_data = file.read(bloom_offset - SIZE_U32 as u64, SIZE_U32 as u64)?;
        let block_meta_offset = (&block_meta_offset_data[..]).get_u32() as u64;
        let block_meta_data = file.read(
            block_meta_offset,
            bloom_offset - SIZE_U32 as u64 - block_meta_offset,
        )?;
        // metadata add bloom
        let (block_meta, max_ts) = BlockMeta::decode_block_meta(&block_meta_data[..]);
        Ok(Self {
            file,
            block_meta_offset: block_meta_offset as usize,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            id,
            block_cache,
            block_meta,
            bloom: Some(bloom),
            max_ts,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let block_offset = self.block_meta[block_idx].offset;
        let mut end = self.block_meta_offset;
        if block_idx + 1 < self.num_of_blocks() {
            end = self.block_meta[block_idx + 1].offset;
        }
        let data = self
            .file
            .read(block_offset as u64, (end - block_offset) as u64)?;
        // checksum
        let checksum = (&data[data.len() - SIZE_U32..]).get_u32();
        assert_eq!(
            checksum,
            crc32fast::hash(&data[..(data.len() - SIZE_U32)]),
            "Block data corruption!!!"
        );
        Ok(Arc::new(Block::decode(&data[..(data.len() - SIZE_U32)])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let block = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(block)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let mut left = 0;
        let mut right = self.num_of_blocks();
        while left < right {
            let mid = (left + right) / 2;
            if self.block_meta[mid].first_key.as_key_slice() <= key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        // In this idx,The first key > key
        left
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
