use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;

use super::{Block, SIZE_U16, SIZE_U64};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let offset = self.data.len() as u16;
        self.add_to_offset(key, value, offset)
    }

    fn cal_block_size(&self) -> usize {
        // Data Section | Offset Section |  Extra |
        self.data.len() + self.offsets.len() * 2 + 2
    }

    fn add_to_offset(&mut self, key: KeySlice, value: &[u8], offset: u16) -> bool {
        // 键值太长
        if key.raw_len() > 0xffff || value.len() > 0xffff {
            return false;
        }
        // 除非第一个键值对超出目标块大小，否则应确保编码后的块大小始终小于或等于target_size。
        // 新增key_len key val_len value offset

        // key_overlap_len (u16) | rest_key_len (u16) | key (rest_key_len) | timestamp (u64)
        let key_len = key.key_len() as u16;
        let val_len = value.len() as u16;
        let key_overlap_len = self.key_overlap_len(key);

        let key_rest_len = key_len - key_overlap_len as u16;
        if offset > 0
            && self.cal_block_size()
                + key_rest_len as usize
                + value.len()
                + 3 * SIZE_U16
                + SIZE_U16
                + SIZE_U64
                > self.block_size
        {
            return false;
        }
        self.offsets.push(offset);

        self.data.put_u16(key_overlap_len as u16);
        self.data.put_u16(key_len - key_overlap_len as u16);
        self.data.extend(key.key_ref()[key_overlap_len..].to_vec());
        // 3.1 mvcc add timestamp
        self.data.put_u64(key.ts());
        self.data.put_u16(val_len);
        self.data.extend(value.to_vec());
        // 第一个插入
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        true
    }

    fn key_overlap_len(&mut self, key: KeySlice) -> usize {
        if self.first_key.is_empty() {
            return 0;
        }
        let mut l = 0;

        while l < key.key_len() && l < self.first_key.key_len() {
            if key.key_ref()[l] != self.first_key.key_ref()[l] {
                break;
            }
            l += 1;
        }
        l
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
