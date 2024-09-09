#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};

use super::Block;

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
        // 第一个插入
        let offset = self.data.len() as u16;
        self.add_to_offset(key, value, offset)
    }

    fn cal_block_size(&self) -> usize {
        // Data Section | Offset Section |  Extra |
        self.data.len() + self.offsets.len() * 2 + 2
    }

    fn add_to_offset(&mut self, key: KeySlice, value: &[u8], offset: u16) -> bool {
        // 键值太长
        if key.len() > 0xffff || value.len() > 0xffff {
            return false;
        }
        // 除非第一个键值对超出目标块大小，否则应确保编码后的块大小始终小于或等于target_size。
        // 新增key_len key val_len value offset
        if offset > 0 && self.cal_block_size() + key.len() + value.len() + 4 + 2 > self.block_size {
            return false;
        }
        self.offsets.push(offset);
        let key_len = key.len() as u16;
        let val_len = value.len() as u16;

        self.data
            .extend(vec![((key_len >> 8) & 0xff) as u8, (key_len & 0xff) as u8]);
        self.data.extend(key.into_inner());
        self.data
            .extend(vec![((val_len >> 8) & 0xff) as u8, (val_len & 0xff) as u8]);
        self.data.extend(value.to_vec());
        true
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
