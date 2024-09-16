#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use super::Block;
use crate::key::{Key, KeySlice, KeyVec};
use bytes::Buf;
use std::sync::Arc;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

const SIZE_U16: usize = std::mem::size_of::<u16>();
impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut this = BlockIterator::new(block);
        this.seek_to_first();
        this
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut this = BlockIterator::new(block);
        this.seek_to_key(key);
        this
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        if !self.is_valid() {
            return &[];
        }
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.next();
    }

    fn update_key_value(&mut self, offset: u16) {
        let mut offset = offset as usize;
        // let key_len = (&self.block.data[offset..offset + SIZE_U16]).get_u16();
        // offset += 2;
        // self.key = Key::from_vec(self.block.data[(offset)..(offset + key_len as usize)].to_vec());

        // key_overlap_len (u16) | rest_key_len (u16) | key (rest_key_len)
        let key_overlap_len = (&self.block.data[offset..offset + SIZE_U16]).get_u16();
        offset += SIZE_U16;
        let key_rest_len = (&self.block.data[offset..offset + SIZE_U16]).get_u16();
        offset += SIZE_U16;
        let mut key = self.first_key.raw_ref()[..key_overlap_len as usize].to_vec();
        key.extend(self.block.data[offset..offset + key_rest_len as usize].to_vec());
        self.key = Key::from_vec(key);
        offset += key_rest_len as usize;
        let val_len = (&self.block.data[offset..offset + SIZE_U16]).get_u16();
        offset += SIZE_U16;
        self.value_range = (offset, offset + val_len as usize);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx >= self.block.offsets.len() {
            self.key.clear();
            return;
        }

        self.update_key_value(self.block.offsets[self.idx]);
        if self.idx == 0 {
            self.first_key = self.key.clone();
        }
        self.idx += 1;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // 重置为零开始寻找
        self.seek_to_first();
        while self.is_valid() && self.key() < key {
            self.next();
        }
    }
}
