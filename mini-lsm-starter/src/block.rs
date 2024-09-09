#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut data = self.data.clone();
        let offsets = self.offsets.clone();
        for offset in offsets {
            data.put_u16(offset);
        }
        data.put_u16(self.offsets.len() as u16);
        data.into()
    }

    // 获取字段长度u16
    pub fn get_u16(data: &[u8], idx: usize) -> u16 {
        let high = data[idx];
        let low = data[idx + 1];
        ((high as u16) << 8) + low as u16
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let mut offsets = Vec::new();
        let mut cur_idx = data.len() - 2;
        // 获取元素总长度
        let num_entry = Self::get_u16(data, cur_idx);
        dbg!(num_entry);
        for _ in 0..num_entry {
            cur_idx -= 2;
            offsets.push(Self::get_u16(data, cur_idx));
        }
        offsets.reverse();
        Self {
            data: data[0..cur_idx].to_vec(),
            offsets,
        }
    }
}
