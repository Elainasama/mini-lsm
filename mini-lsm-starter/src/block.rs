#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

const SIZE_U16: usize = std::mem::size_of::<u16>();
const SIZE_U64: usize = std::mem::size_of::<u64>();
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

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        // 获取元素总长度
        let num_entry = (&data[data.len() - SIZE_U16..]).get_u16();
        let data_end = data.len() - SIZE_U16 - SIZE_U16 * num_entry as usize;
        let offsets = data[data_end..data.len() - SIZE_U16]
            .chunks(SIZE_U16)
            .map(|mut x| x.get_u16())
            .collect();
        Self {
            data: data[0..data_end].to_vec(),
            offsets,
        }
    }
}
