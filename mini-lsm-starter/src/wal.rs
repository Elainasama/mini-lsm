#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};
use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

const SIZE_U16: usize = std::mem::size_of::<u16>();
const SIZE_U32: usize = std::mem::size_of::<u32>();

const SIZE_U64: usize = std::mem::size_of::<u64>();
impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        let file = File::create(_path)?;
        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(_path)
            .context("failed to recover wal")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut data = &buf[..];
        while data.has_remaining() {
            let batch_size = data.get_u32();
            // checksum
            let mut hasher: Vec<u8> = Vec::new();
            hasher.put_u32(batch_size);
            for _ in 0..batch_size {
                let key_len = data.get_u16();
                let key = &data[..key_len as usize];
                data.advance(key_len as usize);
                let ts = data.get_u64();
                let val_len = data.get_u16();
                let val = &data[..val_len as usize];
                data.advance(val_len as usize);

                hasher.put_u16(key_len);
                hasher.put_slice(key);
                hasher.put_u64(ts);
                hasher.put_u16(val_len);
                hasher.put_slice(val);

                _skiplist.insert(
                    KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(key), ts),
                    Bytes::copy_from_slice(val),
                );
            }
            let hash = data.get_u32();
            assert_eq!(hash, crc32fast::hash(&hasher), "Wal data corruption!!!");
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, _key: KeySlice, _value: &[u8]) -> Result<()> {
        let file = self.file.lock();
        let mut buf = Vec::with_capacity(2 * SIZE_U16 + _key.raw_len() + _value.len() + SIZE_U32);
        buf.put_u16(_key.key_len() as u16);
        buf.put_slice(_key.key_ref());
        buf.put_u64(_key.ts());
        buf.put_u16(_value.len() as u16);
        buf.put_slice(_value);
        // checksum
        let hash = crc32fast::hash(&buf);
        buf.put_u32(hash);
        file.get_ref().write_all(&buf)?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        let file = self.file.lock();
        // add batch_size
        let mut add_size = 2 * SIZE_U32;
        for (k, v) in _data.iter() {
            add_size += 2 * SIZE_U16 + k.raw_len() + v.len()
        }
        let mut buf = Vec::with_capacity(add_size);
        buf.put_u32(_data.len() as u32);
        for &(k, v) in _data.iter() {
            buf.put_u16(k.key_len() as u16);
            buf.put_slice(k.key_ref());
            buf.put_u64(k.ts());
            buf.put_u16(v.len() as u16);
            buf.put_slice(v);
        }
        // checksum
        let hash = crc32fast::hash(&buf);
        buf.put_u32(hash);
        assert_eq!(buf.len(), add_size);
        file.get_ref().write_all(&buf)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
