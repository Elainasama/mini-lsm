#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::{Buf, BufMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}
const SIZE_U32: usize = size_of::<u32>();
impl Manifest {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(File::create(_path)?)),
        })
    }

    pub fn recover(_path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut data = Vec::new();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(_path)
            .context("failed to recover manifest")?;

        file.read_to_end(&mut data)?;
        let mut records = Vec::new();
        // for stream in serde_json::Deserializer::from_slice(&data[..]).into_iter::<ManifestRecord>()
        // {
        //     records.push(stream?);
        // }

        // checksum
        let mut buf = &data[..];
        while buf.has_remaining() {
            let len = buf.get_u32();
            let check_sum = (&buf[len as usize - SIZE_U32..len as usize]).get_u32();
            let record_data = &buf[..len as usize - SIZE_U32];
            assert_eq!(
                check_sum,
                crc32fast::hash(record_data),
                "Manifest data corruption!!!"
            );
            records.push(serde_json::from_slice::<ManifestRecord>(record_data)?);
            buf.advance(len as usize);
        }
        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, _record: ManifestRecord) -> Result<()> {
        let mut json = serde_json::to_vec(&_record)?;
        // checksum
        let hash = crc32fast::hash(&json);
        json.put_u32(hash);
        let mut file = self.file.lock();
        file.write_all(&(json.len() as u32).to_be_bytes())?;
        file.write_all(&json)?;
        file.sync_all()?;
        Ok(())
    }
}
