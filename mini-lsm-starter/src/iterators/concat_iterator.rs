use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }
        let mut sstables = sstables;
        sstables.sort_by(|a, b| a.first_key().cmp(b.first_key()));
        Ok(Self {
            current: Some(SsTableIterator::create_and_seek_to_first(
                sstables[0].clone(),
            )?),
            next_sst_idx: 0,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut sstables = sstables;
        sstables.sort_by(|a, b| a.first_key().cmp(b.first_key()));

        let mut start_idx = 0;
        for table in &sstables {
            if table.last_key().as_key_slice() >= key {
                break;
            }
            start_idx += 1;
        }
        if start_idx >= sstables.len() {
            return Ok(Self {
                current: None,
                next_sst_idx: start_idx,
                sstables,
            });
        }
        Ok(Self {
            current: Some(SsTableIterator::create_and_seek_to_key(
                sstables[start_idx].clone(),
                key,
            )?),
            next_sst_idx: start_idx,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        match &self.current {
            None => &[],
            Some(iter) => iter.value(),
        }
    }

    fn key(&self) -> KeySlice {
        match &self.current {
            None => KeySlice::default(),
            Some(iter) => iter.key(),
        }
    }

    fn is_valid(&self) -> bool {
        self.next_sst_idx != self.sstables.len()
    }

    fn next(&mut self) -> Result<()> {
        if self.is_valid() {
            if let Some(iter) = &mut self.current {
                iter.next()?;
                if !iter.is_valid() {
                    self.next_sst_idx += 1;
                    if self.is_valid() {
                        self.current = Some(SsTableIterator::create_and_seek_to_first(
                            self.sstables[self.next_sst_idx].clone(),
                        )?)
                    } else {
                        self.current = None;
                    }
                }
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
