use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::table::SsTableIterator;
use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};
use anyhow::{bail, Result};
use bytes::Bytes;
use std::ops::Bound;

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end: Bound<Bytes>,
    pre_key: Vec<u8>,
    read_ts: u64,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end: Bound<Bytes>, ts: u64) -> Result<Self> {
        let mut this = Self {
            inner: iter,
            end,
            pre_key: vec![],
            read_ts: ts,
        };
        this.move_to_nxt_key()?;
        Ok(this)
    }

    fn move_to_nxt_key(&mut self) -> Result<()> {
        // 忽略已经删除的键值对,最开始的值也可能是被删除的。
        // 跳过超越read_ts时间戳的键
        loop {
            while self.inner.is_valid()
                && (self.inner.key().ts() > self.read_ts
                    || self.inner.key().key_ref() == self.pre_key)
            {
                self.inner.next()?;
            }
            if !self.inner.is_valid() {
                break;
            }
            self.pre_key.clear();
            self.pre_key.extend(self.inner.key().key_ref());
            if self.inner.value().is_empty() {
                continue;
            }
            break;
        }
        Ok(())
    }

    fn is_over_end(&self) -> bool {
        match &self.end {
            Bound::Included(x) => self.key() > x,
            Bound::Excluded(x) => self.key() >= x,
            Bound::Unbounded => false,
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn is_valid(&self) -> bool {
        self.inner.is_valid() && !self.is_over_end()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        self.move_to_nxt_key()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("Access an invalid iterator!");
        }
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if !self.is_valid() {
            panic!("Access an invalid iterator!");
        }
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("The next method has encountered an error!")
        }
        match self.iter.next() {
            Ok(_) => Ok(()),
            Err(e) => {
                self.has_errored = true;
                Err(e)
            }
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
