#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::lsm_storage::WriteBatchRecord;
use crate::mvcc::CommittedTxnData;
use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::LsmStorageInner,
};
use anyhow::{bail, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;
use std::sync::atomic::Ordering;
use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(key_hashes) = &self.key_hashes {
            let mut guard = key_hashes.lock();
            guard.0.insert(farmhash::fingerprint32(key));
        }
        if let Some(x) = self.local_storage.get(key) {
            if x.value().is_empty() {
                return Ok(None);
            }
            return Ok(Some(Bytes::copy_from_slice(x.value())));
        }
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        // todo 增加范围锁定
        let old_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        let mut new_iter = TxnLocalIterator::new(
            self.local_storage.clone(),
            |map| map.range((map_bound(lower), map_bound(upper))),
            (Bytes::new(), Bytes::new()),
        );
        new_iter.next()?;
        TxnIterator::create(self.clone(), TwoMergeIterator::create(new_iter, old_iter)?)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if let Some(key_hashes) = &self.key_hashes {
            let mut guard = key_hashes.lock();
            guard.1.insert(farmhash::fingerprint32(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        self.put(key, &[])
    }

    pub fn commit(&self) -> Result<()> {
        let _commit_lock = self.inner.mvcc().commit_lock.lock();
        self.committed.store(true, Ordering::SeqCst);
        // serializable check
        let mut serializable = false;
        if self.inner.options.serializable {
            let guard = self.inner.mvcc().committed_txns.as_ref().lock();
            let (read_set, write_set) = &*self.key_hashes.as_ref().unwrap().lock();
            if !write_set.is_empty() {
                for (_, txn_data) in guard.range(self.read_ts + 1..) {
                    for hash in read_set {
                        if txn_data.key_hashes.contains(hash) {
                            bail!("serializable check failed")
                        }
                    }
                }
            }
            serializable = true;
        }
        let mut batch = vec![];
        for entry in self
            .local_storage
            .range((map_bound(Bound::Unbounded), map_bound(Bound::Unbounded)))
        {
            batch.push(WriteBatchRecord::Put(
                entry.key().clone(),
                entry.value().clone(),
            ));
        }
        let commit_ts = self
            .inner
            .write_batch_inner(&batch)
            .expect("Txn Commit Fail");
        if serializable {
            self.inner.mvcc().committed_txns.lock().insert(
                commit_ts,
                CommittedTxnData {
                    key_hashes: std::mem::take(&mut self.key_hashes.as_ref().unwrap().lock().1),
                    read_ts: self.read_ts,
                    commit_ts,
                },
            );

            // GC
            let watermark = self.inner.mvcc().watermark();
            let commit_txns = &mut *self.inner.mvcc().committed_txns.lock();
            while let Some(entry) = commit_txns.first_entry() {
                if *entry.key() <= watermark {
                    entry.remove();
                } else {
                    break;
                }
            }
        }
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts)
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.with_item(|item| &item.1)
    }

    fn key(&self) -> &[u8] {
        self.with_item(|item| &item.0)
    }

    fn is_valid(&self) -> bool {
        self.with_item(|item| !item.0.is_empty())
    }

    fn next(&mut self) -> Result<()> {
        let nxt_item = {
            self.with_iter_mut(|iter| {
                if let Some(entry) = iter.next() {
                    (entry.key().clone(), entry.value().clone())
                } else {
                    (Bytes::new(), Bytes::from_static(&[]))
                }
            })
        };
        self.with_item_mut(|item| *item = nxt_item);
        Ok(())
    }
}

pub struct TxnIterator {
    _txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = Self {
            _txn: txn.clone(),
            iter,
        };
        iter.move_to_non_delete()?;
        if iter.is_valid() {
            iter.add_to_read_set(iter.key());
        };
        Ok(iter)
    }

    fn move_to_non_delete(&mut self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }

    fn add_to_read_set(&self, key: &[u8]) {
        if let Some(key_hashes) = &self._txn.key_hashes {
            let mut guard = key_hashes.lock();
            guard.0.insert(farmhash::fingerprint32(key));
        }
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.move_to_non_delete()?;
        if self.is_valid() {
            self.add_to_read_set(self.key());
        };
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
