#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    pub(crate) map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Create a bound of `KeyBytes` from a bound of `KeySlice`.
pub(crate) fn map_key_slice_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Excluded(x) => Bound::Excluded(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Create a bound of `KeyBytes` from a bound of `Bytes`.
pub(crate) fn map_bytes_bound_with_ts(bound: Bound<&[u8]>, ts: u64) -> Bound<KeySlice> {
    match bound {
        Bound::Included(x) => Bound::Included(KeySlice::from_slice(x, ts)),
        Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice(x, ts)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(_id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(Wal::create(_path)?),
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        let map = Arc::new(SkipMap::new());
        let wal = Wal::recover(_path.as_ref(), &map)?;
        Ok(Self {
            map,
            wal: Some(wal),
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::from_slice(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(
            map_bytes_bound_with_ts(lower, TS_DEFAULT),
            map_bytes_bound_with_ts(upper, TS_DEFAULT),
        )
    }

    /// Get a value by key.
    pub fn get(&self, _key: KeySlice) -> Option<Bytes> {
        let entry = self.map.get(&KeyBytes::from_bytes_with_ts(
            Bytes::from_static(unsafe { std::mem::transmute::<&[u8], &[u8]>(_key.key_ref()) }),
            _key.ts(),
        ))?;
        Some(entry.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, _key: KeySlice, _value: &[u8]) -> Result<()> {
        self.map.insert(
            KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(_key.key_ref()), _key.ts()),
            Bytes::from(_value.to_vec()),
        );
        self.approximate_size.fetch_add(
            _key.raw_len() + _value.len(),
            std::sync::atomic::Ordering::SeqCst,
        );
        if let Some(wal) = &self.wal {
            wal.put(_key, _value)?;
            wal.sync()?;
        }
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, _lower: Bound<KeySlice>, _upper: Bound<KeySlice>) -> MemTableIterator {
        let (lower, upper) = (map_key_slice_bound(_lower), map_key_slice_bound(_upper));
        let mut iter = MemTableIterator::new(
            self.map.clone(),
            |map| map.range((lower, upper)),
            (KeyBytes::new(), Bytes::new()),
        );
        iter.next().unwrap();
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, _builder: &mut SsTableBuilder) -> Result<()> {
        let mut iter = self.scan(Bound::Unbounded, Bound::Unbounded);
        while iter.is_valid() {
            _builder.add(iter.key(), iter.value());
            iter.next()?
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl MemTableIterator {
    fn next_item(&mut self) -> Result<(KeyBytes, Bytes)> {
        Ok(self.with_iter_mut(|iter| {
            if let Some(entry) = iter.next() {
                (entry.key().clone(), entry.value().clone())
            } else {
                (KeyBytes::new(), Bytes::from_static(&[]))
            }
        }))
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.with_item(|item| &item.1)
    }

    fn key(&self) -> KeySlice {
        self.with_item(|item| item.0.as_key_slice())
    }

    fn is_valid(&self) -> bool {
        self.with_item(|item| !item.0.is_empty())
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.next_item()?;
        self.with_item_mut(|item| {
            *item = entry;
        });
        Ok(())
    }
}
