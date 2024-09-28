#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();

        let mut compaction_thread = self.compaction_thread.lock();
        let mut flush_thread = self.flush_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread.join().map_err(|e| anyhow!("{:?}", e))?
        }
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread.join().map_err(|e| anyhow!("{:?}", e))?
        }

        // 将剩余的memtable存盘
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }

        while !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // memtables
        if let Some(value) = snapshot.memtable.get(_key) {
            // In the mini-lsm implementation,
            // deletion is represented as a key corresponding to an empty value.
            if value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(value));
        }
        for table in &snapshot.imm_memtables {
            if let Some(value) = table.get(_key) {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        // l0_sst
        for sst_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables.get(sst_id).unwrap().clone();
            if let Some(bloom) = &table.bloom {
                // 键不存在该sst中
                if !bloom.may_contain(farmhash::fingerprint32(_key)) {
                    continue;
                }
            }
            let key = KeySlice::from_slice(_key);
            // 快速判断
            if table.first_key().as_key_slice() <= key && key <= table.last_key().as_key_slice() {
                let iter = SsTableIterator::create_and_seek_to_key(table, key)?;
                if iter.is_valid() && iter.key() == key {
                    if iter.value().is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(Bytes::copy_from_slice(iter.value())));
                }
            }
        }
        {
            // debug
            // 严格有序性
            for level in 0..snapshot.levels.len() {
                for idx in 1..snapshot.levels[level].1.len() {
                    let front = snapshot
                        .sstables
                        .get(&snapshot.levels[level].1[idx - 1])
                        .unwrap();
                    let cur = snapshot
                        .sstables
                        .get(&snapshot.levels[level].1[idx - 1])
                        .unwrap();
                    assert!(front.first_key() <= cur.first_key())
                }
            }
        }
        // l1_sst之后的sst之间严格有序 考虑二分
        for level in 0..snapshot.levels.len() {
            let mut left = 0;
            let mut right = snapshot.levels[level].1.len();
            while left < right {
                let mid = (left + right) / 2;
                let sst_id = &snapshot.levels[level].1[mid];
                let table = snapshot.sstables.get(sst_id).unwrap().clone();
                if table.first_key().as_key_slice() <= KeySlice::from_slice(_key) {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }
            if left > 0 {
                let target_idx = left - 1;
                let sst_id = &snapshot.levels[level].1[target_idx];
                let table = snapshot.sstables.get(sst_id).unwrap().clone();
                if let Some(bloom) = &table.bloom {
                    // 键不存在该sst中
                    if !bloom.may_contain(farmhash::fingerprint32(_key)) {
                        return Ok(None);
                    }
                }
                let key = KeySlice::from_slice(_key);
                let iter = SsTableIterator::create_and_seek_to_key(table, key)?;
                if iter.is_valid() && iter.key() == key {
                    // 压缩后不应该存在删除墓碑
                    assert!(!iter.value().is_empty());
                    return Ok(Some(Bytes::copy_from_slice(iter.value())));
                }
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    pub fn check_over_capacity(&self, _key: &[u8], _value: &[u8]) -> bool {
        let size = self.state.read().memtable.approximate_size();
        size + _key.len() + _value.len() > self.options.target_sst_size
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        // 超过memtable的容量
        if self.check_over_capacity(_key, _value) {
            let state_lock = self.state_lock.lock();
            if self.check_over_capacity(_key, _value) {
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        // 可以支持不可变引用更新
        self.state.read().memtable.put(_key, _value)
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.put(_key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let id = self.next_sst_id();

        let new_table = Arc::new(MemTable::create_with_wal(id, self.path_of_wal(id))?);
        {
            let mut state_lock = self.state.write();
            let mut state = state_lock.as_ref().clone();

            let old_table = std::mem::replace(&mut state.memtable, new_table);
            state.imm_memtables.insert(0, old_table);
            *state_lock = Arc::new(state);
        }
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();
        let flush_table = self
            .state
            .read()
            .imm_memtables
            .last()
            .expect("No imm_memtable!")
            .clone();
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        let sst_id = flush_table.id();
        flush_table.flush(&mut sst_builder)?;
        let sst_table = sst_builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;

        // flush SST表，并且删除flush_table
        {
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();
            state.imm_memtables.pop();
            // Tiered Compaction 不需要 l0层
            if self.compaction_controller.flush_to_l0() {
                state.l0_sstables.insert(0, sst_id);
            } else {
                state.levels.insert(0, (sst_id, vec![sst_id]))
            }

            state.sstables.insert(sst_id, Arc::new(sst_table));
            *guard = Arc::new(state);
        }
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    fn overlap_range(
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        begin: KeySlice,
        end: KeySlice,
    ) -> bool {
        match lower {
            Bound::Included(x) => {
                if end < KeySlice::from_slice(x) {
                    return false;
                }
            }
            Bound::Excluded(x) => {
                if end <= KeySlice::from_slice(x) {
                    return false;
                }
            }
            Bound::Unbounded => {}
        }
        match upper {
            Bound::Included(x) => {
                if begin > KeySlice::from_slice(x) {
                    return false;
                }
            }
            Bound::Excluded(x) => {
                if begin >= KeySlice::from_slice(x) {
                    return false;
                }
            }
            Bound::Unbounded => {}
        }
        true
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        // 这里不上读锁，自身数据结构不可变的Arc可以实现并发的读写。
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        // memtable
        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(_lower, _upper)));

        for memtable in &snapshot.imm_memtables {
            memtable_iters.push(Box::new(memtable.scan(_lower, _upper)));
        }

        // l0-sst
        let mut l0_sst_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for sst_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables.get(sst_id).unwrap().clone();
            if Self::overlap_range(
                _lower,
                _upper,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                let iter = match _lower {
                    Bound::Included(x) => {
                        SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(x))?
                    }
                    Bound::Excluded(x) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            table,
                            KeySlice::from_slice(x),
                        )?;
                        if iter.is_valid() && iter.key() == KeySlice::from_slice(x) {
                            iter.next()?
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
                };

                l0_sst_iters.push(Box::new(iter));
            }
        }

        // level-sst
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for level in 0..snapshot.levels.len() {
            let mut level_sst = Vec::with_capacity(snapshot.levels[level].1.len());
            for id in &snapshot.levels[level].1 {
                level_sst.push(snapshot.sstables.get(id).unwrap().clone());
            }

            level_iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                level_sst,
            )?));
        }
        let two_merge_iter = TwoMergeIterator::create(
            MergeIterator::create(memtable_iters),
            MergeIterator::create(l0_sst_iters),
        )?;

        // 右边界upper通过LsmIterator上层终止,之前的代码并未测试到这里。
        Ok(FusedIterator::new(LsmIterator::new(
            TwoMergeIterator::create(two_merge_iter, MergeIterator::create(level_iters))?,
            map_bound(_upper),
        )?))
    }
}
