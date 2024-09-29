#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn create_new_sst(&self, sst: Vec<Arc<SsTable>>, is_bottom: bool) -> Result<Vec<Arc<SsTable>>> {
        let mut sst_iters = Vec::with_capacity(sst.len());
        for table in sst {
            sst_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(table)?))
        }
        let mut iter = MergeIterator::create(sst_iters);
        let mut new_sst = Vec::new();
        let mut builder = SsTableBuilder::new(self.options.block_size);
        while iter.is_valid() {
            // 只有最后一层可以删除delete墓碑。
            if !is_bottom || !iter.value().is_empty() {
                let key = iter.key();
                let value = iter.value();
                builder.add(key, value);
                if builder.estimated_size() > self.options.target_sst_size {
                    let old_builder = std::mem::replace(
                        &mut builder,
                        SsTableBuilder::new(self.options.block_size),
                    );
                    let id = self.next_sst_id();
                    new_sst.push(Arc::new(old_builder.build(
                        id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(id),
                    )?));
                }
            }
            iter.next()?
        }
        // 最后一块sst
        if !builder.is_empty() {
            let id = self.next_sst_id();
            new_sst.push(Arc::new(builder.build(
                id,
                Some(self.block_cache.clone()),
                self.path_of_sst(id),
            )?));
        }
        Ok(new_sst)
    }
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut sst = Vec::with_capacity(l0_sstables.len() + l1_sstables.len());
                {
                    let read_lock = self.state.read();
                    for sst_id in l0_sstables {
                        sst.push(read_lock.sstables.get(sst_id).unwrap().clone());
                    }
                    for sst_id in l1_sstables {
                        sst.push(read_lock.sstables.get(sst_id).unwrap().clone());
                    }
                }

                self.create_new_sst(sst, true)
            }
            CompactionTask::Simple(task) => {
                let mut sst = Vec::with_capacity(
                    task.lower_level_sst_ids.len() + task.upper_level_sst_ids.len(),
                );
                {
                    let read_lock = self.state.read();
                    for sst_id in &task.upper_level_sst_ids {
                        sst.push(read_lock.sstables.get(sst_id).unwrap().clone());
                    }
                    for sst_id in &task.lower_level_sst_ids {
                        sst.push(read_lock.sstables.get(sst_id).unwrap().clone());
                    }
                }
                println!(
                    "Compact completed level {},up_sst_len {} low_sst_len {}",
                    task.lower_level,
                    task.upper_level_sst_ids.len(),
                    task.lower_level_sst_ids.len()
                );
                self.create_new_sst(sst, task.is_lower_level_bottom_level)
            }
            CompactionTask::Tiered(task) => {
                let mut sst = Vec::new();
                {
                    let read_lock = self.state.read();
                    for level in task.tiers.iter() {
                        for sst_id in level.1.iter() {
                            sst.push(read_lock.sstables.get(sst_id).unwrap().clone());
                        }
                    }
                }
                self.create_new_sst(sst, task.bottom_tier_included)
            }
            CompactionTask::Leveled(task) => {
                let mut sst = Vec::with_capacity(
                    task.lower_level_sst_ids.len() + task.upper_level_sst_ids.len(),
                );
                {
                    let read_lock = self.state.read();
                    for sst_id in &task.upper_level_sst_ids {
                        sst.push(read_lock.sstables.get(sst_id).unwrap().clone());
                    }
                    for sst_id in &task.lower_level_sst_ids {
                        sst.push(read_lock.sstables.get(sst_id).unwrap().clone());
                    }
                }
                self.create_new_sst(sst, task.is_lower_level_bottom_level)
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let ssts_to_compact = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: ssts_to_compact.0.clone(),
            l1_sstables: ssts_to_compact.1.clone(),
        };
        let new_sst = self.compact(&task)?;
        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();
            let mut to_remove =
                Vec::with_capacity(ssts_to_compact.0.len() + ssts_to_compact.1.len());
            for id in ssts_to_compact.0.iter() {
                state.sstables.remove(id);
                to_remove.push(*id);
            }
            for id in ssts_to_compact.1.iter() {
                state.sstables.remove(id);
                to_remove.push(*id);
            }

            state
                .l0_sstables
                .retain(|&x| state.sstables.contains_key(&x));
            state.levels[0]
                .1
                .retain(|&x| state.sstables.contains_key(&x));
            let mut new_sst_id = Vec::with_capacity(new_sst.len());
            for sst in &new_sst {
                state.levels[0].1.push(sst.sst_id());
                new_sst_id.push(sst.sst_id());
                state.sstables.insert(sst.sst_id(), sst.clone());
            }
            *self.state.write() = Arc::new(state);
            self.sync_dir()?;
            // manifest
            if let Some(manifest) = &self.manifest {
                manifest.add_record(&_state_lock, ManifestRecord::Compaction(task, new_sst_id))?
            }
        }
        // 删除压缩前文件
        for sst_id in &ssts_to_compact.0 {
            std::fs::remove_file(self.path_of_sst(*sst_id))?
        }
        for sst_id in &ssts_to_compact.1 {
            std::fs::remove_file(self.path_of_sst(*sst_id))?
        }
        self.sync_dir()?;
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = self.state.read().as_ref().clone();
        if let Some(compaction_task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        {
            let new_sst = self.compact(&compaction_task)?;
            let to_remove = {
                let _state_lock = self.state_lock.lock();
                let mut state = self.state.write();
                let mut output = Vec::with_capacity(new_sst.len());
                // 这里一定要读取最新的状态
                let mut snapshot = state.as_ref().clone();
                for table in new_sst.iter() {
                    output.push(table.sst_id());
                    // For level Compaction 前置
                    snapshot.sstables.insert(table.sst_id(), table.clone());
                }

                let (mut new_state, to_remove) = self
                    .compaction_controller
                    .apply_compaction_result(&snapshot, &compaction_task, &output[..], false);
                for sst_id in &to_remove {
                    new_state.sstables.remove(sst_id);
                }
                {
                    // debug
                    println!(
                        "Compact completed,{} files remove,{} files add,output {:?}",
                        to_remove.len(),
                        output.len(),
                        output
                    );
                    for level in new_state.levels.iter() {
                        println!("level {} sst {:?}", level.0, level.1);
                    }
                }
                *state = Arc::new(new_state);
                self.sync_dir()?;
                // manifest
                if let Some(manifest) = &self.manifest {
                    manifest.add_record(
                        &_state_lock,
                        ManifestRecord::Compaction(compaction_task, output),
                    )?
                }
                to_remove
            };
            for sst_id in to_remove.iter() {
                std::fs::remove_file(self.path_of_sst(*sst_id))?;
            }
            self.sync_dir()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let table_num = {
            let guard = self.state.read();
            1 + guard.imm_memtables.len()
        };
        if table_num > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
