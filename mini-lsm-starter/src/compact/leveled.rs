use crate::key::KeyBytes;
use crate::lsm_storage::LsmStorageState;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        let target_sst_ids = &_snapshot.levels[_in_level - 1].1;
        let first_key = _snapshot.sstables.get(&_sst_ids[0]).unwrap().first_key();
        let last_key = _snapshot.sstables.get(&_sst_ids[0]).unwrap().last_key();
        target_sst_ids[self.find_first_key_overlapping_idx(first_key, _snapshot, target_sst_ids)
            ..self.find_last_key_overlapping_idx(last_key, _snapshot, target_sst_ids)]
            .to_vec()
    }
    fn find_first_key_overlapping_idx(
        &self,
        key: &KeyBytes,
        _snapshot: &LsmStorageState,
        target_sst_ids: &[usize],
    ) -> usize {
        let mut left = 0;
        let mut right = target_sst_ids.len();
        while left < right {
            let mid = (left + right) / 2;
            let table = _snapshot.sstables.get(&target_sst_ids[mid]).unwrap();
            if table.last_key() < key {
                left = mid + 1
            } else {
                right = mid;
            }
        }
        left
    }

    fn find_last_key_overlapping_idx(
        &self,
        key: &KeyBytes,
        _snapshot: &LsmStorageState,
        target_sst_ids: &[usize],
    ) -> usize {
        let mut left = 0;
        let mut right = target_sst_ids.len();
        while left < right {
            let mid = (left + right) / 2;
            let table = _snapshot.sstables.get(&target_sst_ids[mid]).unwrap();
            if table.first_key() <= key {
                left = mid + 1
            } else {
                right = mid;
            }
        }
        left
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let mut real_level_sizes = Vec::with_capacity(self.options.max_levels);
        for level in &_snapshot.levels {
            real_level_sizes.push(
                level
                    .1
                    .iter()
                    .map(|x| _snapshot.sstables.get(x).unwrap().table_size())
                    .sum::<u64>(),
            );
        }
        println!("level_size {:?}", real_level_sizes);
        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            // 跳过中间空白层，找到最近层。
            let mut level_size_threshold = real_level_sizes[self.options.max_levels - 1];
            for level_idx in (0..self.options.max_levels).rev() {
                if level_idx == 0
                    || level_size_threshold < (self.options.base_level_size_mb * 1024 * 1024) as u64
                {
                    return Some(LeveledCompactionTask {
                        upper_level: None,
                        upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                        lower_level: level_idx + 1,
                        lower_level_sst_ids: _snapshot.levels[level_idx].1.clone(),
                        is_lower_level_bottom_level: level_idx == self.options.max_levels - 1,
                    });
                }
                level_size_threshold /= self.options.level_size_multiplier as u64;
            }
        }
        // 检查每一层是否超过限制
        let mut level_size_threshold = real_level_sizes[self.options.max_levels - 1];

        let mut compression_level_id = 0;
        let mut compression_score = 0;

        for level in (0..self.options.max_levels).rev() {
            let level_size = &real_level_sizes[level];
            if level_size_threshold > 0
                && level_size_threshold < *level_size
                && level_size / level_size_threshold > compression_score
            {
                compression_score = level_size / level_size_threshold;
                compression_level_id = level + 1;
            }
            if level_size_threshold >= self.options.base_level_size_mb as u64 * 1024 * 1024 {
                level_size_threshold /= self.options.level_size_multiplier as u64;
            } else {
                level_size_threshold = 0;
            }
        }

        if compression_level_id != 0 {
            let mi_sst_id = _snapshot.levels[compression_level_id - 1]
                .1
                .iter()
                .min()
                .copied()
                .unwrap();
            return Some(LeveledCompactionTask {
                upper_level: Some(compression_level_id),
                upper_level_sst_ids: vec![mi_sst_id],
                lower_level: compression_level_id + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    _snapshot,
                    &[mi_sst_id],
                    compression_level_id + 1,
                ),
                is_lower_level_bottom_level: compression_level_id == self.options.max_levels - 1,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut remove_sst_ids = _task.upper_level_sst_ids.clone();
        if let Some(upper_level_idx) = _task.upper_level {
            assert_eq!(_task.upper_level_sst_ids.len(), 1);
            snapshot.levels[upper_level_idx - 1]
                .1
                .retain(|&x| x != _task.upper_level_sst_ids[0]);
        } else {
            let hash_set = _task
                .upper_level_sst_ids
                .iter()
                .cloned()
                .collect::<HashSet<usize>>();
            snapshot.l0_sstables.retain(|x| !hash_set.contains(x));
        }
        let hash_set = _task
            .lower_level_sst_ids
            .iter()
            .cloned()
            .collect::<HashSet<usize>>();
        let mut new_sst_ids = Vec::with_capacity(
            snapshot.levels[_task.lower_level - 1].1.len() + _output.len() - hash_set.len(),
        );
        // 判断插入的位置
        let first_key = snapshot.sstables.get(&_output[0]).unwrap().first_key();
        let mut is_extend = false;
        for sst_id in snapshot.levels[_task.lower_level - 1].1.iter() {
            if !is_extend {
                let table = snapshot.sstables.get(sst_id).unwrap();
                if first_key <= table.first_key() {
                    new_sst_ids.extend(_output);
                    is_extend = true;
                }
            }
            if !hash_set.contains(sst_id) {
                new_sst_ids.push(*sst_id);
            }
        }
        if !is_extend {
            new_sst_ids.extend(_output);
        }

        snapshot.levels[_task.lower_level - 1] = (_task.lower_level, new_sst_ids);
        remove_sst_ids.extend(_task.lower_level_sst_ids.clone());
        (snapshot, remove_sst_ids)
    }
}
