use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            _snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        // Universal compaction will only trigger tasks when the number of tiers (sorted runs) is larger than num_tiers.
        // Otherwise, it does not trigger any compaction.
        // 实际实现需要再减少一层才可以通过测试。
        if _snapshot.levels.len() < self.options.num_tiers {
            return None;
        }
        assert_ne!(_snapshot.levels.len(), 0);
        // Triggered by max_size_amplification_percent
        let mut total_size = 0;
        for level in _snapshot.levels.iter() {
            total_size += level.1.len();
        }
        let last_level_size = _snapshot.levels[_snapshot.levels.len() - 1].1.len();
        if (total_size - last_level_size) * 100
            >= last_level_size * self.options.max_size_amplification_percent
        {
            let mut tiers = Vec::new();
            for level in _snapshot.levels.iter() {
                tiers.push(level.clone());
            }
            return Some(TieredCompactionTask {
                tiers,
                bottom_tier_included: true,
            });
        }
        // Triggered by Size Ratio
        // previous tiers / this tier >= (100 + size_ratio) * 100%
        // We only do this compaction with there are more than min_merge_width tiers to be merged.
        let mut pre_size = 0;
        for level_id in 0.._snapshot.levels.len() {
            let level = &_snapshot.levels[level_id];

            if pre_size * 100 >= level.1.len() * (100 + self.options.size_ratio)
                && level_id + 1 > self.options.min_merge_width
            {
                let mut tiers = Vec::new();
                for pre_level in _snapshot.levels.iter() {
                    tiers.push(pre_level.clone());
                    if pre_level.0 == level.0 {
                        break;
                    }
                }
                return Some(TieredCompactionTask {
                    tiers,
                    bottom_tier_included: level_id == _snapshot.levels.len() - 1,
                });
            }
            pre_size += level.1.len();
        }
        // Reduce Sorted Runs
        // We will simply take the top-most tiers to compact into one tier, so that the final state will have exactly num_tiers tiers
        let mut tiers = Vec::with_capacity(_snapshot.levels.len() - self.options.num_tiers + 2);
        for level_id in 0..(_snapshot.levels.len() - self.options.num_tiers + 2) {
            let level = &_snapshot.levels[level_id];
            tiers.push(level.clone());
        }
        Some(TieredCompactionTask {
            tiers,
            bottom_tier_included: false,
        })
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut to_remove = Vec::new();
        let mut delete_level = HashSet::new();
        for tier in _task.tiers.iter() {
            to_remove.extend(tier.1.clone());
            delete_level.insert(tier.0);
        }
        // 在snapshot中删除tier中的层
        snapshot.levels.retain(|x| !delete_level.contains(&x.0));
        // 插入
        assert!(!_output.is_empty());
        let mut insert_pos = 0;
        // 这里以先前最大层号作为合成层的层号
        // todo You may use the first output SST id as the level/tier id for your new sorted run.
        let insert_level_id = _task.tiers[0].0;
        while insert_pos < snapshot.levels.len() {
            if snapshot.levels[insert_pos].0 < insert_level_id {
                break;
            }
            insert_pos += 1;
        }
        snapshot
            .levels
            .insert(insert_pos, (insert_level_id, _output.to_vec()));
        (snapshot, to_remove)
    }
}
