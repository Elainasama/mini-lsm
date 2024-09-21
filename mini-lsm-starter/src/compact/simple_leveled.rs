use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        let max_level = _snapshot.levels.len();
        // ensure l0_sst_num < level0_file_num_compaction_trigger
        // l0 -> l1压缩
        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: _snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: false,
            });
        }
        for low_level in 1..max_level {
            let low_level_sst = &_snapshot.levels[low_level];
            let upper_level = low_level - 1;
            let upper_level_sst = &_snapshot.levels[upper_level];
            if low_level_sst.1.len() * 100
                < self.options.size_ratio_percent * upper_level_sst.1.len()
            {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(upper_level + 1),
                    upper_level_sst_ids: upper_level_sst.1.clone(),
                    lower_level: low_level + 1,
                    lower_level_sst_ids: low_level_sst.1.clone(),
                    is_lower_level_bottom_level: low_level_sst.0 == self.options.max_levels,
                });
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut file_to_remove = Vec::new();
        let mut snapshot = _snapshot.clone();
        file_to_remove.extend(&_task.upper_level_sst_ids);
        if let Some(upper_level) = _task.upper_level {
            assert_eq!(
                upper_level,
                snapshot.levels[upper_level - 1].0,
                "sst mismatch"
            );
            assert_eq!(
                snapshot.levels[upper_level - 1].1,
                _task.upper_level_sst_ids,
                "sst mismatch"
            );
            snapshot.levels[upper_level - 1].1.clear();
        } else {
            // l0 -> l1
            let sst_set = _task.upper_level_sst_ids.iter().collect::<HashSet<_>>();
            snapshot.l0_sstables.retain(|&x| !sst_set.contains(&x));
        }
        assert_eq!(
            _task.lower_level,
            snapshot.levels[_task.lower_level - 1].0,
            "sst mismatch"
        );
        assert_eq!(
            _task.lower_level_sst_ids,
            snapshot.levels[_task.lower_level - 1].1,
            "sst mismatch"
        );
        file_to_remove.extend(&snapshot.levels[_task.lower_level - 1].1);
        snapshot.levels[_task.lower_level - 1].1 = _output.to_vec();
        (snapshot, file_to_remove)
    }
}
