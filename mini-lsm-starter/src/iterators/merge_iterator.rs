use crate::key::{Key, KeySlice};
use anyhow::Result;
use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut this = Self {
            iters: BinaryHeap::new(),
            current: None,
        };
        // 将有效值加入堆中
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                this.iters.push(HeapWrapper(idx, iter));
            }
        }
        // current保留堆顶值
        this.current = this.iters.pop();

        this
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        if let Some(wrapper) = self.current.as_ref() {
            wrapper.1.value()
        } else {
            &[]
        }
    }

    fn key(&self) -> KeySlice {
        if let Some(wrapper) = self.current.as_ref() {
            wrapper.1.key()
        } else {
            Key::default()
        }
    }

    fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    fn next(&mut self) -> Result<()> {
        if self.current.is_none() {
            return Ok(());
        }
        let cur = self.current.as_mut().unwrap();

        // 去掉重复键
        while let Some(mut top) = self.iters.peek_mut() {
            if top.1.key() == cur.1.key() {
                if let e @ Err(_) = top.1.next() {
                    PeekMut::pop(top);
                    return e;
                }
                if !top.1.is_valid() {
                    PeekMut::pop(top);
                }
            } else {
                break;
            }
        }

        cur.1.next()?;

        if cur.1.is_valid() {
            self.iters.push(std::mem::take(&mut self.current).unwrap());
        }

        self.current = self.iters.pop();
        Ok(())
    }
}
