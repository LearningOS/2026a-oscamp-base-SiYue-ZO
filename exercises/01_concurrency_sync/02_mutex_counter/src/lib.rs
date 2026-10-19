//! # Mutex Shared State
//!
//! In this exercise, you will use `Arc<Mutex<T>>` to safely share and modify data between multiple threads.
//!
//! ## Concepts
//! - `Mutex<T>` mutex protects shared data
//! - `Arc<T>` atomic reference counting enables cross-thread sharing
//! - `lock()` acquires the lock and accesses data

use std::sync::{Arc, Mutex};
use std::thread;

/// Increment a counter concurrently using `n_threads` threads.
/// Each thread increments the counter `count_per_thread` times.
/// Returns the final counter value.
///
/// Hint: Use `Arc<Mutex<usize>>` as the shared counter.
pub fn concurrent_counter(n_threads: usize, count_per_thread: usize) -> usize {
    // TODO: Create Arc<Mutex<usize>> with initial value 0
    let counter = Arc::new(Mutex::new(0));
    // TODO: Spawn n_threads threads
    thread::scope(|s| {
        for _ in 0..n_threads {
            // 每个线程拿一份 Arc 克隆（只增加引用计数，不复制数据）
            let counter = Arc::clone(&counter);

            // move 把这份 Arc 移进闭包，因为线程可能比当前迭代活得更久
            s.spawn(move || {
                for _ in 0..count_per_thread {
                    // lock() 返回 LockResult<MutexGuard<usize>>
                    // unwrap() 处理 poisoned 情况
                    // MutexGuard 在语句结束/drop 时自动释放锁
                    let mut num = counter.lock().unwrap();
                    *num += 1;
                }
            });
        }
    }); // 这里自动 join 所有线程
    // TODO: In each thread, lock() and increment count_per_thread times
    // TODO: Join all threads, return final value
    let result = *counter.lock().unwrap();
    result
}

/// Add elements to a shared vector concurrently using multiple threads.
/// Each thread pushes its own id (0..n_threads) to the vector.
/// Returns the sorted vector.
///
/// Hint: Use `Arc<Mutex<Vec<usize>>>`.
pub fn concurrent_collect(n_threads: usize) -> Vec<usize> {
    // TODO: Create Arc<Mutex<Vec<usize>>>
    let counter = Arc::new(Mutex::new(Vec::<usize>::new()));
    // TODO: Each thread pushes its own id
    thread::scope(|s| {
        for i in 0..n_threads {
            let counter = Arc::clone(&counter);
            s.spawn(move || {
                let mut vec = counter.lock().unwrap();
                vec.push(i);
            });
        }
    }); // 这里自动 join 所有线程
    // TODO: After joining all threads, sort the result and return
    let mut result = counter.lock().unwrap().clone();
    result.sort();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_single_thread() {
        assert_eq!(concurrent_counter(1, 100), 100);
    }

    #[test]
    fn test_counter_multi_thread() {
        assert_eq!(concurrent_counter(10, 100), 1000);
    }

    #[test]
    fn test_counter_zero() {
        assert_eq!(concurrent_counter(5, 0), 0);
    }

    #[test]
    fn test_collect() {
        let result = concurrent_collect(5);
        assert_eq!(result, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_collect_single() {
        assert_eq!(concurrent_collect(1), vec![0]);
    }
}
