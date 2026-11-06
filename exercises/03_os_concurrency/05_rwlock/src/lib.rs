//! # Read-Write Lock (Writer-Priority)
//!
//! In this exercise, you will implement a **writer-priority** read-write lock from scratch using atomics.
//! Multiple readers may hold the lock concurrently; a writer holds it exclusively.
//!
//! **Note:** Rust's standard library already provides [`std::sync::RwLock`]. This exercise implements
//! a minimal version for learning the protocol and policy without using the standard one.
//!
//! ## Common policies for read-write locks
//! Different implementations can give different **priority** when both readers and writers are waiting:
//!
//! - **Reader-priority (读者优先)**: New readers are allowed to enter while a writer is waiting, so writers
//!   may be starved if readers keep arriving.
//! - **Writer-priority (写者优先)**: Once a writer is waiting, no new readers are admitted until that writer
//!   has run; this exercise implements this policy.
//! - **Read-write fair (读写公平)**: Requests are served in a fair order (e.g. FIFO or round-robin), so
//!   neither readers nor writers are systematically starved.
//!
//! ## Key Concepts
//! - **Readers**: share access; many threads can hold a read lock at once.
//! - **Writer**: exclusive access; only one writer, and no readers while the writer holds the lock.
//! - **Writer-priority (this implementation)**: when at least one writer is waiting, new readers block
//!   until the writer runs.
//!
//! ## State
//! We use one `AtomicU32`: low bits = reader count, two flags = writer holding / writer waiting.
//! A separate waiting-writer count keeps readers blocked between consecutive writers.
//! All logic is implemented with compare_exchange and load/store; no use of `std::sync::RwLock`.

use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU32, Ordering};

/// Maximum number of concurrent readers (fits in state bits).
const READER_MASK: u32 = (1 << 30) - 1;
/// Bit set when a writer holds the lock.
const WRITER_HOLDING: u32 = 1 << 30;
/// Bit set when at least one writer is waiting (writer-priority: block new readers).
const WRITER_WAITING: u32 = 1 << 31;

/// Writer-priority read-write lock. Implemented from scratch; does not use `std::sync::RwLock`.
pub struct RwLock<T> {
    state: AtomicU32,
    waiting_writers: AtomicU32,
    data: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send + Sync> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    pub const fn new(data: T) -> Self {
        Self {
            state: AtomicU32::new(0),
            waiting_writers: AtomicU32::new(0),
            data: UnsafeCell::new(data),
        }
    }

    /// Acquire a read lock. Blocks (spins) until no writer holds and no writer is waiting (writer-priority).
    ///
    /// The writer count also blocks readers when a writer has just cleared the state flags.
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        loop {
            let state = self.state.load(Ordering::Acquire);
            if self.waiting_writers.load(Ordering::Acquire) != 0
                || state & (WRITER_HOLDING | WRITER_WAITING) != 0
                || state & READER_MASK == READER_MASK
            {
                core::hint::spin_loop();
                continue;
            }
            if self
                .state
                .compare_exchange(state, state + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // A writer may have queued between our count check and CAS.
                if self.waiting_writers.load(Ordering::Acquire) == 0 {
                    return RwLockReadGuard { lock: self };
                }
                self.state.fetch_sub(1, Ordering::Release);
            }
            core::hint::spin_loop();
        }
    }

    /// Acquire the write lock. Blocks until no readers and no other writer.
    ///
    /// Register before waiting so readers cannot pass other queued writers after an unlock.
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.waiting_writers.fetch_add(1, Ordering::AcqRel);
        loop {
            // Reassert WAITING because the previous writer may have cleared it.
            let state = self.state.fetch_or(WRITER_WAITING, Ordering::AcqRel);
            if state & (READER_MASK | WRITER_HOLDING) == 0
                && self
                    .state
                    .compare_exchange(
                        WRITER_WAITING,
                        WRITER_HOLDING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
            {
                self.waiting_writers.fetch_sub(1, Ordering::Release);
                return RwLockWriteGuard { lock: self };
            }
            core::hint::spin_loop();
        }
    }
}

/// Guard for a read lock; releases the read lock on drop.
pub struct RwLockReadGuard<'a, T> {
    lock: &'a RwLock<T>,
}

impl<T> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the live read guard prevents writers from accessing data.
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.state.fetch_sub(1, Ordering::Release);
    }
}

/// Guard for a write lock; releases the write lock on drop.
pub struct RwLockWriteGuard<'a, T> {
    lock: &'a RwLock<T>,
}

impl<T> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the live write guard excludes all other readers and writers.
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: the write lock is held and the guard is exclusively borrowed.
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T> Drop for RwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        self.lock
            .state
            .fetch_and(!(WRITER_HOLDING | WRITER_WAITING), Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_multiple_readers() {
        let lock = Arc::new(RwLock::new(0u32));
        let mut handles = vec![];
        for _ in 0..10 {
            let l = Arc::clone(&lock);
            handles.push(thread::spawn(move || {
                let g = l.read();
                assert_eq!(*g, 0);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_writer_excludes_readers() {
        let lock = Arc::new(RwLock::new(0u32));
        let lock_w = Arc::clone(&lock);
        let writer = thread::spawn(move || {
            let mut g = lock_w.write();
            *g = 42;
        });
        writer.join().unwrap();
        let g = lock.read();
        assert_eq!(*g, 42);
    }

    #[test]
    fn test_concurrent_reads_after_write() {
        let lock = Arc::new(RwLock::new(Vec::<i32>::new()));
        {
            let mut g = lock.write();
            g.push(1);
            g.push(2);
        }
        let mut handles = vec![];
        for _ in 0..5 {
            let l = Arc::clone(&lock);
            handles.push(thread::spawn(move || {
                let g = l.read();
                assert_eq!(g.len(), 2);
                assert_eq!(&*g, &[1, 2]);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_concurrent_writes_serialized() {
        let lock = Arc::new(RwLock::new(0u64));
        let mut handles = vec![];
        for _ in 0..10 {
            let l = Arc::clone(&lock);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let mut g = l.write();
                    *g += 1;
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(*lock.read(), 1000);
    }

    #[test]
    fn test_readers_can_hold_lock_simultaneously() {
        let lock = Arc::new(RwLock::new(42));
        let first_reader = lock.read();
        let other_lock = Arc::clone(&lock);
        let (tx, rx) = std::sync::mpsc::channel();
        let reader = thread::spawn(move || {
            let guard = other_lock.read();
            tx.send(*guard).unwrap();
        });
        assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(5)), Ok(42));
        drop(first_reader);
        reader.join().unwrap();
    }

    #[test]
    fn test_all_waiting_writers_precede_new_reader() {
        use std::sync::mpsc;
        use std::time::{Duration, Instant};

        for writer_count in [1, 4] {
            let lock = Arc::new(RwLock::new(0));
            let first_reader = lock.read();
            let (tx, rx) = mpsc::channel();
            let mut writers = Vec::new();
            for _ in 0..writer_count {
                let lock = Arc::clone(&lock);
                let tx = tx.clone();
                writers.push(thread::spawn(move || {
                    let mut guard = lock.write();
                    *guard += 1;
                    tx.send("writer").unwrap();
                }));
            }

            // Keep the first reader alive until all writers have queued.
            let deadline = Instant::now() + Duration::from_secs(5);
            while lock.waiting_writers.load(Ordering::Acquire) != writer_count {
                assert!(Instant::now() < deadline);
                thread::yield_now();
            }

            let reader_lock = Arc::clone(&lock);
            let (started_tx, started_rx) = mpsc::channel();
            let reader = thread::spawn(move || {
                started_tx.send(()).unwrap();
                let guard = reader_lock.read();
                tx.send("reader").unwrap();
                assert_eq!(*guard, writer_count);
            });
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            drop(first_reader);

            for _ in 0..writer_count {
                assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), "writer");
            }
            assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), "reader");
            for writer in writers {
                writer.join().unwrap();
            }
            reader.join().unwrap();
        }
    }
}
