//! Buffer Pool — LRU page cache for disk-backed storage.
//!
//! Provides a fixed-size in-memory page cache on top of raw page files.
//! Pages are loaded from disk on demand ("fetch") and evicted under an LRU
//! policy when the pool is full.  Dirty pages are written back to disk before
//! eviction (write-back / write-through is configurable).
//!
//! # Page layout
//!
//! Each page is `PAGE_SIZE` bytes (default 8 KB).  The first 8 bytes are
//! reserved for a page header:
//!
//! ```text
//! offset  size  field
//! ──────  ────  ─────────────────────────────
//!      0     4  page_id      (u32, big-endian)
//!      4     1  flags        (bit 0 = dirty, bit 1 = pinned)
//!      5     3  __padding__
//!      8  PAGE_SIZE-8  payload
//! ```
//!
//! # Thread safety
//!
//! `BufferPool` wraps its internals in a `Mutex`.  Callers that need to hold
//! multiple pages simultaneously (e.g. a B-tree split) should pin each page
//! before acquiring the next, then unpin + write-back all at once.

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

// ── Constants ─────────────────────────────────────────────────────────────

/// Default page size: 8 KiB.
pub const PAGE_SIZE: usize = 8 * 1024;

/// Header occupies the first 8 bytes of every page.
pub const PAGE_HEADER_SIZE: usize = 8;

/// Usable payload bytes per page.
pub const PAGE_PAYLOAD_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

// ── Page ID ───────────────────────────────────────────────────────────────

/// Identifier for a page within a given file.  Page 0 is the file header page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId(pub u32);

impl PageId {
    /// Byte offset of this page in the file.
    pub fn offset(self) -> u64 {
        self.0 as u64 * PAGE_SIZE as u64
    }
}

// ── Page ──────────────────────────────────────────────────────────────────

/// In-memory page buffer.
pub struct Page {
    /// The raw bytes, exactly `PAGE_SIZE` in length.
    pub data: Box<[u8; PAGE_SIZE]>,
    /// Whether this page has been modified since it was last written to disk.
    pub dirty: bool,
    /// Pin count: the page may not be evicted while pin_count > 0.
    pub pin_count: u32,
    /// Which page on disk this buffer holds.
    pub page_id: PageId,
}

impl Page {
    fn new(page_id: PageId) -> Self {
        Self {
            data: Box::new([0u8; PAGE_SIZE]),
            dirty: false,
            pin_count: 0,
            page_id,
        }
    }

    /// Read a `u32` value at `offset` within the page.
    pub fn read_u32(&self, offset: usize) -> u32 {
        let b = &self.data[offset..offset + 4];
        u32::from_be_bytes([b[0], b[1], b[2], b[3]])
    }

    /// Write a `u32` value at `offset` within the page.
    pub fn write_u32(&mut self, offset: usize, val: u32) {
        self.data[offset..offset + 4].copy_from_slice(&val.to_be_bytes());
        self.dirty = true;
    }

    /// Payload slice (everything after the page header).
    pub fn payload(&self) -> &[u8] {
        &self.data[PAGE_HEADER_SIZE..]
    }

    /// Mutable payload slice — marks the page dirty.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        self.dirty = true;
        &mut self.data[PAGE_HEADER_SIZE..]
    }

    /// Write the page ID into the header (called on new or recovered pages).
    pub fn encode_header(&mut self) {
        let id = self.page_id.0;
        self.data[0..4].copy_from_slice(&id.to_be_bytes());
    }
}

// ── Buffer pool internals ─────────────────────────────────────────────────

struct PoolInner {
    /// Frame index → Page.
    frames: Vec<Option<Page>>,
    /// Page ID → frame index (for O(1) lookup).
    page_table: HashMap<PageId, usize>,
    /// LRU queue of unpinned frame indexes (front = LRU / oldest).
    lru: VecDeque<usize>,
    /// Total number of frames in the pool.
    capacity: usize,
    /// Path to the backing file.
    path: PathBuf,
    /// Total pages currently allocated in the file.
    num_pages: u32,
    /// file handle (lazy: opened on first use)
    file: Option<std::fs::File>,
}

impl PoolInner {
    fn open_file(&mut self) -> std::io::Result<&mut std::fs::File> {
        if self.file.is_none() {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&self.path)?;
            self.file = Some(f);
        }
        Ok(self.file.as_mut().unwrap())
    }

    /// Read a page from disk into `frame`.
    fn load_page(&mut self, page_id: PageId, frame: usize) -> std::io::Result<()> {
        let offset = page_id.offset();

        // Ensure the file is open.
        if self.file.is_none() {
            let f = std::fs::OpenOptions::new()
                .read(true).write(true).create(true)
                .open(&self.path)?;
            self.file = Some(f);
        }

        let file_len = self.file.as_ref().unwrap().metadata()?.len();

        if offset + PAGE_SIZE as u64 > file_len {
            // Page doesn't exist on disk yet — zero fill.
            let mut p = Page::new(page_id);
            p.encode_header();
            self.frames[frame] = Some(p);
            return Ok(());
        }

        // Read into a heap-allocated Vec to avoid large stack allocations.
        let mut buf = vec![0u8; PAGE_SIZE];
        {
            let f = self.file.as_mut().unwrap();
            f.seek(SeekFrom::Start(offset))?;
            f.read_exact(&mut buf)?;
        }

        let mut p = Page::new(page_id);
        p.data.copy_from_slice(&buf);
        p.page_id = page_id;
        p.dirty   = false;
        self.frames[frame] = Some(p);
        Ok(())
    }

    /// Write a dirty page back to disk.
    fn flush_page(&mut self, frame: usize) -> std::io::Result<()> {
        let (offset, data) = match self.frames[frame].as_ref() {
            None => return Ok(()),
            Some(p) => {
                if !p.dirty { return Ok(()); }
                (p.page_id.offset(), p.data.to_vec())
            }
        };

        // Expand the file if needed.
        let f = self.open_file()?;
        let current_len = f.metadata()?.len();
        if offset + PAGE_SIZE as u64 > current_len {
            f.set_len(offset + PAGE_SIZE as u64)?;
        }
        f.seek(SeekFrom::Start(offset))?;
        f.write_all(&data)?;
        f.flush()?;
        if let Some(p) = &mut self.frames[frame] {
            p.dirty = false;
        }
        Ok(())
    }

    /// Evict the LRU unpinned frame, flushing it if dirty.
    /// Returns the freed frame index, or an error if all frames are pinned.
    fn evict(&mut self) -> std::io::Result<usize> {
        loop {
            let frame = self.lru.pop_front().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::Other, "buffer pool exhausted: all frames pinned")
            })?;
            let pinned = self.frames[frame].as_ref().map(|p| p.pin_count > 0).unwrap_or(false);
            if pinned {
                self.lru.push_back(frame); // can't evict pinned page
                continue;
            }
            self.flush_page(frame)?;
            if let Some(p) = &self.frames[frame] {
                self.page_table.remove(&p.page_id);
            }
            return Ok(frame);
        }
    }
}

// ── Public API ────────────────────────────────────────────────────────────

/// Thread-safe LRU buffer pool.
pub struct BufferPool {
    inner: Mutex<PoolInner>,
}

impl BufferPool {
    /// Create a new buffer pool backed by `path` with `capacity` frames.
    pub fn new(path: impl AsRef<Path>, capacity: usize) -> Self {
        let capacity = capacity.max(4); // minimum 4 frames
        let mut frames = Vec::with_capacity(capacity);
        for _ in 0..capacity { frames.push(None); }
        Self {
            inner: Mutex::new(PoolInner {
                frames,
                page_table: HashMap::new(),
                lru: VecDeque::from_iter(0..capacity),
                capacity,
                path: path.as_ref().to_path_buf(),
                num_pages: 0,
                file: None,
            }),
        }
    }

    /// Fetch a page, loading it from disk if not already cached.
    /// The returned `Arc<Mutex<…>>` frame is pinned until the caller drops it.
    pub fn fetch_page(&self, page_id: PageId) -> std::io::Result<PageHandle<'_>> {
        let mut inner = self.inner.lock().unwrap();

        if let Some(&frame) = inner.page_table.get(&page_id) {
            if let Some(p) = &mut inner.frames[frame] {
                p.pin_count += 1;
                inner.lru.retain(|&f| f != frame); // remove from LRU while pinned
            }
            return Ok(PageHandle { pool: unsafe { &*(self as *const BufferPool) }, frame });
        }

        // Not in cache — need a frame.
        let frame = if inner.page_table.len() < inner.capacity {
            // There are still free frames.
            inner.lru.pop_front().unwrap()
        } else {
            inner.evict()?
        };

        inner.load_page(page_id, frame)?;
        inner.page_table.insert(page_id, frame);
        if let Some(p) = &mut inner.frames[frame] {
            p.pin_count += 1;
        }

        Ok(PageHandle { pool: unsafe { &*(self as *const BufferPool) }, frame })
    }

    /// Allocate a new page and return its ID.  The file is extended by one page.
    pub fn new_page(&self) -> std::io::Result<(PageId, PageHandle<'_>)> {
        let page_id = {
            let mut inner = self.inner.lock().unwrap();
            let id = PageId(inner.num_pages);
            inner.num_pages += 1;
            id
        };
        let handle = self.fetch_page(page_id)?;
        {
            let mut inner = self.inner.lock().unwrap();
            if let Some(p) = &mut inner.frames[handle.frame] {
                p.encode_header();
                p.dirty = true;
            }
        }
        Ok((page_id, handle))
    }

    /// Write all dirty pages to disk.
    pub fn flush_all(&self) -> std::io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        for frame in 0..inner.capacity {
            inner.flush_page(frame)?;
        }
        Ok(())
    }

    /// Unpin a frame, making it eligible for LRU eviction.
    fn unpin(&self, frame: usize) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(p) = &mut inner.frames[frame] {
            if p.pin_count > 0 { p.pin_count -= 1; }
            if p.pin_count == 0 {
                inner.lru.push_back(frame);
            }
        }
    }
}

// ── Page handle (RAII pin) ────────────────────────────────────────────────

/// A temporary reference to a pinned page in the buffer pool.
/// Unpins the frame when dropped.
pub struct PageHandle<'pool> {
    pool: &'pool BufferPool,
    pub frame: usize,
}

impl<'pool> Drop for PageHandle<'pool> {
    fn drop(&mut self) {
        self.pool.unpin(self.frame);
    }
}

impl<'pool> PageHandle<'pool> {
    /// Read access to the page.
    pub fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Page) -> R,
    {
        let inner = self.pool.inner.lock().unwrap();
        f(inner.frames[self.frame].as_ref().unwrap())
    }

    /// Write access to the page — marks it dirty.
    pub fn write<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Page) -> R,
    {
        let mut inner = self.pool.inner.lock().unwrap();
        let p = inner.frames[self.frame].as_mut().unwrap();
        p.dirty = true;
        f(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    fn temp_pool(capacity: usize) -> BufferPool {
        // Use an in-memory path in the temp directory.
        let path = std::env::temp_dir().join(format!("flowdb_bp_test_{}.bin", capacity));
        // Clean up from a previous run.
        let _ = std::fs::remove_file(&path);
        BufferPool::new(path, capacity)
    }

    #[test]
    fn test_alloc_and_write_page() -> io::Result<()> {
        let pool = temp_pool(8);
        let (pid, handle) = pool.new_page()?;
        assert_eq!(pid, PageId(0));
        handle.write(|p| p.write_u32(PAGE_HEADER_SIZE, 0xDEAD_BEEF));
        let val = handle.read(|p| p.read_u32(PAGE_HEADER_SIZE));
        assert_eq!(val, 0xDEAD_BEEF);
        pool.flush_all()?;
        Ok(())
    }

    #[test]
    fn test_lru_eviction() -> io::Result<()> {
        // Pool with 4 frames and 8 pages — should evict and reload correctly.
        let pool = temp_pool(4);
        let ids: Vec<PageId> = (0..8)
            .map(|_| pool.new_page().unwrap().0)
            .collect();

        let handle = pool.fetch_page(ids[0])?;
        handle.write(|p| p.write_u32(PAGE_HEADER_SIZE, 42));
        drop(handle);
        pool.flush_all()?;

        // Thrash the cache to evict page 0.
        for id in &ids[1..] {
            let h = pool.fetch_page(*id)?;
            drop(h);
        }

        // Re-fetch page 0 — should be reloaded from disk.
        let h = pool.fetch_page(ids[0])?;
        let v = h.read(|p| p.read_u32(PAGE_HEADER_SIZE));
        assert_eq!(v, 42, "page 0 value should survive eviction and reload");
        Ok(())
    }
}
