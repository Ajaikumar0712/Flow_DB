//! LSM Tree — Log-Structured Merge-Tree for durable key-value storage.
//!
//! Provides a persistent, write-optimised key-value store that underpins
//! FlowDB's disk storage tier.
//!
//! # Architecture
//!
//! ```text
//!   Writes ──► MemTable (in-memory BTreeMap, mutable)
//!                │
//!                │  (when MemTable reaches size threshold)
//!                ▼
//!            ImmutableMemTable ──► flush ──► SSTable (sorted file on disk)
//!
//!   SSTables ──► Compaction (merges overlapping levels, drops tombstones)
//! ```
//!
//! # Read path
//!   1. Check MemTable (O(log n))
//!   2. Check ImmutableMemTable if present (O(log n))
//!   3. Search SSTables from newest to oldest (O(log n) per SSTable with bloom filter)
//!
//! # Write path
//!   1. Append to WAL (durability — not implemented here, delegated to crate::wal)
//!   2. Insert into MemTable (O(log n))
//!   3. If MemTable > threshold, rotate to immutable and schedule flush
//!
//! # Tombstones
//!   Deletes are represented as tombstone entries (`Value::Tombstone`).
//!   Tombstones are physically removed during compaction.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

// ── Key / Value types ────────────────────────────────────────────────────

pub type LsmKey = Vec<u8>;

/// Value stored in the LSM tree.
#[derive(Debug, Clone)]
pub enum LsmValue {
    /// Live data.
    Data(Vec<u8>),
    /// Deletion marker.
    Tombstone,
}

impl LsmValue {
    pub fn is_tombstone(&self) -> bool {
        matches!(self, LsmValue::Tombstone)
    }
}

// ── MemTable ─────────────────────────────────────────────────────────────

/// Mutable in-memory write buffer.
pub struct MemTable {
    map: BTreeMap<LsmKey, LsmValue>,
    size_bytes: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self { map: BTreeMap::new(), size_bytes: 0 }
    }

    /// Insert or update a key.
    pub fn put(&mut self, key: LsmKey, value: Vec<u8>) {
        let extra = key.len() + value.len();
        self.map.insert(key, LsmValue::Data(value));
        self.size_bytes += extra;
    }

    /// Mark a key as deleted.
    pub fn delete(&mut self, key: &LsmKey) {
        let extra = key.len() + 1;
        self.map.insert(key.clone(), LsmValue::Tombstone);
        self.size_bytes += extra;
    }

    /// Point lookup.
    pub fn get(&self, key: &LsmKey) -> Option<&LsmValue> {
        self.map.get(key)
    }

    /// Approximate memory usage in bytes.
    pub fn size(&self) -> usize { self.size_bytes }

    /// Number of entries (including tombstones).
    pub fn len(&self) -> usize { self.map.len() }

    /// Consume the MemTable, returning all entries in sorted order.
    pub fn into_sorted(self) -> Vec<(LsmKey, LsmValue)> {
        self.map.into_iter().collect()
    }
}

// ── SSTable ───────────────────────────────────────────────────────────────

/// On-disk sorted string table.
///
/// File format (simple, no bloom filter, no block compression):
/// ```text
/// [entry count: u32 big-endian]
/// For each entry:
///   [key_len: u32 big-endian]
///   [key bytes]
///   [value_type: u8]  0 = data, 1 = tombstone
///   [value_len: u32]  (0 when tombstone)
///   [value bytes]
/// ```
pub struct Sstable {
    pub path: PathBuf,
    /// Smallest key in this SSTable (for range pruning).
    pub min_key: LsmKey,
    /// Largest key in this SSTable.
    pub max_key: LsmKey,
    /// Sequence number — higher = newer.
    pub seq: u64,
}

impl Sstable {
    /// Flush a sorted iterator of entries to disk.
    pub fn write(
        path: impl AsRef<Path>,
        seq: u64,
        entries: &[(LsmKey, LsmValue)],
    ) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::create(&path)?;
        let mut w = BufWriter::new(file);

        let count = entries.len() as u32;
        w.write_all(&count.to_be_bytes())?;

        for (key, val) in entries {
            let klen = key.len() as u32;
            w.write_all(&klen.to_be_bytes())?;
            w.write_all(key)?;
            match val {
                LsmValue::Data(data) => {
                    w.write_all(&[0u8])?; // type = data
                    let vlen = data.len() as u32;
                    w.write_all(&vlen.to_be_bytes())?;
                    w.write_all(data)?;
                }
                LsmValue::Tombstone => {
                    w.write_all(&[1u8])?; // type = tombstone
                    w.write_all(&0u32.to_be_bytes())?;
                }
            }
        }
        w.flush()?;

        let min_key = entries.first().map(|(k, _)| k.clone()).unwrap_or_default();
        let max_key = entries.last().map(|(k, _)| k.clone()).unwrap_or_default();
        Ok(Self { path, min_key, max_key, seq })
    }

    /// Read all entries from this SSTable. Returns sorted (key, value) pairs.
    pub fn read_all(&self) -> std::io::Result<Vec<(LsmKey, LsmValue)>> {
        let file = File::open(&self.path)?;
        let mut r = BufReader::new(file);

        let mut count_buf = [0u8; 4];
        r.read_exact(&mut count_buf)?;
        let count = u32::from_be_bytes(count_buf) as usize;

        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            // key
            let mut len_buf = [0u8; 4];
            r.read_exact(&mut len_buf)?;
            let klen = u32::from_be_bytes(len_buf) as usize;
            let mut key = vec![0u8; klen];
            r.read_exact(&mut key)?;

            // value type
            let mut type_buf = [0u8; 1];
            r.read_exact(&mut type_buf)?;

            // value length
            let mut vlen_buf = [0u8; 4];
            r.read_exact(&mut vlen_buf)?;
            let vlen = u32::from_be_bytes(vlen_buf) as usize;

            let value = if type_buf[0] == 0 {
                let mut data = vec![0u8; vlen];
                r.read_exact(&mut data)?;
                LsmValue::Data(data)
            } else {
                LsmValue::Tombstone
            };

            entries.push((key, value));
        }
        Ok(entries)
    }

    /// Binary search for a specific key within the SSTable.
    /// Loads the whole file — a real impl would use a block index.
    pub fn get(&self, key: &LsmKey) -> std::io::Result<Option<LsmValue>> {
        if key < &self.min_key || key > &self.max_key {
            return Ok(None); // key not in range
        }
        let entries = self.read_all()?;
        Ok(entries.into_iter().find(|(k, _)| k == key).map(|(_, v)| v))
    }
}

// ── LSM Tree ──────────────────────────────────────────────────────────────

/// Configuration for the LSM tree.
#[derive(Debug, Clone)]
pub struct LsmConfig {
    /// Directory to store SSTable files.
    pub data_dir: PathBuf,
    /// Flush MemTable to disk after it exceeds this many bytes.
    pub memtable_max_bytes: usize,
    /// Trigger level-0 compaction after this many SSTables accumulate.
    pub l0_compaction_threshold: usize,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("lsm_data"),
            memtable_max_bytes: 4 * 1024 * 1024, // 4 MiB
            l0_compaction_threshold: 4,
        }
    }
}

/// The LSM tree.
pub struct LsmTree {
    config: LsmConfig,
    /// Active write buffer.
    memtable: Mutex<MemTable>,
    /// SSTables from newest (highest seq) to oldest.
    sstables: RwLock<Vec<Arc<Sstable>>>,
    /// Monotonically increasing sequence number.
    next_seq: Mutex<u64>,
}

impl LsmTree {
    /// Open (or create) an LSM tree rooted at `config.data_dir`.
    pub fn open(config: LsmConfig) -> std::io::Result<Self> {
        fs::create_dir_all(&config.data_dir)?;
        let mut sstables = Vec::new();

        // Discover existing SSTable files (*.sst).
        let mut entries: Vec<(u64, PathBuf)> = fs::read_dir(&config.data_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "sst").unwrap_or(false))
            .filter_map(|e| {
                let name = e.file_name().into_string().ok()?;
                let seq: u64 = name.trim_end_matches(".sst").parse().ok()?;
                Some((seq, e.path()))
            })
            .collect();

        entries.sort_by_key(|(seq, _)| *seq);

        let max_seq = entries.last().map(|(s, _)| *s).unwrap_or(0);
        for (seq, path) in &entries {
            // Quick scan to determine key range.
            let all = Sstable {
                path: path.clone(),
                min_key: Vec::new(),
                max_key: Vec::new(),
                seq: *seq,
            };
            // Attempt to read to get key range.
            if let Ok(data) = all.read_all() {
                if !data.is_empty() {
                    let st = Sstable {
                        path: path.clone(),
                        min_key: data.first().unwrap().0.clone(),
                        max_key: data.last().unwrap().0.clone(),
                        seq: *seq,
                    };
                    sstables.push(Arc::new(st));
                }
            }
        }
        // Newest first.
        sstables.sort_by(|a, b| b.seq.cmp(&a.seq));

        Ok(Self {
            config,
            memtable: Mutex::new(MemTable::new()),
            sstables: RwLock::new(sstables),
            next_seq: Mutex::new(max_seq + 1),
        })
    }

    /// Write a key–value pair.
    pub fn put(&self, key: LsmKey, value: Vec<u8>) -> std::io::Result<()> {
        let should_flush = {
            let mut mem = self.memtable.lock().unwrap();
            mem.put(key, value);
            mem.size() >= self.config.memtable_max_bytes
        };
        if should_flush { self.flush()?; }
        Ok(())
    }

    /// Delete a key (inserts a tombstone).
    pub fn delete(&self, key: &LsmKey) -> std::io::Result<()> {
        let should_flush = {
            let mut mem = self.memtable.lock().unwrap();
            mem.delete(key);
            mem.size() >= self.config.memtable_max_bytes
        };
        if should_flush { self.flush()?; }
        Ok(())
    }

    /// Point lookup.  Checks MemTable first, then SSTables from newest to oldest.
    pub fn get(&self, key: &LsmKey) -> std::io::Result<Option<Vec<u8>>> {
        // 1. MemTable
        {
            let mem = self.memtable.lock().unwrap();
            if let Some(v) = mem.get(key) {
                return Ok(match v {
                    LsmValue::Data(d)  => Some(d.clone()),
                    LsmValue::Tombstone => None,
                });
            }
        }

        // 2. SSTables (newest first)
        let tables = self.sstables.read().unwrap();
        for sst in tables.iter() {
            if let Some(v) = sst.get(key)? {
                return Ok(match v {
                    LsmValue::Data(d)  => Some(d),
                    LsmValue::Tombstone => None,
                });
            }
        }
        Ok(None)
    }

    /// Flush the MemTable to a new SSTable on disk.
    pub fn flush(&self) -> std::io::Result<()> {
        let entries: Vec<(LsmKey, LsmValue)> = {
            let mut mem = self.memtable.lock().unwrap();
            let old = std::mem::replace(&mut *mem, MemTable::new());
            old.into_sorted()
        };
        if entries.is_empty() { return Ok(()); }

        let seq = {
            let mut s = self.next_seq.lock().unwrap();
            let cur = *s;
            *s += 1;
            cur
        };
        let path = self.config.data_dir.join(format!("{seq}.sst"));
        let sst = Sstable::write(&path, seq, &entries)?;

        let mut tables = self.sstables.write().unwrap();
        tables.insert(0, Arc::new(sst)); // newest first

        // Optional: trigger compaction when too many L0 files.
        if tables.len() >= self.config.l0_compaction_threshold {
            drop(tables); // release write lock before compaction
            self.compact()?;
        }
        Ok(())
    }

    /// Merge all SSTables into one, removing superseded entries and tombstones.
    pub fn compact(&self) -> std::io::Result<()> {
        let snapshot: Vec<Arc<Sstable>> = self.sstables.read().unwrap().clone();
        if snapshot.len() < 2 { return Ok(()); }

        // Merge-sort all SSTables, keeping the newest value per key.
        // SSTables are already sorted by descending seq → first occurrence wins.
        let mut merged: BTreeMap<LsmKey, LsmValue> = BTreeMap::new();
        for sst in snapshot.iter().rev() {
            // Read from oldest to newest so newer values overwrite older.
            let entries = sst.read_all()?;
            for (k, v) in entries {
                merged.insert(k, v);
            }
        }

        // Filter out tombstones (they've served their purpose after full compaction).
        let compacted: Vec<(LsmKey, LsmValue)> = merged
            .into_iter()
            .filter(|(_, v)| !v.is_tombstone())
            .collect();

        if compacted.is_empty() {
            // Nothing left — delete all SSTables.
            let mut tables = self.sstables.write().unwrap();
            for sst in tables.drain(..) {
                let _ = fs::remove_file(&sst.path);
            }
            return Ok(());
        }

        let seq = {
            let mut s = self.next_seq.lock().unwrap();
            let cur = *s;
            *s += 1;
            cur
        };
        let new_path = self.config.data_dir.join(format!("{seq}.sst"));
        let new_sst = Sstable::write(&new_path, seq, &compacted)?;

        let mut tables = self.sstables.write().unwrap();
        // Remove old files.
        for sst in tables.drain(..) {
            let _ = fs::remove_file(&sst.path);
        }
        *tables = vec![Arc::new(new_sst)];
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_lsm() -> LsmTree {
        let dir = std::env::temp_dir().join(format!(
            "flowdb_lsm_{}", std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().subsec_nanos()
        ));
        LsmTree::open(LsmConfig {
            data_dir: dir,
            memtable_max_bytes: 64, // tiny threshold to force flush in tests
            l0_compaction_threshold: 2,
        }).unwrap()
    }

    #[test]
    fn test_put_and_get_in_memtable() {
        let lsm = temp_lsm();
        // Force a large threshold so data stays in MemTable.
        let lsm2 = LsmTree::open(LsmConfig {
            data_dir: std::env::temp_dir().join("flowdb_lsm_mem"),
            memtable_max_bytes: 1_000_000,
            l0_compaction_threshold: 4,
        }).unwrap();
        lsm2.put(b"hello".to_vec(), b"world".to_vec()).unwrap();
        let v = lsm2.get(&b"hello".to_vec()).unwrap();
        assert_eq!(v.as_deref(), Some(b"world".as_ref()));
    }

    #[test]
    fn test_delete_returns_none() {
        let lsm = LsmTree::open(LsmConfig {
            data_dir: std::env::temp_dir().join("flowdb_lsm_del"),
            memtable_max_bytes: 1_000_000,
            l0_compaction_threshold: 4,
        }).unwrap();
        lsm.put(b"key".to_vec(), b"value".to_vec()).unwrap();
        lsm.delete(&b"key".to_vec()).unwrap();
        let v = lsm.get(&b"key".to_vec()).unwrap();
        assert!(v.is_none(), "deleted key should return None");
    }

    #[test]
    fn test_flush_and_sstable_read() {
        let dir = std::env::temp_dir().join("flowdb_lsm_flush");
        let _ = std::fs::remove_dir_all(&dir);
        let lsm = LsmTree::open(LsmConfig {
            data_dir: dir.clone(),
            memtable_max_bytes: 1_000_000,
            l0_compaction_threshold: 4,
        }).unwrap();
        lsm.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        lsm.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        lsm.flush().unwrap();

        // After flush, data should be in SSTable.
        let v = lsm.get(&b"a".to_vec()).unwrap();
        assert_eq!(v.as_deref(), Some(b"1".as_ref()));
    }

    #[test]
    fn test_compaction_removes_tombstones() {
        let dir = std::env::temp_dir().join("flowdb_lsm_compact");
        let _ = std::fs::remove_dir_all(&dir);
        let lsm = LsmTree::open(LsmConfig {
            data_dir: dir.clone(),
            memtable_max_bytes: 1_000_000,
            l0_compaction_threshold: 10,
        }).unwrap();
        lsm.put(b"x".to_vec(), b"alive".to_vec()).unwrap();
        lsm.put(b"y".to_vec(), b"dead".to_vec()).unwrap();
        lsm.flush().unwrap();

        lsm.delete(&b"y".to_vec()).unwrap();
        lsm.flush().unwrap();

        lsm.compact().unwrap();

        let tables = lsm.sstables.read().unwrap();
        assert_eq!(tables.len(), 1, "compaction should produce one SSTable");
        let entries = tables[0].read_all().unwrap();
        assert!(entries.iter().all(|(k, _)| k != &b"y".to_vec()), "tombstone should be removed");
    }
}
