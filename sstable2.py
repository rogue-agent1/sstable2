#!/usr/bin/env python3
"""sstable2 - SSTable (Sorted String Table) with index, bloom filter, and compaction.

Usage: python sstable2.py [--demo]
"""
import sys, hashlib, struct, json, os, tempfile

class BloomFilter:
    def __init__(self, size=1024, hashes=3):
        self.bits = bytearray(size // 8 + 1)
        self.size = size; self.hashes = hashes
    def _hash(self, key, i):
        h = int(hashlib.md5(f"{key}:{i}".encode()).hexdigest(), 16)
        return h % self.size
    def add(self, key):
        for i in range(self.hashes):
            h = self._hash(key, i)
            self.bits[h // 8] |= 1 << (h % 8)
    def might_contain(self, key):
        return all(self.bits[self._hash(key, i) // 8] & (1 << (self._hash(key, i) % 8))
                   for i in range(self.hashes))

class SSTable:
    def __init__(self, entries=None, level=0):
        self.entries = sorted(entries or [], key=lambda e: e[0])  # [(key, value, tombstone)]
        self.level = level
        self.bloom = BloomFilter()
        self.index = {}  # sparse index: key -> position
        for i, (k, v, t) in enumerate(self.entries):
            self.bloom.add(k)
            if i % 16 == 0:  # Sparse index every 16 entries
                self.index[k] = i
        self.reads = 0

    def get(self, key):
        self.reads += 1
        if not self.bloom.might_contain(key):
            return None, False  # Definitely not here
        # Binary search
        lo, hi = 0, len(self.entries) - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            if self.entries[mid][0] == key:
                k, v, tomb = self.entries[mid]
                return v, not tomb
            elif self.entries[mid][0] < key:
                lo = mid + 1
            else:
                hi = mid - 1
        return None, False

    def range_scan(self, lo, hi):
        results = []
        for k, v, tomb in self.entries:
            if k > hi: break
            if k >= lo and not tomb:
                results.append((k, v))
        return results

    @property
    def min_key(self):
        return self.entries[0][0] if self.entries else None
    @property
    def max_key(self):
        return self.entries[-1][0] if self.entries else None

class LSMStore:
    def __init__(self, memtable_limit=32, level_ratio=4):
        self.memtable = {}  # key -> (value, tombstone)
        self.memtable_limit = memtable_limit
        self.levels = [[] for _ in range(4)]  # L0-L3
        self.level_ratio = level_ratio
        self.stats = {"writes": 0, "reads": 0, "compactions": 0}

    def put(self, key, value):
        self.memtable[key] = (value, False)
        self.stats["writes"] += 1
        if len(self.memtable) >= self.memtable_limit:
            self._flush()

    def delete(self, key):
        self.memtable[key] = (None, True)  # Tombstone
        self.stats["writes"] += 1

    def get(self, key):
        self.stats["reads"] += 1
        # Check memtable first
        if key in self.memtable:
            v, tomb = self.memtable[key]
            return v if not tomb else None
        # Check SSTables newest to oldest
        for level in self.levels:
            for sst in reversed(level):
                v, found = sst.get(key)
                if found: return v
        return None

    def _flush(self):
        entries = [(k, v, t) for k, (v, t) in sorted(self.memtable.items())]
        sst = SSTable(entries, level=0)
        self.levels[0].append(sst)
        self.memtable.clear()
        # Check if L0 needs compaction
        if len(self.levels[0]) >= self.level_ratio:
            self._compact(0)

    def _compact(self, level):
        if level >= len(self.levels) - 1:
            return
        self.stats["compactions"] += 1
        # Merge all SSTables at this level
        all_entries = {}
        for sst in self.levels[level]:
            for k, v, t in sst.entries:
                all_entries[k] = (v, t)
        # Also merge with overlapping L+1 tables
        for sst in self.levels[level + 1]:
            for k, v, t in sst.entries:
                if k not in all_entries:
                    all_entries[k] = (v, t)
        # Remove tombstoned entries at bottom level
        entries = [(k, v, t) for k, (v, t) in sorted(all_entries.items())
                   if not t or level + 1 < len(self.levels) - 1]
        self.levels[level] = []
        self.levels[level + 1] = [SSTable(entries, level=level+1)]
        # Check next level
        max_size = self.level_ratio ** (level + 2)
        if sum(len(s.entries) for s in self.levels[level+1]) > max_size * self.memtable_limit:
            self._compact(level + 1)

    def info(self):
        return {
            "memtable": len(self.memtable),
            "levels": [f"L{i}: {len(l)} SSTs, {sum(len(s.entries) for s in l)} entries"
                      for i, l in enumerate(self.levels)],
            "stats": self.stats,
        }

def main():
    print("=== SSTable + LSM-Tree Store ===\n")
    store = LSMStore(memtable_limit=16)

    # Write 200 keys
    for i in range(200):
        store.put(f"key_{i:04d}", f"value_{i}")
    print(f"Wrote 200 keys")
    print(f"Info: {json.dumps(store.info(), indent=2)}")

    # Read
    for k in ["key_0000", "key_0099", "key_0199", "key_9999"]:
        v = store.get(k)
        print(f"  get({k}) = {v}")

    # Delete + read
    store.delete("key_0050")
    print(f"\nAfter delete key_0050: {store.get('key_0050')}")

    # Range scan on L1
    if store.levels[1]:
        results = store.levels[1][0].range_scan("key_0010", "key_0020")
        print(f"\nRange scan [key_0010, key_0020]: {len(results)} results")

    print(f"\nFinal stats: {store.stats}")

if __name__ == "__main__":
    main()
