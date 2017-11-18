/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "deltafs_plfsio.h"
#include "deltafs_plfsio_format.h"
#include "deltafs_plfsio_log.h"

#include "pdlfs-common/env_files.h"
#include "pdlfs-common/port.h"

#ifndef NDEBUG
#include <set>
#endif
#include <string>
#include <vector>

namespace pdlfs {
namespace plfsio {

// Config for HashTable
struct HashOptions {
  HashOptions();
  // Default: 4MB
  size_t data_buffer;
  // Default: 4KB
  size_t bucket_size;
  // Default: 8bytes
  size_t key_size;
  // Default: 32bytes;
  size_t value_size;
};

/* 
  Non-thread-safe in-memory hash table.
*/
class HashWriteBuffer {
public:
  explicit HashWriteBuffer(const HashOptions& options);
  ~HashWriteBuffer() {}

  size_t memory_usage() const;  // Report real memory usage
  size_t bytes_per_entry() const {
    return key_size_ + value_size_;
  }
  void Reserve(size_t bytes_to_reserve);
  // Return the size of all the key/value pair in buffer
  size_t CurrentBufferSize() const { 
    return num_entries_ * bytes_per_entry();
  }
  int32_t NumEntries() const { 
    return num_entries_; 
  }
  bool Add(const Slice& key, const Slice& value);
  // need to change from string to Slice
  std::string Get(std::string key, int& k);
  Iterator* NewIterator() const;
  void Finish(bool skip_sort = false);
  void Reset();

private:
  void insertBucket(uint16_t id, const Slice& key, const Slice& value);
  bool evict(uint16_t id, const Slice& key, const Slice& value);
  uint32_t Hash_1(const Slice& key);
  uint32_t Hash_2(const Slice& key);

  std::vector<uint16_t> entries_per_bucket_;
  std::string buffer_;
  uint32_t num_entries_;
  uint16_t num_of_bucket_;   // Number of entries packed per table
  uint16_t slot_per_bucket_; // Target table size
  size_t table_size_;
  size_t bucket_size_;
  size_t key_size_;
  size_t value_size_;
  bool finished_;

  // No copying allowed
  void operator=(const HashWriteBuffer&);
  HashWriteBuffer(const HashWriteBuffer&);

  class Iter;
};

// Write hash table contents into a pair of log files.
class HashTableLogger {
public:
 HashTableLogger(const DirOptions& dir_options, const HashOptions& hash_options, LogSink* data, LogSink* indx);
 ~HashTableLogger();

 bool ok() const { return status_.ok(); }
 Status status() const { return status_; }

 void Add(const Slice& key, const Slice& value);

 // Finish a 4MB (default) MemTable
 // Force the start of a new table.
 // Filter should be null for HashTableLogger
 // REQUIRES: Finish() has not been called.
 template <typename T>
 void EndTable(T* filter, ChunkType filter_type);

 // Force the start of a new epoch.
 // REQUIRES: Finish() has not been called.
 void MakeEpoch();

 // Finalize table contents.
 // No further writes.
 Status Finish();

private:
 // 
 // Finish a 32KB (default) data block
 // End the current block and force the start of a new data block.
 // REQUIRES: Finish() has not been called.
 void EndBlock();

 // Flush buffered data blocks and finalize their indexes.
 // REQUIRES: Finish() has not been called.
 void Commit();

 void write8bytes(size_t info);

 const DirOptions& dir_options_;
 const HashOptions& hash_options_;

 template <typename T>
 friend class DirLogger;

 // No copying allowed
 void operator=(const HashTableLogger&);
 HashTableLogger(const HashTableLogger&);

 Status status_;
 std::string data_block_;
 std::vector<uint16_t> table_per_epoch_; 
 size_t dblock_per_table_;
 size_t block_batch_size_;
 uint32_t total_num_blocks_;
 uint32_t total_num_tables_;
 uint32_t num_tables_;  // Number of tables generated within the current epoch
 uint32_t num_epochs_;  // Number of epochs generated
 uint32_t uncommitted_data_block_;
 LogSink* data_sink_;
 LogSink* indx_sink_;
 bool finished_;
};


}  // namespace plfsio
}  // namespace pdlfs
