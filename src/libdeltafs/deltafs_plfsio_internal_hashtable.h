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


}  // namespace plfsio
}  // namespace pdlfs
