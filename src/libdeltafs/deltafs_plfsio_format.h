#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

// XXX: Reuse formats defined by leveldb

#include "../../external/pdlfs-common/src/leveldb/block.h"
#include "../../external/pdlfs-common/src/leveldb/block_builder.h"
#include "../../external/pdlfs-common/src/leveldb/format.h"

#include "pdlfs-common/leveldb/comparator.h"
#include "pdlfs-common/leveldb/iterator.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/env.h"

namespace pdlfs {
namespace plfsio {
static const uint32_t kMaxTablesPerEpoch = 9999;
static const uint32_t kMaxEpoches = 9999;

// Formats used by keys in the epoch index block.
extern std::string EpochKey(uint32_t epoch, uint32_t table);
extern Status ParseEpochKey(const Slice& input, uint32_t* epoch,
                            uint32_t* table);

// Table handle is a pointer to extends of a file that store the index and
// filter data of a table. In addition, table handle also stores
// the key range.
class TableHandle {
 public:
  TableHandle();

  // The offset of the filter block in a file.
  uint64_t filter_offset() const { return filter_offset_; }
  void set_filter_offset(uint64_t offset) { filter_offset_ = offset; }

  // The size of the filter block.
  uint64_t filter_size() const { return filter_size_; }
  void set_filter_size(uint64_t size) { filter_size_ = size; }

  // The offset of the index block in a file.
  uint64_t offset() const { return offset_; }
  void set_offset(uint64_t offset) { offset_ = offset; }

  // The size of the index block.
  uint64_t size() const { return size_; }
  void set_size(uint64_t size) { size_ = size; }

  // The smallest key within the table.
  Slice smallest_key() const { return smallest_key_; }
  void set_smallest_key(const Slice& key) { smallest_key_ = key.ToString(); }

  // The largest key within the table.
  Slice largest_key() const { return largest_key_; }
  void set_largest_key(const Slice& key) { largest_key_ = key.ToString(); }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

 private:
  // Key range of the table
  std::string smallest_key_;
  std::string largest_key_;
  // Handle to the filter and the index block
  uint64_t filter_offset_;
  uint64_t filter_size_;
  uint64_t offset_;
  uint64_t size_;
};

// Fixed information stored at the tail end
// of every log file.
class Footer {
 public:
  Footer();

  // The block handle for the epoch index.
  const BlockHandle& epoch_index_handle() const { return epoch_index_handle_; }
  void set_epoch_index_handle(const BlockHandle& h) { epoch_index_handle_ = h; }

  // Total number of epoches
  uint32_t num_epoches() const { return num_epoches_; }
  void set_num_epoches(uint32_t num) { num_epoches_ = num; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

  enum { kEncodeLength = BlockHandle::kMaxEncodedLength + 4 };

 private:
  BlockHandle epoch_index_handle_;
  uint32_t num_epoches_;
};

inline TableHandle::TableHandle()
    : filter_offset_(~static_cast<uint64_t>(0) /* Invalid offset */),
      filter_size_(~static_cast<uint64_t>(0) /* Invalid size */),
      offset_(~static_cast<uint64_t>(0) /* Invalid offset */),
      size_(~static_cast<uint64_t>(0) /* Invalid size */) {
  // Empty
}

inline Footer::Footer()
    : num_epoches_(~static_cast<uint32_t>(0) /* Invalid num */) {
  // Empty
}

}  // namespace plfsio
}  // namespace pdlfs
