/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_internal_hashtable.h"
#include "deltafs_plfsio_events.h"
#include "deltafs_plfsio_filter.h"

#include "pdlfs-common/logging.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/strutil.h"

#include <assert.h>
#include <math.h>
#include <algorithm>

namespace pdlfs {
namespace plfsio {

// Config for HashTable
HashOptions::HashOptions() : 
              data_buffer(4 << 20), // 4 GB
              bucket_size(4 << 10), // 4 KB
              key_size(8),          // 8 bytes
              value_size(32) {}     // 32 bytes

HashWriteBuffer::HashWriteBuffer(const HashOptions& options)
    : num_entries_(0), finished_(false) {
    key_size_ = options.key_size;
    value_size_ = options.value_size;
    table_size_ = options.data_buffer;
    bucket_size_ = options.bucket_size;
    num_of_bucket_ = table_size_ / bucket_size_;
    slot_per_bucket_ = bucket_size_ / (key_size_ + value_size_);
}

class HashWriteBuffer::Iter : public Iterator {
public:
  explicit Iter(const HashWriteBuffer* hash_write_buffer)
    : bucket_cursor_(-1),
      slot_cursor_(-1),
      padding(false),
      entries_per_bucket_(&hash_write_buffer->entries_per_bucket_[0]),
      num_of_bucket_(hash_write_buffer->num_of_bucket_),
      slot_per_bucket_(hash_write_buffer->slot_per_bucket_),
      key_size_(hash_write_buffer->key_size_),
      value_size_(hash_write_buffer->value_size_),
      buffer_(hash_write_buffer->buffer_) {}

  virtual ~Iter() {}

  virtual void Next() { 
    if (padding) {
      padding = false;
      slot_cursor_ = 0;
      bucket_cursor_ += 1;
      return;
    } 
    if (slot_cursor_ >= entries_per_bucket_[bucket_cursor_]) {
      if (entries_per_bucket_[bucket_cursor_] * (key_size_ + value_size_) < bucket_size_) {
        padding = true;
      }
    }
  }

  virtual void Prev() { 
    slot_cursor_ -= 1;
    if ( slot_cursor_ < 0 ) {
      slot_cursor_ = 0;
      bucket_cursor_ -= 1;
    }
  }

  virtual Status status() const { return Status::OK(); }

  virtual bool Valid() const { 
    if ( padding ) return true;
    if ( bucket_cursor_ < 0 || bucket_cursor_ >= num_of_bucket_ ) 
      return false;
    return slot_cursor_ >= 0 && slot_cursor_ < entries_per_bucket_[bucket_cursor_];
  }

  virtual void SeekToFirst() { 
    bucket_cursor_ = 0;
    slot_cursor_ = 0;
  }

  virtual void SeekToLast() { 
    bucket_cursor_ = num_of_bucket_ - 1;
    slot_cursor_ = entries_per_bucket_[bucket_cursor_] - 1;
  }

  virtual void Seek(const Slice& target) {
    /* Not supported */
  }

  virtual Slice key() const {
    assert(Valid());
    if (padding) {
      std::string key_;
      key_.resize(1,'.');
      return key_;
    }
    uint32_t offset = bucket_cursor_ * slot_per_bucket_ * (key_size_ + value_size_);
    offset += slot_cursor_ * (key_size_ + value_size_);
    std::string key_ = buffer_.substr(offset,key_size_);
    return key_;
  }

  virtual Slice value() const {
    assert(Valid());
    if (padding) {
      std::string value_;
      value.resize(bucket_size_ - entries_per_bucket_[bucket_cursor_] * (key_size_ + value_size_) - 1,'.');
      return value_;
    }
    uint32_t offset = bucket_cursor_ * slot_per_bucket_ * (key_size_ + value_size_);
    offset += slot_cursor_ * (key_size_ + value_size_);
    offset += key_size_;
    std::string value_ = buffer_.substr(offset,value_size_);
    return value_;
  }

private:
  int slot_cursor_;
  int bucket_cursor_;
  const uint16_t* entries_per_bucket_;
  uint16_t num_of_bucket_; 
  uint16_t slot_per_bucket_;
  size_t key_size_;
  size_t value_size_;
  bool padding;
  std::string buffer_;
};

Iterator* HashWriteBuffer::NewIterator() const {
  assert(finished_);
  return new Iter(this);
}

void HashWriteBuffer::Finish(bool skip_sort) {
  assert(!finished_);
  finished_ = true;
}

void HashWriteBuffer::Reset() {
  num_entries_ = 0;
  finished_ = false;
  entries_per_bucket_.clear();
  buffer_.clear();
}

void HashWriteBuffer::Reserve(size_t bytes_to_reserve) {
  assert(bytes_to_reserve == table_size_);
  size_t num_entries = num_of_bucket_ * slot_per_bucket_;
  buffer_.resize(num_entries * bytes_per_entry(),'.');
  entries_per_bucket_.resize(num_of_bucket_,0);
}

// Hash function for Hash Table
uint32_t HashWriteBuffer::Hash_1(const Slice& key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);  // from LevelDB
}

// Hash function for Hash Table
uint32_t HashWriteBuffer::Hash_2(const Slice& key) {
  return Hash(key.data(), key.size(), 5381); // Magic seed
}

std::string HashWriteBuffer::Get(std::string key, int& k) {
  uint16_t hash1 = (uint16_t)(Hash_1(key) % num_of_bucket_);
  uint16_t hash2 = (uint16_t)(Hash_2(key) % num_of_bucket_);
  uint32_t offset = hash1 * slot_per_bucket_ * bytes_per_entry();
  uint32_t offset_end = offset + entries_per_bucket_[hash1] * bytes_per_entry();
  while(offset < offset_end) {
    std::string key_ = buffer_.substr(offset,key_size_);
    offset += key_size_;
    std::string value_ = buffer_.substr(offset,value_size_);
    offset += value_size_;
    if ( key == key_ ) {
      k = 1;
      return value_;
    }
  }
  offset = hash2 * slot_per_bucket_ * bytes_per_entry();
  offset_end = offset + entries_per_bucket_[hash2] * bytes_per_entry();
  while(offset < offset_end) {
    std::string key_ = buffer_.substr(offset,key_size_);
    offset += key_size_;
    std::string value_ = buffer_.substr(offset,value_size_);
    offset += value_size_;
    if ( key == key_ ) {
      k = 2;
      return value_;
    }
  }
  k = 0;
  return "";
}

void HashWriteBuffer::insertBucket(uint16_t id, const Slice& key, const Slice& value) {
  uint32_t offset = id * slot_per_bucket_ * bytes_per_entry();
  offset += entries_per_bucket_[id] * bytes_per_entry();
  buffer_.replace(offset,key.size(),key.data());
  offset += key_size_;
  buffer_.replace(offset,value.size(),value.data());
  entries_per_bucket_[id] += 1;
}

bool HashWriteBuffer::evict(uint16_t id, const Slice& key, const Slice& value) {
  uint32_t offset = id * slot_per_bucket_ * bytes_per_entry();
  uint32_t offset_end = (id + 1) * slot_per_bucket_ * bytes_per_entry();
  while(offset < offset_end) {
    std::string key_ = buffer_.substr(offset,key_size_);
    offset += key_size_;
    std::string value_ = buffer_.substr(offset,value_size_);
    offset += value_size_;
    uint16_t hash1 = (uint16_t)(Hash_1(key_) % num_of_bucket_);
    uint16_t hash2 = (uint16_t)(Hash_2(key_) % num_of_bucket_);
    if ( id == hash1 && entries_per_bucket_[hash2] < slot_per_bucket_ ) {
      insertBucket(hash2,key_,value_);
      buffer_.replace(offset-bytes_per_entry(),key.size(),key.data());
      buffer_.replace(offset-value_size_,value.size(),value.data());
      return true;
    }
  }
  return false;
}

bool HashWriteBuffer::Add(const Slice& key, const Slice& value) {
  assert(!finished_);
  assert(key.size() != 0);
  assert(key.size() == key_size_);
  assert(value.size() == value_size_);
  uint16_t hash1 = (uint16_t)(Hash_1(key) % num_of_bucket_);
  uint16_t hash2 = (uint16_t)(Hash_2(key) % num_of_bucket_);
  if ( entries_per_bucket_[hash1] < slot_per_bucket_ ) {
    insertBucket(hash1,key,value);
  } else if ( entries_per_bucket_[hash2] < slot_per_bucket_ ) {
    insertBucket(hash2,key,value);
  } else {
    if ( evict(hash1,key,value) ) {}
    else if ( evict(hash2,key,value) ) {}
    else return false;
  }
  num_entries_++;
  return true;
}

size_t HashWriteBuffer::memory_usage() const {
  size_t result = 0;
  result += sizeof(uint16_t) * entries_per_bucket_.size();
  result += buffer_.size();
  result += sizeof(uint32_t);
  result += sizeof(uint16_t) * 2;
  result += sizeof(size_t) * 4;
  return result;
}

TableLogger::TableLogger(const DirOptions& dir_options, const HashOptions& hash_options
  LogSink* data, LogSink* indx)
: dir_options_(dir_options),
  hash_options_(hash_options),
  last_size_(0),
  total_num_blocks_(0),
  total_num_tables_(0),
  num_tables_(0),
  num_epochs_(0),
  uncommitted_data_block_(0),
  data_sink_(data),
  indx_sink_(indx),
  finished_(false) {
  // Sanity checks
  assert(indx_sink_ != NULL && data_sink_ != NULL);

  indx_sink_->Ref();
  data_sink_->Ref();

  dblock_per_table_ = hash_options_.data_buffer / dir_options_.block_size;
  if (!dir_options_.block_batch_size % dir_options_.block_size) {
    block_batch_size_ = dir_options_.block_batch_size;
  } else {
    block_batch_size_ = dir_options_.block_batch_size / dir_options_.block_size;
    block_batch_size_ *= dir_options_.block_size;
  }
  if ( block_batch_size_ == 0 ) 
    block_batch_size_ = dir_options_.block_size;

  data_block_size_ = dir_options_.block_size;
  
  // Allocate memory
  data_block_.reserve(block_batch_size_);

  // write meta data into index file
  write8bytes(hash_options_.bucket_size);
  write8bytes(hash_options_.key_size);
  write8bytes(hash_options_.value_size);
  write8bytes(dblock_per_table_);
}

HashTableLogger::~HashTableLogger() {
  indx_sink_->Unref();
  data_sink_->Unref();
}

HashTableLogger::write8bytes(size_t info) {
  std::string s_info;
  s_info.resize(sizeof(size_t),'0');
  std::string info2s = to_string(info);
  s_info.replace(sizeof(size_t)-info2s.length(),info2s.length(),info2s);
  status_ = indx_sink_->Lwrite(s_info);
}

void HashTableLogger::Commit() {
  assert(!finished_);  // Finish() has not been called
  if (!ok()) return;  // Abort
  if (!uncommitted_data_block_) return; // skip empty commit

  data_sink_->Lock();
  const size_t base = data_sink_->Ptell();
  status_ = data_sink_->Lwrite(data_block_);
  data_sink_->Unlock();
  if (!ok()) return;  // Abort

  for (int i = 0; i < uncommitted_data_block_; i++) {
    write8bytes(base);
    base += data_block_size_;
  }

  uncommitted_data_block_ = 0;
  last_size_ = 0;
  data_block_.clear();
}

void HashTableLogger::Add(const Slice& key, const Slice& value) {
  assert(!finished_);       // Finish() has not been called
  assert(key.size() != 0);  // Keys cannot be empty
  if (!ok()) return;        // Abort
  data_block_.append(key.data());
  data_block_.append(value.data());
  if (data_block_.size() - last_size_ >= dir_options_.block_size * dir_options_.block_util) {
    total_num_blocks_ += 1;
    uncommitted_data_block_ += 1;
    last_size_ = data_block_.size();
    if (data_block_.size() >= block_batch_size_) {
      commit();
    }
  }
}

template <typename T>
void HashTableLogger::EndTable(T* filter_block, ChunkType filter_type) {
  assert(!finished_);  // Finish() has not been called
  assert(filter_block == NULL);
  assert(filter_type == NULL);
  if (!ok()) {
    return;
  }
  if (num_tables_ > kMaxTableNo) {
    status_ = Status::AssertionFailed("Too many tables");
  }
  if (ok()) {
    total_num_tables_ += 1;
    num_tables_ += 1;
  }
}

void HashTableLogger::MakeEpoch() {
  assert(!finished_);  // Finish() has not been called
  if (!ok()) {
    return;  // Abort
  } else if (num_epochs_ >= kMaxEpochNo) {
    status_ = Status::AssertionFailed("Too many epochs");
    return;
  }
  table_per_epoch_.push_back(num_tables_);
  num_tables_ = 0;
  num_epoches_ += 1;
}

Status HashTableLogger::Finish() {
  assert(!finished_);  // Finish() has not been called
  finished_ = true;
  if (!ok()) {
    return status_;
  }
  commit();
  for (int i = 0; i < table_per_epoch_.size(); i++) {
    write8bytes((size_t)(table_per_epoch_[i]));
  }
  write8bytes((size_t)num_epochs_);
  table_per_epoch_.clear();
  data_block_.clear();
  return status_;
}


}  // namespace plfsio
}  // namespace pdlfs