/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_internal.h"

#include "pdlfs-common/logging.h"
#include "pdlfs-common/strutil.h"

#include <assert.h>
#include <math.h>
#include <algorithm>

namespace pdlfs {
extern const char* GetLengthPrefixedSlice(const char* p, const char* limit,
                                          Slice* result);
namespace plfsio {

class WriteBuffer::Iter : public Iterator {
 public:
  explicit Iter(const WriteBuffer* write_buffer)
      : cursor_(-1),
        offsets_(&write_buffer->offsets_[0]),
        num_entries_(write_buffer->num_entries_),
        buffer_(write_buffer->buffer_) {}

  virtual ~Iter() {}
  virtual void Next() { cursor_++; }
  virtual void Prev() { cursor_--; }
  virtual Status status() const { return Status::OK(); }
  virtual bool Valid() const { return cursor_ >= 0 && cursor_ < num_entries_; }
  virtual void SeekToFirst() { cursor_ = 0; }
  virtual void SeekToLast() { cursor_ = num_entries_ - 1; }
  virtual void Seek(const Slice& target) {
    // Not supported
  }

  virtual Slice key() const {
    assert(Valid());
    Slice result;
    const char* p = &buffer_[offsets_[cursor_]];
    Slice input = buffer_;
    assert(p - buffer_.data() >= 0);
    input.remove_prefix(p - buffer_.data());
    if (GetLengthPrefixedSlice(&input, &result)) {
      return result;
    } else {
      assert(false);
      result = Slice();
      return result;
    }
  }

  virtual Slice value() const {
    assert(Valid());
    Slice result;
    const char* p = &buffer_[offsets_[cursor_]];
    Slice input = buffer_;
    assert(p - buffer_.data() >= 0);
    input.remove_prefix(p - buffer_.data());
    if (GetLengthPrefixedSlice(&input, &result) &&
        GetLengthPrefixedSlice(&input, &result)) {
      return result;
    } else {
      assert(false);
      result = Slice();
      return result;
    }
  }

 private:
  int cursor_;
  const uint32_t* offsets_;
  int num_entries_;
  Slice buffer_;
};

Iterator* WriteBuffer::NewIterator() const {
  assert(finished_);
  return new Iter(this);
}

namespace {
struct STLLessThan {
  Slice buffer_;

  STLLessThan(const std::string& buffer) : buffer_(buffer) {}
  bool operator()(uint32_t a, uint32_t b) {
    Slice key_a = GetKey(a);
    Slice key_b = GetKey(b);
    assert(!key_a.empty() && !key_b.empty());
    return key_a < key_b;
  }

  Slice GetKey(uint32_t offset) {
    Slice result;
    bool ok = GetLengthPrefixedSlice(buffer_.data() + offset,
                                     buffer_.data() + buffer_.size(),  // Limit
                                     &result);
    if (ok) {
      return result;
    } else {
      assert(false);
      return result;
    }
  }
};
}  // namespace

void WriteBuffer::Finish() {
  // Sort entries
  assert(!finished_);
  std::vector<uint32_t>::iterator begin = offsets_.begin();
  std::vector<uint32_t>::iterator end = offsets_.end();
  std::sort(begin, end, STLLessThan(buffer_));
  finished_ = true;
}

void WriteBuffer::Reset() {
  num_entries_ = 0;
  finished_ = false;
  offsets_.clear();
  buffer_.clear();
}

void WriteBuffer::Reserve(uint32_t num_entries, size_t size_per_entry) {
  buffer_.reserve(num_entries * (size_per_entry + 2));
  offsets_.reserve(num_entries);
}

void WriteBuffer::Add(const Slice& key, const Slice& value) {
  assert(!finished_);       // Finish() has not been called
  assert(key.size() != 0);  // Key cannot be empty
  size_t offset = buffer_.size();
  PutLengthPrefixedSlice(&buffer_, key);
  PutLengthPrefixedSlice(&buffer_, value);
  offsets_.push_back(offset);
  num_entries_++;
}

TableLogger::TableLogger(const Options& options, LogSink* data, LogSink* index)
    : options_(options),
      data_block_(kDataBlkRestartInt),
      index_block_(kNonDataBlkRestartInt),
      epoch_block_(kNonDataBlkRestartInt),
      pending_index_entry_(false),
      pending_epoch_entry_(false),
      num_tables_(0),
      num_epoches_(0),
      data_log_(data),
      index_log_(index),
      finished_(false) {
  assert(index_log_ != NULL && data_log_ != NULL);
  index_log_->Ref();
  data_log_->Ref();

#if VERBOSE >= 4
  Verbose(__LOG_ARGS__, 4, "Epoch #%d: started", int(num_epoches_));
#endif
}

TableLogger::~TableLogger() {
  index_log_->Unref();
  data_log_->Unref();
  if (num_tables_ == 0) {
#if VERBOSE >= 4
    Verbose(__LOG_ARGS__, 4, "Epoch #%d: discarded", int(num_epoches_));
#endif
  }
}

void TableLogger::EndEpoch() {
  assert(!finished_);  // Finish() has not been called
  EndTable();
  if (ok() && num_tables_ != 0) {
#if VERBOSE >= 4
    Verbose(__LOG_ARGS__, 4, "Epoch #%d: (#%d tables) closed",
            int(num_epoches_), int(num_tables_));
#endif
    num_tables_ = 0;
    assert(num_epoches_ < kMaxEpoches);
    num_epoches_++;
#if VERBOSE >= 4
    Verbose(__LOG_ARGS__, 4, "Epoch #%d: started", int(num_epoches_));
#endif
  }
}

void TableLogger::EndTable() {
  assert(!finished_);  // Finish() has not been called
  EndBlock();
  if (!ok()) return;  // Abort
  if (pending_index_entry_) {
    assert(data_block_.empty());
    BytewiseComparator()->FindShortSuccessor(&last_key_);
    std::string handle_encoding;
    pending_index_handle_.EncodeTo(&handle_encoding);
    index_block_.Add(last_key_, handle_encoding);
    pending_index_entry_ = false;
  } else if (index_block_.empty()) {
    return;  // No more work
  }

  assert(!pending_epoch_entry_);
  Slice contents = index_block_.Finish();
  const uint64_t offset = index_log_->Ltell();
  status_ = index_log_->Lwrite(contents);

#if VERBOSE >= 6
  Verbose(__LOG_ARGS__, 6, "Index block written: (offset=%llu, size=%llu) %s",
          static_cast<unsigned long long>(offset),
          static_cast<unsigned long long>(contents.size()),
          status_.ToString().c_str());
#endif

  if (ok()) {
    index_block_.Reset();
    pending_epoch_handle_.set_size(contents.size());
    pending_epoch_handle_.set_offset(offset);
    pending_epoch_entry_ = true;
  }

  if (pending_epoch_entry_) {
    assert(index_block_.empty());
    pending_epoch_handle_.set_smallest_key(smallest_key_);
    BytewiseComparator()->FindShortSuccessor(&largest_key_);
    pending_epoch_handle_.set_largest_key(largest_key_);
    std::string handle_encoding;
    pending_epoch_handle_.EncodeTo(&handle_encoding);
    epoch_block_.Add(EpochKey(num_epoches_, num_tables_), handle_encoding);
    pending_epoch_entry_ = false;
  }

  if (ok()) {
    smallest_key_.clear();
    largest_key_.clear();
    last_key_.clear();
    assert(num_tables_ < kMaxTablesPerEpoch);
    num_tables_++;
  }
}

void TableLogger::EndBlock() {
  assert(!finished_);               // Finish() has not been called
  if (data_block_.empty()) return;  // No more work
  if (!ok()) return;                // Abort
  assert(!pending_index_entry_);
  Slice contents = data_block_.Finish();
  const uint64_t offset = data_log_->Ltell();
  status_ = data_log_->Lwrite(contents);

#if VERBOSE >= 6
  Verbose(__LOG_ARGS__, 6, "Data block written: (offset=%llu, size=%llu) %s",
          static_cast<unsigned long long>(offset),
          static_cast<unsigned long long>(contents.size()),
          status_.ToString().c_str());
#endif

  if (ok()) {
    data_block_.Reset();
    pending_index_handle_.set_size(contents.size());
    pending_index_handle_.set_offset(offset);
    pending_index_entry_ = true;
  }
}

void TableLogger::Add(const Slice& key, const Slice& value) {
  assert(!finished_);       // Finish() has not been called
  assert(key.size() != 0);  // Key cannot be empty
  if (!ok()) return;        // Abort

  if (!last_key_.empty()) {
    // Keys within a single table are expected to be added in a sorted order.
    // Duplicated keys are allowed
    assert(key.compare(last_key_) >= 0);
  }
  if (smallest_key_.empty()) {
    smallest_key_ = key.ToString();
  }
  largest_key_ = key.ToString();

  // Add an index entry if there is one pending insertion
  if (pending_index_entry_) {
    assert(data_block_.empty());
    BytewiseComparator()->FindShortestSeparator(&last_key_, key);
    std::string handle_encoding;
    pending_index_handle_.EncodeTo(&handle_encoding);
    index_block_.Add(last_key_, handle_encoding);
    pending_index_entry_ = false;
  }

  last_key_ = key.ToString();
  data_block_.Add(key, value);
  if (data_block_.CurrentSizeEstimate() >= options_.block_size) {
    EndBlock();
  }
}

Status TableLogger::Finish() {
  assert(!finished_);
  EndEpoch();
  finished_ = true;
  if (!ok()) return status_;
  BlockHandle epoch_index_handle;
  std::string tail;
  Footer footer;

  assert(!pending_epoch_entry_);
  Slice contents = epoch_block_.Finish();
  const uint64_t offset = index_log_->Ltell();
  status_ = index_log_->Lwrite(contents);

#if VERBOSE >= 6
  Verbose(__LOG_ARGS__, 6, "Epoch index written: (offset=%llu, size=%llu) %s",
          static_cast<unsigned long long>(offset),
          static_cast<unsigned long long>(contents.size()),
          status_.ToString().c_str());
#endif

  if (ok()) {
    epoch_index_handle.set_size(contents.size());
    epoch_index_handle.set_offset(offset);

    footer.set_epoch_index_handle(epoch_index_handle);
    footer.set_num_epoches(num_epoches_);
    footer.EncodeTo(&tail);
  }

  if (ok()) {
    status_ = index_log_->Lwrite(tail);
  }

#if VERBOSE >= 6
  Verbose(__LOG_ARGS__, 6, "Tail written: (size=%llu) %s",
          static_cast<unsigned long long>(tail.size()),
          status_.ToString().c_str());
#endif

  return status_;
}

PlfsIoLogger::PlfsIoLogger(const Options& options, port::Mutex* mu,
                           port::CondVar* cv, LogSink* data, LogSink* index)
    : options_(options),
      mutex_(mu),
      bg_cv_(cv),
      has_bg_compaction_(false),
      pending_epoch_flush_(false),
      pending_finish_(false),
      table_logger_(options, data, index),
      mem_buf_(NULL),
      imm_buf_(NULL),
      imm_buf_is_epoch_flush_(false),
      imm_buf_is_finish_(false) {
  // Sanity checks
  assert(mu != NULL && cv != NULL);

  // Determine the right table size and bloom filter size
  size_t bytes_per_entry =
      VarintLength(options_.key_size) + VarintLength(options_.value_size);
  bytes_per_entry += options.key_size + options.value_size;
  size_t total_bits_per_entry = 8 * bytes_per_entry + options.bfbits_per_key;
  // Estimated amount of entries per table
  entries_per_table_ = static_cast<uint32_t>(
      ceil(8 * options_.memtable_size / total_bits_per_entry));

  table_size_ = entries_per_table_ * bytes_per_entry;
  // Compute bloom filter size (in both bits and bytes)
  bf_bits_ = entries_per_table_ * options.bfbits_per_key;
  // For small n, we can see a very high false positive rate.  Fix it
  // by enforcing a minimum bloom filter length.
  if (bf_bits_ > 0 && bf_bits_ < 64) {
    bf_bits_ = 64;
  }
  bf_bytes_ = (bf_bits_ + 7) / 8;
  bf_bits_ = bf_bytes_ * 8;

#if VERBOSE >= 2
  Verbose(__LOG_ARGS__, 2, "plfsdir.memtable.entries_per_table -> %d",
          int(entries_per_table_));
  Verbose(__LOG_ARGS__, 2, "plfsdir.memtable.table_size -> %s",
          PrettySize(table_size_).c_str());
  Verbose(__LOG_ARGS__, 2, "plfsdir.memtable.bf_size -> %s",
          PrettySize(bf_bytes_).c_str());
#endif

  mem_buf_ = &buf0_;
}

PlfsIoLogger::~PlfsIoLogger() {
  mutex_->AssertHeld();
  while (has_bg_compaction_) {
    bg_cv_->Wait();
  }
}

// If dry_run is set, we will only perform status checks (which includes write
// errors, buffer space, and compaction queue depth) such that no
// compaction jobs will be scheduled.
Status PlfsIoLogger::Finish(bool dry_run) {
  mutex_->AssertHeld();
  while (pending_finish_ ||
         pending_epoch_flush_ ||  // The previous job is still in-progress
         imm_buf_ != NULL) {      // There's an on-going compaction job
    if (dry_run || options_.non_blocking) {
      return Status::BufferFull(Slice());
    } else {
      bg_cv_->Wait();
    }
  }

  Status status;
  if (dry_run) {
    // Status check only
    status = table_logger_.status();
  } else {
    pending_finish_ = true;
    pending_epoch_flush_ = true;
    status = Prepare(true, true);
    if (!status.ok()) {
      pending_epoch_flush_ = false;  // Avoid blocking future attempts
      pending_finish_ = false;
    } else if (status.ok() && !options_.non_blocking) {
      while (pending_epoch_flush_ || pending_finish_) {
        bg_cv_->Wait();
      }
    }
  }

  return status;
}

// If dry_run is set, we will only perform status checks (which includes write
// errors, buffer space, and compaction queue depth) such that no
// compaction jobs will be scheduled.
Status PlfsIoLogger::MakeEpoch(bool dry_run) {
  mutex_->AssertHeld();
  while (pending_epoch_flush_ ||  // The previous job is still in-progress
         imm_buf_ != NULL) {      // There's an on-going compaction job
    if (dry_run || options_.non_blocking) {
      return Status::BufferFull(Slice());
    } else {
      bg_cv_->Wait();
    }
  }

  Status status;
  if (dry_run) {
    // Status check only
    status = table_logger_.status();
  } else {
    pending_epoch_flush_ = true;
    status = Prepare(true, false);
    if (!status.ok()) {
      pending_epoch_flush_ = false;  // Avoid blocking future attempts
    } else if (status.ok() && !options_.non_blocking) {
      while (pending_epoch_flush_) {
        bg_cv_->Wait();
      }
    }
  }

  return status;
}

Status PlfsIoLogger::Add(const Slice& key, const Slice& value) {
  mutex_->AssertHeld();
  Status status = Prepare(false, false);
  if (status.ok()) {
    mem_buf_->Add(key, value);
  }

  return status;
}

Status PlfsIoLogger::Prepare(bool flush, bool finish) {
  mutex_->AssertHeld();
  Status status;
  assert(mem_buf_ != NULL);
  while (true) {
    if (!table_logger_.ok()) {
      status = table_logger_.status();
      break;
    } else if (!flush && mem_buf_->CurrentBufferSize() < table_size_) {
      // There is room in current write buffer
      break;
    } else if (imm_buf_ != NULL) {
      if (options_.non_blocking) {
        status = Status::BufferFull(Slice());
        break;
      } else {
        bg_cv_->Wait();
      }
    } else {
      // Attempt to switch to a new write buffer
      mem_buf_->Finish();
      assert(imm_buf_ == NULL);
      imm_buf_ = mem_buf_;
      if (flush) {
        imm_buf_is_epoch_flush_ = true;
        flush = false;
      }
      if (finish) {
        imm_buf_is_finish_ = true;
        finish = false;
      }
      WriteBuffer* const current_buf = mem_buf_;
      MaybeSchedualCompaction();
      if (current_buf == &buf0_) {
        mem_buf_ = &buf1_;
      } else {
        mem_buf_ = &buf0_;
      }
    }
  }

  return status;
}

void PlfsIoLogger::MaybeSchedualCompaction() {
  mutex_->AssertHeld();
  if (!has_bg_compaction_) {
    if (imm_buf_ != NULL) {
      has_bg_compaction_ = true;
      if (options_.compaction_pool != NULL) {
        options_.compaction_pool->Schedule(PlfsIoLogger::BGWork, this);
      } else {
        // Run in current thread context
        DoCompaction();
      }
    }
  }
}

void PlfsIoLogger::BGWork(void* arg) {
  PlfsIoLogger* io = reinterpret_cast<PlfsIoLogger*>(arg);
  io->mutex_->Lock();
  io->DoCompaction();
  io->mutex_->Unlock();
}

void PlfsIoLogger::DoCompaction() {
  mutex_->AssertHeld();
  assert(has_bg_compaction_);
  if (imm_buf_ != NULL) {
    CompactWriteBuffer();
    imm_buf_->Reset();
    imm_buf_is_epoch_flush_ = false;
    imm_buf_is_finish_ = false;
    imm_buf_ = NULL;
  }
  has_bg_compaction_ = false;
  MaybeSchedualCompaction();
  bg_cv_->SignalAll();
}

void PlfsIoLogger::CompactWriteBuffer() {
  mutex_->AssertHeld();
  const WriteBuffer* const buffer = imm_buf_;
  assert(buffer != NULL);
  const bool pending_finish = pending_finish_;
  const bool pending_epoch_flush = pending_epoch_flush_;
  const bool is_finish = imm_buf_is_finish_;
  const bool is_epoch_flush = imm_buf_is_epoch_flush_;
  TableLogger* const dest = &table_logger_;
  mutex_->Unlock();
#if VERBOSE >= 2
  static unsigned long long seq = 0;
  Verbose(__LOG_ARGS__, 2, "Compacting #%llu (epoch_flush=%d, finish=%d) ...",
          ++seq, int(is_epoch_flush), int(is_finish));
  unsigned long long size = 0;
  unsigned num_keys = 0;
#endif

  Iterator* const iter = buffer->NewIterator();
  iter->SeekToFirst();
  for (; iter->Valid(); iter->Next()) {
#if VERBOSE >= 2
    size += iter->value().size();
    size += iter->key().size();
    num_keys++;
#endif
    dest->Add(iter->key(), iter->value());
    if (!dest->ok()) {
      break;
    }
  }

  if (dest->ok()) {
    // Empty tables will be implicitly discarded
    dest->EndTable();
  }
  if (dest->ok() && is_epoch_flush) {
    // Empty epoches will be implicitly discarded
    dest->EndEpoch();
  }
  if (dest->ok() && is_finish) {
    dest->Finish();
  }

#if VERBOSE >= 2
  Verbose(__LOG_ARGS__, 2, "Compacted #%llu: %llu bytes (%u records) %s", seq,
          size, num_keys, dest->status().ToString().c_str());
#endif

  delete iter;
  mutex_->Lock();
  if (is_epoch_flush) {
    if (pending_epoch_flush) {
      pending_epoch_flush_ = false;
    }
  }
  if (is_finish) {
    if (pending_finish) {
      pending_finish_ = false;
    }
  }
}

template <typename T>
static Status ReadBlock(LogSource* file, const Options& options,
                        const T& handle, BlockContents* result) {
  result->data = Slice();
  result->heap_allocated = false;
  result->cachable = false;

  assert(file != NULL);
  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n];
  Slice contents;
  Status s = file->Read(handle.offset(), n, &contents, buf);
  if (s.ok()) {
    if (contents.size() != n) {
      s = Status::Corruption("truncated block read");
    }
  }
  if (!s.ok()) {
    delete[] buf;
    return s;
  }

  // Pointer to where read put the data
  const char* data = contents.data();
  if (data != buf) {
    // File implementation gave us pointer to some other data.
    // Use it directly under the assumption that it will be live
    // while the file is open.
    delete[] buf;
    result->data = Slice(data, n);
    result->heap_allocated = false;
    result->cachable = false;  // Avoid double cache
  } else {
    result->data = Slice(buf, n);
    result->heap_allocated = true;
    result->cachable = true;
  }

  return s;
}

Status PlfsIoReader::Get(const Slice& key, const BlockHandle& handle,
                         Saver saver, void* arg) {
  Status s;
  BlockContents contents;
  s = ReadBlock(data_src_, options_, handle, &contents);
  if (!s.ok()) {
    return s;
  }

  Block* block = new Block(contents);
  Iterator* iter = block->NewIterator(BytewiseComparator());
  iter->Seek(key);
  if (iter->Valid()) {
    if (iter->key() == key) {
      saver(arg, key, iter->value());
    }
  }

  delete iter;
  delete block;
  return s;
}

Status PlfsIoReader::Get(const Slice& key, const TableHandle& handle,
                         Saver saver, void* arg) {
  Status s;
  BlockContents contents;
  s = ReadBlock(index_src_, options_, handle, &contents);
  if (!s.ok()) {
    return s;
  }

  Block* block = new Block(contents);
  Iterator* iter = block->NewIterator(BytewiseComparator());
  iter->Seek(key);
  if (iter->Valid()) {
    BlockHandle handle;
    Slice handle_encoding = iter->value();
    s = handle.DecodeFrom(&handle_encoding);
    if (s.ok()) {
      s = Get(key, handle, saver, arg);
    }
  }

  delete iter;
  delete block;
  return s;
}

namespace {
struct SaverState {
  std::string* dst;
  bool found;
};

static void SaveValue(void* arg, const Slice& key, const Slice& value) {
  SaverState* state = reinterpret_cast<SaverState*>(arg);
  state->dst->append(value.data(), value.size());
  state->found = true;
}

static inline Iterator* NewEpochIterator(Block* epoch_index) {
  return epoch_index->NewIterator(BytewiseComparator());
}
}  // namespace

Status PlfsIoReader::Get(const Slice& key, uint32_t epoch, std::string* dst) {
  Status s;
  if (epoch_iter_ == NULL) {
    epoch_iter_ = NewEpochIterator(epoch_index_);
  }
  std::string epoch_key;
  uint32_t table = 0;
  while (s.ok()) {
    SaverState state;
    state.found = false;
    state.dst = dst;
    TableHandle handle;
    epoch_key = EpochKey(epoch, table);
    epoch_iter_->Seek(epoch_key);
    if (!epoch_iter_->Valid()) break;
    if (epoch_iter_->key() != epoch_key) break;
    Slice handle_encoding = epoch_iter_->value();
    s = handle.DecodeFrom(&handle_encoding);
    if (s.ok()) {
      s = Get(key, handle, SaveValue, &state);
      if (s.ok()) {
        if (state.found) {
          break;
        }
      }
    }

    table++;
  }

  return s;
}

Status PlfsIoReader::Gets(const Slice& key, std::string* dst) {
  Status s;
  if (num_epoches_ != 0) {
    if (epoch_iter_ == NULL) {
      epoch_iter_ = NewEpochIterator(epoch_index_);
    }
    uint32_t epoch = 0;
    while (s.ok()) {
      s = Get(key, epoch, dst);
      if (epoch < num_epoches_ - 1) {
        epoch++;
      } else {
        break;
      }
    }
  }

  return s;
}

PlfsIoReader::PlfsIoReader(const Options& o, LogSource* d, LogSource* i)
    : options_(o),
      num_epoches_(0),
      epoch_iter_(NULL),
      epoch_index_(NULL),
      index_src_(i),
      data_src_(d) {
  assert(index_src_ != NULL && data_src_ != NULL);
  index_src_->Ref();
  data_src_->Ref();
}

PlfsIoReader::~PlfsIoReader() {
  delete epoch_iter_;
  delete epoch_index_;
  index_src_->Unref();
  data_src_->Unref();
}

Status PlfsIoReader::Open(const Options& options, LogSource* data,
                          LogSource* index, PlfsIoReader** result) {
  *result = NULL;
  Status s;
  char space[Footer::kEncodeLength];
  Slice input;
  if (index->Size() >= sizeof(space)) {
    s = index->Read(index->Size() - sizeof(space), sizeof(space), &input,
                    space);
  } else {
    s = Status::Corruption("index too short to be valid");
  }

  if (!s.ok()) {
    return s;
  }

  Footer footer;
  s = footer.DecodeFrom(&input);
  if (!s.ok()) {
    return s;
  }

  BlockContents contents;
  s = ReadBlock(index, options, footer.epoch_index_handle(), &contents);
  if (!s.ok()) {
    return s;
  }

  PlfsIoReader* reader = new PlfsIoReader(options, data, index);
  reader->num_epoches_ = footer.num_epoches();
  Block* block = new Block(contents);
  reader->epoch_index_ = block;

  *result = reader;
  return s;
}

}  // namespace plfsio
}  // namespace pdlfs
