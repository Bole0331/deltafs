#pragma once

/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "mds_api.h"

#include "pdlfs-common/dcntl.h"
#include "pdlfs-common/lease.h"
#include "pdlfs-common/map.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

class MDS::SRV : public MDS {
 public:
  SRV(const MDSOptions&);
  virtual ~SRV();

#define DEC_OP(OP) virtual Status OP(const OP##Options&, OP##Ret*);

  DEC_OP(Fstat)
  DEC_OP(Fcreat)
  DEC_OP(Mkdir)
  DEC_OP(Chmod)
  DEC_OP(Utime)
  DEC_OP(Trunc)
  DEC_OP(Lookup)
  DEC_OP(Listdir)
  DEC_OP(Readidx)
  DEC_OP(Opensession)
  DEC_OP(Getinput)
  DEC_OP(Getoutput)

#undef DEC_OP

 private:
  Status LoadDir(const DirId& id, DirInfo* info, DirIndex* index);
  Status FetchDir(const DirId& id, Dir::Ref** ref);
  Status ProbeDir(const Dir* dir);

  // Constant after construction
  MDSEnv* mds_env_;
  uint64_t NowMicros() { return mds_env_->env->NowMicros(); }
  void SleepForMicroseconds(int micros) {
    mds_env_->env->SleepForMicroseconds(micros);
  }
  MDB* mdb_;
  typedef DirIndexOptions GIGA;
  GIGA giga_;
  bool paranoid_checks_;
  uint64_t lease_duration_;
  uint64_t snap_id_;
  uint64_t reg_id_;
  int srv_id_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  LeaseTable* leases_;
  HashSet loading_dirs_;  // A set of dirs being loaded into a memory cache
  port::CondVar loading_cv_;
  DirTable* dirs_;
  uint32_t NextSession();
  uint32_t session_;  // The last session id we allocated
  void TryReuseIno(uint64_t ino);
  uint64_t NextIno();
  uint64_t ino_;  // The last ino num we allocated
  Status status_;

  friend class MDS;
  // No copying allowed
  void operator=(const SRV&);
  SRV(const SRV&);
};

}  // namespace pdlfs
