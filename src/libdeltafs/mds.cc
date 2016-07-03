/*
* Copyright (c) 2014-2016 Carnegie Mellon University.
*
* All rights reserved.
*
* Use of this source code is governed by a BSD-style license that can be
* found in the LICENSE file. See the AUTHORS file for names of contributors.
*/

#include "mds_api.h"
#include "mds_srv.h"

namespace pdlfs {

MDSOptions::MDSOptions()
    : env(NULL),
      mdb(NULL),
      dir_table_size(1 << 16),
      lease_table_size(1 << 18),
      snap_id(0),
      reg_id(0),
      paranoid_checks(false),
      num_virtual_servers(1),
      num_servers(1),
      srv_id(0) {}

MDS::SRV::SRV(const MDSOptions& options)
    : env_(options.env),
      mdb_(options.mdb),
      paranoid_checks_(options.paranoid_checks),
      lease_duration_(options.lease_duration),
      snap_id_(options.snap_id),
      reg_id_(options.reg_id),
      srv_id_(options.srv_id),
      loading_cv_(&mutex_),
      ino_(0) {
  giga_.num_servers = options.num_servers;
  giga_.num_virtual_servers = options.num_virtual_servers;
  giga_.paranoid_checks = options.paranoid_checks;

  LeaseOptions lease_options;
  lease_options.max_lease_duration = options.lease_duration;
  lease_options.max_num_leases = options.lease_table_size;
  leases_ = new LeaseTable(lease_options);

  dirs_ = new DirTable(options.dir_table_size);
  ino_ = srv_id_;
}

MDS::SRV::~SRV() {
  delete leases_;
  delete dirs_;
}

Status MDS::NewServer(const MDSOptions& options, MDS** mdsptr) {
  *mdsptr = NULL;

  Status s;
  if (options.env == NULL) {
    s = Status::InvalidArgument(Slice());
  } else if (options.mdb == NULL) {
    s = Status::InvalidArgument(Slice());
  }

  *mdsptr = new SRV(options);
  return s;
}

}  // namespace pdlfs
