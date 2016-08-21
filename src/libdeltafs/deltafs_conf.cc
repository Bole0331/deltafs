/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdlib.h>
#include <string>
static std::string LoadFromEnv(const char* key) {
  const char* v = getenv(key);
  if (v == NULL) {
    return "";
  } else {
    return v;
  }
}
#if defined(GFLAGS)
#include <gflags/gflags.h>
#define DEFINE_FLAG_PORT(n, v)                                               \
  static std::string FLAGS_load_##n() { return LoadFromEnv("DELTAFS_" #n); } \
  DEFINE_string(D##n, v, "deltafs");
#else
#define DEFINE_FLAG_PORT(n, v)                                               \
  static std::string FLAGS_load_##n() { return LoadFromEnv("DELTAFS_" #n); } \
  static std::string FLAGS_D##n = v;
#endif
#include <ctype.h>
static void ToLowerCase(std::string* str) {
  std::string::iterator it;
  for (it = str->begin(); it != str->end(); ++it) {
    *it = tolower(*it);
  }
}
#define DEFINE_FLAG(n, v)                  \
  DEFINE_FLAG_PORT(n, v)                   \
  std::string n() {                        \
    std::string result = FLAGS_load_##n(); \
    if (result.empty()) {                  \
      result = FLAGS_D##n;                 \
    }                                      \
    ToLowerCase(&result);                  \
    return result;                         \
  }

#include "deltafs_conf.h"
namespace pdlfs {
namespace config {

DEFINE_FLAG(NumOfMetadataSrvs, "1")
DEFINE_FLAG(NumOfVirMetadataSrvs, "1")
DEFINE_FLAG(InstanceId, "0")
DEFINE_FLAG(RPCProto, "bmi+tcp")
DEFINE_FLAG(MDSTracing, "false")
DEFINE_FLAG(MetadataSrvAddrs, "")
DEFINE_FLAG(SizeOfSrvLeaseTable, "4k")
DEFINE_FLAG(SizeOfSrvDirTable, "1k")
DEFINE_FLAG(SizeOfCliLookupCache, "4k")
DEFINE_FLAG(SizeOfCliIndexCache, "1k")
DEFINE_FLAG(AtomicPathRes, "false")
DEFINE_FLAG(ParanoidChecks, "false")
DEFINE_FLAG(VerifyChecksums, "false")
DEFINE_FLAG(Inputs, "/tmp/deltafs_inputs")
DEFINE_FLAG(Outputs, "/tmp/deltafs_outputs")
DEFINE_FLAG(EnvName, "posix")
DEFINE_FLAG(EnvConf, "")
DEFINE_FLAG(FioName, "posix")
DEFINE_FLAG(FioConf, "")

}  // namespace config
}  // namespace pdlfs