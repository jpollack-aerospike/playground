#pragma once
#include "as_proto.hpp"
#include <array>
#include "cf_ripemd160.h"
#include "util.hpp"

int str_set (int fd, uint64_t mrtid, int64_t id, const std::string& str, uint32_t gen, uint32_t deadline);
int str_get (int fd, uint64_t mrtid, int64_t id, std::string& str, uint32_t *gen, void *recver);

int int_set (int fd, uint64_t mrtid, int64_t id, int64_t val, uint32_t gen, uint32_t deadline);
int int_get (int fd, uint64_t mrtid, int64_t id, int64_t& val, uint32_t *gen, void *recver);

int str_verify (int fd, int64_t id, void *recver);
int str_roll (int fd, uint64_t mrtid, int64_t id, bool is_fwd);
int str_delete (int fd, uint64_t mrtid, int64_t id, uint32_t deadline);
int str_debug (int fd, int64_t id, std::string& str);
int str_insert (int fd, uint64_t mrtid, int64_t id, const std::string& str, uint32_t gen, uint32_t deadline);
bool str_truncate (int fd);

int txn_add_str (int fd, uint64_t mrtid, int64_t id, uint32_t *deadline);
int txn_fwd (int fd, uint64_t mrtid);
int txn_delete (int fd, uint64_t mrtid);

