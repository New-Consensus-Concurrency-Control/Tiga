#pragma once
// clang-format off
#include "Common.h"
// clang-format on
// Define messages and must write the Marshaling/UnMarshaling method by
// ourselves
struct ClientRecord {
   uint64_t cmdId_;
   uint64_t ssidLow_;
   uint64_t ssidHigh_;
   uint64_t ssidNew_;
};

struct ClientRecordRep {
   uint64_t cmdId_;
};
struct PaxosAppend {
   ClientRecord record_;
   uint32_t viewId_;
   uint32_t logId_;
   uint32_t shardId_;
   uint32_t replicaId_;
};

struct PaxosAppendRep {
   uint32_t viewId_;
   uint32_t logId_;
   uint32_t shardId_;
   uint32_t replicaId_;
};

struct PaxosCommitReq {
   uint32_t viewId_;
   uint32_t logId_;
};

Marshal& operator<<(Marshal& m, const ClientRecord& msg);

Marshal& operator>>(Marshal& m, ClientRecord& msg);

Marshal& operator<<(Marshal& m, const ClientRecordRep& msg);

Marshal& operator>>(Marshal& m, ClientRecordRep& msg);

Marshal& operator<<(Marshal& m, const PaxosAppend& msg);

Marshal& operator>>(Marshal& m, PaxosAppend& msg);

Marshal& operator<<(Marshal& m, const PaxosAppendRep& msg);

Marshal& operator>>(Marshal& m, PaxosAppendRep& msg);

Marshal& operator<<(Marshal& m, const PaxosCommitReq& msg);

Marshal& operator>>(Marshal& m, PaxosCommitReq& msg);