#pragma once
// clang-format off
#include "Common.h"
// clang-format on
// Define messages and must write the Marshaling/UnMarshaling method by
// ourselves
struct DetockTxn {
   uint64_t sendTime_;
   uint32_t bound_;
   uint32_t clientId_;
   uint32_t reqId_;
   uint32_t txnType_;
   std::vector<uint32_t> shardIds_;
   std::vector<uint32_t> replicaIds_;
   // <shardId, replicaId> identifies one unique server
   std::map<int32_t, Value> ws_;

   DetockTxn() : clientId_(0), reqId_(0), txnType_(~0) {}
   DetockTxn(const DetockTxn& cmd)
       : sendTime_(cmd.sendTime_),
         bound_(cmd.bound_),
         clientId_(cmd.clientId_),
         reqId_(cmd.reqId_),
         txnType_(cmd.txnType_),
         shardIds_(cmd.shardIds_),
         replicaIds_(cmd.replicaIds_),

         ws_(cmd.ws_) {}
   uint64_t TxnKey() const { return CONCAT_UINT32(clientId_, reqId_); }

   bool operator<(const DetockTxn& rhs) const {
      if (clientId_ < rhs.clientId_ ||
          (clientId_ == rhs.clientId_ && reqId_ < rhs.reqId_)) {
         return true;
      }
      return false;
   }
};

struct DetockReply {
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t clientId_;
   uint32_t reqId_;
   uint32_t owd_;
   std::map<int32_t, Value> result_;
};

struct DetockDispatchRequest {
   uint32_t txnType_;
   std::map<int32_t, Value> input_;
};

struct DetockDispatchReply {
   uint32_t shardId_;
   std::map<int32_t, Value> result_;
};

struct DetockBatch {
   uint32_t batchId_;
   std::vector<DetockTxn> batchContent_;
};

struct DetockPaxosAppend {
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   DetockBatch batch_;
};

struct DetockPaxosAppendReply {
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t batchId_;
};

struct DetockPaxosCommit {
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t batchId_;
};

struct DetockTxnMetaData {
   uint64_t txnKey_;
   std::vector<int32_t> localKeys_;
   std::vector<uint32_t> shardIds_;  // related shards
};

struct DetockLocalLogSync {
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t msgId_;
   std::vector<DetockTxnMetaData> txnMetas_;
};

struct DetockEntry {
   DetockTxn* txn_;
   uint32_t owd_;
   std::map<uint32_t, std::set<int32_t>> shardKeyMap_;
   std::vector<int32_t> localKeys_;
   DetockReply* reply_;

   std::function<void(const DetockReply&)> replyHandler_;
   DetockEntry() : txn_(NULL), owd_(0), reply_(NULL), replyHandler_(NULL) {}
};

Marshal& operator<<(Marshal& m, const DetockDispatchRequest& req);

Marshal& operator>>(Marshal& m, DetockDispatchRequest& req);

Marshal& operator<<(Marshal& m, const DetockDispatchReply& req);

Marshal& operator>>(Marshal& m, DetockDispatchReply& req);

Marshal& operator<<(Marshal& m, const DetockTxn& msg);

Marshal& operator>>(Marshal& m, DetockTxn& msg);

Marshal& operator<<(Marshal& m, const DetockReply& msg);

Marshal& operator>>(Marshal& m, DetockReply& msg);

Marshal& operator<<(Marshal& m, const DetockBatch& msg);

Marshal& operator>>(Marshal& m, DetockBatch& msg);

Marshal& operator<<(Marshal& m, const DetockPaxosAppend& msg);

Marshal& operator>>(Marshal& m, DetockPaxosAppend& msg);

Marshal& operator<<(Marshal& m, const DetockPaxosAppendReply& msg);

Marshal& operator>>(Marshal& m, DetockPaxosAppendReply& msg);

Marshal& operator<<(Marshal& m, const DetockPaxosCommit& msg);

Marshal& operator>>(Marshal& m, DetockPaxosCommit& msg);

Marshal& operator<<(Marshal& m, const DetockLocalLogSync& msg);

Marshal& operator>>(Marshal& m, DetockLocalLogSync& msg);