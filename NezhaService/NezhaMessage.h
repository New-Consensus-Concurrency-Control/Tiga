#pragma once
// clang-format off
#include "Common.h"
// clang-format on
// Define messages and must write the Marshaling/UnMarshaling method by
// ourselves

struct NezhaRecordMessage {
   uint64_t txnKey_;
   uint64_t cmdId_;
   uint64_t ssidLow_;
   uint64_t ssidHigh_;
   uint64_t ssidNew_;
   std::vector<uint32_t> keys_;
};

struct NezhaAck {
   uint64_t token_;
};

struct NezhaRequest {
   uint32_t clientId_;
   uint32_t reqId_;
   uint64_t sendTime_;
   uint32_t bound_;
   NezhaRecordMessage cmd_;
   NezhaRequest() {}
   NezhaRequest(const NezhaRequest& req)
       : clientId_(req.clientId_), reqId_(req.reqId_) {}
   uint64_t TxnKey() const { return CONCAT_UINT32(clientId_, reqId_); }

   bool operator<(const NezhaRequest& rhs) const {
      if (clientId_ < rhs.clientId_ ||
          (clientId_ == rhs.clientId_ && reqId_ < rhs.reqId_)) {
         return true;
      }
      return false;
   }
};

struct NezhaInterReplicaSync {
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t logIdStart_;
   std::vector<uint64_t> txnKeys_;
   std::vector<uint64_t> deadlineRanks_;
};

struct NezhaFastReply {
   uint64_t token_;
   uint32_t owd_;
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t clientId_;
   uint32_t reqId_;
   uint32_t logId_;
   uint32_t latestSyncedLogId_;
   uint8_t hasHash_;
   NezhaHash hash_;
   std::map<int32_t, Value> result_;
};

struct NezhaInquireRequest {
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
};

struct NezhaInquireReply {
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t latestSyncedLogId_;
};

struct NezhaLogEntry {
   uint32_t logId_;
   uint64_t deadlineRank_;
   uint64_t sendTime_;
   uint32_t owd_;
   uint32_t clientId_;
   uint32_t reqId_;
   NezhaRecordMessage* cmd_;
   std::set<int32_t> keys_;
   NezhaHash myHash_;
   std::unordered_map<uint32_t, NezhaHash> accumulativeHashByKey_;
   NezhaFastReply* reply_;
   std::function<void(const NezhaFastReply&)> replyHandler_;
   NezhaLogEntry()
       : logId_(0),
         deadlineRank_(0),
         sendTime_(0),
         owd_(0),
         clientId_(0),
         reqId_(0),
         cmd_(NULL),
         reply_(NULL),
         replyHandler_(NULL) {}
   uint64_t TxnKey() const { return CONCAT_UINT32(clientId_, reqId_); }
   bool operator<(const NezhaLogEntry& info) const {
      return (deadlineRank_ < info.deadlineRank_ ||
              (deadlineRank_ == info.deadlineRank_ &&
               this->TxnKey() < info.TxnKey()));
   }
};

Marshal& operator<<(Marshal& m, const NezhaRecordMessage& cmd);

Marshal& operator>>(Marshal& m, NezhaRecordMessage& cmd);

Marshal& operator<<(Marshal& m, const NezhaAck& cmd);

Marshal& operator>>(Marshal& m, NezhaAck& cmd);

Marshal& operator<<(Marshal& m, const NezhaRequest& req);

Marshal& operator>>(Marshal& m, NezhaRequest& req);

Marshal& operator<<(Marshal& m, const NezhaInterReplicaSync& req);

Marshal& operator>>(Marshal& m, NezhaInterReplicaSync& req);

Marshal& operator<<(Marshal& m, const NezhaFastReply& req);

Marshal& operator>>(Marshal& m, NezhaFastReply& req);

Marshal& operator<<(Marshal& m, const NezhaInquireRequest& req);

Marshal& operator>>(Marshal& m, NezhaInquireRequest& req);

Marshal& operator<<(Marshal& m, const NezhaInquireReply& req);

Marshal& operator>>(Marshal& m, NezhaInquireReply& req);
