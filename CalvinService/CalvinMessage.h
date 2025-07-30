#pragma once
// clang-format off
#include "Common.h"
// clang-format on
// Define messages and must write the Marshaling/UnMarshaling method by
// ourselves

struct CalvinRequest {
   uint32_t clientId_;
   uint32_t reqId_;
   uint32_t txnType_;
   uint32_t designateShardId_;
   uint32_t designateReplicaId_;
   std::map<int32_t, Value> ws_;
   CalvinRequest() : clientId_(0), reqId_(0), txnType_(~0) {}
   CalvinRequest(const CalvinRequest& cmd)
       : clientId_(cmd.clientId_),
         reqId_(cmd.reqId_),
         txnType_(cmd.txnType_),
         designateShardId_(cmd.designateShardId_),
         designateReplicaId_(cmd.designateReplicaId_),
         ws_(cmd.ws_) {}
   uint64_t TxnKey() const { return CONCAT_UINT32(clientId_, reqId_); }

   bool operator<(const CalvinRequest& rhs) const {
      if (clientId_ < rhs.clientId_ ||
          (clientId_ == rhs.clientId_ && reqId_ < rhs.reqId_)) {
         return true;
      }
      return false;
   }
};

struct CalvinReply {
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t clientId_;
   uint32_t reqId_;
   uint32_t owd_;
   std::map<int32_t, Value> result_;
};

struct CalvinDispatchRequest {
   uint32_t txnType_;
   std::map<int32_t, Value> input_;
};

struct CalvinDispatchReply {
   uint32_t shardId_;
   std::map<int32_t, Value> result_;
};

struct CalvinLogEntry {
   CalvinRequest* cmd_;
   std::map<uint32_t, std::set<int32_t>> shardKeyMap_;
   std::function<void(const CalvinReply&)> replyHandler_;
   CalvinLogEntry() : cmd_(NULL), replyHandler_(NULL) {}
   bool operator<(const CalvinLogEntry& rhs) const {
      return *cmd_ < *(rhs.cmd_);
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
   uint32_t owd_;
   uint32_t designateShardId_;
   uint32_t designateReplicaId_;
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t sequenceNo_;
   uint32_t logId_;
   uint32_t latestSyncedLogId_;
   uint8_t hasHash_;
   NezhaHash hash_;
   std::map<int32_t, Value> result_;
   uint64_t TxnKey() {
      // uint32_t serverId = CONCAT_UINT16(shardId_, replicaId_);
      // return CONCAT_UINT32(serverId, sequenceNo_);
      return CONCAT_UINT32(shardId_, sequenceNo_);
   }
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

struct EpochRequest {
   uint32_t sequenceNo_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint64_t sendTime_;
   uint32_t bound_;
   std::vector<CalvinRequest> cmdVec_;
   bool operator<(const EpochRequest& rhs) const {
      return (sequenceNo_ < rhs.sequenceNo_ ||
              (sequenceNo_ == rhs.sequenceNo_ && shardId_ < rhs.shardId_));
   }
   uint64_t TxnKey() {
      // uint32_t serverId = CONCAT_UINT16(shardId_, replicaId_);
      // return CONCAT_UINT32(serverId, sequenceNo_);
      return CONCAT_UINT32(shardId_, sequenceNo_);
   }
};

struct CalvinEpochEntry {
   uint32_t logId_;
   uint64_t deadlineRank_;
   uint32_t owd_;
   EpochRequest* cmd_;
   std::set<int32_t> keys_;
   NezhaHash myHash_;
   std::unordered_map<uint32_t, NezhaHash> accumulativeHashByKey_;
   NezhaFastReply* reply_;
   CalvinEpochEntry()
       : logId_(0), deadlineRank_(0), owd_(0), cmd_(NULL), reply_(NULL) {}
   bool operator<(const CalvinEpochEntry& info) const {
      return (deadlineRank_ < info.deadlineRank_ ||
              (deadlineRank_ == info.deadlineRank_ && (*cmd_) < *(info.cmd_)));
   }
};

struct EpochReply {
   uint32_t sequenceNo_;
   uint32_t shardId_;
   uint32_t replicaId_;
   std::vector<uint64_t> txnKeys_;
   std::vector<std::map<int32_t, Value>> resultVec_;
};

struct MasterSyncRequest {
   uint32_t shardId_;
   uint32_t replicaId_;
};

struct MasterSyncReply {
   uint64_t startTime_;
};

struct InquireSeqNoRequest {
   uint32_t clientId_;
};

struct InquireSeqNoReply {
   uint32_t shardId_;
   uint32_t replicaId_;
   std::vector<uint32_t> maxSeqNos_;
};

Marshal& operator<<(Marshal& m, const CalvinRequest& req);

Marshal& operator>>(Marshal& m, CalvinRequest& req);

Marshal& operator<<(Marshal& m, const CalvinReply& req);

Marshal& operator>>(Marshal& m, CalvinReply& req);

Marshal& operator<<(Marshal& m, const CalvinDispatchRequest& req);

Marshal& operator>>(Marshal& m, CalvinDispatchRequest& req);

Marshal& operator<<(Marshal& m, const CalvinDispatchReply& rep);

Marshal& operator>>(Marshal& m, CalvinDispatchReply& rep);

Marshal& operator<<(Marshal& m, const NezhaInterReplicaSync& req);

Marshal& operator>>(Marshal& m, NezhaInterReplicaSync& req);

Marshal& operator<<(Marshal& m, const NezhaFastReply& req);

Marshal& operator>>(Marshal& m, NezhaFastReply& req);

Marshal& operator<<(Marshal& m, const NezhaInquireRequest& req);

Marshal& operator>>(Marshal& m, NezhaInquireRequest& req);

Marshal& operator<<(Marshal& m, const NezhaInquireReply& req);

Marshal& operator>>(Marshal& m, NezhaInquireReply& req);

Marshal& operator<<(Marshal& m, const EpochRequest& req);

Marshal& operator>>(Marshal& m, EpochRequest& req);

Marshal& operator<<(Marshal& m, const EpochReply& req);

Marshal& operator>>(Marshal& m, EpochReply& req);

Marshal& operator<<(Marshal& m, const MasterSyncRequest& req);

Marshal& operator>>(Marshal& m, MasterSyncRequest& req);

Marshal& operator<<(Marshal& m, const MasterSyncReply& req);

Marshal& operator>>(Marshal& m, MasterSyncReply& req);

Marshal& operator<<(Marshal& m, const InquireSeqNoRequest& req);

Marshal& operator>>(Marshal& m, InquireSeqNoRequest& req);

Marshal& operator<<(Marshal& m, const InquireSeqNoReply& rep);

Marshal& operator>>(Marshal& m, InquireSeqNoReply& rep);