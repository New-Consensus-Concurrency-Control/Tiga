#pragma once

// clang-format off

#include "Common.h"

// clang-format on
// Define messages and must write the Marshaling/UnMarshaling method by
// ourselves

struct TigaDispatchRequest {
   uint32_t txnType_;
   std::map<int32_t, Value> input_;
};

struct TigaDispatchReply {
   uint32_t shardId_;
   std::map<int32_t, Value> result_;
};

struct TigaReq {
   uint64_t sendTime_;
   int32_t bound_;
   ClientCommand cmd_;
   uint32_t isDispatchRead_;
};

struct TigaReply {
   uint32_t owd_;
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t clientId_;
   uint32_t reqId_;
   uint32_t logId_;
   uint32_t specLogId_;
   uint32_t latestSyncedLogId_;
   uint32_t latestSyncedSpecLogId_;
   uint64_t deadline_;
   uint8_t hasHash_;
   TigaHash hash_;
   std::map<int32_t, Value> result_;
   uint32_t status_;
};

struct TigaSlowReply {
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t logId_;
   uint32_t shardId_;
   uint32_t replicaId_;
};

struct TigaServerSyncStatusRequest {
   uint32_t clientId_;
};

struct TigaServerSyncStatusReply {
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t latestSyncedLogId_;
   uint32_t latestSyncedSpecLogId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t signal_;
   uint32_t status_;
};

struct TigaDeadlineAgreeRequest {
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   std::vector<uint64_t> txnKeys_;
   std::vector<uint64_t> deadlineRanks_;
   // phase-2
   std::vector<uint64_t> txnKeys2_;
   std::vector<uint64_t> deadlineRanks2_;
};

struct TigaExecAgreeRequest {
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   std::vector<uint64_t> txnKeys_;
};

struct TigaInterReplicaSync {
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t commitPoint_;
   uint32_t logIdStart_;
   std::vector<uint64_t> txnKeys_;
   std::vector<uint64_t> deadlineRanks_;
   std::vector<uint32_t> specLogIds_;
};

struct MissRequestReq {
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   std::vector<uint64_t> txnKeys_;
};

struct MissRequestRep {
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   std::vector<ClientCommand> cmds_;
};

struct TigaCommitRequest {
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t logId_;
};

struct TigaCommitReply {
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t logId_;
   std::map<int32_t, Value> result_;
};

struct TigaReconcliationReq {
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t clientId_;
   uint32_t reqId_;
};

// Failure-Recovery Related

struct TigaViewChangeReq {
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t gViewId_;
   std::vector<uint32_t> gVec_;
};

struct TigaViewChange {
   uint32_t viewId_;
   uint32_t gViewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t lastNormalViewId_;
   uint32_t commitPoint_;
   uint32_t syncPoint_;
   uint32_t status_;
};

struct TigaHeartBeat {
   uint32_t shardId_;
   uint32_t replicaId_;
};

struct TigaHeartBeatAck {
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t status_;
};

struct TigaStateTransferRequest {
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t viewId_;
   uint32_t nonce_;
   uint32_t syncedLogBegin_;
   uint32_t syncedLogEnd_;
};

struct TigaStateTransferReply {
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t viewId_;
   uint32_t nonce_;
   std::vector<uint64_t> syncedTxnDeadlines_;
   std::vector<ClientCommand> syncedTxns_;
   std::vector<uint64_t> unsyncedTxnDeadlines_;
   std::vector<ClientCommand> unsyncedTxns_;
};

struct TigaCrossShardVerifyReq {
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t gViewId_;
   uint32_t viewId_;
   uint64_t syncedDeadline_;
};

struct TigaCrossShardVerifyRep {
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t gViewId_;
   uint32_t viewId_;
   std::vector<uint64_t> txnKeys_;
   std::vector<uint64_t> deadlines_;
};

struct TigaStartView {
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t logBegin_;
   std::vector<uint64_t> deadlines_;
   std::vector<ClientCommand> txns_;
};

struct TigaStartViewReq {
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t missingLogIdBegin_;
   uint32_t missingLogIdEnd_;
};

struct TigaStartViewRep {
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t missingLogIdBegin_;
   uint32_t missingLogIdEnd_;
   std::vector<uint64_t> deadlines_;
   std::vector<ClientCommand> txns_;
};

// Follower reports to its leader
struct TigaSyncStatus {
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t syncPoint_;
};

struct TigaCMPrepare {
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t cViewId_;
   uint32_t gViewId_;
   std::vector<uint32_t> gVec_;
};

struct TigaCMPrepareReply {
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t cViewId_;
   uint32_t gViewId_;
};

struct TigaCMCommit {
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t cViewId_;
   uint32_t gViewId_;
   std::vector<uint32_t> gVec_;
};

struct TigaFailSignal {
   uint32_t failedGView_;
   uint32_t failedShardId_;
   uint32_t failedReplicaId_;
};

struct TigaFailAck {
   uint32_t shardId_;
   uint32_t replicaId_;
};

// Lease related
struct TigaGuard {
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t guardUs_;
};
// The holder starts a guard timer once it receives TigaGuard
// The holder must reply to grantor
// Grantor will only continue to send the promise after receiving TigaGuardAck
struct TigaGuardAck {
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
};

// After receiving TigaGuardAck, grantor sends TigaPromise to holder
// The holder will only beleive the Promise is valid if it can receive it
// before guard timer expires, initially validDuration=guardUs_,
// lastPromiseAckId_=0
struct TigaPromise {
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t promiseId_;  // starts from 1
   uint32_t leaseUs_;
   uint32_t lastPromiseAckId_;
   uint32_t validDuration_;
};
// If the holder recieves the Promise before guard timer expires
// Then the Promise is valid, the lease timer starts (lease is active)
// Meanwhile, guard timer continues, hold sends Promise Ack to granter
// Every time the holder sends a Promise Ack, it needs to record the sending
// time, which is used to judge whether the future Promise is valid
struct TigaPromiseAck {
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t promiseId_;
};

Marshal& operator<<(Marshal& m, const TigaDispatchRequest& req);

Marshal& operator>>(Marshal& m, TigaDispatchRequest& req);

Marshal& operator<<(Marshal& m, const TigaDispatchReply& req);

Marshal& operator>>(Marshal& m, TigaDispatchReply& req);

Marshal& operator<<(Marshal& m, const TigaReq& req);

Marshal& operator>>(Marshal& m, TigaReq& req);

Marshal& operator<<(Marshal& m, const TigaReply& rep);

Marshal& operator>>(Marshal& m, TigaReply& rep);

Marshal& operator<<(Marshal& m, const TigaDeadlineAgreeRequest& req);

Marshal& operator>>(Marshal& m, TigaDeadlineAgreeRequest& req);

Marshal& operator<<(Marshal& m, const TigaExecAgreeRequest& req);

Marshal& operator>>(Marshal& m, TigaExecAgreeRequest& req);

Marshal& operator<<(Marshal& m, const TigaInterReplicaSync& req);

Marshal& operator>>(Marshal& m, TigaInterReplicaSync& req);

Marshal& operator<<(Marshal& m, const MissRequestReq& req);

Marshal& operator>>(Marshal& m, MissRequestReq& req);

Marshal& operator<<(Marshal& m, const MissRequestRep& req);

Marshal& operator>>(Marshal& m, MissRequestRep& req);

Marshal& operator<<(Marshal& m, const TigaSlowReply& req);

Marshal& operator>>(Marshal& m, TigaSlowReply& req);

Marshal& operator<<(Marshal& m, const TigaServerSyncStatusRequest& req);

Marshal& operator>>(Marshal& m, TigaServerSyncStatusRequest& req);

Marshal& operator<<(Marshal& m, const TigaServerSyncStatusReply& rep);

Marshal& operator>>(Marshal& m, TigaServerSyncStatusReply& rep);

Marshal& operator<<(Marshal& m, const TigaCommitRequest& req);

Marshal& operator>>(Marshal& m, TigaCommitRequest& req);

Marshal& operator<<(Marshal& m, const TigaCommitReply& rep);

Marshal& operator>>(Marshal& m, TigaCommitReply& rep);

Marshal& operator<<(Marshal& m, const TigaReconcliationReq& req);

Marshal& operator>>(Marshal& m, TigaReconcliationReq& req);

// Failure Recovery related
Marshal& operator<<(Marshal& m, const TigaViewChangeReq& vcr);

Marshal& operator>>(Marshal& m, TigaViewChangeReq& vcr);

Marshal& operator<<(Marshal& m, const TigaViewChange& vr);

Marshal& operator>>(Marshal& m, TigaViewChange& vr);

Marshal& operator<<(Marshal& m, const TigaHeartBeat& hb);

Marshal& operator>>(Marshal& m, TigaHeartBeat& hb);

Marshal& operator<<(Marshal& m, const TigaHeartBeatAck& hba);

Marshal& operator>>(Marshal& m, TigaHeartBeatAck& hba);

Marshal& operator<<(Marshal& m, const TigaStateTransferRequest& sr);

Marshal& operator>>(Marshal& m, TigaStateTransferRequest& sr);

Marshal& operator<<(Marshal& m, const TigaStateTransferReply& srp);

Marshal& operator>>(Marshal& m, TigaStateTransferReply& srp);

Marshal& operator<<(Marshal& m, const TigaCrossShardVerifyReq& msg);

Marshal& operator>>(Marshal& m, TigaCrossShardVerifyReq& msg);

Marshal& operator<<(Marshal& m, const TigaCrossShardVerifyRep& msg);

Marshal& operator>>(Marshal& m, TigaCrossShardVerifyRep& msg);

Marshal& operator<<(Marshal& m, const TigaStartView& srp);

Marshal& operator>>(Marshal& m, TigaStartView& srp);

Marshal& operator<<(Marshal& m, const TigaSyncStatus& srp);

Marshal& operator>>(Marshal& m, TigaSyncStatus& srp);

// Config Manager Related
Marshal& operator<<(Marshal& m, const TigaCMPrepare& msg);

Marshal& operator>>(Marshal& m, TigaCMPrepare& msg);

Marshal& operator<<(Marshal& m, const TigaCMPrepareReply& msg);

Marshal& operator>>(Marshal& m, TigaCMPrepareReply& msg);

Marshal& operator<<(Marshal& m, const TigaCMCommit& msg);

Marshal& operator>>(Marshal& m, TigaCMCommit& msg);

Marshal& operator<<(Marshal& m, const TigaFailSignal& msg);

Marshal& operator>>(Marshal& m, TigaFailSignal& msg);

Marshal& operator<<(Marshal& m, const TigaFailAck& msg);

Marshal& operator>>(Marshal& m, TigaFailAck& msg);

Marshal& operator<<(Marshal& m, const TigaGuard& msg);

Marshal& operator>>(Marshal& m, TigaGuard& msg);

Marshal& operator<<(Marshal& m, const TigaGuardAck& msg);

Marshal& operator>>(Marshal& m, TigaGuardAck& msg);

Marshal& operator<<(Marshal& m, const TigaPromise& msg);

Marshal& operator>>(Marshal& m, TigaPromise& msg);

Marshal& operator<<(Marshal& m, const TigaPromiseAck& msg);

Marshal& operator>>(Marshal& m, TigaPromiseAck& msg);

enum DEADLINE_STATUS {
   DEADLINE_INIT = 1,
   DEADLINE_LOCAL,
   DEADLINE_AGREED_INIT,
   DEADLINE_AGGREED_BEFORE_FLUSH,
   DEADLINE_AGGREED_FLUSHED,
   DEADLINE_AGGREED_FAST,
   DEADLINE_AGGREED
};

enum AGREE_STATUS {
   AGREE_INIT = 1,
   AGREE_FLUSHING,
   AGREE_FLUSHED,
   AGREE_CONFIRMING,
   AGREE_COMPLETE,
   AGREE_CHECK1,
   AGREE_CHECK2,
   AGREE_CHECK3
};

enum EXEC_STATUS {
   EXEC_INIT = 1,
   EXEC_SPEC,
   EXEC_COMMITING,
   EXEC_ROLLBACK,
   EXEC_REPOSITIONED,  // used by detective
   EXEC_DIRECT,
   EXEC_COMPLETE,
   EXEC_ABANDONED
};

enum REPLY_STATUS { REPLY_SPEC = 1, REPLY_AGREED };

struct AgreeLog {
   uint32_t agreeStatus_ = 0;
   uint32_t comeTimes = 0;
   uint64_t ddls_[MAX_SHARD_NUM] = {};
   uint32_t phases_[MAX_SHARD_NUM] = {};
   std::string ToString() {
      std::string ans = "\n";
      ans += std::to_string(agreeStatus_);
      ans += "\n";
      ans += std::to_string(comeTimes);
      ans += "\n";
      ans += std::to_string(ddls_[0]) + "|" + std::to_string(ddls_[1]) + "|" +
             std::to_string(ddls_[2]) + "|";
      ans += "\n";
      ans += std::to_string(phases_[0]) + "|" + std::to_string(phases_[1]) +
             "|" + std::to_string(phases_[2]) + "|";
      return ans;
   }
};

struct TigaLogEntry {
   uint64_t localDdlRank_;
   uint64_t agreedDdlRank_;
   uint64_t dqRank_;  // for debug
   // uint64_t ddlSyncTime_;
   // uint64_t ddlAgreeTime_;
   // uint64_t ddlAgreeNotificationTime_;
   uint32_t comeTimes_;
   std::atomic<uint32_t> preAgreeStatus_;
   std::atomic<uint32_t> agreeStatus_;
   std::atomic<uint32_t> execStatus_;
   std::atomic<bool> deadlineAgreed_;
   std::atomic<bool> execAgreed_;
   // std::vector<AgreeLog> agreeLogs_;
   ClientCommand* cmd_;
   std::function<void(const TigaReply&)> replyHandler_;
   std::atomic<bool> askingCommitReply_;
   uint64_t sendTime_;
   uint32_t owd_;
   // key=> shardId, value=> the keys that will be touched on this shard
   std::map<uint32_t, std::set<int32_t>> shardKeyMap_;
   std::vector<int32_t> localKeys_;

   // Log Related
   uint32_t logId_;      // only sycned log entries have logId
   uint32_t specLogId_;  // only speculative executed entry has this specLogId
   std::atomic<uint32_t> replyStatus_;
   TigaHash myHash_;
   std::unordered_map<uint32_t, TigaHash> accumulativeHashByKey_;
   // std::vector<TigaHash> accumulativeHashByKeyIndex_;
   std::mutex replyMtx_;
   TigaReply* specReply_;
   TigaReply* reply_;

   TigaLogEntry()
       : localDdlRank_(0),
         agreedDdlRank_(0),
         comeTimes_(0),
         preAgreeStatus_(AGREE_INIT),
         agreeStatus_(AGREE_INIT),
         execStatus_(EXEC_INIT),
         deadlineAgreed_(false),
         execAgreed_(false),
         replyHandler_(NULL),
         askingCommitReply_(false),
         sendTime_(0),
         owd_(0),
         logId_(0),
         specLogId_(0),
         replyStatus_(0),
         specReply_(NULL),
         reply_(NULL) {
      dqRank_ = 0;
   }
   bool operator<(const TigaLogEntry& rhs) const {
      if (cmd_->clientId_ < rhs.cmd_->clientId_ ||
          (cmd_->clientId_ == rhs.cmd_->clientId_ &&
           cmd_->reqId_ < rhs.cmd_->reqId_)) {
         return true;
      }
      return false;
   }
   std::string ID() {
      if (cmd_ == NULL) return "|0:0|";
      return "|" + std::to_string(cmd_->clientId_) + ":" +
             std::to_string(cmd_->reqId_) + "|";
   }
};

// obsolete
struct LogInfo {
   uint32_t logId_;  // only sycned log entries have logId
   uint32_t status_;
   TigaLogEntry* entry_;
   TigaHash myHash_;
   std::unordered_map<uint32_t, TigaHash> accumulativeHashByKey_;
   TigaReply* specReply_;
   TigaReply* reply_;

   LogInfo() : logId_(0), status_(0), specReply_(NULL), reply_(NULL) {}
   bool operator<(const LogInfo& info) const {
      return (entry_->agreedDdlRank_ < info.entry_->agreedDdlRank_ ||
              (entry_->agreedDdlRank_ == info.entry_->agreedDdlRank_ &&
               entry_ < info.entry_));
   }
};
