#pragma once
// clang-format off
#include "DetockService/DetockMessage.h"
#include "DetockService/DetockService.h"
#include "StateMachine/MicroStateMachine.h"
#include "StateMachine/TPCCStateMachine.h"
using namespace DetockRPC;

// clang-format on

class DetockLogManager {
  protected:
   uint32_t viewId_;
   std::string serverName_;
   StateMachine* sm_;

   std::vector<DetockBatch*> detockLogList_;
   uint32_t preparedLogId_;
   uint32_t committedLogId_;
   std::map<uint32_t, DetockBatch*> pendingBatches_;
   uint32_t pendingCommitPoint_;
   std::mutex logMtx_;
   // logId, <replicaId>
   std::unordered_map<uint32_t, std::set<uint32_t>> quorumInfo_;

   rrr::PollMgr* logMangerRPCPoll_;
   DetockLogManagerProxy* logManagerProxies_[MAX_REPLICA_NUM];
   rrr::Client* logManagerRPCClients_[MAX_REPLICA_NUM];
   std::string localManager;
   std::string logManagerAddrs_[MAX_SHARD_NUM][MAX_REPLICA_NUM];

  public:
   DetockLogManager(const std::string& serverName, StateMachine* sm);

   void ConnectOtherLogManager();
   void CloseConnection();

   void onDetockPaxosAppend(const DetockPaxosAppend& req);
   void onDetockPaxosAppendReply(const DetockPaxosAppendReply& rep);
   void onDetockPaxosCommit(const DetockPaxosCommit& req);
   DetockBatch* GetBatch(uint32_t logId);
   uint32_t GetCommitedLogId();
   void ProcessPendingBatches();
   void AppendNewBatch(DetockBatch* batch);
   uint32_t LeaderReplicaId();
   ~DetockLogManager();
};
