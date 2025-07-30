#pragma once
#include "TigaService/TigaCommunicator.h"
#include "TigaService/TigaServiceImpl.h"
#include "TxnGenerator/MicroTxnGenerator.h"
#include "TxnGenerator/TPCCTxnGenerator.h"

class TigaCoordinator;

struct TigaFastReplyQuorum {
   std::unordered_map<uint32_t, TigaReply> fastReplies_[MAX_SHARD_NUM];
   uint32_t globalViewId_;
   uint32_t viewIds_[MAX_SHARD_NUM];
   uint32_t clientId_;
   uint32_t reqId_;
   TigaCoordinator* coord_;
};

struct GlobalInfo {
   std::map<uint32_t, std::map<uint32_t, uint32_t>> testCnt_;
   std::shared_mutex seqMtx_;
   std::atomic<uint64_t> serverClock_; // logical clock
   uint32_t coordinatorId_;
   uint32_t shardNum_;
   uint32_t replicaNum_;
   TigaCommunicator* comm_;
   std::atomic<uint32_t> nextRequestIdByProxy_;
   std::shared_mutex viewMtx_;
   uint32_t currentGlobalViews_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   uint32_t currentViews_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   uint32_t currentSyncedLogIds_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   uint32_t currentSyncedSpecLogIds_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   std::atomic<uint32_t> serverSignal_;
   uint32_t serverStatus_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   ConcurrentQueue<uint32_t> owdQus_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   std::atomic<uint32_t> estimatedOWDs_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   std::vector<uint32_t> owdSequences_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   std::unordered_map<uint64_t, std::unordered_set<uint64_t>>
       usedDeadlineRecords_;
   std::thread* daemonThread_;
   std::thread* inquiryThread_;
   std::atomic<uint32_t> slowTriggers_;
   uint32_t replyNumPerNode_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   std::atomic<uint32_t> fastReplyNum1_;
   std::atomic<uint32_t> fastReplyNum2_;
   uint64_t markTime_;
   uint32_t cap_;
   uint32_t initBound_;
   uint32_t yieldPeriodUs_;
   uint32_t owdDeltaUs_;
   uint32_t owdEstimationPercentile_;
   ConcurrentQueue<std::pair<TigaReply, TigaCoordinator*>> replyQu_;
   std::unordered_set<uint32_t> committedCoordReqIds_;
   std::map<uint32_t, TigaFastReplyQuorum>
       quorumSets_;  // indexed by Coord's ReqIds
   std::atomic<bool> isRunning_;
   bool debug_;
   std::unordered_map<uint32_t, uint64_t> uncommittedCnters_;
   GlobalInfo(const uint32_t coordinatorId, const uint32_t shardNum,
              const uint32_t replicaNum, const uint32_t cap = 0,
              const uint32_t initBound = 0, const uint32_t yieldPeriodUs = 0,
              TigaCommunicator* comm = NULL);
   ~GlobalInfo();
   void RunDaemon();
   void RunIniquiry();
   void UpdateOWDStats();
   void UpdateSyncStatus(const uint32_t shardId, const uint32_t replicaId,
                         const uint32_t gViewId, const uint32_t viewId,
                         const uint32_t latestSyncedLogId,
                         const uint32_t latestSyncedSpecLogId,
                         const uint32_t signal = 0);
   bool AddToQuorumSet(const TigaReply& rep, TigaCoordinator* coord);
   void InquireServerSyncStatus();
   void OnServerSyncStatusReply(const TigaServerSyncStatusReply& rep);
   int isTxnCommitted(TigaFastReplyQuorum& q);
   int isTxnCommittedDebug(TigaFastReplyQuorum& q);
   bool isNonSerializable(TigaFastReplyQuorum& q, uint64_t* agreedDeadline);
   void IssueReconcliationRequest(TigaFastReplyQuorum& q,
                                  uint64_t agreedDeadline);
   void UpdateQuorumSet();
   void CheckQuorum();
};

class TigaCoordinator {
  protected:
   YAML::Node config_;
   uint32_t owdDeltaUs_;
   uint32_t owdEstimationPercentile_;
   uint32_t shardNum_;
   uint32_t replicaNum_;
   int clockOffsetMean_;
   int clockOffsetStd_;
   GlobalInfo* gInfo_;

  public:
   std::recursive_mutex mtx_;
   uint32_t phase_;
   uint32_t stage_;
   uint64_t sendTime_;
   TigaReq reqInProcess_;
   TxnGenerator* txnGen_;
   uint32_t clientId_;
   uint32_t requestIdByClient_;
   bool detectReplicationInconsistency_;
   bool detectNonSerial_;
   std::function<void(const ClientReply& rep)> callback_;
   std::set<uint32_t> targetShards_;
   std::set<uint32_t> dispatchShards_;
   TigaCoordinator(const uint32_t coordinatorId, const YAML::Node& config);
   void SetGlobalInfo(GlobalInfo* info);
   void DoOne(const ClientRequest& req, TxnGenerator* txnGen);
   void OnDispatchReply(const uint32_t phase, const TigaDispatchReply& rep);
   void OnFastReply(const uint32_t phase, const TigaReply& rep);

   void Finish(TigaFastReplyQuorum& q);
   void Finish2();
   void Reset();
   void Dispatch();
   void Launch();
   void Abort();
   // void TryUnblock();
   ~TigaCoordinator();
};