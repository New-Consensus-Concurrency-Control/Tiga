#pragma once
#include <cstddef>
#include "CalvinService/CalvinCommunicator.h"
#include "TxnGenerator/MicroTxnGenerator.h"
#include "TxnGenerator/TPCCTxnGenerator.h"
using namespace CalvinRPC;

class CalvinCoordinator {
  protected:
   YAML::Node config_;
   uint32_t shardNum_;
   uint32_t replicaNum_;
   TxnGenerator* txnGen_;
   uint32_t coordinatorId_;
   int32_t designatedShardId_;
   int32_t designatedReplicaId_;
   CalvinCommunicator* comm_;
   std::set<uint32_t> targetShards_;
   std::set<uint32_t> dispatchShards_;

  public:
   static std::atomic<int32_t> outstandingTxnNum_[MAX_REPLICA_NUM];
   static std::atomic<int32_t> submittedTxnNUm_[MAX_REPLICA_NUM];
   static std::atomic<int32_t> repliedTxnNum_[MAX_REPLICA_NUM];
   static std::atomic<int32_t> totalSubmittedTxn_;
   static std::atomic<uint32_t> maxSeqNos_[MAX_REPLICA_NUM][MAX_SHARD_NUM];
   static void initialize();

   std::recursive_mutex mtx_;
   uint32_t phase_;
   uint32_t stage_;
   uint64_t sendTime_;
   CalvinRequest reqInProcess_;
   std::function<void(const ClientReply& rep)> callback_;
   CalvinCoordinator(const uint32_t coordinatorId, const uint32_t shardNum,
                     const uint32_t replicaNum, const int32_t designatedShardId,
                     const int32_t designatedReplicaId,
                     CalvinCommunicator* comm, const YAML::Node& config);
   void DoOne(const ClientRequest& req, TxnGenerator* txnGen);
   void onReply(const uint32_t currentPhase, const CalvinReply& rep);
   void OnDispatchReply(const uint32_t phase, const CalvinDispatchReply& rep);
   ~CalvinCoordinator();
   void Launch();
   void Dispatch();
   void Reset();
};
