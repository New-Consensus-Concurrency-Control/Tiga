#pragma once
#include <algorithm>
#include <cstddef>
#include "DetockService/DetockCommunicator.h"
#include "TxnGenerator/MicroTxnGenerator.h"
#include "TxnGenerator/TPCCTxnGenerator.h"
#include "concurrentqueue.h"
template <typename T>
using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

using namespace DetockRPC;

struct OWDSample {
   uint32_t shardId_;
   uint32_t owd_;
};
struct GlobalInfo {
   ConcurrentQueue<OWDSample> owdQu_;
   std::atomic<uint32_t> owdByShard_[MAX_SHARD_NUM];
   uint32_t owdSampleCnt_[MAX_SHARD_NUM];
   std::thread* owdTd_;
   GlobalInfo();
   void OWDCalc();
};

class DetockCoordinator {
  protected:
   YAML::Node config_;
   uint32_t shardNum_;
   uint32_t replicaNum_;
   uint32_t coordinatorId_;
   uint32_t viewId_;
   TxnGenerator* txnGen_;
   DetockCommunicator* comm_;
   std::set<uint32_t> targetShards_;
   std::set<uint32_t> dispatchShards_;

  public:
   static GlobalInfo* gInfo_;
   std::recursive_mutex mtx_;
   uint32_t phase_;
   uint32_t stage_;
   uint64_t sendTime_;
   DetockTxn reqInProcess_;
   std::function<void(const ClientReply& rep)> callback_;
   DetockCoordinator(const uint32_t coordinatorId, const uint32_t shardNum,
                     const uint32_t replicaNum, DetockCommunicator* comm,
                     const YAML::Node& config);
   void DoOne(const ClientRequest& req, TxnGenerator* txnGen);
   void onReply(const uint32_t currentPhase, const DetockReply& rep);
   void OnDispatchReply(const uint32_t phase, const DetockDispatchReply& rep);
   ~DetockCoordinator();
   void Launch();
   void Dispatch();
   void Reset();
   uint32_t LeaderReplicaId(const uint32_t viewId, const uint32_t shardId);
};
