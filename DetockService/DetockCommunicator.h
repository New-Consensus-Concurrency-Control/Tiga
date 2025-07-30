#pragma once
#include "DetockMessage.h"
#include "DetockServiceImpl.h"

class DetockCommunicator {
  private:
   uint32_t id_;
   YAML::Node config_;
   uint32_t shardNum_;
   uint32_t replicaNum_;
   std::string serverAddrs_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   DetockProxy* proxies_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   rrr::PollMgr* rpcPoll_;

  public:
   DetockCommunicator(const uint32_t id, const YAML::Node& config);
   void Connect();
   DetockProxy* ProxyAt(const uint32_t shardId, const uint32_t replicaId);
   ~DetockCommunicator();
};
