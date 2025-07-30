#pragma once
#include "CalvinMessage.h"
#include "CalvinServiceImpl.h"

class CalvinCommunicator {
  private:
   uint32_t id_;
   YAML::Node config_;
   uint32_t shardNum_;
   uint32_t replicaNum_;
   std::string serverAddrs_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   CalvinProxy* proxies_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   rrr::PollMgr* rpcPoll_;

  public:
   CalvinCommunicator(const uint32_t id, const YAML::Node& config);
   void Connect();
   CalvinProxy* ProxyAt(const uint32_t shardId, const uint32_t replicaId);
   ~CalvinCommunicator();
};
