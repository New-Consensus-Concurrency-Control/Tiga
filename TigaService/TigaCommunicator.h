#pragma once
#include <glog/logging.h>
#include <yaml-cpp/yaml.h>
#include "TigaMessage.h"
#include "TigaServiceImpl.h"

class TigaCommunicator {
  private:
   uint32_t id_;
   YAML::Node config_;
   uint32_t shardNum_;
   uint32_t replicaNum_;
   std::string serverAddrs_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   TigaProxy* proxies_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   rrr::PollMgr* rpcPoll_;

  public:
   TigaCommunicator(const uint32_t id, const YAML::Node& config);
   YAML::Node Config();
   void Connect();
   TigaProxy* ProxyAt(const uint32_t shardId, const uint32_t replicaId);
   ~TigaCommunicator();
};
