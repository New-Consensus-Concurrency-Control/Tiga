
#include "StateMachine.h"

StateMachine::StateMachine(const uint32_t shardId, const uint32_t replicaId,
                           const uint32_t shardNum, const uint32_t replicaNum,
                           const YAML::Node& config)
    : shardId_(shardId),
      replicaId_(replicaId),
      shardNum_(shardNum),
      replicaNum_(replicaNum),
      config_(config) {}

uint32_t StateMachine::ShardNum() { return shardNum_; }
uint32_t StateMachine::ReplicaNum() { return replicaNum_; }
uint32_t StateMachine::ShardId() { return shardId_; }
uint32_t StateMachine::ReplicaId() { return replicaId_; }
YAML::Node StateMachine::Config() { return config_; }

StateMachine::~StateMachine() {}

uint32_t StateMachine::TotalNumberofKeys() { return 1000ul * 1000ul; }

void StateMachine::PreRead(const uint32_t txnType,
                           const std::map<int32_t, Value>* input,
                           std::map<int32_t, Value>* output) {}
void StateMachine::ExecuteReadOnlyTxn(const uint32_t txnType,
                                      const std::map<int32_t, Value>* input,
                                      std::map<int32_t, Value>* output) {}