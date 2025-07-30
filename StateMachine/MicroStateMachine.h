
#pragma once
#include "StateMachine.h"
#define MAX_KEY_NUM (1000005)

class MicroStateMachine : public StateMachine {
  private:
   uint32_t kvStore_[MAX_KEY_NUM];
   // Whenever we do speculative execution, keep the prev version in case of
   // rollback
   VersionInfo speculativeVersion_[MAX_KEY_NUM];

  public:
   MicroStateMachine(const uint32_t shardId, const uint32_t replicaId,
                     const uint32_t shardNum, const uint32_t replicaNum,
                     const YAML::Node& config);
   std::string RTTI() override;

   void Execute(const uint32_t txnType, const std::vector<int32_t>* localKeys,
                std::map<int32_t, Value>* input,
                std::map<int32_t, Value>* output,
                const uint64_t txnId = 0) override;
   void SpecExecute(const uint32_t txnType,
                    const std::vector<int32_t>* localKeys,
                    std::map<int32_t, Value>* input,
                    std::map<int32_t, Value>* output,
                    const uint64_t txnId = 0) override;
   void CommitExecute(const uint32_t txnType,
                      const std::vector<int32_t>* localKeys,
                      std::map<int32_t, Value>* input,
                      std::map<int32_t, Value>* output,
                      const uint64_t txnId = 0) override;
   void RollbackExecute(const uint32_t txnType,
                        const std::vector<int32_t>* localKeys,
                        std::map<int32_t, Value>* input,
                        std::map<int32_t, Value>* output,
                        const uint64_t txnId = 0) override;

   void InitializeRelatedShards(
       const uint32_t txnType, std::map<int32_t, Value>* ws,
       std::map<uint32_t, std::set<int32_t>>* shardKeyMap) override;

   uint32_t TotalNumberofKeys() override;
   ~MicroStateMachine();
};