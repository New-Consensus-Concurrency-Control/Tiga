#pragma once
#include "StateMachine.h"
#define MAX_KEY_NUM (1000005)

struct YCSBVersionInfo {
   uint64_t txnId_;
   std::string value_;
};
using TimeStampVersionKV =
    std::map<uint64_t, std::string, std::greater<uint64_t>>;
class YCSBStateMachine : public StateMachine {
  private:
   // Whenever we do speculative execution, keep the prev version in case of
   // rollback
   std::unordered_map<int32_t, YCSBVersionInfo> speculativeVersion_;
   std::unordered_map<int32_t, std::string> kvStore_;
   std::unordered_map<int32_t, TimeStampVersionKV> multiVersionKVStore_;
   uint32_t numOfKeys_;
   uint32_t numOfFields_;
   uint32_t fieldLength_;

  public:
   YCSBStateMachine(const uint32_t shardId, const uint32_t replicaId,
                    const uint32_t shardNum, const uint32_t replicaNum,
                    const YAML::Node& config);
   std::string RTTI() override;

   void RecordTimestampVersion(const std::vector<int32_t>* localKeys,
                               std::map<int32_t, Value>* input,
                               uint64_t tmstmp);

   void ReadCommittedVersionByTimestamp(const std::vector<int32_t>* localKeys,
                                        std::map<int32_t, Value>* output,
                                        uint64_t tmstmp);

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
   ~YCSBStateMachine();
};