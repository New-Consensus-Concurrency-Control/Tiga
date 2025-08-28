
#pragma once
#include "Common.h"

class StateMachine {
  protected:
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t shardNum_;
   uint32_t replicaNum_;
   YAML::Node config_;

  public:
   StateMachine(const uint32_t shardId, const uint32_t replicaId,
                const uint32_t shardNum, const uint32_t replicaNum,
                const YAML::Node& config);

   uint32_t ShardNum();
   uint32_t ReplicaNum();
   uint32_t ShardId();
   uint32_t ReplicaId();
   YAML::Node Config();

   virtual std::string RTTI() = 0;

   virtual void RecordTimestampVersion(const std::vector<int32_t>* localKeys,
                                       std::map<int32_t, Value>* input,
                                       uint64_t tmstmp);
   // If the specified timestamp is larger
   // than the committed timestamp of any key to read,
   // then this read fails, otherwise, we get the latest committed key-value
   virtual void ReadCommittedVersionByTimestamp(
       const std::vector<int32_t>* localKeys, std::map<int32_t, Value>* output,
       uint64_t tmstmp);
   virtual void Execute(const uint32_t txnType,
                        const std::vector<int32_t>* localKeys,
                        std::map<int32_t, Value>* input,
                        std::map<int32_t, Value>* output,
                        const uint64_t txnId = 0) = 0;
   virtual void SpecExecute(const uint32_t txnType,
                            const std::vector<int32_t>* localKeys,
                            std::map<int32_t, Value>* input,
                            std::map<int32_t, Value>* output,
                            const uint64_t txnId = 0) = 0;
   virtual void CommitExecute(const uint32_t txnType,
                              const std::vector<int32_t>* localKeys,
                              std::map<int32_t, Value>* input,
                              std::map<int32_t, Value>* output,
                              const uint64_t txnId = 0) = 0;
   virtual void RollbackExecute(const uint32_t txnType,
                                const std::vector<int32_t>* localKeys,
                                std::map<int32_t, Value>* input,
                                std::map<int32_t, Value>* output,
                                const uint64_t txnId = 0) = 0;

   virtual void InitializeRelatedShards(
       const uint32_t txnType, std::map<int32_t, Value>* ws,
       std::map<uint32_t, std::set<int32_t>>* shardKeyMap) = 0;
   virtual uint32_t TotalNumberofKeys();
   ~StateMachine();

   virtual void PreRead(const uint32_t txnType,
                        const std::map<int32_t, Value>* input,
                        std::map<int32_t, Value>* output);
};