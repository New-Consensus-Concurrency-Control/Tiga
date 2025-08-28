
#include "MicroStateMachine.h"

MicroStateMachine::MicroStateMachine(const uint32_t shardId,
                                     const uint32_t replicaId,
                                     const uint32_t shardNum,
                                     const uint32_t replicaNum,
                                     const YAML::Node& config)
    : StateMachine(shardId, replicaId, shardNum, replicaNum, config) {
   memset(kvStore_, '\0', sizeof(uint32_t) * MAX_KEY_NUM);
}

std::string MicroStateMachine::RTTI() { return "MicroStateMachine"; }

void MicroStateMachine::InitializeRelatedShards(
    const uint32_t txnType, std::map<int32_t, Value>* ws,
    std::map<uint32_t, std::set<int32_t>>* shardKeyMap) {
   shardKeyMap->clear();
   for (auto& kv : *ws) {
      uint32_t key = kv.first;
      (*shardKeyMap)[key % shardNum_].insert(key);
   }
}

void MicroStateMachine::RecordTimestampVersion(
    const std::vector<int32_t>* localKeys, std::map<int32_t, Value>* input,
    uint64_t tmstmp) {
   for (auto& key : (*localKeys)) {
      // to keep consistent with Janus codebase
      uint32_t mappedKeyId =
          key / shardNum_ + MAX_KEY_NUM / shardNum_ * shardId_;
      multiVersionKVStore_[mappedKeyId][tmstmp] = kvStore_[mappedKeyId];
   }
}

void MicroStateMachine::ReadCommittedVersionByTimestamp(
    const std::vector<int32_t>* localKeys, std::map<int32_t, Value>* output,
    uint64_t tmstmp) {
   for (auto& key : (*localKeys)) {
      // to keep consistent with Janus codebase
      uint32_t mappedKeyId =
          key / shardNum_ + MAX_KEY_NUM / shardNum_ * shardId_;
      multiVersionKVStore_[mappedKeyId][tmstmp] = kvStore_[mappedKeyId];
      auto iter = multiVersionKVStore_[mappedKeyId].lower_bound(tmstmp);
      // it will return the first kv that <= tmstmp
      if (iter == multiVersionKVStore_[mappedKeyId].end()) {
         // This KV has never been updated,
         // Or its updates all happens after the timestamp
         // so directly read it from kvStore
         (*output)[key].set_i32(kvStore_[mappedKeyId]);
      } else {
         (*output)[key].set_i32(iter->second);
      }
   }
}

void MicroStateMachine::Execute(const uint32_t txnType,
                                const std::vector<int32_t>* localKeys,
                                std::map<int32_t, Value>* input,
                                std::map<int32_t, Value>* output,
                                const uint64_t txnId) {
   output->clear();
   for (auto& key : (*localKeys)) {
      // to keep consistent with Janus codebase
      uint32_t mappedKeyId =
          key / shardNum_ + MAX_KEY_NUM / shardNum_ * shardId_;
      kvStore_[mappedKeyId]++;
      (*output)[key].set_i32(kvStore_[mappedKeyId]);
   }
}

void MicroStateMachine::SpecExecute(const uint32_t txnType,
                                    const std::vector<int32_t>* localKeys,
                                    std::map<int32_t, Value>* input,
                                    std::map<int32_t, Value>* output,
                                    const uint64_t txnId) {

   output->clear();
   for (auto& key : *localKeys) {
      // to keep consistent with Janus codebase
      uint32_t mappedKeyId =
          key / shardNum_ + MAX_KEY_NUM / shardNum_ * shardId_;
      // Keep a prev Version
      uint32_t value = kvStore_[mappedKeyId] + 1;
      // LOG(INFO) << " txn Spec " << HIGH_32BIT(txnId) << ":" <<
      // LOW_32BIT(txnId);
      speculativeVersion_[mappedKeyId] = {txnId, value};
      kvStore_[mappedKeyId]++;
      (*output)[key].set_i32(value);
   }
}

void MicroStateMachine::CommitExecute(const uint32_t txnType,
                                      const std::vector<int32_t>* localKeys,
                                      std::map<int32_t, Value>* input,
                                      std::map<int32_t, Value>* output,
                                      const uint64_t txnId) {

   for (auto& key : *localKeys) {
      uint32_t mappedKeyId =
          key / shardNum_ + MAX_KEY_NUM / shardNum_ * shardId_;
      if (speculativeVersion_[mappedKeyId].txnId_ != txnId) {
         LOG(INFO) << "existing "
                   << HIGH_32BIT(speculativeVersion_[mappedKeyId].txnId_) << ":"
                   << LOW_32BIT(speculativeVersion_[mappedKeyId].txnId_) << "\t"
                   << "my id=" << HIGH_32BIT(txnId) << ":" << LOW_32BIT(txnId);
      }
      assert(speculativeVersion_[mappedKeyId].txnId_ == txnId);
      kvStore_[mappedKeyId] = speculativeVersion_[mappedKeyId].value_;
      // Delete speculative versions
      speculativeVersion_[mappedKeyId] = {UINT64_MAX, UINT32_MAX};
   }
}

void MicroStateMachine::RollbackExecute(const uint32_t txnType,
                                        const std::vector<int32_t>* localKeys,
                                        std::map<int32_t, Value>* input,
                                        std::map<int32_t, Value>* output,
                                        const uint64_t txnId) {
   output->clear();
   for (auto& key : *localKeys) {
      uint32_t mappedKeyId =
          key / shardNum_ + MAX_KEY_NUM / shardNum_ * shardId_;
      // Delete speculative versions
      speculativeVersion_[mappedKeyId] = {UINT64_MAX, UINT32_MAX};
   }
}

uint32_t MicroStateMachine::TotalNumberofKeys() { return MAX_KEY_NUM; }

MicroStateMachine::~MicroStateMachine() {}