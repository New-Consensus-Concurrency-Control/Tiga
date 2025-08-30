
#include "YCSBStateMachine.h"

YCSBStateMachine::YCSBStateMachine(const uint32_t shardId,
                                   const uint32_t replicaId,
                                   const uint32_t shardNum,
                                   const uint32_t replicaNum,
                                   const YAML::Node& config)
    : StateMachine(shardId, replicaId, shardNum, replicaNum, config) {
   numOfKeys_ = config["bench"]["record-count"].as<uint32_t>();
   numOfFields_ = config["bench"]["field-num"].as<uint32_t>();
}

std::string YCSBStateMachine::RTTI() { return "YCSBStateMachine"; }

void YCSBStateMachine::InitializeRelatedShards(
    const uint32_t txnType, std::map<int32_t, Value>* ws,
    std::map<uint32_t, std::set<int32_t>>* shardKeyMap) {
   shardKeyMap->clear();
   for (auto& kv : *ws) {
      uint32_t key = kv.first;
      (*shardKeyMap)[key % shardNum_].insert(key);
   }
}

void YCSBStateMachine::RecordTimestampVersion(
    const std::vector<int32_t>* localKeys, std::map<int32_t, Value>* input,
    uint64_t tmstmp) {
   for (auto& key : (*localKeys)) {
      multiVersionKVStore_[key][tmstmp] = kvStore_[key];
   }
}

void YCSBStateMachine::ReadCommittedVersionByTimestamp(
    const std::vector<int32_t>* localKeys, std::map<int32_t, Value>* output,
    uint64_t tmstmp) {
   for (auto& key : (*localKeys)) {
      auto iter = multiVersionKVStore_[key].lower_bound(tmstmp);
      // it will return the first kv that <= tmstmp
      if (iter == multiVersionKVStore_[key].end()) {
         // This KV has never been updated,
         // Or its updates all happens after the timestamp
         // so directly read it from kvStore
         if (kvStore_.find(key) == kvStore_.end()) {
            (*output)[key].set_str("NULL");
         } else {
            (*output)[key].set_str(kvStore_[key]);
         }
      } else {
         (*output)[key].set_str(iter->second);
      }
   }
}

void YCSBStateMachine::Execute(const uint32_t txnType,
                               const std::vector<int32_t>* localKeys,
                               std::map<int32_t, Value>* input,
                               std::map<int32_t, Value>* output,
                               const uint64_t txnId) {
   output->clear();
   if (txnType == TXN_TYPE::YCSB_UPDATE_TXN) {
      for (auto& key : (*localKeys)) {
         kvStore_[key] = (*input)[key].get_str();
         (*output)[key].set_str("OK");
      }
   } else if (txnType == TXN_TYPE::YCSB_READ_TXN) {
      for (auto& key : (*localKeys)) {
         if (kvStore_.find(key) == kvStore_.end()) {
            (*output)[key].set_str("NULL");
         } else {
            (*output)[key].set_str(kvStore_[key]);
         }
      }
   } else {
      LOG(ERROR) << "Unsupported txn type " << txnType;
   }
}

void YCSBStateMachine::SpecExecute(const uint32_t txnType,
                                   const std::vector<int32_t>* localKeys,
                                   std::map<int32_t, Value>* input,
                                   std::map<int32_t, Value>* output,
                                   const uint64_t txnId) {

   output->clear();
   if (txnType == TXN_TYPE::YCSB_UPDATE_TXN) {
      for (auto& key : *localKeys) {
         speculativeVersion_[key] = {txnId, (*input)[key].get_str()};
         (*output)[key].set_str("OK");
      }
   } else if (txnType == TXN_TYPE::YCSB_READ_TXN) {
      for (auto& key : *localKeys) {
         if (kvStore_.find(key) != kvStore_.end()) {
            (*output)[key].set_str(kvStore_[key]);
         } else {
            (*output)[key].set_str("NULL");
         }
      }
   } else {
      LOG(ERROR) << "Unsupported txn type " << txnType;
   }
}

void YCSBStateMachine::CommitExecute(const uint32_t txnType,
                                     const std::vector<int32_t>* localKeys,
                                     std::map<int32_t, Value>* input,
                                     std::map<int32_t, Value>* output,
                                     const uint64_t txnId) {

   if (txnType == TXN_TYPE::YCSB_UPDATE_TXN) {
      for (auto& key : *localKeys) {
         if (speculativeVersion_.find(key) == speculativeVersion_.end() ||
             speculativeVersion_[key].txnId_ != txnId) {
            LOG(ERROR) << "key=" << key << "\t existing "
                       << HIGH_32BIT(speculativeVersion_[key].txnId_) << ":"
                       << LOW_32BIT(speculativeVersion_[key].txnId_) << "\t"
                       << "my id=" << HIGH_32BIT(txnId) << ":"
                       << LOW_32BIT(txnId);
         }
         assert(speculativeVersion_[key].txnId_ == txnId);
         kvStore_[key] = speculativeVersion_[key].value_;
         // Delete speculative versions
         speculativeVersion_.erase(key);
      }
   } else if (txnType == TXN_TYPE::YCSB_READ_TXN) {
      // Nothing to do
   } else {
      LOG(ERROR) << "Unsupported txn type " << txnType;
   }
}

void YCSBStateMachine::RollbackExecute(const uint32_t txnType,
                                       const std::vector<int32_t>* localKeys,
                                       std::map<int32_t, Value>* input,
                                       std::map<int32_t, Value>* output,
                                       const uint64_t txnId) {
   output->clear();
   for (auto& key : *localKeys) {
      // Delete speculative versions
      speculativeVersion_.erase(key);
   }
}

uint32_t YCSBStateMachine::TotalNumberofKeys() {
   return numOfKeys_ * (numOfFields_ + 1);
}

YCSBStateMachine::~YCSBStateMachine() {}