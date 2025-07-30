#include "DetockMessage.h"

/**
 * Message Serialization and Deserialization
 */

Marshal& operator<<(Marshal& m, const DetockTxn& msg) {
   m << msg.sendTime_ << msg.bound_;
   m << msg.clientId_ << msg.reqId_ << msg.txnType_;
   uint32_t size = msg.shardIds_.size();
   m << size;
   for (uint32_t i = 0; i < size; i++) {
      m << msg.shardIds_[i];
   }
   for (uint32_t i = 0; i < size; i++) {
      m << msg.replicaIds_[i];
   }
   uint32_t wsSize = msg.ws_.size();
   m << wsSize;
   for (auto& kv : msg.ws_) {
      m << kv.first << kv.second;
   }
   return m;
}

Marshal& operator>>(Marshal& m, DetockTxn& msg) {
   m >> msg.sendTime_ >> msg.bound_;
   m >> msg.clientId_ >> msg.reqId_ >> msg.txnType_;
   uint32_t size = 0;
   m >> size;
   msg.shardIds_.resize(size);
   msg.replicaIds_.resize(size);
   for (uint32_t i = 0; i < size; i++) {
      m >> msg.shardIds_[i];
   }
   for (uint32_t i = 0; i < size; i++) {
      m >> msg.replicaIds_[i];
   }
   uint32_t wsSize = 0;
   m >> wsSize;
   for (uint32_t i = 0; i < wsSize; i++) {
      int32_t k;
      mdb::Value v;
      m >> k >> v;
      msg.ws_[k] = v;
   }
   return m;
}

Marshal& operator<<(Marshal& m, const DetockReply& msg) {
   m << msg.shardId_;
   m << msg.replicaId_;
   m << msg.clientId_;
   m << msg.reqId_;
   m << msg.owd_;
   m << msg.result_;
   return m;
}

Marshal& operator>>(Marshal& m, DetockReply& msg) {
   m >> msg.shardId_;
   m >> msg.replicaId_;
   m >> msg.clientId_;
   m >> msg.reqId_;
   m >> msg.owd_;
   m >> msg.result_;
   return m;
}

Marshal& operator<<(Marshal& m, const DetockDispatchRequest& req) {
   m << req.txnType_;
   uint32_t size = req.input_.size();
   m << size;
   for (auto& kv : req.input_) {
      m << kv.first << kv.second;
   }
   return m;
}

Marshal& operator>>(Marshal& m, DetockDispatchRequest& req) {
   m >> req.txnType_;
   uint32_t size = 0;
   m >> size;
   for (uint32_t i = 0; i < size; i++) {
      int32_t key;
      mdb::Value val;
      m >> key >> val;
      req.input_[key] = val;
   }
   return m;
}

Marshal& operator<<(Marshal& m, const DetockDispatchReply& rep) {
   m << rep.shardId_;
   uint32_t size = rep.result_.size();
   m << size;
   for (auto& kv : rep.result_) {
      m << kv.first << kv.second;
   }
   return m;
}

Marshal& operator>>(Marshal& m, DetockDispatchReply& rep) {
   m >> rep.shardId_;
   uint32_t size = 0;
   m >> size;
   for (uint32_t i = 0; i < size; i++) {
      int32_t key;
      mdb::Value val;
      m >> key >> val;
      rep.result_[key] = val;
   }
   return m;
}

Marshal& operator<<(Marshal& m, const DetockBatch& msg) {
   m << msg.batchId_;
   uint32_t size = msg.batchContent_.size();
   m << size;
   for (uint32_t i = 0; i < size; i++) {
      m << msg.batchContent_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, DetockBatch& msg) {
   m >> msg.batchId_;
   uint32_t size = 0;
   m >> size;
   msg.batchContent_.resize(size);
   for (uint32_t i = 0; i < size; i++) {
      m >> msg.batchContent_[i];
   }
   return m;
}

Marshal& operator<<(Marshal& m, const DetockPaxosAppend& msg) {
   m << msg.viewId_ << msg.shardId_ << msg.replicaId_ << msg.batch_;
   return m;
}

Marshal& operator>>(Marshal& m, DetockPaxosAppend& msg) {
   m >> msg.viewId_ >> msg.shardId_ >> msg.replicaId_ >> msg.batch_;
   return m;
}

Marshal& operator<<(Marshal& m, const DetockPaxosAppendReply& msg) {
   m << msg.viewId_ << msg.shardId_ << msg.replicaId_ << msg.batchId_;
   return m;
}

Marshal& operator>>(Marshal& m, DetockPaxosAppendReply& msg) {
   m >> msg.viewId_ >> msg.shardId_ >> msg.replicaId_ >> msg.batchId_;
   return m;
}

Marshal& operator<<(Marshal& m, const DetockPaxosCommit& msg) {
   m << msg.viewId_ << msg.shardId_ << msg.replicaId_ << msg.batchId_;
   return m;
}

Marshal& operator>>(Marshal& m, DetockPaxosCommit& msg) {
   m >> msg.viewId_ >> msg.shardId_ >> msg.replicaId_ >> msg.batchId_;
   return m;
}

Marshal& operator<<(Marshal& m, const DetockTxnMetaData& msg) {
   m << msg.txnKey_;
   uint32_t keySize = msg.localKeys_.size();
   m << keySize;
   for (uint32_t i = 0; i < keySize; i++) {
      m << msg.localKeys_[i];
   }
   uint32_t size = msg.shardIds_.size();
   m << size;
   for (uint32_t i = 0; i < size; i++) {
      m << msg.shardIds_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, DetockTxnMetaData& msg) {
   m >> msg.txnKey_;
   uint32_t keySize = 0;
   m >> keySize;
   msg.localKeys_.resize(keySize);
   for (uint32_t i = 0; i < keySize; i++) {
      m >> msg.localKeys_[i];
   }
   uint32_t size = 0;
   m >> size;
   msg.shardIds_.resize(size);
   for (uint32_t i = 0; i < size; i++) {
      m >> msg.shardIds_[i];
   }
   return m;
}

Marshal& operator<<(Marshal& m, const DetockLocalLogSync& msg) {
   m << msg.viewId_ << msg.shardId_ << msg.replicaId_ << msg.msgId_;
   uint32_t nodeSize = msg.txnMetas_.size();
   m << nodeSize;
   for (uint32_t i = 0; i < nodeSize; i++) {
      m << msg.txnMetas_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, DetockLocalLogSync& msg) {
   m >> msg.viewId_ >> msg.shardId_ >> msg.replicaId_ >> msg.msgId_;
   uint32_t nodeSize = 0;
   m >> nodeSize;
   msg.txnMetas_.resize(nodeSize);
   for (uint32_t i = 0; i < nodeSize; i++) {
      m >> msg.txnMetas_[i];
   }
   return m;
}