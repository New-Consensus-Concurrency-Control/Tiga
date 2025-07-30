#include "NezhaMessage.h"

/**
 * Message Serialization and Deserialization
 */

Marshal& operator<<(Marshal& m, const NezhaRecordMessage& msg) {
   m << msg.txnKey_ << msg.cmdId_ << msg.ssidLow_ << msg.ssidHigh_
     << msg.ssidNew_;
   uint32_t keySize = msg.keys_.size();
   m << keySize;
   for (uint32_t i = 0; i < keySize; i++) {
      m << msg.keys_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, NezhaRecordMessage& msg) {
   m >> msg.txnKey_ >> msg.cmdId_ >> msg.ssidLow_ >> msg.ssidHigh_ >>
       msg.ssidNew_;
   uint32_t keySize = 0;
   m >> keySize;
   msg.keys_.resize(keySize, 0);
   for (uint32_t i = 0; i < keySize; i++) {
      m >> msg.keys_[i];
   }
   return m;
}

Marshal& operator<<(Marshal& m, const NezhaAck& cmd) {
   m << cmd.token_;
   return m;
}

Marshal& operator>>(Marshal& m, NezhaAck& cmd) {
   m >> cmd.token_;
   return m;
}

Marshal& operator<<(Marshal& m, const NezhaRequest& req) {
   m << req.clientId_ << req.reqId_ << req.sendTime_ << req.bound_ << req.cmd_;
   return m;
}

Marshal& operator>>(Marshal& m, NezhaRequest& req) {
   m >> req.clientId_ >> req.reqId_ >> req.sendTime_ >> req.bound_ >> req.cmd_;
   return m;
}

Marshal& operator<<(Marshal& m, const NezhaInterReplicaSync& req) {
   m << req.viewId_;
   m << req.shardId_;
   m << req.replicaId_;
   m << req.logIdStart_;
   uint32_t txnNum = req.txnKeys_.size();
   m << txnNum;
   for (uint32_t i = 0; i < txnNum; i++) {
      m << req.txnKeys_[i];
   }
   for (uint32_t i = 0; i < txnNum; i++) {
      m << req.deadlineRanks_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, NezhaInterReplicaSync& req) {
   m >> req.viewId_;
   m >> req.shardId_;
   m >> req.replicaId_;
   m >> req.logIdStart_;
   uint32_t txnNum = 0;
   m >> txnNum;
   req.txnKeys_.resize(txnNum);
   req.deadlineRanks_.resize(txnNum);
   for (uint32_t i = 0; i < txnNum; i++) {
      m >> req.txnKeys_[i];
   }
   for (uint32_t i = 0; i < txnNum; i++) {
      m >> req.deadlineRanks_[i];
   }
   return m;
}

Marshal& operator<<(Marshal& m, const NezhaFastReply& rep) {
   m << rep.token_;
   m << rep.owd_;
   m << rep.viewId_;
   m << rep.shardId_;
   m << rep.replicaId_;
   m << rep.clientId_;
   m << rep.reqId_;
   m << rep.logId_;
   m << rep.latestSyncedLogId_;
   m << rep.hasHash_;
   m << rep.hash_;
   m << rep.result_;
   return m;
}

Marshal& operator>>(Marshal& m, NezhaFastReply& rep) {
   m >> rep.token_;
   m >> rep.owd_;
   m >> rep.viewId_;
   m >> rep.shardId_;
   m >> rep.replicaId_;
   m >> rep.clientId_;
   m >> rep.reqId_;
   m >> rep.logId_;
   m >> rep.latestSyncedLogId_;
   m >> rep.hasHash_;
   m >> rep.hash_;
   m >> rep.result_;
   return m;
}

Marshal& operator<<(Marshal& m, const NezhaInquireRequest& req) {
   m << req.viewId_;
   m << req.shardId_;
   m << req.replicaId_;
   return m;
}

Marshal& operator>>(Marshal& m, NezhaInquireRequest& req) {
   m >> req.viewId_;
   m >> req.shardId_;
   m >> req.replicaId_;
   return m;
}

Marshal& operator<<(Marshal& m, const NezhaInquireReply& rep) {
   m << rep.viewId_;
   m << rep.shardId_;
   m << rep.replicaId_;
   m << rep.latestSyncedLogId_;
   return m;
}

Marshal& operator>>(Marshal& m, NezhaInquireReply& rep) {
   m >> rep.viewId_;
   m >> rep.shardId_;
   m >> rep.replicaId_;
   m >> rep.latestSyncedLogId_;
   return m;
}
