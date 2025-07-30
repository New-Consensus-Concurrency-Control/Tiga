#include "CalvinMessage.h"

/**
 * Message Serialization and Deserialization
 */

Marshal& operator<<(Marshal& m, const CalvinRequest& req) {
   uint32_t sz = req.ws_.size();
   m << req.clientId_ << req.reqId_ << req.txnType_ << req.designateShardId_
     << req.designateReplicaId_ << sz;
   if (req.txnType_ == TXN_TYPE::MICRO_TXN) {
      // only care about keys
      for (auto& kv : req.ws_) {
         m << kv.first;
      }
   } else if (req.txnType_ > TXN_TYPE::TPCC_TXN_MIN &&
              req.txnType_ < TXN_TYPE::TPCC_TXN_MAX) {
      for (auto& kv : req.ws_) {
         m << kv.first << kv.second;
      }
   } else {
      LOG(ERROR) << "Not Implemented";
      assert(0);
   }
   return m;
}

Marshal& operator>>(Marshal& m, CalvinRequest& req) {
   m >> req.clientId_ >> req.reqId_ >> req.txnType_ >> req.designateShardId_ >>
       req.designateReplicaId_;
   uint32_t sz = 0;
   m >> sz;
   for (uint32_t i = 0; i < sz; i++) {
      int32_t k;
      Value v;
      if (req.txnType_ == TXN_TYPE::MICRO_TXN) {
         m >> k;
         req.ws_[k].set_i32(0);  // value has no use
      } else if (req.txnType_ > TXN_TYPE::TPCC_TXN_MIN &&
                 req.txnType_ < TXN_TYPE::TPCC_TXN_MAX) {
         m >> k >> v;
         req.ws_[k] = v;
      } else {
         LOG(ERROR) << "Not Implemented";
         assert(0);
      }
   }
   return m;
}

Marshal& operator<<(Marshal& m, const CalvinReply& rep) {
   m << rep.shardId_;
   m << rep.replicaId_;
   m << rep.clientId_;
   m << rep.reqId_;
   m << rep.owd_;
   m << rep.result_;
   return m;
}

Marshal& operator>>(Marshal& m, CalvinReply& rep) {
   m >> rep.shardId_;
   m >> rep.replicaId_;
   m >> rep.clientId_;
   m >> rep.reqId_;
   m >> rep.owd_;
   m >> rep.result_;
   return m;
}

Marshal& operator<<(Marshal& m, const CalvinDispatchRequest& req) {
   m << req.txnType_;
   uint32_t size = req.input_.size();
   m << size;
   for (auto& kv : req.input_) {
      m << kv.first << kv.second;
   }
   return m;
}

Marshal& operator>>(Marshal& m, CalvinDispatchRequest& req) {
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

Marshal& operator<<(Marshal& m, const CalvinDispatchReply& rep) {
   m << rep.shardId_;
   uint32_t size = rep.result_.size();
   m << size;
   for (auto& kv : rep.result_) {
      m << kv.first << kv.second;
   }
   return m;
}

Marshal& operator>>(Marshal& m, CalvinDispatchReply& rep) {
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
   m << rep.owd_;
   m << rep.designateShardId_;
   m << rep.designateReplicaId_;
   m << rep.viewId_;
   m << rep.shardId_;
   m << rep.replicaId_;
   m << rep.sequenceNo_;
   m << rep.logId_;
   m << rep.latestSyncedLogId_;
   m << rep.hasHash_;
   m << rep.hash_;
   m << rep.result_;
   return m;
}

Marshal& operator>>(Marshal& m, NezhaFastReply& rep) {
   m >> rep.owd_;
   m >> rep.designateShardId_;
   m >> rep.designateReplicaId_;
   m >> rep.viewId_;
   m >> rep.shardId_;
   m >> rep.replicaId_;
   m >> rep.sequenceNo_;
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

Marshal& operator<<(Marshal& m, const EpochRequest& req) {
   m << req.sequenceNo_;
   m << req.shardId_;
   m << req.replicaId_;
   m << req.sendTime_;
   m << req.bound_;
   uint32_t cmdSize = req.cmdVec_.size();
   m << cmdSize;
   for (auto& cmd : req.cmdVec_) {
      m << cmd;
   }
   return m;
}

Marshal& operator>>(Marshal& m, EpochRequest& req) {
   m >> req.sequenceNo_;
   m >> req.shardId_;
   m >> req.replicaId_;
   m >> req.sendTime_;
   m >> req.bound_;
   uint32_t cmdSize = 0;
   m >> cmdSize;
   req.cmdVec_.resize(cmdSize);
   for (uint32_t i = 0; i < cmdSize; i++) {
      m >> req.cmdVec_[i];
   }
   return m;
}

Marshal& operator<<(Marshal& m, const EpochReply& rep) {
   m << rep.sequenceNo_;
   m << rep.shardId_;
   m << rep.replicaId_;
   uint32_t resultSize = rep.resultVec_.size();
   m << resultSize;
   for (uint32_t i = 0; i < resultSize; i++) {
      m << rep.txnKeys_[i];
   }
   for (uint32_t i = 0; i < resultSize; i++) {
      m << rep.resultVec_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, EpochReply& rep) {
   m >> rep.sequenceNo_;
   m >> rep.shardId_;
   m >> rep.replicaId_;
   uint32_t resultSize = 0;
   m >> resultSize;
   rep.txnKeys_.resize(resultSize);
   rep.resultVec_.resize(resultSize);
   for (uint32_t i = 0; i < resultSize; i++) {
      m >> rep.txnKeys_[i];
   }
   for (uint32_t i = 0; i < resultSize; i++) {
      m >> rep.resultVec_[i];
   }
   return m;
}

Marshal& operator<<(Marshal& m, const MasterSyncRequest& req) {
   m << req.replicaId_ << req.shardId_;
   return m;
}

Marshal& operator>>(Marshal& m, MasterSyncRequest& req) {
   m >> req.replicaId_ >> req.shardId_;
   return m;
}

Marshal& operator<<(Marshal& m, const MasterSyncReply& req) {
   m << req.startTime_;
   return m;
}

Marshal& operator>>(Marshal& m, MasterSyncReply& req) {
   m >> req.startTime_;
   return m;
}

Marshal& operator<<(Marshal& m, const InquireSeqNoRequest& req) {
   m << req.clientId_;
   return m;
}

Marshal& operator>>(Marshal& m, InquireSeqNoRequest& req) {
   m >> req.clientId_;
   return m;
}

Marshal& operator<<(Marshal& m, const InquireSeqNoReply& rep) {
   m << rep.shardId_ << rep.replicaId_;
   uint32_t shardNum = rep.maxSeqNos_.size();
   m << shardNum;
   for (uint32_t i = 0; i < shardNum; i++) {
      m << rep.maxSeqNos_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, InquireSeqNoReply& rep) {
   m >> rep.shardId_ >> rep.replicaId_;
   uint32_t shardNum = 0;
   m >> shardNum;
   rep.maxSeqNos_.resize(shardNum);
   for (uint32_t i = 0; i < shardNum; i++) {
      m >> rep.maxSeqNos_[i];
   }
   return m;
}