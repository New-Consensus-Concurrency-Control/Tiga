#include "TigaMessage.h"

/**
 * Message Serialization and Deserialization
 */
Marshal& operator<<(Marshal& m, const TigaDispatchRequest& req) {
   m << req.txnType_;
   uint32_t size = req.input_.size();
   m << size;
   for (auto& kv : req.input_) {
      m << kv.first << kv.second;
   }
   return m;
}

Marshal& operator>>(Marshal& m, TigaDispatchRequest& req) {
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

Marshal& operator<<(Marshal& m, const TigaDispatchReply& rep) {
   m << rep.shardId_;
   uint32_t size = rep.result_.size();
   m << size;
   for (auto& kv : rep.result_) {
      m << kv.first << kv.second;
   }
   return m;
}

Marshal& operator>>(Marshal& m, TigaDispatchReply& rep) {
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

Marshal& operator<<(Marshal& m, const TigaReq& req) {
   m << req.sendTime_;
   m << req.bound_;
   m << req.cmd_;
   m << req.isDispatchRead_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaReq& req) {
   m >> req.sendTime_ >> req.bound_;
   m >> req.cmd_;
   m >> req.isDispatchRead_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaReply& rep) {
   m << rep.owd_;
   m << rep.gViewId_;
   m << rep.viewId_;
   m << rep.shardId_;
   m << rep.replicaId_;
   m << rep.clientId_;
   m << rep.reqId_;
   m << rep.logId_;
   m << rep.specLogId_;
   m << rep.latestSyncedLogId_;
   m << rep.latestSyncedSpecLogId_;
   m << rep.deadline_;
   m << rep.hasHash_;
   // printf("rep.hasHash=%d\n", rep.hasHash_);
   if (rep.hasHash_ == 1) {
      m << rep.hash_.h1_ << rep.hash_.h2_ << rep.hash_.h3_ << rep.hash_.h4_
        << rep.hash_.h5_;
   }
   uint32_t resultSize = rep.result_.size();
   m << resultSize;
   for (auto& kv : rep.result_) {
      m << kv.first << kv.second;
   }
   m << rep.status_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaReply& rep) {
   // printf("reply >>\n");
   m >> rep.owd_;
   m >> rep.gViewId_;
   m >> rep.viewId_;
   m >> rep.shardId_;
   m >> rep.replicaId_;
   m >> rep.clientId_;
   m >> rep.reqId_;
   m >> rep.logId_;
   m >> rep.specLogId_;
   m >> rep.latestSyncedLogId_;
   m >> rep.latestSyncedSpecLogId_;
   m >> rep.deadline_;
   m >> rep.hasHash_;
   // printf("hasHash_=%d >>\n", rep.hasHash_);
   //  printf("rep.hasHash=%d\n", rep.hasHash_);
   if (rep.hasHash_ == 1) {
      m >> rep.hash_.h1_ >> rep.hash_.h2_ >> rep.hash_.h3_ >> rep.hash_.h4_ >>
          rep.hash_.h5_;
   }
   uint32_t resultSize = 0;
   m >> resultSize;
   // printf("resultSize=%d >>\n", resultSize);
   for (uint32_t i = 0; i < resultSize; i++) {
      int32_t k;
      Value v;
      m >> k >> v;
      rep.result_[k] = v;
   }
   // printf("Done\n");
   m >> rep.status_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaSlowReply& req) {
   m << req.gViewId_ << req.viewId_ << req.logId_ << req.shardId_
     << req.replicaId_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaSlowReply& req) {
   m >> req.gViewId_ >> req.viewId_ >> req.logId_ >> req.shardId_ >>
       req.replicaId_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaServerSyncStatusRequest& req) {
   m << req.clientId_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaServerSyncStatusRequest& req) {
   m >> req.clientId_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaServerSyncStatusReply& rep) {
   m << rep.gViewId_ << rep.viewId_ << rep.latestSyncedLogId_
     << rep.latestSyncedSpecLogId_ << rep.shardId_ << rep.replicaId_
     << rep.signal_ << rep.status_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaServerSyncStatusReply& rep) {
   m >> rep.gViewId_ >> rep.viewId_ >> rep.latestSyncedLogId_ >>
       rep.latestSyncedSpecLogId_ >> rep.shardId_ >> rep.replicaId_ >>
       rep.signal_ >> rep.status_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaDeadlineAgreeRequest& req) {
   m << req.gViewId_ << req.viewId_ << req.shardId_ << req.replicaId_;
   uint32_t sz = req.txnKeys_.size();
   uint32_t sz2 = req.txnKeys2_.size();
   m << sz << sz2;
   for (uint32_t i = 0; i < sz; i++) {
      m << req.txnKeys_[i] << req.deadlineRanks_[i];
   }

   for (uint32_t i = 0; i < sz2; i++) {
      m << req.txnKeys2_[i] << req.deadlineRanks2_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, TigaDeadlineAgreeRequest& req) {
   uint32_t sz = 0;
   uint32_t sz2 = 0;
   m >> req.gViewId_ >> req.viewId_ >> req.shardId_ >> req.replicaId_ >> sz >>
       sz2;
   if (sz > 0) {
      req.txnKeys_.resize(sz, 0);
      req.deadlineRanks_.resize(sz, 0);
      for (uint32_t i = 0; i < sz; i++) {
         m >> req.txnKeys_[i] >> req.deadlineRanks_[i];
      }
   }
   if (sz2 > 0) {
      req.txnKeys2_.resize(sz2, 0);
      req.deadlineRanks2_.resize(sz2, 0);
      for (uint32_t i = 0; i < sz2; i++) {
         m >> req.txnKeys2_[i] >> req.deadlineRanks2_[i];
      }
   }
   return m;
}

Marshal& operator<<(Marshal& m, const TigaExecAgreeRequest& req) {
   m << req.gViewId_ << req.viewId_ << req.shardId_ << req.replicaId_;
   uint32_t sz = req.txnKeys_.size();
   m << sz;
   for (uint32_t i = 0; i < sz; i++) {
      m << req.txnKeys_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, TigaExecAgreeRequest& req) {
   uint32_t sz = 0;
   m >> req.gViewId_ >> req.viewId_ >> req.shardId_ >> req.replicaId_ >> sz;
   if (sz > 0) {
      req.txnKeys_.resize(sz, 0);
      for (uint32_t i = 0; i < sz; i++) {
         m >> req.txnKeys_[i];
      }
   }
   return m;
}

Marshal& operator<<(Marshal& m, const TigaInterReplicaSync& req) {
   m << req.gViewId_ << req.viewId_ << req.shardId_ << req.replicaId_
     << req.commitPoint_ << req.logIdStart_;
   uint32_t sz = req.txnKeys_.size();
   m << sz;
   for (uint32_t i = 0; i < sz; i++) {
      m << req.txnKeys_[i];
   }
   for (uint32_t i = 0; i < sz; i++) {
      m << req.deadlineRanks_[i];
   }
   for (uint32_t i = 0; i < sz; i++) {
      m << req.specLogIds_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, TigaInterReplicaSync& req) {
   m >> req.gViewId_ >> req.viewId_ >> req.shardId_ >> req.replicaId_ >>
       req.commitPoint_ >> req.logIdStart_;
   uint32_t sz = 0;
   m >> sz;
   req.txnKeys_.resize(sz, 0);
   for (uint32_t i = 0; i < sz; i++) {
      m >> req.txnKeys_[i];
   }
   req.deadlineRanks_.resize(sz, 0);
   for (uint32_t i = 0; i < sz; i++) {
      m >> req.deadlineRanks_[i];
   }
   req.specLogIds_.resize(sz, 0);
   for (uint32_t i = 0; i < sz; i++) {
      m >> req.specLogIds_[i];
   }
   return m;
}

Marshal& operator<<(Marshal& m, const MissRequestReq& req) {
   m << req.gViewId_ << req.viewId_ << req.shardId_ << req.replicaId_;
   uint32_t sz = req.txnKeys_.size();
   m << sz;
   for (uint32_t i = 0; i < sz; i++) {
      m << req.txnKeys_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, MissRequestReq& req) {
   m >> req.gViewId_ >> req.viewId_ >> req.shardId_ >> req.replicaId_;
   uint32_t sz = 0;
   m >> sz;
   req.txnKeys_.resize(sz, 0);
   for (uint32_t i = 0; i < sz; i++) {
      m >> req.txnKeys_[i];
   }
   return m;
}

Marshal& operator<<(Marshal& m, const MissRequestRep& req) {
   m << req.gViewId_ << req.viewId_ << req.shardId_ << req.replicaId_;
   uint32_t sz = req.cmds_.size();
   m << sz;
   for (uint32_t i = 0; i < sz; i++) {
      m << req.cmds_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, MissRequestRep& req) {
   m >> req.gViewId_ >> req.viewId_ >> req.shardId_ >> req.replicaId_;
   uint32_t sz = 0;
   m >> sz;
   req.cmds_.resize(sz);
   for (uint32_t i = 0; i < sz; i++) {
      m >> req.cmds_[i];
   }
   return m;
}

Marshal& operator<<(Marshal& m, const TigaCommitRequest& req) {
   m << req.gViewId_ << req.viewId_ << req.shardId_ << req.replicaId_
     << req.logId_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaCommitRequest& req) {
   m >> req.gViewId_ >> req.viewId_ >> req.shardId_ >> req.replicaId_ >>
       req.logId_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaCommitReply& rep) {
   m << rep.gViewId_ << rep.viewId_ << rep.shardId_ << rep.replicaId_
     << rep.logId_;
   uint32_t resultSize = rep.result_.size();
   m << resultSize;
   for (auto& kv : rep.result_) {
      m << kv.first << kv.second;
   }
   return m;
}

Marshal& operator>>(Marshal& m, TigaCommitReply& rep) {
   uint32_t resultSize = 0;
   m >> rep.gViewId_ >> rep.viewId_ >> rep.shardId_ >> rep.replicaId_ >>
       rep.logId_ >> resultSize;
   for (uint32_t i = 0; i < resultSize; i++) {
      int32_t k;
      Value v;
      m >> k >> v;
      rep.result_[k] = v;
   }
   return m;
}

Marshal& operator<<(Marshal& m, const TigaReconcliationReq& req) {
   m << req.gViewId_ << req.viewId_ << req.clientId_ << req.reqId_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaReconcliationReq& req) {
   m >> req.gViewId_ >> req.viewId_ >> req.clientId_ >> req.reqId_;
   return m;
}

// Failure Recovery related
Marshal& operator<<(Marshal& m, const TigaViewChangeReq& vcr) {
   uint32_t sz = vcr.gVec_.size();
   m << vcr.shardId_ << vcr.replicaId_ << vcr.gViewId_ << sz;
   for (uint32_t i = 0; i < sz; i++) {
      m << vcr.gVec_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, TigaViewChangeReq& vcr) {
   uint32_t sz = 0;
   m >> vcr.shardId_ >> vcr.replicaId_ >> vcr.gViewId_ >> sz;
   vcr.gVec_.resize(sz);
   for (uint32_t i = 0; i < sz; i++) {
      m >> vcr.gVec_[i];
   }
   return m;
}

Marshal& operator<<(Marshal& m, const TigaViewChange& vr) {
   m << vr.viewId_ << vr.gViewId_ << vr.shardId_ << vr.replicaId_
     << vr.lastNormalViewId_ << vr.commitPoint_ << vr.syncPoint_ << vr.status_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaViewChange& vr) {
   m >> vr.viewId_ >> vr.gViewId_ >> vr.shardId_ >> vr.replicaId_ >>
       vr.lastNormalViewId_ >> vr.commitPoint_ >> vr.syncPoint_ >> vr.status_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaHeartBeat& hb) {
   m << hb.shardId_ << hb.replicaId_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaHeartBeat& hb) {
   m >> hb.shardId_ >> hb.replicaId_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaHeartBeatAck& hba) {
   m << hba.shardId_ << hba.replicaId_ << hba.gViewId_ << hba.viewId_
     << hba.status_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaHeartBeatAck& hba) {
   m >> hba.shardId_ >> hba.replicaId_ >> hba.gViewId_ >> hba.viewId_ >>
       hba.status_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaStateTransferRequest& sr) {
   m << sr.shardId_ << sr.replicaId_ << sr.viewId_ << sr.nonce_
     << sr.syncedLogBegin_ << sr.syncedLogEnd_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaStateTransferRequest& sr) {
   m >> sr.shardId_ >> sr.replicaId_ >> sr.viewId_ >> sr.nonce_ >>
       sr.syncedLogBegin_ >> sr.syncedLogEnd_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaStateTransferReply& srp) {
   uint32_t syncedSize = srp.syncedTxns_.size();
   uint32_t unsyncedSize = srp.unsyncedTxns_.size();
   m << srp.shardId_ << srp.replicaId_ << srp.viewId_ << srp.nonce_
     << syncedSize << unsyncedSize;
   for (uint32_t i = 0; i < syncedSize; i++) {
      m << srp.syncedTxnDeadlines_[i];
   }
   for (uint32_t i = 0; i < syncedSize; i++) {
      m << srp.syncedTxns_[i];
   }
   for (uint32_t j = 0; j < unsyncedSize; j++) {
      m << srp.unsyncedTxnDeadlines_[j];
   }
   for (uint32_t j = 0; j < unsyncedSize; j++) {
      m << srp.unsyncedTxns_[j];
   }
   return m;
}

Marshal& operator>>(Marshal& m, TigaStateTransferReply& srp) {
   uint32_t syncedSize = 0;
   uint32_t unsyncedSize = 0;
   m >> srp.shardId_ >> srp.replicaId_ >> srp.viewId_ >> srp.nonce_ >>
       syncedSize >> unsyncedSize;
   srp.syncedTxnDeadlines_.resize(syncedSize);
   srp.syncedTxns_.resize(syncedSize);
   srp.unsyncedTxnDeadlines_.resize(unsyncedSize);
   srp.unsyncedTxns_.resize(unsyncedSize);

   for (uint32_t i = 0; i < syncedSize; i++) {
      m >> srp.syncedTxnDeadlines_[i];
   }
   for (uint32_t i = 0; i < syncedSize; i++) {
      m >> srp.syncedTxns_[i];
   }
   for (uint32_t j = 0; j < unsyncedSize; j++) {
      m >> srp.unsyncedTxnDeadlines_[j];
   }
   for (uint32_t j = 0; j < unsyncedSize; j++) {
      m >> srp.unsyncedTxns_[j];
   }
   return m;
}

Marshal& operator<<(Marshal& m, const TigaCrossShardVerifyReq& msg) {
   m << msg.shardId_ << msg.replicaId_ << msg.gViewId_ << msg.viewId_
     << msg.syncedDeadline_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaCrossShardVerifyReq& msg) {
   m >> msg.shardId_ >> msg.replicaId_ >> msg.gViewId_ >> msg.viewId_ >>
       msg.syncedDeadline_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaCrossShardVerifyRep& msg) {
   uint32_t sz = msg.deadlines_.size();
   m << msg.shardId_ << msg.replicaId_ << msg.gViewId_ << msg.viewId_ << sz;
   for (uint32_t i = 0; i < sz; i++) {
      m << msg.txnKeys_[i];
   }
   for (uint32_t i = 0; i < sz; i++) {
      m << msg.deadlines_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, TigaCrossShardVerifyRep& msg) {
   uint32_t sz = 0;
   m >> msg.shardId_ >> msg.replicaId_ >> msg.gViewId_ >> msg.viewId_ >> sz;
   msg.txnKeys_.resize(sz);
   msg.deadlines_.resize(sz);
   for (uint32_t i = 0; i < sz; i++) {
      m >> msg.txnKeys_[i];
   }
   for (uint32_t i = 0; i < sz; i++) {
      m >> msg.deadlines_[i];
   }
   return m;
}

Marshal& operator<<(Marshal& m, const TigaStartView& msg) {
   uint32_t sz = msg.deadlines_.size();
   m << msg.shardId_ << msg.replicaId_ << msg.gViewId_ << msg.viewId_
     << msg.logBegin_ << sz;

   for (uint32_t i = 0; i < sz; i++) {
      m << msg.deadlines_[i];
   }
   for (uint32_t i = 0; i < sz; i++) {
      m << msg.txns_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, TigaStartView& msg) {
   uint32_t sz = 0;
   m >> msg.shardId_ >> msg.replicaId_ >> msg.gViewId_ >> msg.viewId_ >>
       msg.logBegin_ >> sz;

   msg.deadlines_.resize(sz);
   msg.txns_.resize(sz);
   for (uint32_t i = 0; i < sz; i++) {
      m >> msg.deadlines_[i];
   }
   for (uint32_t i = 0; i < sz; i++) {
      m >> msg.txns_[i];
   }
   return m;
}

Marshal& operator<<(Marshal& m, const TigaSyncStatus& msg) {
   m << msg.shardId_ << msg.replicaId_ << msg.gViewId_ << msg.viewId_
     << msg.syncPoint_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaSyncStatus& msg) {
   m >> msg.shardId_ >> msg.replicaId_ >> msg.gViewId_ >> msg.viewId_ >>
       msg.syncPoint_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaCMPrepare& msg) {
   uint32_t sz = msg.gVec_.size();
   m << msg.shardId_ << msg.replicaId_ << msg.cViewId_ << msg.gViewId_ << sz;
   for (uint32_t i = 0; i < sz; i++) {
      m << msg.gVec_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, TigaCMPrepare& msg) {
   uint32_t sz = 0;
   m >> msg.shardId_ >> msg.replicaId_ >> msg.cViewId_ >> msg.gViewId_ >> sz;
   msg.gVec_.resize(sz);
   for (uint32_t i = 0; i < sz; i++) {
      m >> msg.gVec_[i];
   }
   return m;
}

Marshal& operator<<(Marshal& m, const TigaCMPrepareReply& msg) {
   m << msg.shardId_ << msg.replicaId_ << msg.cViewId_ << msg.gViewId_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaCMPrepareReply& msg) {
   m >> msg.shardId_ >> msg.replicaId_ >> msg.cViewId_ >> msg.gViewId_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaCMCommit& msg) {
   uint32_t sz = msg.gVec_.size();
   m << msg.shardId_ << msg.replicaId_ << msg.cViewId_ << msg.gViewId_ << sz;

   for (uint32_t i = 0; i < sz; i++) {
      m << msg.gVec_[i];
   }
   return m;
}

Marshal& operator>>(Marshal& m, TigaCMCommit& msg) {
   uint32_t sz = 0;
   m >> msg.shardId_ >> msg.replicaId_ >> msg.cViewId_ >> msg.gViewId_ >> sz;
   msg.gVec_.resize(sz);
   for (uint32_t i = 0; i < sz; i++) {
      m >> msg.gVec_[i];
   }
   return m;
}

Marshal& operator<<(Marshal& m, const TigaFailSignal& msg) {
   m << msg.failedGView_ << msg.failedShardId_ << msg.failedReplicaId_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaFailSignal& msg) {
   m >> msg.failedGView_ >> msg.failedShardId_ >> msg.failedReplicaId_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaFailAck& msg) {
   m << msg.shardId_ << msg.replicaId_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaFailAck& msg) {
   m >> msg.shardId_ >> msg.replicaId_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaGuard& msg) {
   m << msg.gViewId_ << msg.viewId_ << msg.shardId_ << msg.replicaId_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaGuard& msg) {
   m >> msg.gViewId_ >> msg.viewId_ >> msg.shardId_ >> msg.replicaId_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaGuardAck& msg) {
   m << msg.gViewId_ << msg.viewId_ << msg.shardId_ << msg.replicaId_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaGuardAck& msg) {
   m >> msg.gViewId_ >> msg.viewId_ >> msg.shardId_ >> msg.replicaId_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaPromise& msg) {
   m << msg.gViewId_ << msg.viewId_ << msg.shardId_ << msg.replicaId_
     << msg.promiseId_ << msg.leaseUs_ << msg.lastPromiseAckId_
     << msg.validDuration_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaPromise& msg) {
   m >> msg.gViewId_ >> msg.viewId_ >> msg.shardId_ >> msg.replicaId_ >>
       msg.promiseId_ >> msg.leaseUs_ >> msg.lastPromiseAckId_ >>
       msg.validDuration_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaPromiseAck& msg) {
   m << msg.gViewId_ << msg.viewId_ << msg.shardId_ << msg.replicaId_
     << msg.promiseId_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaPromiseAck& msg) {
   m >> msg.gViewId_ >> msg.viewId_ >> msg.shardId_ >> msg.replicaId_ >>
       msg.promiseId_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaPromiseRevoke& msg) {
   m << msg.gViewId_ << msg.viewId_ << msg.shardId_ << msg.replicaId_
     << msg.promiseId_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaPromiseRevoke& msg) {
   m >> msg.gViewId_ >> msg.viewId_ >> msg.shardId_ >> msg.replicaId_ >>
       msg.promiseId_;
   return m;
}

Marshal& operator<<(Marshal& m, const TigaPromiseRevokeAck& msg) {
   m << msg.gViewId_ << msg.viewId_ << msg.shardId_ << msg.replicaId_
     << msg.promiseId_;
   return m;
}

Marshal& operator>>(Marshal& m, TigaPromiseRevokeAck& msg) {
   m >> msg.gViewId_ >> msg.viewId_ >> msg.shardId_ >> msg.replicaId_ >>
       msg.promiseId_;
   return m;
}