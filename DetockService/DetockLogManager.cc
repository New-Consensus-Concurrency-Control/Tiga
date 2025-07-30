#include "DetockLogManager.h"

DetockLogManager::DetockLogManager(const std::string& serverName,
                                   StateMachine* sm)
    : serverName_(serverName), sm_(sm) {
   detockLogList_.reserve(1000 * 1000 * 1000ul);
   detockLogList_.push_back(NULL);  // index-0 is dummy
   preparedLogId_ = 0;
   committedLogId_ = 0;
   pendingCommitPoint_ = 0;
   viewId_ = 0;  // ViewChange not implemented for Detock

   logMangerRPCPoll_ = new PollMgr(2);
   YAML::Node config = sm_->Config();
   // Get my IDs <shardId, replicaId>
   for (uint32_t sid = 0; sid < sm_->ShardNum(); sid++) {
      for (uint32_t rid = 0; rid < sm_->ReplicaNum(); rid++) {
         std::string fullName =
             config["site"]["server"][sid][rid].as<std::string>();
         std::string thisServerName = fullName.substr(0, fullName.find(':'));
         std::string portName = fullName.substr(fullName.find(':') + 1);
         std::string ip = config["host"][thisServerName].as<std::string>();
         int port = std::stoi(portName);
         logManagerAddrs_[sid][rid] = ip + ":" + std::to_string(port + 2);
      }
   }
}

void DetockLogManager::ConnectOtherLogManager() {
   // only connects to the replicas belonging to the same shard
   uint32_t sid = sm_->ShardId();
   LOG(INFO) << "ConnectToLogManager sid= " << sid;
   for (uint32_t rid = 0; rid < sm_->ReplicaNum(); rid++) {
      int ret = -1;
      logManagerRPCClients_[rid] = new rrr::Client(logMangerRPCPoll_);
      do {
         LOG(INFO) << "Log Manager Connecting to sid=" << sid
                   << "\t rid=" << rid << "\t"
                   << "ip=" << logManagerAddrs_[sid][rid];
         ret = logManagerRPCClients_[rid]->connect(
             logManagerAddrs_[sid][rid].c_str());
         if (ret == 0) {
            // success
            logManagerProxies_[rid] =
                new DetockLogManagerProxy(logManagerRPCClients_[rid]);
            LOG(INFO) << "Log Manager Connected to sid=" << sid
                      << "\t rid=" << rid << "\t" << logManagerAddrs_[sid][rid];
         } else {
            ThreadSleepFor(1200000);
         }
      } while (ret != 0);
   }
}

void DetockLogManager::CloseConnection() {
   LOG(INFO) << "Close Connections";
   for (uint32_t i = 0; i < sm_->ReplicaNum(); i++) {
      if (logManagerRPCClients_[i]) {
         logManagerRPCClients_[i]->close_and_release();
         delete logManagerProxies_[i];
      }
   }
   LOG(INFO) << "logManager closed";
}

void DetockLogManager::onDetockPaxosAppend(const DetockPaxosAppend& req) {
   if (req.viewId_ != viewId_) {
      LOG(ERROR) << "View MisMatch, View Change not implemented for Detock";
      return;
   }
   std::lock_guard<std::mutex> lk(logMtx_);
   if (preparedLogId_ + 1 == req.batch_.batchId_) {
      DetockBatch* batch = new DetockBatch(req.batch_);
      detockLogList_.push_back(batch);
      preparedLogId_++;
      assert(preparedLogId_ + 1 == detockLogList_.size());
      committedLogId_ = std::min(preparedLogId_, pendingCommitPoint_);
      // Reply
      DetockPaxosAppendReply rep;
      rep.viewId_ = viewId_;
      rep.replicaId_ = sm_->ReplicaId();
      rep.shardId_ = sm_->ShardId();
      rep.batchId_ = req.batch_.batchId_;
      // LOG(INFO) << "rep=" << rep.replicaId_ << ":" << rep.shardId_
      //           << "--batchId=" << rep.batchId_
      //           << "--rid=" << LeaderReplicaId();
      logManagerProxies_[LeaderReplicaId()]->async_ReplicateBatchReply(rep);

   } else if (preparedLogId_ + 1 < req.batch_.batchId_) {
      DetockBatch* batch = new DetockBatch(req.batch_);
      pendingBatches_[batch->batchId_] = batch;
   }
   ProcessPendingBatches();
}

void DetockLogManager::ProcessPendingBatches() {
   while (!pendingBatches_.empty()) {
      if (preparedLogId_ + 1 == pendingBatches_.begin()->first) {
         detockLogList_.push_back(pendingBatches_.begin()->second);
         preparedLogId_++;
         committedLogId_ = std::min(preparedLogId_, pendingCommitPoint_);
         pendingBatches_.erase(pendingBatches_.begin());
         // Reply
         DetockPaxosAppendReply rep;
         rep.viewId_ = viewId_;
         rep.replicaId_ = sm_->ReplicaId();
         rep.shardId_ = sm_->ShardId();
         rep.batchId_ = preparedLogId_;
         logManagerProxies_[LeaderReplicaId()]->async_ReplicateBatchReply(rep);
      } else if (preparedLogId_ >= pendingBatches_.begin()->first) {
         delete pendingBatches_.begin()->second;
         pendingBatches_.erase(pendingBatches_.begin());
      } else {
         break;
      }
   }
}

void DetockLogManager::onDetockPaxosAppendReply(
    const DetockPaxosAppendReply& rep) {
   if (rep.viewId_ != viewId_) {
      LOG(ERROR) << "View MisMatch, View Change not implemented for Detock";
      return;
   }
   // LOG(INFO) << "rep batchId=" << rep.batchId_ << "\t from replica "
   //           << rep.replicaId_;
   assert(sm_->ReplicaId() == LeaderReplicaId());  // I am leader
   std::lock_guard<std::mutex> lk(logMtx_);
   if (committedLogId_ >= rep.batchId_) {
      return;
   }
   quorumInfo_[rep.batchId_].insert(rep.replicaId_);
   if (quorumInfo_[rep.batchId_].size() >= sm_->ReplicaNum() / 2 + 1) {
      committedLogId_ = std::max(committedLogId_, rep.batchId_);
      pendingCommitPoint_ = committedLogId_;
      // LOG(INFO) << "committedLogId=" << committedLogId_;
      quorumInfo_.erase(rep.batchId_);
      for (uint32_t rid = 0; rid < sm_->ReplicaNum(); rid++) {
         if (rid == sm_->ReplicaId()) {
            continue;
         }
         DetockPaxosCommit cmt;
         cmt.viewId_ = viewId_;
         cmt.replicaId_ = sm_->ReplicaId();
         cmt.shardId_ = sm_->ShardId();
         cmt.batchId_ = committedLogId_;
         logManagerProxies_[rid]->async_CommitBatch(cmt);
      }
   }
}

void DetockLogManager::onDetockPaxosCommit(const DetockPaxosCommit& req) {
   if (req.viewId_ != viewId_) {
      LOG(ERROR) << "View MisMatch, View Change not implemented for Detock";
      return;
   }
   std::lock_guard<std::mutex> lk(logMtx_);
   if (req.batchId_ <= pendingCommitPoint_) {
      return;
   }
   pendingCommitPoint_ = req.batchId_;
   committedLogId_ = std::min(preparedLogId_, pendingCommitPoint_);
}

void DetockLogManager::AppendNewBatch(DetockBatch* batch) {
   // Only leader will call this API
   std::lock_guard<std::mutex> lk(logMtx_);
   detockLogList_.push_back(batch);
   preparedLogId_++;
   assert(preparedLogId_ + 1 == detockLogList_.size());
   for (uint32_t rid = 0; rid < sm_->ReplicaNum(); rid++) {
      DetockPaxosAppend req;
      req.viewId_ = viewId_;
      req.shardId_ = sm_->ShardId();
      req.replicaId_ = sm_->ReplicaId();
      req.batch_ = *batch;
      // LOG(INFO) << "Append " << req.batch_.batchId_ << " to replica " << rid;
      logManagerProxies_[rid]->async_ReplicateBatch(req);
   }
}

DetockBatch* DetockLogManager::GetBatch(uint32_t logId) {
   std::lock_guard<std::mutex> lk(logMtx_);
   if (preparedLogId_ < logId) {
      LOG(WARNING) << "preparedLogId < logId " << preparedLogId_ << "<"
                   << logId;
      return NULL;
   }
   return detockLogList_[logId];
}

uint32_t DetockLogManager::GetCommitedLogId() { return committedLogId_; }

uint32_t DetockLogManager::LeaderReplicaId() {
   return ((viewId_ % sm_->ReplicaNum()) + sm_->ShardId()) % sm_->ReplicaNum();
}

DetockLogManager::~DetockLogManager() {}