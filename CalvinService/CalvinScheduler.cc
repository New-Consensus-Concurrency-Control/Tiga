#include "CalvinScheduler.h"

CalvinScheduler::CalvinScheduler(const std::string& serverName,
                                 StateMachine* sm)
    : sm_(sm) {
   nextEpochKey_ = {1, 0};
   for (uint32_t s = 0; s < sm->ShardNum(); s++) {
      maxSeqNoByShard_[s] = 0;
   }
}

void CalvinScheduler::onEpochRequest(const EpochRequest& req, EpochReply* rep,
                                     const std::function<void()>& cb) {
   std::lock_guard<std::mutex> lk(executionMtx_);
   // if (req.sequenceNo_ % 100 == 1) {
   //    LOG(INFO) << "seqNo=" << req.sequenceNo_ << ": shardId=" <<
   //    req.shardId_
   //              << " next is: " << nextEpochKey_.second << ":"
   //              << nextEpochKey_.first;
   // }
   // if (req.cmdVec_.size() > 0) {
   //    LOG(INFO) << "seqNo=" << req.sequenceNo_ << "--shardId=" <<
   //    req.shardId_;
   // }
   maxSeqNoByShard_[req.shardId_] =
       std::max(maxSeqNoByShard_[req.shardId_], req.sequenceNo_);
   if (nextEpochKey_.first == req.sequenceNo_ &&
       nextEpochKey_.second == req.shardId_) {
      // if (req.cmdVec_.size() > 0) {
      //    LOG(INFO) << "Process seqNo=" << req.sequenceNo_
      //              << "--shardId=" << req.shardId_;
      // }
      ProcessEpoch(req, rep, cb);
   } else {
      // if (req.cmdVec_.size() > 0) {
      //    LOG(INFO) << "Pending... Need " << nextEpochKey_.first << ":"
      //              << nextEpochKey_.second;
      // }
      // Put it to pending set
      EpochEntry* entry = new EpochEntry(req, rep, cb);
      // if (req.cmdVec_.size() > 0) {
      //    LOG(INFO) << "Entry " << entry->req_.cmdVec_.size();
      // }
      pendingEpochEntries_[{req.sequenceNo_, req.shardId_}] = entry;
   }
   ProcessPendingEpochEntries();
}

void CalvinScheduler::ProcessPendingEpochEntries() {
   while (!pendingEpochEntries_.empty()) {
      if (pendingEpochEntries_.begin()->first == nextEpochKey_) {
         EpochEntry* entry = pendingEpochEntries_.begin()->second;
         ProcessEpoch(entry->req_, entry->rep_, entry->cb_);
         delete entry;
         pendingEpochEntries_.erase(pendingEpochEntries_.begin());
      } else {
         return;
      }
   }
}

void CalvinScheduler::ProcessEpoch(const EpochRequest& req, EpochReply* rep,
                                   const std::function<void()>& cb) {
   rep->replicaId_ = sm_->ReplicaId();
   rep->shardId_ = sm_->ShardId();
   rep->sequenceNo_ = req.sequenceNo_;
   rep->txnKeys_.resize(req.cmdVec_.size());
   rep->resultVec_.resize(req.cmdVec_.size());
   // if (req.cmdVec_.size() > 0) {
   //    LOG(INFO) << "TO Execute " << rep->sequenceNo_;
   // }
   // if (rep->sequenceNo_ % 100 == 1) {
   //    LOG(INFO) << "TO Process " << rep->sequenceNo_
   //              << " next is: " << nextEpochKey_.second << ":"
   //              << nextEpochKey_.first;
   // }
   for (uint32_t i = 0; i < req.cmdVec_.size(); i++) {
      std::map<int32_t, mdb::Value> ws = req.cmdVec_[i].ws_;
      std::map<uint32_t, std::set<int32_t>> shardKeyMap;
      std::vector<int32_t> localKeys;
      rep->txnKeys_[i] = req.cmdVec_[i].TxnKey();
      sm_->InitializeRelatedShards(req.cmdVec_[i].txnType_, &(ws),
                                   &shardKeyMap);
      // for (auto sv : shardKeyMap) {
      //    LOG(INFO) << "sid=" << sv.first << "--" << sv.second.size();
      // }
      for (auto& key : shardKeyMap[rep->shardId_]) {
         localKeys.push_back(key);
      }
      if (localKeys.size() > 0) {
         sm_->Execute(req.cmdVec_[i].txnType_, &localKeys, &(ws),
                      &(rep->resultVec_[i]));
      }
   }

   // Advance nextEpochKey
   if (nextEpochKey_.second == sm_->ShardNum() - 1) {
      nextEpochKey_ = {nextEpochKey_.first + 1, 0};
   } else {
      nextEpochKey_.second++;
   }

   cb();
}

void CalvinScheduler::onDispatchRequest(const CalvinDispatchRequest req,
                                        CalvinDispatchReply* rep,
                                        const std::function<void()>& cb) {
   sm_->PreRead(req.txnType_, &(req.input_), &(rep->result_));
   rep->shardId_ = sm_->ShardId();
   cb();
}

CalvinScheduler::~CalvinScheduler() {}