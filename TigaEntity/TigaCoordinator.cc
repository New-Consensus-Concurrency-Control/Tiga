#include "TigaCoordinator.h"

TigaCoordinator::TigaCoordinator(const uint32_t coordinatorId,
                                 const YAML::Node& config)
    : config_(config) {
   shardNum_ = config["site"]["server"].size();
   replicaNum_ = config["site"]["server"][0].size();
}

void TigaCoordinator::SetGlobalInfo(GlobalInfo* info) { gInfo_ = info; }

void TigaCoordinator::DoOne(const ClientRequest& creq, TxnGenerator* txnGen) {
   Reset();
   std::lock_guard<std::recursive_mutex> lock(this->mtx_);
   txnGen_ = txnGen;
   sendTime_ = GetMicrosecondTimestamp();
   // Store on coordinator side and use coordiantor's id and requestId
   clientId_ = creq.cmd_.clientId_;
   requestIdByClient_ = creq.cmd_.reqId_;
   reqInProcess_.cmd_ = creq.cmd_;
   reqInProcess_.cmd_.clientId_ = gInfo_->coordinatorId_;
   reqInProcess_.cmd_.reqId_ = gInfo_->nextRequestIdByProxy_.fetch_add(1);
   targetShards_ = creq.targetShards_;
   callback_ = creq.callback_;
   // if (creq.cmd_.reqId_ >= 100) {
   //    LOG(INFO) << "Sleep";
   //    sleep(10000);
   //    exit(0);
   // }

   // LOG(INFO) << "Submitted " << reqInProcess_.cmd_.reqId_;
   if (txnGen_->NeedDisPatch(creq)) {
      stage_ = STAGE::Dispatching;
      // LOG(INFO) << "Dispatch " << reqInProcess_.cmd_.reqId_;
      Dispatch();
   } else {
      stage_ = STAGE::Commiting;
      Launch();
   }
}

void TigaCoordinator::Dispatch() {
   uint32_t currentPhase = phase_;
   TigaDispatchRequest dispatchReq;
   dispatchReq.txnType_ = reqInProcess_.cmd_.txnType_;
   txnGen_->GetInquireKeys(dispatchReq.txnType_, &(reqInProcess_.cmd_.ws_),
                           &(dispatchReq.input_));
   // LOG(INFO) << "DisPatching " << reqInProcess_.cmd_.reqId_;
   for (auto& sid : targetShards_) {
      uint32_t leaderRid = gInfo_->currentViews_[sid][0] % replicaNum_;
      rrr::FutureAttr fuattr;
      std::function<void(Future*)> cb =
          [this, currentPhase /**pass by value */](Future* fu) {
             TigaDispatchReply rep;
             fu->get_reply() >> rep;
             this->OnDispatchReply(currentPhase, rep);
          };
      fuattr.callback = cb;
      Future::safe_release(gInfo_->comm_->ProxyAt(sid, leaderRid)
                               ->async_DispatchRequest(dispatchReq, fuattr));
   }
}

void TigaCoordinator::Launch() {
   // LOG(INFO) << "Launch " << reqInProcess_.cmd_.reqId_;
   reqInProcess_.bound_ = 0;
   for (auto& sid : targetShards_) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         uint32_t estimatedOwd = gInfo_->estimatedOWDs_[sid][rid];
         if (reqInProcess_.bound_ < estimatedOwd) {
            reqInProcess_.bound_ = estimatedOwd;
         }
         // for debug
         // reqInProcess_.bound_ = 5000;
      }
   }
   if (false && reqInProcess_.cmd_.reqId_ >= 10000) {
      LOG(INFO) << "reqId=" << reqInProcess_.cmd_.reqId_
                << "\t bound=" << reqInProcess_.bound_;
      for (uint32_t sid = 0; sid < shardNum_; sid++) {
         for (uint32_t rid = 0; rid < replicaNum_; rid++) {
            LOG(INFO) << "sid=" << sid << "\t" << "rid=" << rid
                      << "\tOWD=" << gInfo_->estimatedOWDs_[sid][rid];
         }
      }
   }
   // LOG(INFO) << "bound=" << reqInProcess_.bound_;

   uint32_t currentPhase = phase_;
   std::unique_lock lck(gInfo_->seqMtx_);

   reqInProcess_.sendTime_ = GetMicrosecondTimestamp();
   if (reqInProcess_.bound_ < 0) {
      LOG(INFO) << "Prior " << reqInProcess_.cmd_.clientId_ << ":"
                << reqInProcess_.cmd_.reqId_
                << "\t bound=" << reqInProcess_.bound_
                << "\t owdDeltaUs=" << gInfo_->owdDeltaUs_;
   }
   reqInProcess_.bound_ += gInfo_->owdDeltaUs_;
   if (reqInProcess_.bound_ < 0) {
      reqInProcess_.bound_ = 0;
   }

   if (config_["clock_approach"].IsDefined() &&
       config_["clock_approach"].as<std::string>() == "logical") {
      reqInProcess_.sendTime_ = gInfo_->serverClock_.fetch_add(1);
      reqInProcess_.bound_ = 0;  // bound no longer indicates OWD
   }
   // if (reqInProcess_.cmd_.reqId_ % 1000 == 1) {
   //    LOG(INFO) << "reqId=" << reqInProcess_.cmd_.reqId_
   //              << "--bound=" << reqInProcess_.bound_;
   // }

   // std::string targetIds = "";
   // for (auto& sid : targetShards_) {
   //    targetIds += std::to_string(sid) + "\t";
   // }
   // LOG(INFO) << "reqId=" << reqInProcess_.cmd_.reqId_ << "\t"
   //           << "TargetShards=" << targetIds
   //           << "\t bound=" << reqInProcess_.bound_;
   // std::string keyStr = "";
   // for (auto& kv : reqInProcess_.cmd_.ws_) {
   //    keyStr += std::to_string(kv.first) + "\t";
   // }
   // LOG(INFO) << "InProcess "
   //           << "clientId=" << reqInProcess_.cmd_.clientId_ << "\t"
   //           << "reqId=" << reqInProcess_.cmd_.reqId_ << "\t"
   //           << "txnType=" << reqInProcess_.cmd_.txnType_ << "\t"
   //           << "TargetShards=" << targetIds << "\t"
   //           << "keyStr=" << keyStr << "\t"
   //           << "ddl=" << (reqInProcess_.bound_ + reqInProcess_.sendTime_);

   uint64_t safeCommitWaterMark = UINT64_MAX;
   // Only pick one replica in case of read-only optimization
   uint32_t targetReplicaId = reqInProcess_.cmd_.reqId_ % replicaNum_;
   if (gInfo_->closestReplicaId_ >= 0) {
      targetReplicaId = gInfo_->closestReplicaId_;
   }

   for (auto& sid : targetShards_) {
      if (false && reqInProcess_.cmd_.reqId_ >= 40000 &&
          reqInProcess_.cmd_.reqId_ <= 50000) {
         LOG(INFO) << "sid=" << sid << "--replicaId=" << targetReplicaId
                   << "-- wmk="
                   << gInfo_->committedWaterMarks_[sid][targetReplicaId];
      }
      safeCommitWaterMark =
          std::min(safeCommitWaterMark,
                   gInfo_->committedWaterMarks_[sid][targetReplicaId]);
   }

   for (auto& sid : targetShards_) {
      if (gInfo_->enableReadOnlyOptimization_ &&
          reqInProcess_.cmd_.isReadOnly_) {

         // choose a smaller timestamp (-500ms)
         if (safeCommitWaterMark == 0) {
            reqInProcess_.bound_ = -500000;
         } else {
            if (reqInProcess_.sendTime_ > safeCommitWaterMark) {
               int32_t diff = reqInProcess_.sendTime_ - safeCommitWaterMark;
               reqInProcess_.bound_ = 0 - diff;
            } else {
               reqInProcess_.bound_ = 0;
            }
         }
         if (reqInProcess_.cmd_.reqId_ >= 40000 &&
             reqInProcess_.cmd_.reqId_ <= 50000) {
            LOG(INFO) << "reqId=" << reqInProcess_.cmd_.reqId_ << "\t"
                      << "safeCommitWaterMark=" << safeCommitWaterMark << "\t"
                      << "sendTime=" << reqInProcess_.sendTime_ << "\t"
                      << "bound=" << reqInProcess_.bound_;
         }

         if (gInfo_->serverStatus_[sid][targetReplicaId] == STATUS_FAILING) {
            continue;
         }
         rrr::FutureAttr fuattr;
         std::function<void(Future*)> cb =
             [this, currentPhase /**pass by value */](Future* fu) {
                TigaReply rep;
                fu->get_reply() >> rep;
                this->OnFastReply(currentPhase, rep);
             };
         fuattr.callback = cb;
         Future::safe_release(gInfo_->comm_->ProxyAt(sid, targetReplicaId)
                                  ->async_NormalRequest(reqInProcess_, fuattr));

      } else {
         for (uint32_t rid = 0; rid < replicaNum_; rid++) {
            if (gInfo_->serverStatus_[sid][rid] == STATUS_FAILING) {
               continue;
            }
            // if (sid == 1 && rid == 0) {
            //    LOG(INFO) << "currentGView=" <<
            //    gInfo_->currentGlobalViews_[1][0]
            //              << "Status " << gInfo_->serverStatus_[sid][rid];
            // }
            rrr::FutureAttr fuattr;
            std::function<void(Future*)> cb =
                [this, currentPhase /**pass by value */](Future* fu) {
                   TigaReply rep;
                   fu->get_reply() >> rep;
                   this->OnFastReply(currentPhase, rep);
                };
            fuattr.callback = cb;

            // LOG(INFO) << "Send req " << reqInProcess_.cmd_.clientId_ << ":"
            //           << reqInProcess_.cmd_.reqId_ << "-- from " << sid <<
            //           ":"
            //           << rid;
            Future::safe_release(
                gInfo_->comm_->ProxyAt(sid, rid)->async_NormalRequest(
                    reqInProcess_, fuattr));
         }
      }
   }
}

void TigaCoordinator::RetryRead() {
   std::lock_guard<std::recursive_mutex> guard(mtx_);
   phase_++;
   uint32_t currentPhase = phase_;
   for (auto& sid : targetShards_) {
      if (gInfo_->enableReadOnlyOptimization_ &&
          reqInProcess_.cmd_.isReadOnly_) {
         // Only pick one replica
         uint32_t rid = reqInProcess_.cmd_.reqId_ % replicaNum_;
         // choose a smaller timestamp (-500ms)
         reqInProcess_.bound_ = -500000;
         if (gInfo_->serverStatus_[sid][rid] == STATUS_FAILING) {
            continue;
         }
         rrr::FutureAttr fuattr;
         std::function<void(Future*)> cb =
             [this, currentPhase /**pass by value */](Future* fu) {
                TigaReply rep;
                fu->get_reply() >> rep;
                this->OnFastReply(currentPhase, rep);
             };
         fuattr.callback = cb;
         Future::safe_release(
             gInfo_->comm_->ProxyAt(sid, rid)->async_NormalRequest(
                 reqInProcess_, fuattr));

      } else {
         for (uint32_t rid = 0; rid < replicaNum_; rid++) {
            if (gInfo_->serverStatus_[sid][rid] == STATUS_FAILING) {
               continue;
            }
            rrr::FutureAttr fuattr;
            std::function<void(Future*)> cb =
                [this, currentPhase /**pass by value */](Future* fu) {
                   TigaReply rep;
                   fu->get_reply() >> rep;
                   this->OnFastReply(currentPhase, rep);
                };
            fuattr.callback = cb;
            Future::safe_release(
                gInfo_->comm_->ProxyAt(sid, rid)->async_NormalRequest(
                    reqInProcess_, fuattr));
         }
      }
   }
}

void TigaCoordinator::Abort() {
   // not implemented yet
   assert(0);
}

void TigaCoordinator::OnFastReply(const uint32_t phase, const TigaReply& rep) {
   std::lock_guard<std::recursive_mutex> guard(mtx_);
   gInfo_->fastReplyNum1_++;

   if (false && rep.reqId_ <= 100000 && rep.reqId_ >= 50000) {
      // LOG(INFO) << "rep=(" << rep.clientId_ << ":" << rep.reqId_ << ")"
      //           << "ID=" << rep.shardId_ << ":" << rep.replicaId_
      //           << "---owd=" << rep.owd_ << "--logId=" << rep.logId_
      //           << "--specLogId=" << rep.specLogId_
      //           << "--syncedLogId=" << rep.latestSyncedLogId_
      //           << "--syncedSpecLogId=" << rep.latestSyncedSpecLogId_;
      LOG(INFO) << "rep=(" << rep.clientId_ << ":" << rep.reqId_ << ")---"
                << rep.hash_.ToString() << "--deadline=" << rep.deadline_
                << "---shardId=" << rep.shardId_
                << "--replicaId=" << rep.replicaId_ << "--logId=" << rep.logId_
                << "--syncedLogId=" << rep.latestSyncedLogId_
                << "--syncedSpecLogId=" << rep.latestSyncedSpecLogId_;
      std::string s = "";
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         s += std::to_string(rid) + "\t" +
              std::to_string(gInfo_->currentSyncedLogIds_[rep.shardId_][rid]) +
              "\n";
      }
      LOG(INFO) << s;
   }

   gInfo_->replyNumPerNode_[rep.shardId_][rep.replicaId_]++;
   // if (gInfo_->fastReplyNum1_ % 10000 == 1) {
   //    for (uint32_t sid = 0; sid < shardNum_; sid++) {
   //       for (uint32_t rid = 0; rid < replicaNum_; rid++) {
   //          LOG(INFO) << "(" << sid << "," << rid << "):\t"
   //                    << gInfo_->replyNumPerNode_[sid][rid];
   //       }
   //    }
   // }
   if (rep.owd_ > 0) {
      // if (rep.replicaId_ == 2) {
      //    LOG(INFO) << "owd " << rep.shardId_ << ":" << rep.replicaId_ << "--"
      //              << rep.owd_;
      // }
      gInfo_->owdQus_[rep.shardId_][rep.replicaId_].enqueue(rep.owd_);
   }

   if (gInfo_->enableReadOnlyOptimization_) {
      if (gInfo_->committedWaterMarks_[rep.shardId_][rep.replicaId_] <
          rep.committedWaterMark_) {
         gInfo_->committedWaterMarks_[rep.shardId_][rep.replicaId_] =
             rep.committedWaterMark_;
      }
      // LOG(INFO) << "wmk " << rep.shardId_ << ":" << rep.replicaId_ << "--"
      //           << rep.committedWaterMark_;
   }
   // if (rep.shardId_ == 0 && rep.replicaId_ == 1) {
   //    LOG(INFO) << "owd=" << rep.owd_;
   // }

   if (phase != phase_) return;
   gInfo_->fastReplyNum2_++;
   if (gInfo_->fastReplyNum2_ % 100000 == 1) {
      LOG(INFO) << "totalReply " << gInfo_->fastReplyNum1_ << ":"
                << gInfo_->fastReplyNum2_;
   }

   // LOG(INFO) << "rep=(" << rep.clientId_ << ":" << rep.reqId_ << ")---"
   //           << rep.hash_.ToString() << "---shardId=" << rep.shardId_
   //           << "--replicaId=" << rep.replicaId_;

   if (gInfo_->enableReadOnlyOptimization_ &&
       reqInProcess_.cmd_.isReadOnly_ > 0) {
      gInfo_->readReplyQu_.enqueue({rep, this});
   } else {
      if (rep.deadline_ == UINT64_MAX) {
         // dummy replies
         return;
      }
      // Speculative Check
      gInfo_->replyQu_.enqueue({rep, this});
   }

   // LOG(INFO) << "replicaId=" << rep.replicaId_ << "--"
   //           << "shardId=" << rep.shardId_ << "---" << rep.reqId_;

   // LOG(INFO) << "replicaId=" << rep.replicaId_ << "--"
   //           << "shardId=" << rep.shardId_ << "---" << rep.reqId_ << "--"
   //           << rep.hash_.ToString() << "----logId=" << rep.logId_;
}

void TigaCoordinator::OnDispatchReply(const uint32_t phase,
                                      const TigaDispatchReply& rep) {
   std::lock_guard<std::recursive_mutex> guard(mtx_);
   assert(phase == phase_);
   assert(stage_ == STAGE::Dispatching);
   if (!rep.result_.empty()) {
      reqInProcess_.cmd_.ws_.insert(rep.result_.begin(), rep.result_.end());
   }
   dispatchShards_.insert(rep.shardId_);
   if (dispatchShards_.size() == targetShards_.size()) {
      stage_ = STAGE::Commiting;
      // LOG(INFO) << "DisPatch reply" << rep.shardId_
      //           << " my reqId = " << reqInProcess_.cmd_.reqId_
      //           << " duration=" << GetMicrosecondTimestamp() - sendTime_;
      Launch();
   }
}

void TigaCoordinator::Finish2() {
   std::lock_guard<std::recursive_mutex> guard(mtx_);
   ClientReply crep;
   crep.clientId_ = clientId_;
   crep.reqId_ = requestIdByClient_;
   crep.result_.clear();
   phase_++;
   if (crep.reqId_ < 100) {
      LOG(INFO) << "reqId=" << requestIdByClient_
                << " duration=" << GetMicrosecondTimestamp() - sendTime_;
      for (auto& kv : crep.result_) {
         LOG(INFO) << "key=" << kv.first << "--value=" << kv.second;
      }
      LOG(INFO) << "----------";
   }

   callback_(crep);
}

void TigaCoordinator::Finish(TigaFastReplyQuorum& q) {
   std::lock_guard<std::recursive_mutex> guard(mtx_);
   ClientReply crep;
   crep.clientId_ = clientId_;
   crep.reqId_ = requestIdByClient_;
   crep.result_.clear();
   // Merge result
   for (auto& sId : targetShards_) {
      uint32_t leaderReplicaId = q.viewIds_[sId] % replicaNum_;
      TigaReply& reply = q.fastReplies_[sId][leaderReplicaId];
      crep.result_.insert(reply.result_.begin(), reply.result_.end());

      if (reply.result_.find(ABORT_FLAG) != reply.result_.end() &&
          reply.result_[ABORT_FLAG].get_i32() == 1) {
         LOG(INFO) << "Abort Triggered";
         assert(0);
      }
   }

   if (false && crep.reqId_ < 100) {
      LOG(INFO) << "reqId=" << requestIdByClient_
                << " duration=" << GetMicrosecondTimestamp() - sendTime_;
      for (auto& kv : crep.result_) {
         LOG(INFO) << "key=" << kv.first << "--value=" << kv.second;
      }
      LOG(INFO) << "----------";
   }

   phase_++;
   // LOG(INFO) << "reqId=" << requestIdByClient_
   //           << " duration=" << GetMicrosecondTimestamp() - sendTime_;
   callback_(crep);
}

void TigaCoordinator::Reset() {
   std::lock_guard<std::recursive_mutex> guard(mtx_);
   txnGen_ = NULL;
   // LOG(INFO) << "Before Reset requestIdByClient_=" << requestIdByClient_;
   targetShards_.clear();
   dispatchShards_.clear();
   clientId_ = UINT32_MAX;
   requestIdByClient_ = UINT32_MAX;
   detectReplicationInconsistency_ = false;
   detectNonSerial_ = false;
   phase_++;
}

TigaCoordinator::~TigaCoordinator() {}

/// Global Info

GlobalInfo::GlobalInfo(const uint32_t coordinatorId, const uint32_t shardNum,
                       const uint32_t replicaNum, const uint32_t cap,
                       const uint32_t initBound, const uint32_t yieldPeriodUs,
                       TigaCommunicator* comm, const int32_t closestReplicaId)
    : coordinatorId_(coordinatorId),
      shardNum_(shardNum),
      replicaNum_(replicaNum),
      cap_(cap),
      initBound_(initBound),
      yieldPeriodUs_(yieldPeriodUs),
      comm_(comm),
      closestReplicaId_(closestReplicaId) {
   markTime_ = 0;
   fastReplyNum2_ = 0;
   debug_ = false;
   LOG(INFO) << "coordiantorId=" << coordinatorId_ << "\tcap=" << cap_ << "\t"
             << "initBound=" << initBound_ << "\t"
             << "yieldPeriod=" << yieldPeriodUs_;
   nextRequestIdByProxy_ = 1;
   slowTriggers_ = 0;
   // To highlight clocks are not synchronized
   serverClock_ = coordinatorId_ * 1000ul * 1000ul;

   YAML::Node config = comm->Config();
   if (config["owd_delta_us"].IsDefined()) {
      owdDeltaUs_ = config["owd_delta_us"].as<int32_t>();
   } else {
      owdDeltaUs_ = 0;
   }
   if (config["owd_estimate_percentile"].IsDefined()) {
      owdEstimationPercentile_ =
          config["owd_estimate_percentile"].as<uint32_t>();
   } else {
      owdEstimationPercentile_ = 50;
   }
   LOG(INFO) << "owdDeltaUs_=" << owdDeltaUs_ << "\t"
             << "owdEstimationPercentile_=" << owdEstimationPercentile_;

   if (config["enable_read_only_optim"].IsDefined()) {
      enableReadOnlyOptimization_ = config["enable_read_only_optim"].as<bool>();
   } else {
      enableReadOnlyOptimization_ = false;
   }
   LOG(INFO) << "EnableReadOnly Optimization=" << enableReadOnlyOptimization_;
   LOG(INFO) << "closestReplicaId_=" << closestReplicaId_;

   for (uint32_t sid = 0; sid < MAX_SHARD_NUM; sid++) {
      for (uint32_t rid = 0; rid < MAX_REPLICA_NUM; rid++) {
         estimatedOWDs_[sid][rid] = initBound_;
         if (owdEstimationPercentile_ == 0) {
            estimatedOWDs_[sid][rid] = 0;
         }
         currentViews_[sid][rid] = 0;
         currentGlobalViews_[sid][rid] = 0;
         currentSyncedLogIds_[sid][rid] = 0;
         serverStatus_[sid][rid] = STATUS_NORMAL;
         replyNumPerNode_[sid][rid] = 0;

         committedWaterMarks_[sid][rid] = 0;
      }
   }

   serverSignal_ = CSTATUS_RUN;
   isRunning_ = true;
   daemonThread_ = new std::thread(&GlobalInfo::RunDaemon, this);
   inquiryThread_ = new std::thread(&GlobalInfo::RunIniquiry, this);
}

GlobalInfo::~GlobalInfo() {
   isRunning_ = false;
   daemonThread_->join();
   inquiryThread_->join();
   delete daemonThread_;
   delete inquiryThread_;
}

void GlobalInfo::RunDaemon() {
   while (isRunning_) {
      UpdateOWDStats();
      UpdateQuorumSet();
      if (enableReadOnlyOptimization_) {
         UpdateReadQuorumSet();
      }
      CheckQuorum();
   }
}

void GlobalInfo::RunIniquiry() {
   uint64_t lastInquireSyncStatus = 0;
   while (isRunning_) {
      // every 10ms
      uint64_t nowTime = GetMicrosecondTimestamp();
      if (nowTime - lastInquireSyncStatus > yieldPeriodUs_) {
         InquireServerSyncStatus();
         lastInquireSyncStatus = GetMicrosecondTimestamp();
      }
      uint64_t endTime = GetMicrosecondTimestamp();
      if (yieldPeriodUs_ > 0 && endTime - nowTime < yieldPeriodUs_) {
         uint64_t sleepTime = yieldPeriodUs_ - (endTime - nowTime);
         std::chrono::microseconds duration(sleepTime);
         std::this_thread::yield();
         std::this_thread::sleep_for(duration);
      }
   }
}

void GlobalInfo::UpdateOWDStats() {
   // Update owd stats
   uint32_t owd;
   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         while (owdQus_[sid][rid].try_dequeue(owd)) {
            if (owd < 1000ul * 1000ul) {
               owdSequences_[sid][rid].push_back(owd);
            }
         }
      }
   }
   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         if (owdSequences_[sid][rid].size() > 1000) {
            uint32_t percentile =
                owdSequences_[sid][rid].size() * owdEstimationPercentile_ / 100;
            sort(owdSequences_[sid][rid].begin(),
                 owdSequences_[sid][rid].end());
            uint32_t powd = owdSequences_[sid][rid][percentile];

            if (powd > cap_) {  // cap
               LOG(INFO) << "Abnormal " << sid << "\trid=" << rid
                         << "--value=" << powd;
               powd = cap_;
            }
            if (owdEstimationPercentile_ == 0) {
               powd = 0;
            }

            estimatedOWDs_[sid][rid] = powd;
            owdSequences_[sid][rid].clear();

            // LOG(INFO) << "Update sid=" << sid << "\trid=" << rid
            //           << "--value=" << powd;
         }
      }
   }
}

void GlobalInfo::UpdateQuorumSet() {
   std::pair<TigaReply, TigaCoordinator*> eVec[UINT8_MAX];
   int cnt = 0;
   while ((cnt = replyQu_.try_dequeue_bulk(eVec, UINT8_MAX)) > 0) {
      for (int i = 0; i < cnt; i++) {
         TigaReply& rep = eVec[i].first;
         TigaCoordinator* coord = eVec[i].second;

         // LOG(INFO) << "rep =" << rep.shardId_ << ":" << rep.replicaId_ <<
         // "--"
         //           << "reqId=" << rep.reqId_
         //           << "--hash=" << rep.hash_.ToString();
         // testCnt_[rep.reqId_][rep.shardId_]++;
         // bool ok =
         //     (testCnt_[rep.reqId_][0] >= 3 && testCnt_[rep.reqId_][1] >= 2);
         // if (ok) {
         //    testCnt_.erase(rep.reqId_);
         //    coord->Finish2();
         // }

         uint32_t coordReqId = rep.reqId_;
         if (committedCoordReqIds_.find(rep.reqId_) !=
             committedCoordReqIds_.end()) {
            continue;
         }
         AddToQuorumSet(rep, coord);
         // Quorum Check
         TigaFastReplyQuorum& q = quorumSets_[rep.reqId_];
         int ret = isTxnCommitted(q);
         // complete but nonSerializable
         uint64_t agreedDeadline = 0;
         bool nonSerializable = isNonSerializable(q, &agreedDeadline);
         // bool nonSerializable = false;  // debug
         if (ret == 0 && (!nonSerializable)) {
            // LOG(INFO) << "committed-1 " << coordReqId
            //           << "--fastReplies=" << q.fastReplies_[0].size();
            // LOG(INFO) << "syncStatus=" << currentSyncedLogIds_[0][0] << "-"
            //           << currentSyncedLogIds_[0][1] << "-"
            //           << currentSyncedLogIds_[0][2];
            committedCoordReqIds_.insert(coordReqId);
            coord->Finish(q);
            quorumSets_.erase(rep.reqId_);
         } else {
            if (nonSerializable) {
               // LOG(INFO) << "uncommitted-1" << coordReqId << "\t"
               //           << rep.clientId_ << ":" << rep.reqId_;
               q.coord_->detectNonSerial_ = true;
               IssueReconcliationRequest(q, agreedDeadline);
            }
         }
      }
   }
}

void GlobalInfo::UpdateReadQuorumSet() {
   std::pair<TigaReply, TigaCoordinator*> eVec[UINT8_MAX];
   int cnt = 0;
   while ((cnt = readReplyQu_.try_dequeue_bulk(eVec, UINT8_MAX)) > 0) {
      for (int i = 0; i < cnt; i++) {
         TigaReply& rep = eVec[i].first;
         TigaCoordinator* coord = eVec[i].second;
         uint32_t coordReqId = rep.reqId_;
         if (committedCoordReqIds_.find(rep.reqId_) !=
             committedCoordReqIds_.end()) {
            continue;
         }
         /****
         if (rep.deadline_ == UINT64_MAX) {
            LOG(INFO) << " Failed Read " << rep.clientId_ << ":" << rep.reqId_
                      << "--shardId=" << rep.shardId_
                      << "--replicaId=" << rep.replicaId_;
         } else {
            LOG(INFO) << "ReadOK=" << rep.clientId_ << ":" << rep.reqId_ << "--"
                      << rep.shardId_ << "|" << rep.replicaId_;
            LOG(INFO) << "QuorumSize=" << quorumSets_.size();
            if (quorumSets_.size() >= 15000) {
               for (auto& kv : quorumSets_) {
                  LOG(INFO) << "uncommitted " << kv.first;
               }
               exit(0);
            }
         }
         ***/
         AddToQuorumSet(rep, coord);
         // Quorum Check
         TigaFastReplyQuorum& q = quorumSets_[rep.reqId_];
         int ret = isReadTxnCompleted(q);
         if (ret == 0) {
            committedCoordReqIds_.insert(coordReqId);
            coord->Finish(q);
            quorumSets_.erase(rep.reqId_);
            // LOG(INFO) << "Read Committed " << coordReqId << "--Erase "
            //           << rep.reqId_;
         } else if (ret == -3) {
            // read failure. retry
            LOG(INFO) << "Retry Read " << rep.reqId_;
            quorumSets_.erase(rep.reqId_);
            coord->RetryRead();
         }
      }
   }
}

bool GlobalInfo::isNonSerializable(TigaFastReplyQuorum& q,
                                   uint64_t* agreedDeadline) {
   TigaCoordinator* coord = q.coord_;
   if (coord->targetShards_.size() == 1) {
      return false;
   }
   *agreedDeadline = 0;
   std::set<uint64_t> differentDeadlines;
   // std::string ddlStr = "";
   for (auto& sId : coord->targetShards_) {
      uint32_t view = q.viewIds_[sId];
      uint32_t leaderReplicaId = view % replicaNum_;
      const auto& iter = q.fastReplies_[sId].find(leaderReplicaId);
      if (iter == q.fastReplies_[sId].end()) {
         // no leader reply; cannot make judgement
         return false;
      }
      const TigaReply& leaderFastReply = iter->second;
      differentDeadlines.insert(leaderFastReply.deadline_);
      *agreedDeadline = std::max(*agreedDeadline, leaderFastReply.deadline_);
      // ddlStr += std::to_string(sId) + ":" +
      //           std::to_string(leaderFastReply.deadline_) + "|";
   }

   if (differentDeadlines.size() > 1) {
      // LOG(INFO) << "ddlStr=" << ddlStr;
      return true;
      // return false;  // rturn false for debug
   } else {
      return false;
   }
}

void GlobalInfo::IssueReconcliationRequest(TigaFastReplyQuorum& q,
                                           uint64_t agreedDeadline) {
   TigaReconcliationReq req;
   TigaCoordinator* coord = q.coord_;
   for (auto& sid : coord->targetShards_) {
      uint32_t leaderReplica = q.viewIds_[sid] % replicaNum_;
      req.gViewId_ = q.globalViewId_;
      req.viewId_ = q.viewIds_[sid];
      req.reqId_ = q.reqId_;
      req.clientId_ = coordinatorId_;

      rrr::FutureAttr fuattr;
      std::function<void(Future*)> cb = [this, coord](Future* fu) {
         TigaReply rep;
         fu->get_reply() >> rep;
         replyQu_.enqueue({rep, coord});
      };
      fuattr.callback = cb;
      if (q.fastReplies_[sid][leaderReplica].deadline_ < agreedDeadline) {
         // This reply is not valid
         q.fastReplies_[sid].erase(leaderReplica);
         Future::safe_release(comm_->ProxyAt(sid, leaderReplica)
                                  ->async_Reconcliation(req, fuattr));
      }
   }
}

void GlobalInfo::CheckQuorum() {
   std::vector<uint32_t> coordReqIdCommitted;
   // LOG(INFO) << "quorumSet =" << quorumSets_.size();
   if (false && GetMicrosecondTimestamp() - markTime_ >= 1000 * 1000) {
      LOG(INFO) << "quorumSet =" << quorumSets_.size();
      if (quorumSets_.size() > 0) {
         LOG(INFO) << "--minReqId=" << quorumSets_.begin()->first;
         int ret = isTxnCommitted(quorumSets_.begin()->second);
         LOG(INFO) << "ret = " << ret;
      }

      for (uint32_t sid = 0; sid < shardNum_; sid++) {
         for (uint32_t rid = 0; rid < replicaNum_; rid++) {
            LOG(INFO) << "(" << sid << "," << rid
                      << ")--syncedLogId=" << currentSyncedLogIds_[sid][rid]
                      << "--specSyncedLogId="
                      << currentSyncedSpecLogIds_[sid][rid];
         }
      }
      markTime_ = GetMicrosecondTimestamp();
   }
   for (auto& kv : quorumSets_) {
      if (committedCoordReqIds_.find(kv.first) != committedCoordReqIds_.end()) {
         // already committed
         coordReqIdCommitted.push_back(kv.first);
      } else if (isTxnCommitted(kv.second) == 0) {
         coordReqIdCommitted.push_back(kv.first);
      }
      // else {
      //    if (uncommittedCnters_.find(kv.first) == uncommittedCnters_.end()) {
      //       uncommittedCnters_[kv.first] = GetMicrosecondTimestamp();
      //    } else {
      //       // uint64_t nowTime = GetMicrosecondTimestamp();
      //       // if (nowTime - uncommittedCnters_[kv.first] >= 10000) {
      //       //    debug_ = true;
      //       // }
      //       // if (nowTime - uncommittedCnters_[kv.first] >= 30000) {
      //       //    LOG(INFO) << "Uncommitted " << kv.first << "--qSize="
      //       //              << quorumSets_[kv.first].fastReplies_[0].size();

      //       //    exit(0);
      //       // }
      //    }
      // }
   }
   for (auto& reqId : coordReqIdCommitted) {
      if (committedCoordReqIds_.find(reqId) == committedCoordReqIds_.end()) {
         committedCoordReqIds_.insert(reqId);
         auto& q = quorumSets_[reqId];
         // LOG(INFO) << "committed-2 " << reqId;
         q.coord_->Finish(q);
      }
      quorumSets_.erase(reqId);
   }
}

bool GlobalInfo::AddToQuorumSet(const TigaReply& rep, TigaCoordinator* coord) {
   // UpdateSyncStatus(rep.shardId_, rep.replicaId_, rep.gViewId_, rep.viewId_,
   //                  rep.latestSyncedLogId_, rep.latestSyncedSpecLogId_);
   // Add to quorumSet
   if (quorumSets_.find(rep.reqId_) == quorumSets_.end()) {
      // LOG(INFO) << "Add to Q " << rep.reqId_;
      TigaFastReplyQuorum& q = quorumSets_[rep.reqId_];
      q.globalViewId_ = rep.gViewId_;
      q.viewIds_[rep.shardId_] = rep.viewId_;
      q.coord_ = coord;
      q.fastReplies_[rep.shardId_][rep.replicaId_] = rep;
      q.clientId_ = rep.clientId_;
      q.reqId_ = rep.reqId_;
      // if (rep.replicaId_ == 0) {
      //    LOG(INFO) << "Leader Add to q " << rep.reqId_;
      // }

      // auto& qq = quorumSets_[rep.reqId_];
      // LOG(INFO) << "reqId=" << rep.reqId_ << "\tFast Quorum "
      //           << qq.fastReplies_[0].size();
      // for (auto& kv : qq.fastReplies_[0]) {
      //    LOG(INFO) << "Got Fast from " << kv.first;
      // }

   } else {
      TigaFastReplyQuorum& q = quorumSets_[rep.reqId_];
      if (q.globalViewId_ < rep.gViewId_) {
         // clear all
         for (uint32_t sid = 0; sid < shardNum_; sid++) {
            q.fastReplies_[sid].clear();
         }
         q.viewIds_[rep.shardId_] = rep.viewId_;
         q.fastReplies_[rep.shardId_][rep.replicaId_] = rep;
         q.coord_ = coord;
      } else if (q.viewIds_[rep.shardId_] < rep.viewId_) {
         q.fastReplies_[rep.shardId_].clear();
         q.viewIds_[rep.shardId_] = rep.viewId_;
         q.fastReplies_[rep.shardId_][rep.replicaId_] = rep;
         q.coord_ = coord;
      } else if (q.viewIds_[rep.shardId_] == rep.viewId_) {
         q.fastReplies_[rep.shardId_][rep.replicaId_] = rep;
      } else {
         // q.viewIds_[rep.shardId_]> rep
         // Reply is stale, ignore it
         return false;
      }
   }
   return true;
}

int GlobalInfo::isReadTxnCompleted(TigaFastReplyQuorum& q) {
   TigaCoordinator* coord = q.coord_;
   for (auto& sId : coord->targetShards_) {
      uint32_t replicaId = coord->reqInProcess_.cmd_.reqId_ % replicaNum_;
      if (closestReplicaId_ >= 0) {
         replicaId = closestReplicaId_;
      }

      const auto& iter = q.fastReplies_[sId].find(replicaId);
      if (iter == q.fastReplies_[sId].end()) {
         // reply not completed
         return -1;
      }
      uint32_t viewId = currentViews_[sId][replicaId];
      if (viewId > q.viewIds_[sId]) {
         q.fastReplies_[sId].clear();
         LOG(INFO) << "Clear " << viewId << ":" << q.viewIds_[sId];
         return -2;
      } else {
         if (iter->second.deadline_ == UINT64_MAX) {
            // Read Fail
            return -3;
         }
      }
   }
   return 0;
}

int GlobalInfo::isTxnCommitted(TigaFastReplyQuorum& q) {
   uint32_t slowQuorumSize = replicaNum_ / 2 + 1;
   uint32_t fastQuorumSize = replicaNum_ / 2 + (replicaNum_ / 2 + 1) / 2 + 1;
   // only for debug
   fastQuorumSize = std::min(fastQuorumSize, replicaNum_);
   // LOG(INFO) << "fastQuorumSize=" << fastQuorumSize
   //           << "--slowQuorumSize=" << slowQuorumSize;
   TigaCoordinator* coord = q.coord_;
   for (auto& sId : coord->targetShards_) {
      uint32_t validFastReplies = 0;
      uint32_t validSlowReplies = 0;
      uint32_t view = q.viewIds_[sId];
      uint32_t leaderReplicaId = view % replicaNum_;
      const auto& iter = q.fastReplies_[sId].find(leaderReplicaId);
      if (iter == q.fastReplies_[sId].end()) {
         // no leader reply
         return -1;
      }
      const TigaReply& leaderFastReply = iter->second;
      for (const auto& kv : q.fastReplies_[sId]) {
         const TigaReply& rep = kv.second;
         if (rep.hasHash_ == 0 || rep.hash_.Equal(leaderFastReply.hash_)) {
            //  hash_hash=0 i.e., slow reply
            validFastReplies++;
         } else {
            if (!rep.hash_.Equal(leaderFastReply.hash_)) {
               q.coord_->detectReplicationInconsistency_ = true;
            }
            uint32_t viewId, syncedLogId;
            {
               std::unique_lock lk(viewMtx_);
               viewId = currentViews_[rep.shardId_][rep.replicaId_];
               syncedLogId = currentSyncedLogIds_[rep.shardId_][rep.replicaId_];
            }

            if (viewId > q.viewIds_[sId]) {
               q.fastReplies_[sId].clear();
               LOG(INFO) << "Clear " << viewId << ":" << q.viewIds_[sId];
               return -2;
            } else {
               assert(viewId == q.viewIds_[sId]);
               if (syncedLogId >= rep.logId_) {
                  validFastReplies++;
               } else {
                  LOG(INFO) << "rep.logId=" << rep.logId_
                            << "--syncedLogId=" << syncedLogId;
               }
            }
         }
      }

      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         if (rid == leaderReplicaId) {
            continue;
         }
         uint32_t viewId, syncedLogId, syncedSpecLogId;
         {
            std::unique_lock lk(viewMtx_);
            viewId = currentViews_[sId][rid];
            syncedLogId = currentSyncedLogIds_[sId][rid];
            syncedSpecLogId = currentSyncedSpecLogIds_[sId][rid];
         }

         if (leaderFastReply.logId_ > 0 &&
             syncedLogId >= leaderFastReply.logId_) {
            // Non-speculative logs
            validSlowReplies++;
            // if (leaderFastReply.logId_ % 10000 == 1) {
            //    LOG(INFO) << "logId=" << leaderFastReply.logId_
            //              << "\t rid =" << rid
            //              << "-- syncedLogId=" << syncedLogId;
            // }
         } else if (leaderFastReply.specLogId_ > 0 &&
                    syncedSpecLogId >= leaderFastReply.specLogId_) {
            validSlowReplies++;
         }
      }

      if (validFastReplies < fastQuorumSize &&
          validSlowReplies + 1 < slowQuorumSize) {
         // LOG(INFO) << "txnId=" << coord->requestIdByClient_ << "--"
         //           << "sid=" << sId
         //           << "--Fast size=" << q.fastReplies_[sId].size() << ""
         //           << "--Slow Size=" << validSlowReplies
         //           << "--leaderLogId=" << leaderFastReply.logId_
         //           << "--R2 syncedLogId=" << currentSyncedLogIds_[sId][2];
         return -3;
      } else {
         // LOG(INFO) << "validFastReplies=" << validFastReplies
         //           << "\t validSlowReplies=" << validSlowReplies;
      }
   }

   return 0;
}

void GlobalInfo::InquireServerSyncStatus() {
   // LOG(INFO) << "InquireServerSyncStatus--token=" << slowTriggers_;
   TigaServerSyncStatusRequest req;
   req.clientId_ = coordinatorId_;
   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         rrr::FutureAttr fuattr;
         std::function<void(Future*)> cb = [this](Future* fu) {
            TigaServerSyncStatusReply rep;
            fu->get_reply() >> rep;
            if (rep.status_ == STATUS_FAILING ||
                rep.status_ == STATUS_BEFORE_FAIL) {
               this->UpdateSyncStatus(rep.shardId_, rep.replicaId_,
                                      rep.gViewId_, rep.viewId_, 0, 0,
                                      rep.signal_);
               if (serverStatus_[rep.shardId_][rep.replicaId_] !=
                   STATUS_FAILING) {
                  serverStatus_[rep.shardId_][rep.replicaId_] = STATUS_FAILING;
                  LOG(INFO)
                      << "Fail " << rep.shardId_ << ":" << rep.replicaId_
                      << "--"
                      << currentGlobalViews_[rep.shardId_][rep.replicaId_];
               }

            } else {
               this->UpdateSyncStatus(rep.shardId_, rep.replicaId_,
                                      rep.gViewId_, rep.viewId_,
                                      rep.latestSyncedLogId_,
                                      rep.latestSyncedSpecLogId_, rep.signal_);
            }
         };
         fuattr.callback = cb;

         // LOG(INFO) << "Inquire sid=" << sid << "\trid=" << rid;
         Future::safe_release(
             comm_->ProxyAt(sid, rid)->async_InquireServerSyncStatus(req,
                                                                     fuattr));
      }
   }
}

void GlobalInfo::UpdateSyncStatus(const uint32_t shardId,
                                  const uint32_t replicaId,
                                  const uint32_t gViewId, const uint32_t viewId,
                                  const uint32_t latestSyncedLogId,
                                  const uint32_t latestSyncedSpecLogId,
                                  const uint32_t signal) {
   std::unique_lock lk(viewMtx_);
   if (currentGlobalViews_[shardId][replicaId] > gViewId ||
       currentViews_[shardId][replicaId] > viewId) {
      return;
   }
   if (currentGlobalViews_[shardId][replicaId] < gViewId ||
       currentViews_[shardId][replicaId] < viewId ||
       currentSyncedLogIds_[shardId][replicaId] < latestSyncedLogId) {
      currentGlobalViews_[shardId][replicaId] = gViewId;
      currentViews_[shardId][replicaId] = viewId;
      currentSyncedLogIds_[shardId][replicaId] = latestSyncedLogId;
      currentSyncedSpecLogIds_[shardId][replicaId] = latestSyncedSpecLogId;
      // LOG(INFO) << "shardId=" << shardId << "--replicaId=" << replicaId
      //           << "--syncedLogId=" << latestSyncedLogId;
   }
   if (signal > 0) {
      if (serverSignal_ == CSTATUS_SUSPEND && signal == CSTATUS_RUN) {
         LOG(INFO) << "Switch by " << shardId << ":" << replicaId;
      }
      serverSignal_ = signal;
   }
}
