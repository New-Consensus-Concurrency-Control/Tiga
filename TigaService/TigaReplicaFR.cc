#include "TigaReplica.h"

void TigaReplica::FailureRecoveryInit(const YAML::Node& config) {
   if (config["test_failure_recovery"].IsDefined()) {
      testFailureRecovery_ = config["test_failure_recovery"].as<bool>();
   } else {
      testFailureRecovery_ = false;
   }
   if (config["synced_logid_before_failure"].IsDefined()) {
      syncedLogIdBeforeFailure_ =
          config["synced_logid_before_failure"].as<uint32_t>();
   } else {
      syncedLogIdBeforeFailure_ = 600000;
   }

   memset(vrRPCClients_, '\0',
          sizeof(rrr::Client*) * MAX_SHARD_NUM * MAX_REPLICA_NUM);

   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         std::string fullName =
             config["site"]["server"][sid][rid].as<std::string>();
         std::string thisServerName = fullName.substr(0, fullName.find(':'));
         std::string portName = fullName.substr(fullName.find(':') + 1);
         std::string ip = config["host"][thisServerName].as<std::string>();
         int port = std::stoi(portName);
         vrServerAddrs_[sid][rid] = ip + ":" + std::to_string(port + 3);
      }
   }
   cm_.Initialize(shardNum_);
   if (shardId_ == 0) {
      cm_.actionStatus_ = CM_MONITORING;
   }

   killed_ = false;
   targetGView_ = 0;
   // HardCode the server to kill
   replicaIdToKill_ = 0;
   shardIdToKill_ = 1;
   gViewId_ = 0;
   gVec_.clear();
   gVec_.resize(shardNum_, 0);
   serverSignalToCoord_ = CSTATUS_RUN;
   toFail_ = 0;
   gNews_.gVec_.clear();
   gNews_.gVec_.resize(shardNum_, 0);
   gNews_.gViewId_ = 0;
   startViewMsg_.gViewId_ = 0;
   startViewMsg_.viewId_ = 0;
   vrRpcPoll_ = new PollMgr(1);
}

void TigaReplica::ActivateFailure() {
   // Check Peer View
   std::shared_lock lck(gNewsMtx_);
   if (gNews_.gViewId_ > gViewId_) {
      LOG(INFO) << "STATUS_CLEAN_BEFORE_VIEWCHANGE";
      status_ = STATUS_CLEAN_BEFORE_VIEWCHANGE;
   } else if (toFail_ == 1) {
      // I must fail
      LOG(INFO) << "I need to fail";
      while (nextSyncedLogId_ - 1 > commitPoint_) {
         ThreadSleepFor(1000);
      }
      TigaFailAck fack;
      fack.replicaId_ = replicaId_;
      fack.shardId_ = shardId_;
      failCb_(fack);
      status_ = STATUS_FAILING;
      LOG(INFO) << "Okay, I failed";
   } else {
      if (serverSignalToCoord_ == CSTATUS_SUSPEND) {
         assert(AmCMLeader());
         bool canResume = true;
         for (uint32_t sid = 0; sid < shardNum_; sid++) {
            for (uint32_t rid = 0; rid < replicaNum_; rid++) {
               if (cm_.serverStatus_[sid][rid].status_ != STATUS_NORMAL &&
                       cm_.serverStatus_[sid][rid].status_ != STATUS_FAILING ||
                   cm_.serverStatus_[sid][rid].gViewId_ < targetGView_) {
                  canResume = false;
                  // LOG(INFO)
                  //     << sid << ":" << rid << "---"
                  //     << "status=" <<
                  //     cm_.serverStatus_[sid][rid].status_
                  //     << "--"
                  //     << "gViewId="
                  //     << cm_.serverStatus_[sid][rid].gViewId_;
               }
            }
         }
         if (canResume) {
            LOG(INFO) << "canResume";
            serverSignalToCoord_ = CSTATUS_RUN;
         }
      }
   }
}

void TigaReplica::FailureRecovery() {
   if (status_ == STATUS_FAILING) {
      std::shared_lock lck(gNewsMtx_);
      if (gNews_.gViewId_ > gViewId_) {
         gViewId_ = gNews_.gViewId_;
         gVec_ = gNews_.gVec_;
         viewId_ = gNews_.gVec_[shardId_];
         SendViewChange();
      }

      uint64_t nowTime = GetMicrosecondTimestamp();
      if (nowTime - lastPrintTime_ >= 1000 * 1000ul) {
         LOG(INFO) << "Failing...";
         lastPrintTime_ = nowTime;
      }
   } else if (status_ == STATUS_CLEAN_BEFORE_VIEWCHANGE) {
      while (activeThreads_ != 0) {
         ThreadSleepFor(1000);
      }
      if (!threadMap_.empty()) {
         for (auto& kv : threadMap_) {
            kv.second->join();
            delete kv.second;
         }
         threadMap_.clear();
      }
      ClearContext();
      std::shared_lock lck(gNewsMtx_);
      gViewId_ = gNews_.gViewId_;
      gVec_ = gNews_.gVec_;
      viewId_ = gNews_.gVec_[shardId_];
      LOG(INFO) << "STATUS_VIEWCHANGE viewId=" << viewId_
                << "\t gViewId=" << gViewId_;
      status_ = STATUS_VIEWCHANGE;
   } else if (status_ == STATUS_VIEWCHANGE) {
      BuildUnSyncedLogList();
      if (AmLeader()) {
         LOG(INFO) << "STATUS_VIEWCHANGE_COLLECT_VC viewId=" << viewId_
                   << "\t gViewId=" << gViewId_;
         status_ = STATUS_VIEWCHANGE_COLLECT_VC;
      } else {
         SendViewChange();
         LOG(INFO) << "STATUS_VIEWCHANGE_WAIT_START_VIEW";
         status_ = STATUS_VIEWCHANGE_WAIT_START_VIEW;
      }
   } else if (status_ == STATUS_VIEWCHANGE_COLLECT_VC) {
      assert(AmLeader());
      std::unique_lock lck(vcQuorumMtx_);
      if (CheckVCQuorum()) {
         CollectLogs();
         LOG(INFO) << "STATUS_VIEWCHANGE_REBUILD_LOG viewId=" << viewId_
                   << "\t gViewId=" << gViewId_;
         status_ = STATUS_VIEWCHANGE_REBUILD_LOG;
      }
   } else if (status_ == STATUS_VIEWCHANGE_REBUILD_LOG) {
      assert(AmLeader());
      std::unique_lock lck(stQuorumMtx_);
      if (stateTransferQuorum_.involvedReplicaIds_.size() ==
          stateTransferQuorum_.Quorum_.size()) {
         // quorum established
         RebuildLogs();
         LOG(INFO) << "STATUS_VIEWCHANGE_LOG_READY";
         status_ = STATUS_VIEWCHANGE_LOG_READY;
      }
   } else if (status_ == STATUS_VIEWCHANGE_LOG_READY) {
      assert(AmLeader());
      // TO confirm logs with the other leaders
      ConfirmLogWithOtherLeaders();
      LOG(INFO) << "STATUS_VIEWCHANGE_LOG_CONFIRMING";
      status_ = STATUS_VIEWCHANGE_LOG_CONFIRMING;
   } else if (status_ == STATUS_VIEWCHANGE_LOG_CONFIRMING) {
      assert(AmLeader());
      // If log confirmed, then start broadcast StartView messages and then
      // start new view
      std::unique_lock lck(crossShardConfirmQuorumMtx_);
      if (verifiedShards_.size() + 1 == shardNum_) {
         LOG(INFO) << "LeaderFinalizeRecoveredLogs";
         LeaderFinalizeRecoveredLogs();
         LOG(INFO) << "BroadcastStartView";
         BroadcastStartView();
         LOG(INFO) << "ExecuteCommittedLogs  nextSyncedLogId_="
                   << nextSyncedLogId_
                   << "--syncLogLisgSize=" << syncedLogList_.size();

         ExecuteCommittedLogs(nextSyncedLogId_ - 1);
         std::unique_lock lck(vcQuorumMtx_);
         vcQuorum_.fullQuorum_.clear();
         vcQuorum_.quorum_.clear();
         LOG(INFO) << "back to STATUS_NORMAL";
         status_ = STATUS_NORMAL;
      } else {
         // LOG(INFO) << "crossShardVerifyReqs_ size="
         //           << crossShardVerifyReqs_.size();
         std::vector<uint32_t> staleMsgs;
         for (auto& kv : crossShardVerifyReqs_) {
            if (gViewId_ != kv.second.gViewId_) {
               staleMsgs.push_back(kv.first);
            }
         }
         for (auto& sid : staleMsgs) {
            crossShardVerifyReqs_.erase(sid);
         }
         for (auto& kv : crossShardVerifyReqs_) {
            if (verifiedShards_.find(kv.first) == verifiedShards_.end()) {
               LOG(INFO) << "SendCrossShardVerifyReplyTo shardId=" << kv.first;
               SendCrossShardVerifyReplyTo(kv.first);
               verifiedShards_.insert(kv.first);
            }
         }
      }
   } else if (status_ == STATUS_VIEWCHANGE_WAIT_START_VIEW) {
      assert(!AmLeader());
      // if received start view, then send start state transfer
      std::shared_lock lck(startViewMtx_);
      if (startViewMsg_.gViewId_ == gViewId_ &&
          startViewMsg_.viewId_ == viewId_) {
         FollowerFinalizeRecoveredLogs();
         LOG(INFO) << "Follower back to STATUS_NORMAL gView=" << gViewId_;
         status_ = STATUS_NORMAL;
      }
   }

   uint64_t nowTime = GetMicrosecondTimestamp();
   if (false && nowTime - lastPrintTime_ >= 1000 * 1000) {
      LOG(INFO) << "commitPoint=" << commitPoint_ << "\t"
                << "syncPoint=" << nextSyncedLogId_ - 1 << "\t"
                << "lastBroadcastSyncedLogId_=" << lastBroadcastSyncedLogId_
                << "\t"
                << "lastBroadcastCommittedLogId_="
                << lastBroadcastCommittedLogId_ << "\t"
                << "serverSignal=" << serverSignalToCoord_ << "\t";
      //   << "earlyBufferSize=" << cnters_ << "\t"
      //   << "lastReqId_=" << lastReqId_ << "\t"
      //   << "lastDdlCheckReqId_=" << lastDdlCheckReqId_ << "\t"
      //   << "lastDdlAgreedReqId_=" << lastDdlAgreedReqId_ << "\t"
      //   << "lastExecReqId_=" << lastExecReqId_ << "\t"
      //   << "lastRepliedReqId_=" << lastRepliedReqId_ << "\t"
      //   << "dqReplyNum_=" << dqReplyNum_ << "\t"
      //   << "replyNum=" << replyNum_ << "\t"
      //   << "holdNum=" << holdNum_ << "\t"
      //   << "releaseNum=" << releaseNum_ << "\t"
      //   << "toExecuQU=" << toExecQu_.size_approx();

      lastPrintTime_ = nowTime;
   }
}

void TigaReplica::ConfigManagerAction() {
   if (shardId_ != 0) {
      return;
   }
   SendHeartBeat();
   if (cm_.actionStatus_ == CM_PREPARING_NEW_GVIEW) {
      verify(AmCMLeader());
      // check quorum of cmprepareReply
      std::unique_lock lck(cm_.mtx_);
      if (cm_.vCMPrepareReps_.size() + 1 >= replicaNum_ / 2) {
         // quorum established
         cm_.gInfo_ = cm_.prepareGInfo_;
         TigaCMCommit cmt;
         cmt.cViewId_ = cm_.viewId_;
         cmt.replicaId_ = replicaId_;
         cmt.shardId_ = shardId_;
         cmt.gViewId_ = cm_.prepareGInfo_.gViewId_;
         cmt.gVec_ = cm_.prepareGInfo_.gVec_;
         for (uint32_t rid = 0; rid < replicaNum_; rid++) {
            if (rid == replicaId_) {
               continue;
            }
            vrProxies_[shardId_][rid]->async_CMCommit(cmt);
         }
         LOG(INFO) << "CM_NEW_GVIEW_READY gView=" << cm_.gInfo_.gViewId_;
         for (uint32_t i = 0; i < shardNum_; i++) {
            LOG(INFO) << "Rady Lview " << i << "--" << cm_.gInfo_.gVec_[i];
         }
         cm_.actionStatus_ = CM_NEW_GVIEW_READY;
      }
   } else if (cm_.actionStatus_ == CM_NEW_GVIEW_READY) {
      assert(AmCMLeader());
      LOG(INFO) << "BroadcastViewChangeReq";
      BroadcastViewChangeReq();
      cm_.actionStatus_ = CM_NO_ACTION;
   } else if (cm_.actionStatus_ == CM_NO_ACTION ||
              cm_.actionStatus_ == CM_MONITORING) {
      // TO Kill servers at proper time
      // Check Server Status, if some leader fails then prepare new gview
      // (remember to reset vmPrepareReplies before swtich status)
      uint64_t nowTime = GetMicrosecondTimestamp();
      if (AmCMLeader() && nextSyncedLogId_ >= syncedLogIdBeforeFailure_ &&
          (!killed_)) {
         cm_.actionStatus_ = CM_KILLING;
         serverSignalToCoord_ = CSTATUS_SUSPEND;
         if (targetGView_ < cm_.gInfo_.gViewId_ + 1) {
            targetGView_ = cm_.gInfo_.gViewId_ + 1;
         }
         LOG(INFO) << "Change to SUSPEND  nowTime=" << nowTime;
         killed_ = true;
         // Send fail signal to a leader
         if (shardNum_ > 1) {
            TigaFailSignal fs;
            fs.failedGView_ = gViewId_;
            // hard code for now
            fs.failedReplicaId_ = replicaIdToKill_;
            fs.failedShardId_ = shardIdToKill_;
            LOG(INFO) << "Fail Signal to " << fs.failedShardId_ << ":"
                      << fs.failedReplicaId_;
            vrProxies_[fs.failedShardId_][fs.failedReplicaId_]
                ->async_FailSignal(fs);
         }
      }
      // switch to CM_KILING
   } else if (cm_.actionStatus_ == CM_KILLING) {
      // Check whether CMLeader has received, if so, broadcast
      // CMPrePare
      int64_t nowTime = GetMicrosecondTimestamp();
      if (cm_.serverStatus_[shardIdToKill_][replicaIdToKill_].status_ ==
          STATUS_FAILING) {
         // TODO: replace the condition with "Receiving failsignalack"
         cm_.prepareGInfo_.gViewId_++;
         // Here can be generalize and specify viewIds which are not
         // colocated in one region
         cm_.prepareGInfo_.gVec_.clear();
         cm_.prepareGInfo_.gVec_.resize(shardNum_, cm_.prepareGInfo_.gViewId_);
         for (uint32_t i = 0; i < shardNum_; i++) {
            LOG(INFO) << "Prepare LView " << i << "\t"
                      << cm_.prepareGInfo_.gVec_[i];
         }
         BroadcastCMPrepare();
         cm_.actionStatus_ = CM_PREPARING_NEW_GVIEW;
      }
   }
}

void TigaReplica::ConnectToOtherVRServers() {
   for (uint32_t s = 0; s < shardNum_; s++) {
      for (uint32_t r = 0; r < replicaNum_; r++) {
         vrProxies_[s][r] = NULL;
         vrRPCClients_[s][r] = NULL;
      }
   }
   bool amCMLeader =
       ((shardId_ == 0) && (cm_.viewId_ % replicaNum_ == replicaId_));
   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         if (rid == replicaId_ && sid == shardId_) {
            // self, no need to connect
            continue;
         }
         int ret = -1;
         vrRPCClients_[sid][rid] = new rrr::Client(vrRpcPoll_);
         do {
            LOG(INFO) << "VR Connecting to sid=" << sid << "\t rid=" << rid
                      << "\t" << vrServerAddrs_[sid][rid];
            ret = vrRPCClients_[sid][rid]->connect(
                vrServerAddrs_[sid][rid].c_str());
            if (ret == 0) {
               // success
               vrProxies_[sid][rid] =
                   new TigaViewChangeProxy(vrRPCClients_[sid][rid]);
               LOG(INFO) << "Connected to " << sid << "\t rid=" << rid << "\t"
                         << vrServerAddrs_[sid][rid];
            } else {
               ThreadSleepFor(1200000);
            }
         } while (ret != 0);
      }
   }
}

void TigaReplica::ConfirmLogWithOtherLeaders() {
   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      if (sid == shardId_) {
         continue;
      }
      uint32_t theOtherLeaderReplicaId = gVec_[sid] % replicaNum_;
      TigaCrossShardVerifyReq req;
      req.shardId_ = shardId_;
      req.replicaId_ = replicaId_;
      req.gViewId_ = gViewId_;
      req.viewId_ = viewId_;
      req.syncedDeadline_ = 0;
      if (!recoveredSyncedLogs_.empty()) {
         req.syncedDeadline_ = recoveredSyncedLogs_.rbegin()->first.first;
         if ((!recoveredUnSyncedLogs_.empty()) &&
             recoveredUnSyncedLogs_.begin()->first.first <
                 req.syncedDeadline_) {
            req.syncedDeadline_ = recoveredUnSyncedLogs_.begin()->first.first;
         }
      }

      assert(vrProxies_[sid][theOtherLeaderReplicaId] != NULL);
      LOG(INFO) << "send CrossShardVerifyReq to " << sid << ":"
                << theOtherLeaderReplicaId;
      vrProxies_[sid][theOtherLeaderReplicaId]->async_CrossShardVerifyReq(req);
   }
}

void TigaReplica::LeaderFinalizeRecoveredLogs() {
   //  std::unique_lock lock(entryRecordMtx_); (still needed?)
   for (auto& kv : crossShardVerifyReps_) {
      const TigaCrossShardVerifyRep& msg = kv.second;
      LOG(INFO) << "msgdeadline size=" << msg.deadlines_.size();
      for (uint32_t i = 0; i < msg.deadlines_.size(); i++) {
         uint64_t txnKey = msg.txnKeys_[i];
         if (syncedTxnKeysToDeadlines_.find(txnKey) !=
             syncedTxnKeysToDeadlines_.end()) {
            LOG(INFO) << "Found";
            uint64_t myDeadline = syncedTxnKeysToDeadlines_[txnKey];
            if (myDeadline < msg.deadlines_[i]) {
               // use the larger deadline
               LOG(INFO) << "Use large ddl";
               TigaLogEntry* entry = recoveredSyncedLogs_[{myDeadline, txnKey}];
               verify(entry != NULL);
               entry->agreedDdlRank_ = msg.deadlines_[i];
               syncedTxnKeysToDeadlines_[txnKey] = msg.deadlines_[i];
               recoveredSyncedLogs_.erase({myDeadline, txnKey});
               recoveredSyncedLogs_[{msg.deadlines_[i], txnKey}] = entry;
            }
         } else {
            LOG(INFO) << "Not Found";
            ClientCommand* cmd = GetEntry(cmdMap_, txnKey);
            if (cmd) {
               LOG(INFO) << "Fetch one";
               TigaLogEntry* entry = new TigaLogEntry();
               entry->deadlineAgreed_ = true;
               entry->agreedDdlRank_ = msg.deadlines_[i];
               entry->cmd_ = cmd;
               syncedTxnKeysToDeadlines_[txnKey] = msg.deadlines_[i];
               recoveredSyncedLogs_[{msg.deadlines_[i], txnKey}] = entry;
            } else {
               LOG(ERROR) << "Not implemented yet. should not come here "
                          << HIGH_32BIT(txnKey) << ":" << LOW_32BIT(txnKey);
            }
         }
      }
   }

   // Put back to syncedLogList
   syncedLogList_.clear();
   syncedLogList_.reserve(recoveredSyncedLogs_.size());
   for (auto& kv : recoveredSyncedLogs_) {
      TigaLogEntry* entry = kv.second;
      if (entry->shardKeyMap_.empty()) {
         sm_->InitializeRelatedShards(entry->cmd_->txnType_,
                                      &(entry->cmd_->ws_),
                                      &(entry->shardKeyMap_));
      }
      syncedLogList_.push_back(kv.second);
      kv.second->logId_ = syncedLogList_.size();
   }
   nextSyncedLogId_ = syncedLogList_.size() + 1;
   recoveredSyncedLogs_.clear();
   syncedTxnKeysToDeadlines_.clear();
}

void TigaReplica::ClearContext() {
   // Clean Deadline Quorum
   for (uint32_t i = 0; i < HASH_PARTITION_NUM; i++) {
      ddlMap_[i].deadlineQ_.clear();
   }

   holdBuffer_.clear();
   lastReleasedTxnDeadlinesW_.clear();
   lastReleasedTxnDeadlinesW_.resize(sm_->TotalNumberofKeys(), 0);
   lastReleasedTxnDeadlinesR_.clear();
   lastReleasedTxnDeadlinesR_.resize(sm_->TotalNumberofKeys(), 0);
   execSequencers_.clear();
   execSequencers_.resize(sm_->TotalNumberofKeys());
   entriesInSpec_.clear();
   entriesInSpec_.resize(sm_->TotalNumberofKeys());
   boundaryLogInfos_.clear();
   boundaryLogInfos_.resize(sm_->TotalNumberofKeys(), NULL);
   boundarySyncedHashMarks_.clear();
   boundarySyncedHashMarks_.resize(sm_->TotalNumberofKeys());

   TigaLogEntry* entry;
   std::pair<TigaLogEntry*, uint32_t> epair;
   CommitReplyInfo ce;

   while (toHoldAndReleaseQu_.try_dequeue(entry)) {
   }
   while (toDdlSyncQu_.try_dequeue(entry)) {
   }
   while (toExecCheckQu_.try_dequeue(entry)) {
   }

   while (toExecQuF_.try_dequeue(epair)) {
   }

   while (toReplyQu_.try_dequeue(entry)) {
   }

   while (toCommitRepyQu_.try_dequeue(ce)) {
   }
   for (uint32_t i = 0; i < HASH_PARTITION_NUM; i++) {
      pendingEntriesToCommitReply_[i].clear();
      pendingCommitRepyHandlers_[i].clear();
   }

   while (syncedLogInfoQu_.try_dequeue(entry)) {
      pendingEntries_[entry->logId_] = entry;
   }
   LOG(INFO) << "pendingEntriesSize_=" << pendingEntries_.size();
   while (!pendingEntries_.empty()) {
      if (pendingEntries_.begin()->first - 1 == syncedLogList_.size()) {
         syncedLogList_.push_back(pendingEntries_.begin()->second);
         pendingEntries_.erase(pendingEntries_.begin());
      }
   }
   LOG(INFO) << "pendingLogInfoSize Size2=" << pendingEntries_.size()
             << "--syncedLogSize=" << syncedLogList_.size()
             << "--nextSyncedLogId=" << nextSyncedLogId_;

   while (followerCommitExecuteQu_.try_dequeue(entry)) {
      rebuffer_[entry->logId_] = entry;
   }
   LOG(INFO) << "rebufferSize=" << rebuffer_.size();
   while (!rebuffer_.empty()) {
      if (rebuffer_.begin()->first - 1 == syncedLogList_.size()) {
         syncedLogList_.push_back(rebuffer_.begin()->second);
         rebuffer_.erase(rebuffer_.begin());
      }
   }
   LOG(INFO) << "rebuffer Size2=" << rebuffer_.size()
             << "--syncedLogSize=" << syncedLogList_.size()
             << "--nextSyncedLogId=" << nextSyncedLogId_;
   pendingTigaInterReplicaSyncs_.clear();
   pendingEntries_.clear();
   LOG(INFO) << "ResetHashEntry";
   // The previous synced entry has been checkponited
   for (auto& kv : syncedEntries_.entryMap_) {
      delete kv.second;
   }
   syncedEntries_.entryMap_.clear();
   for (auto& kv : unSyncedEntries_.entryMap_) {
      delete kv.second;
   }
   unSyncedEntries_.entryMap_.clear();
   latestSyncedHashes_.entryMap_.clear();
}

void TigaReplica::BroadcastStartView() {
   std::unique_lock lck(vcQuorumMtx_);
   const auto& quorum = vcQuorum_.fullQuorum_;
   for (auto& kv : quorum) {
      const TigaViewChange& vc = kv.second;
      if (vc.replicaId_ == replicaId_ && vc.shardId_ == shardId_) {
         // not myself
         continue;
      }
      if (vc.status_ == STATUS_FAILING) {
         continue;
      }
      TigaStartView sv;
      sv.shardId_ = shardId_;
      sv.replicaId_ = replicaId_;
      sv.gViewId_ = gViewId_;
      sv.viewId_ = viewId_;
      sv.logBegin_ = 1;
      if (vcQuorum_.highestNormalView_ == vc.lastNormalViewId_) {
         // only needs to transfer the remainig part beyond this server's
         // sync point
         sv.logBegin_ =
             std::min(vc.syncPoint_, vcQuorum_.highestSyncPoint_) + 1;
      } else {
         sv.logBegin_ = vc.commitPoint_;
      }
      LOG(INFO) << "logBegin=" << sv.logBegin_ << "\t"
                << "syncPoint=" << vc.syncPoint_ << "\t"
                << "commitPoint=" << vc.commitPoint_;
      for (int i = sv.logBegin_ - 1; i < syncedLogList_.size(); i++) {
         TigaLogEntry* entry = syncedLogList_[i];
         sv.deadlines_.push_back(entry->agreedDdlRank_);
         sv.txns_.push_back(*(entry->cmd_));
      }
      LOG(INFO) << "Send to " << vc.shardId_ << ":" << vc.replicaId_
                << "sz=" << sv.deadlines_.size();
      vrProxies_[vc.shardId_][vc.replicaId_]->async_StartView(sv);
   }
}

void TigaReplica::SendCrossShardVerifyReplyTo(uint32_t targetShard) {
   const TigaCrossShardVerifyReq& req = crossShardVerifyReqs_[targetShard];
   TigaCrossShardVerifyRep rep;
   rep.shardId_ = shardId_;
   rep.replicaId_ = replicaId_;
   rep.gViewId_ = gViewId_;
   rep.viewId_ = viewId_;
   auto iter = recoveredSyncedLogs_.upper_bound({req.syncedDeadline_, 0});
   while (iter != recoveredSyncedLogs_.end()) {
      TigaLogEntry* entry = iter->second;
      if (entry->shardKeyMap_.empty()) {
         sm_->InitializeRelatedShards(entry->cmd_->txnType_,
                                      &(entry->cmd_->ws_),
                                      &(entry->shardKeyMap_));
      }
      if (entry->shardKeyMap_.find(targetShard) != entry->shardKeyMap_.end()) {
         rep.deadlines_.push_back(iter->first.first);
         rep.txnKeys_.push_back(iter->first.second);
      }
      iter++;
   }
   for (auto& kv : recoveredUnSyncedLogs_) {
      TigaLogEntry* entry = kv.second;
      if (entry->shardKeyMap_.empty()) {
         sm_->InitializeRelatedShards(entry->cmd_->txnType_,
                                      &(entry->cmd_->ws_),
                                      &(entry->shardKeyMap_));
      }
      if (entry->shardKeyMap_.find(targetShard) != entry->shardKeyMap_.end()) {
         rep.deadlines_.push_back(kv.first.first);
         rep.txnKeys_.push_back(kv.first.second);
      }
   }
   vrProxies_[req.shardId_][req.replicaId_]->async_CrossShardVerifyRep(rep);
}

void TigaReplica::FollowerFinalizeRecoveredLogs() {
   // std::unique_lock lock(entryRecordMtx_); // is it still needed?
   if (syncedLogList_.size() < startViewMsg_.logBegin_ - 1) {
      LOG(INFO) << "syncedLOgSize=" << syncedLogList_.size() << "--"
                << "logBegin-1=" << startViewMsg_.logBegin_ - 1;
   }
   assert(syncedLogList_.size() >= startViewMsg_.logBegin_ - 1);
   syncedLogList_.resize(startViewMsg_.logBegin_ - 1);
   for (uint32_t i = 0; i < startViewMsg_.deadlines_.size(); i++) {
      uint64_t txnKey = startViewMsg_.txns_[i].TxnKey();
      ClientCommand* cmd = GetEntry(cmdMap_, txnKey);
      if (!cmd) {
         cmd = new ClientCommand(startViewMsg_.txns_[i]);
         InsertEntry(cmdMap_, txnKey, cmd);
      }
      TigaLogEntry* entry = new TigaLogEntry();
      entry->agreedDdlRank_ = startViewMsg_.deadlines_[i];
      entry->deadlineAgreed_ = true;
      entry->cmd_ = cmd;
      entry->logId_ = nextSyncedLogId_.fetch_add(1);
      syncedLogList_.push_back(entry);
   }
   nextSyncedLogId_ = syncedLogList_.size() + 1;
}

void TigaReplica::onStartView(const TigaStartView& msg) {
   std::unique_lock lck(startViewMtx_);
   startViewMsg_ = std::move(msg);
}

void TigaReplica::onViewChangeReq(const TigaViewChangeReq& msg) {
   std::unique_lock lck(gNewsMtx_);
   LOG(INFO) << "Update gNews " << msg.gViewId_;
   for (uint32_t i = 0; i < msg.gVec_.size(); i++) {
      LOG(INFO) << "i=" << i << "\t lView=" << msg.gVec_[i];
   }
   if (gNews_.gViewId_ < msg.gViewId_) {
      gNews_.gViewId_ = msg.gViewId_;
      gNews_.gVec_ = msg.gVec_;
   }
}

void TigaReplica::onViewChange(const TigaViewChange& msg) {
   std::unique_lock lck(vcQuorumMtx_);
   if (vcQuorum_.fullQuorum_.find(msg.replicaId_) ==
       vcQuorum_.fullQuorum_.end()) {
      vcQuorum_.fullQuorum_[msg.replicaId_] = msg;
   }
}

bool TigaReplica::CheckVCQuorum() {
   // This is just for implementaton and test convenience
   // Theorectically, you should just wati for f other VCs
   if (vcQuorum_.fullQuorum_.size() + 1 < replicaNum_) {
      return false;
   }
   // Add myself's viewChange-> quorum sufficent
   TigaViewChange vc;
   vc.shardId_ = shardId_;
   vc.replicaId_ = replicaId_;
   vc.gViewId_ = gViewId_;
   vc.viewId_ = viewId_;
   vc.syncPoint_ = nextSyncedLogId_ - 1;
   vc.commitPoint_ = std::min(executedLogId_.load(), commitPoint_.load());
   vc.lastNormalViewId_ = lastNormalView_;
   vc.status_ = status_;
   vcQuorum_.fullQuorum_[replicaId_] = vc;
   vcQuorum_.quorum_[replicaId_] = vc;

   // update gView
   for (auto& kv : vcQuorum_.fullQuorum_) {
      if (gViewId_ < kv.second.gViewId_ &&
          kv.second.status_ != STATUS_FAILING) {
         gViewId_ = kv.second.gViewId_;
         viewId_ = kv.second.viewId_;
      }
   }

   // Pick Quorum
   for (auto& kv : vcQuorum_.fullQuorum_) {
      if (kv.second.status_ != STATUS_FAILING) {
         vcQuorum_.quorum_[kv.first] = kv.second;
         if (vcQuorum_.quorum_.size() == replicaNum_ / 2 + 1) {
            break;
         }
      }
   }
   return true;
}

void TigaReplica::CollectLogs() {
   vcQuorum_.highestNormalView_ = lastNormalView_;
   vcQuorum_.highestSyncPoint_ = nextSyncedLogId_ - 1;
   vcQuorum_.targetReplicaId_ = replicaId_;
   std::vector<uint32_t> involvedReplicaIds;
   for (uint32_t rid = 0; rid < replicaNum_; rid++) {
      if (vcQuorum_.quorum_.find(rid) == vcQuorum_.quorum_.end()) {
         continue;
      }
      if (vcQuorum_.quorum_[rid].lastNormalViewId_ > lastNormalView_) {
         vcQuorum_.highestNormalView_ =
             vcQuorum_.quorum_[rid].lastNormalViewId_;
         vcQuorum_.highestSyncPoint_ = vcQuorum_.quorum_[rid].syncPoint_;
         vcQuorum_.targetReplicaId_ = rid;
      } else if (vcQuorum_.quorum_[rid].lastNormalViewId_ == lastNormalView_ &&
                 vcQuorum_.quorum_[rid].syncPoint_ >
                     vcQuorum_.highestSyncPoint_) {
         vcQuorum_.highestNormalView_ =
             vcQuorum_.quorum_[rid].lastNormalViewId_;
         vcQuorum_.highestSyncPoint_ = vcQuorum_.quorum_[rid].syncPoint_;
         vcQuorum_.targetReplicaId_ = rid;
      }
   }

   for (uint32_t rid = 0; rid < replicaNum_; rid++) {
      if (rid != replicaId_ &&
          vcQuorum_.quorum_.find(rid) != vcQuorum_.quorum_.end() &&
          vcQuorum_.quorum_[rid].lastNormalViewId_ ==
              vcQuorum_.highestNormalView_) {
         involvedReplicaIds.push_back(rid);
      }
   }

   rrr::FutureAttr fuattr;
   std::function<void(Future*)> cb = [this](Future* fu) {
      TigaStateTransferReply rep;
      fu->get_reply() >> rep;
      this->onStateTransferReply(rep);
   };
   fuattr.callback = cb;

   {
      std::unique_lock stlck(stQuorumMtx_);
      stateTransferQuorum_.replicaIdForSyncedLogs_ = vcQuorum_.targetReplicaId_;
      stateTransferQuorum_.targetViewToRecover_ = vcQuorum_.highestNormalView_;
      stateTransferQuorum_.Quorum_.clear();
      stateTransferQuorum_.involvedReplicaIds_ = involvedReplicaIds;
   }
   // Send StateTransfer Request
   for (uint32_t i = 0; i < involvedReplicaIds.size(); i++) {
      uint32_t rid = involvedReplicaIds[i];
      TigaStateTransferRequest str;
      str.shardId_ = shardId_;
      str.replicaId_ = replicaId_;
      str.syncedLogBegin_ = 0;
      str.syncedLogEnd_ = 0;
      if (rid == vcQuorum_.targetReplicaId_) {
         // This replica needs to send both synced logs and unsynced logs
         str.syncedLogEnd_ = vcQuorum_.highestSyncPoint_;
         if (lastNormalView_ == vcQuorum_.highestNormalView_) {
            // I also have the proper view, and can save some transfer
            str.syncedLogBegin_ = nextSyncedLogId_;
         } else {
            str.syncedLogBegin_ = 1;
         }
      }
      vrProxies_[shardId_][rid]->async_StateTransfer(str, fuattr);
   }
}

void TigaReplica::RebuildLogs() {
   if (stateTransferQuorum_.replicaIdForSyncedLogs_ != replicaId_) {
      // recover synced logs
      const TigaStateTransferReply& rep =
          stateTransferQuorum_
              .Quorum_[stateTransferQuorum_.replicaIdForSyncedLogs_];
      if (lastNormalView_ != stateTransferQuorum_.targetViewToRecover_) {
         // I have no help to recover synced logs, all from scratch
         syncedLogList_.clear();
         nextSyncedLogId_ = 1;
      }
      for (uint32_t i = 0; i < rep.syncedTxnDeadlines_.size(); i++) {
         TigaLogEntry* entry = new TigaLogEntry();
         entry->logId_ = nextSyncedLogId_.fetch_add(1);
         entry->deadlineAgreed_ = true;
         entry->agreedDdlRank_ = rep.syncedTxnDeadlines_[i];
         syncedLogList_.push_back(entry);
         uint64_t txnKey = rep.syncedTxns_[i].TxnKey();
         ClientCommand* cmd = GetEntry(cmdMap_, txnKey);
         if (!cmd) {
            cmd = new ClientCommand(rep.syncedTxns_[i]);
            InsertEntry(cmdMap_, txnKey, cmd);
         }
         entry->cmd_ = cmd;
      }
   }
   syncedTxnKeysToDeadlines_.clear();
   for (auto& entry : syncedLogList_) {
      syncedTxnKeysToDeadlines_[entry->cmd_->TxnKey()] = entry->agreedDdlRank_;
   }

   recoveryUnSyncedCntMap_.clear();
   // recover unsynced logs
   for (uint32_t j = 0; j < stateTransferQuorum_.involvedReplicaIds_.size();
        j++) {
      uint32_t rid = stateTransferQuorum_.involvedReplicaIds_[j];
      TigaStateTransferReply& rep = stateTransferQuorum_.Quorum_[rid];
      for (uint32_t k = 0; k < rep.unsyncedTxnDeadlines_.size(); k++) {
         uint64_t txnKey = rep.unsyncedTxns_[k].TxnKey();
         uint64_t ddl = rep.unsyncedTxnDeadlines_[k];
         if (syncedTxnKeysToDeadlines_.find(txnKey) !=
             syncedTxnKeysToDeadlines_.end()) {
            // do not recover duplicate txns
            continue;
         } else {
            syncedTxnKeysToDeadlines_[txnKey] = ddl;
         }
         ClientCommand* cmd = GetEntry(cmdMap_, txnKey);
         if (!cmd) {
            cmd = new ClientCommand(rep.unsyncedTxns_[k]);
            InsertEntry(cmdMap_, txnKey, cmd);
         }
         recoveryUnSyncedCntMap_[{ddl, txnKey}]++;
      }
   }
   uint32_t recoveryQuorumSize = (replicaNum_ / 2 + 1) / 2 + 1;
   for (auto& kv : recoveryUnSyncedCntMap_) {
      if (kv.second >= recoveryQuorumSize) {
         TigaLogEntry* entry = new TigaLogEntry();
         entry->logId_ = nextSyncedLogId_.fetch_add(1);
         entry->deadlineAgreed_ = true;
         entry->agreedDdlRank_ = kv.first.first;
         entry->cmd_ = GetEntry(cmdMap_, kv.first.second);
         assert(entry->cmd_ != NULL);
         syncedLogList_.push_back(entry);
      }
   }

   // reconstruct synced log list
   for (auto& entry : syncedLogList_) {
      uint64_t ddl = entry->agreedDdlRank_;
      uint64_t txnKey = entry->cmd_->TxnKey();
      recoveredSyncedLogs_[{ddl, txnKey}] = entry;
   }
}

void TigaReplica::BroadcastCMPrepare() {
   TigaCMPrepare tp;
   tp.cViewId_ = cm_.viewId_;
   tp.gViewId_ = cm_.prepareGInfo_.gViewId_;
   tp.gVec_ = cm_.prepareGInfo_.gVec_;
   // for simplicity, just hard code for now
   tp.shardId_ = shardId_;
   tp.replicaId_ = replicaId_;
   for (uint32_t rid = 0; rid < replicaNum_; rid++) {
      if (rid == replicaId_) {
         continue;
      }
      vrProxies_[shardId_][rid]->async_CMPrepare(tp);
   }
}

void TigaReplica::onHeartBeat(const TigaHeartBeat& req, TigaHeartBeatAck* ack) {
   ack->shardId_ = shardId_;
   ack->replicaId_ = replicaId_;
   ack->viewId_ = viewId_;
   ack->gViewId_ = gViewId_;
   ack->status_ = status_;
   // LOG(INFO) << "ack gView=" << ack->gViewId_;
}

void TigaReplica::onStateTransfer(const TigaStateTransferRequest& req,
                                  TigaStateTransferReply* rep) {
   if (viewId_ != req.viewId_) {
      rep->viewId_ = viewId_;
      return;
   }
   rep->replicaId_ = replicaId_;
   rep->shardId_ = shardId_;
   rep->syncedTxnDeadlines_.clear();
   rep->syncedTxns_.clear();
   rep->unsyncedTxnDeadlines_.clear();
   rep->unsyncedTxns_.clear();
   if (req.syncedLogEnd_ > 0) {
      for (uint32_t i = req.syncedLogBegin_; i <= req.syncedLogEnd_; i++) {
         TigaLogEntry* entry = syncedLogList_[i - 1];
         rep->syncedTxnDeadlines_.push_back(entry->agreedDdlRank_);
         rep->syncedTxns_.push_back(*(entry->cmd_));
      }
   }
   for (auto& kv : recoveredUnSyncedLogs_) {
      rep->unsyncedTxnDeadlines_.push_back(kv.first.first);
      rep->unsyncedTxns_.push_back(*(kv.second->cmd_));
   }
}

void TigaReplica::onStateTransferReply(TigaStateTransferReply& rep) {
   std::shared_lock slck(statusMtx_);
   if (rep.viewId_ != viewId_) {
      return;
   }
   std::unique_lock lck(stQuorumMtx_);
   if (stateTransferQuorum_.Quorum_.find(rep.replicaId_) ==
       stateTransferQuorum_.Quorum_.end()) {
      stateTransferQuorum_.Quorum_[rep.replicaId_] = std::move(rep);
   }
}
void TigaReplica::SendHeartBeat() {
   if (cm_.viewId_ % replicaNum_ != replicaId_) {
      return;
   }

   uint64_t nowTime = GetMicrosecondTimestamp();
   if (nowTime < lastHeartBeatTime_ + 500000) {
      return;
   }
   TigaHeartBeat hb;
   hb.replicaId_ = replicaId_;
   hb.shardId_ = shardId_;
   rrr::FutureAttr fuattr;
   std::function<void(Future*)> cb = [this](Future* fu) {
      TigaHeartBeatAck ack;
      fu->get_reply() >> ack;
      this->onHeartBeatAck(ack);
   };
   fuattr.callback = cb;
   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         if (sid == shardId_ && rid == replicaId_) {
            cm_.serverStatus_[sid][rid].gViewId_ = gViewId_;
            cm_.serverStatus_[sid][rid].viewId_ = viewId_;
            cm_.serverStatus_[sid][rid].status_ = status_;
         } else {
            vrProxies_[sid][rid]->async_HeartBeat(hb, fuattr);
         }
      }
   }

   lastHeartBeatTime_ = GetMicrosecondTimestamp();
}

void TigaReplica::onHeartBeatAck(const TigaHeartBeatAck& ack) {
   std::unique_lock lck(cm_.mtx_);
   // LOG(INFO) << "forom " << ack.shardId_ << ":" << ack.replicaId_ << "---"
   //           << ack.gViewId_ << "--status=" << ack.status_;
   cm_.serverStatus_[ack.shardId_][ack.replicaId_].gViewId_ = ack.gViewId_;
   cm_.serverStatus_[ack.shardId_][ack.replicaId_].viewId_ = ack.viewId_;
   cm_.serverStatus_[ack.shardId_][ack.replicaId_].status_ = ack.status_;
}

void TigaReplica::BuildUnSyncedLogList() {
   // Record my unsynced logs to RecoveryMap
   recoveredUnSyncedLogs_.clear();
   while (!preparedLogInfo_.empty()) {
      TigaLogEntry* entry = preparedLogInfo_.begin()->second;
      std::pair<uint64_t, uint64_t> p(entry->localDdlRank_,
                                      entry->cmd_->TxnKey());
      if (recoveredUnSyncedLogs_.find(p) == recoveredUnSyncedLogs_.end()) {
         recoveredUnSyncedLogs_[p] = entry;
      }
      preparedLogInfo_.erase(preparedLogInfo_.begin());
   }
   for (auto& kv : unSyncedEntries_.entryMap_) {
      EntryQu<TigaLogEntry>* qu = kv.second;
      while (!qu->qu_.empty()) {
         TigaLogEntry* entry = qu->qu_.front();
         std::pair<uint64_t, uint64_t> p(entry->agreedDdlRank_,
                                         entry->cmd_->TxnKey());
         if (recoveredUnSyncedLogs_.find(p) == recoveredUnSyncedLogs_.end()) {
            recoveredUnSyncedLogs_[p] = entry;
         }
         qu->qu_.pop();
      }
   }
}

void TigaReplica::SendViewChange() {
   TigaViewChange vc;
   vc.shardId_ = shardId_;
   vc.replicaId_ = replicaId_;
   vc.viewId_ = viewId_;
   vc.gViewId_ = gViewId_;
   vc.syncPoint_ = nextSyncedLogId_ - 1;
   vc.commitPoint_ = commitPoint_;
   vc.lastNormalViewId_ = lastNormalView_;
   vc.status_ = status_;
   // Send to leader
   uint32_t myLeaderReplicaId = viewId_ % replicaNum_;
   LOG(INFO) << "Send VC to " << shardId_ << ":" << myLeaderReplicaId << "--"
             << "syncPoinrt=" << vc.syncPoint_
             << "--syncLogList=" << syncedLogList_.size();
   vrProxies_[shardId_][myLeaderReplicaId]->async_ViewChange(vc);
}

void TigaReplica::onCrossShardVerifyReq(const TigaCrossShardVerifyReq& req) {
   std::unique_lock lck(crossShardConfirmQuorumMtx_);
   LOG(INFO) << "TigaCrossShardVerifyReq from " << req.shardId_ << "--gView "
             << gViewId_ << "--" << req.gViewId_;

   crossShardVerifyReqs_[req.shardId_] = std::move(req);
}
void TigaReplica::onCrossShardVerifyRep(const TigaCrossShardVerifyRep& rep) {
   std::unique_lock lck(crossShardConfirmQuorumMtx_);
   LOG(INFO) << "TigaCrossShardVerifyRep from " << rep.shardId_ << "--gView "
             << gViewId_ << "--" << rep.gViewId_;
   crossShardVerifyReps_[rep.shardId_] = std::move(rep);
}

void TigaReplica::onCMPrepare(const TigaCMPrepare& req) {
   std::unique_lock lck(cm_.mtx_);
   if (req.cViewId_ == cm_.viewId_) {
      if (req.gViewId_ > cm_.prepareGInfo_.gViewId_) {
         cm_.prepareGInfo_.gViewId_ = req.gViewId_;
         cm_.prepareGInfo_.gVec_ = req.gVec_;
         TigaCMPrepareReply rep;
         rep.cViewId_ = req.cViewId_;
         rep.gViewId_ = req.gViewId_;
         rep.replicaId_ = replicaId_;
         rep.shardId_ = shardId_;
         LOG(INFO) << "Send CMPrepareReply";
         vrProxies_[req.shardId_][req.replicaId_]->async_CMPrepareReply(rep);
      }

   }  // else: out-of-scope, use typical VR to support configManager's
      // recovery
}

void TigaReplica::onCMPrepareReply(const TigaCMPrepareReply& rep) {
   std::unique_lock lck(cm_.mtx_);
   if (rep.cViewId_ == cm_.viewId_) {
      if (rep.gViewId_ == cm_.prepareGInfo_.gViewId_) {
         cm_.vCMPrepareReps_[rep.replicaId_] = rep;
      }
   }
}

void TigaReplica::onCMCommit(const TigaCMCommit& msg) {
   std::unique_lock lck(cm_.mtx_);
   if (msg.cViewId_ == cm_.viewId_) {
      if (cm_.gInfo_.gViewId_ < msg.gViewId_) {
         cm_.gInfo_.gViewId_ = msg.gViewId_;
         cm_.gInfo_.gVec_ = msg.gVec_;
      }
   }
}

void TigaReplica::BroadcastViewChangeReq() {
   std::shared_lock lck(cm_.mtx_);
   TigaViewChangeReq req;
   req.shardId_ = shardId_;
   req.replicaId_ = replicaId_;
   req.gVec_ = cm_.gInfo_.gVec_;
   req.gViewId_ = cm_.gInfo_.gViewId_;

   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         if (sid == shardId_ && rid == replicaId_) {
            // myself
            std::unique_lock lck2(gNewsMtx_);
            gNews_.gVec_ = req.gVec_;
            gNews_.gViewId_ = req.gViewId_;
         } else {
            LOG(INFO) << "Brodcast to " << sid << ":" << rid
                      << "--gView=" << req.gViewId_;
            vrProxies_[sid][rid]->async_ViewChangeReq(req);
         }
      }
   }
}

void TigaReplica::onFailSignal(const TigaFailSignal& msg, TigaFailAck* ack,
                               const std::function<void()>& cb) {
   std::unique_lock lck(failedServerRecordMtx_);
   failedServersByGView_[msg.failedGView_].insert(
       {msg.failedShardId_, msg.failedReplicaId_});
   if (msg.failedShardId_ == shardId_ && msg.failedReplicaId_ == replicaId_) {
      // We need to fail
      failCb_ = [ack, cb](const TigaFailAck& r) {
         *ack = r;
         cb();
      };
      toFail_ = 1;
   }
}

void TigaReplica::onFailAck(const TigaFailAck& ack) {
   // We have confirmed that guy has failed, so start view Change
}