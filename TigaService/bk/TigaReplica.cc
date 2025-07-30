#include "TigaReplica.h"

TigaReplica::TigaReplica(const std::string& serverName,
                         const YAML::Node& config) {
   turnOnDebug_ = false;
   lastPrintTime_ = GetMicrosecondTimestamp();
   shardNum_ = config["site"]["server"].size();
   replicaNum_ = config["site"]["server"][0].size();
   clockOffsetMean_ = 0;
   clockOffsetStd_ = 0;
   clockError_ = 0;
   if (config["clockOffset"].IsDefined()) {
      clockOffsetMean_ = config["clockOffset"]["mean"].as<int>();
      clockOffsetStd_ = config["clockOffset"]["std"].as<int>();
      std::default_random_engine generator;
      generator.seed(time(0));
      std::normal_distribution<double> distribution(clockOffsetMean_,
                                                    clockOffsetStd_);
   }

#ifdef USE_SKEEN
   srand(time(0));
   serverLogicalClock_ = random() % 1000000;
#endif

   memset(localProxies_, '\0', sizeof(TigaLocalProxy*) * MAX_SHARD_NUM);
   memset(localRPCClients_, '\0', sizeof(rrr::Client*) * MAX_SHARD_NUM);
   memset(globalProxies_, '\0', sizeof(TigaGlobalProxy*) * MAX_REPLICA_NUM);
   memset(globalRPCClients_, '\0', sizeof(rrr::Client*) * MAX_REPLICA_NUM);
   memset(globalProxies_, '\0',
          sizeof(TigaViewChangeProxy*) * MAX_SHARD_NUM * MAX_REPLICA_NUM);
   memset(vrRPCClients_, '\0',
          sizeof(rrr::Client*) * MAX_SHARD_NUM * MAX_REPLICA_NUM);

   shardId_ = replicaId_ = UINT32_MAX;
   activeThreads_ = 0;
   lastUsedDdl_ = 0;
   cnters_ = 0;
   cnters2_ = 0;
   startTime = 0;
   holdNum_ = 0;
   releaseNum_ = 0;
   dqReplyNum_ = 0;

   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         std::string fullName =
             config["site"]["server"][sid][rid].as<std::string>();
         std::string thisServerName = fullName.substr(0, fullName.find(':'));
         std::string portName = fullName.substr(fullName.find(':') + 1);
         std::string ip = config["host"][thisServerName].as<std::string>();
         int port = std::stoi(portName);
         serverAddrs_[sid][rid] = ip + ":" + std::to_string(port);
         globalServerAddrs_[sid][rid] = ip + ":" + std::to_string(port + 1);
         localServerAddrs_[sid][rid] = ip + ":" + std::to_string(port + 2);
         vrServerAddrs_[sid][rid] = ip + ":" + std::to_string(port + 3);
         if (thisServerName == serverName) {
            shardId_ = sid;
            replicaId_ = rid;
         }
      }
   }

   specExec_ = false;
   if (config["spec_exec"].IsDefined() && config["spec_exec"].as<bool>()) {
      specExec_ = true;
   }
   LOG(INFO) << "my replicaId=" << replicaId_ << "\t shardId=" << shardId_
             << "--spec?=" << specExec_
             << "\t clockOffsetMean=" << clockOffsetMean_
             << "\t clockOffsetStd=" << clockOffsetStd_;
   cm_.Initialize(shardNum_);
   if (shardId_ == 0) {
      cm_.actionStatus_ = CM_MONITORING;
   }
   viewId_ = 0;
   gViewId_ = 0;
   lastNormalView_ = 0;
   gVec_.clear();
   gVec_.resize(shardNum_, 0);
   status_ = SERVER_STATUS::STATUS_NORMAL;
   serverSignalToCoord_ = CSTATUS_RUN;
   toFail_ = 0;
   gNews_.gVec_.clear();
   gNews_.gVec_.resize(shardNum_, 0);
   gNews_.gViewId_ = 0;
   startViewMsg_.gViewId_ = 0;
   startViewMsg_.viewId_ = 0;

   std::string workloadStr = config["bench"]["workload"].as<std::string>();
   if (workloadStr == "tpca") {
      sm_ = new MicroStateMachine(shardId_, replicaId_, shardNum_, replicaNum_,
                                  config);
   } else if (workloadStr == "tpcc") {
      sm_ = new TPCCStateMachine(shardId_, replicaId_, shardNum_, replicaNum_,
                                 config);
   } else {
      LOG(ERROR) << workloadStr << "--not implemented yet";
      assert(0);
   }

   execSequencer_.resize(sm_->TotalNumberofKeys());

   lastReleasedTxnDeadlines_.resize(sm_->TotalNumberofKeys(), 0);

   // to change to global and local proxy
   // intra-DC needs more requests, to be optimized in the future
   localRpcPoll_ = new PollMgr(2);
   globalRpcPoll_ = new PollMgr(2);
   vrRpcPoll_ = new PollMgr(1);

   nextSyncedLogId_ = 1;
   commitPoint_ = 0;
   executedLogId_ = 0;

   syncedLogList_.reserve(1000ul * 1000ul * 100);
   lastBroadcastSyncedLogId_ = 0;
   lastBroadcastCommittedLogId_ = 0;
   missingTxnKey_ = 0;
}

TigaReplica::~TigaReplica() {}

void TigaReplica::ConnectToOtherGlobalServers() {
   // only connects to the replicas belonging to the same shard
   uint32_t sid = shardId_;
   LOG(INFO) << "ConnectToOtherGlobalServers sid= " << sid;
   for (uint32_t rid = 0; rid < replicaNum_; rid++) {
      if (rid == replicaId_) {
         continue;
      }
      int ret = -1;
      LOG(INFO) << "Try rid= " << rid;
      globalRPCClients_[rid] = new rrr::Client(globalRpcPoll_);
      do {
         LOG(INFO) << "Connecting to sid=" << sid << "\t rid=" << rid << "\t"
                   << globalServerAddrs_[sid][rid];
         ret = globalRPCClients_[rid]->connect(
             globalServerAddrs_[sid][rid].c_str());
         if (ret == 0) {
            // success
            globalProxies_[rid] = new TigaGlobalProxy(globalRPCClients_[rid]);
            LOG(INFO) << "Connected to " << sid << "\t rid=" << rid << "\t"
                      << globalServerAddrs_[sid][rid];
         } else {
            ThreadSleepFor(1200000);
         }
      } while (ret != 0);
   }
}

void TigaReplica::ConnectToOtherLocalServers() {

   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      if (sid == shardId_) {
         continue;
      }
      int ret = -1;
      localRPCClients_[sid] = new rrr::Client(localRpcPoll_);
      do {
         LOG(INFO) << "Connecting to sid=" << sid << "\t"
                   << localServerAddrs_[sid][replicaId_];
         ret = localRPCClients_[sid]->connect(
             localServerAddrs_[sid][replicaId_].c_str());
         if (ret == 0) {
            // success
            localProxies_[sid] = new TigaLocalProxy(localRPCClients_[sid]);
            LOG(INFO) << "Connected to " << sid << "\t"
                      << localServerAddrs_[sid][replicaId_];
         } else {
            ThreadSleepFor(1200000);  // 1200ms
         }
      } while (ret != 0);
   }
}

void TigaReplica::Run() {
   mainTd_ = new std::thread(&TigaReplica::MainTd, this);
}

void TigaReplica::MainTd() {
   killed_ = false;
   replyNum_ = 0;
   uint64_t lastReportTime = GetMicrosecondTimestamp();
   uint64_t launchTime = GetMicrosecondTimestamp();

   uint32_t targetGView = 0;
   // HardCode the server to kill
   uint32_t replicaIdToKill = 0;
   uint32_t shardIdToKill = 1;
   std::string name;
   while (status_ != STATUS_TERMINATE) {
      if (status_ == STATUS_NORMAL) {
         if (threadMap_.empty()) {
            if (AmLeader()) {
               name = "LeaderDeadlineAgreementTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] = new std::thread(
                   &TigaReplica::LeaderDeadlineAgreementTd, this);

               name = "LeaderExecAgreementTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] =
                   new std::thread(&TigaReplica::LeaderExecAgreementTd, this);

               name = "LeaderCrossReplicaSyncTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] = new std::thread(
                   &TigaReplica::LeaderCrossReplicaSyncTd, this);

               if (specExec_) {
                  name = "LeadererHoldAndReleaseTdS";
                  LOG(INFO) << "Run " << name;
                  threadMap_[name] = new std::thread(
                      &TigaReplica::LeaderHoldAndReleaseTdS, this);

                  name = "LeaderSpecExecCheckTd";
                  LOG(INFO) << "Run " << name;
                  threadMap_[name] = new std::thread(
                      &TigaReplica::LeaderSpecExecCheckTd, this);

                  name = "LeaderSpecExecTd";
                  LOG(INFO) << "Run " << name;
                  threadMap_[name] =
                      new std::thread(&TigaReplica::LeaderSpecExecTd, this);

               } else {
                  name = "LeadererHoldAndReleaseTd";
                  LOG(INFO) << "Run " << name;
                  threadMap_[name] = new std::thread(
                      &TigaReplica::LeaderHoldAndReleaseTd, this);

                  // This td makes sure execAgree=True, i.e., agreement is
                  // complete
                  name = "LeaderExecCheckTd";
                  LOG(INFO) << "Run " << name;
                  threadMap_[name] =
                      new std::thread(&TigaReplica::LeaderExecCheckTd, this);

                  name = "LeaderExecuteTd";
                  LOG(INFO) << "Run " << name;
                  threadMap_[name] =
                      new std::thread(&TigaReplica::LeaderExecuteTd, this);
               }

               name = "LeaderReplyTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] =
                   new std::thread(&TigaReplica::LeaderReplyTd, this);

            } else {
               name = "FollowerHoldAndReleaseTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] = new std::thread(
                   &TigaReplica::FollowerHoldAndReleaseTd, this);

               name = "FollowerCrossReplicaSyncTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] = new std::thread(
                   &TigaReplica::FollowerCrossReplicaSyncTd, this);

               name = "FollowerExecuteTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] =
                   new std::thread(&TigaReplica::FollowerExecuteTd, this);

               // name = "FollowerReplyTd";
               // LOG(INFO) << "Run " << name;
               // threadMap_[name] =
               //     new std::thread(&TigaReplica::FollowerReplyTd, this);

#ifdef ENABLE_CHECKPOINT
               name = "FollowerExecuteCommitTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] =
                   new std::thread(&TigaReplica::FollowerExecuteCommitTd, this);
#endif
            }
            // sleep(1000);
         }
         lastNormalView_ = viewId_;
#ifdef ENABLE_FAILURE_RECOVERY
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
                             cm_.serverStatus_[sid][rid].status_ !=
                                 STATUS_FAILING ||
                         cm_.serverStatus_[sid][rid].gViewId_ < targetGView) {
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
#endif
      }
#ifdef ENABLE_FAILURE_RECOVERY
      else if (status_ == STATUS_FAILING) {
         std::shared_lock lck(gNewsMtx_);
         if (gNews_.gViewId_ > gViewId_) {
            gViewId_ = gNews_.gViewId_;
            gVec_ = gNews_.gVec_;
            viewId_ = gNews_.gVec_[shardId_];
            SendViewChange();
         }
         LOG(INFO) << "Failing...";
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
                  LOG(INFO)
                      << "SendCrossShardVerifyReplyTo shardId=" << kv.first;
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
      } else if (status_ == STATUS_TERMINATE) {
         break;
      }

      uint64_t nowTime = GetMicrosecondTimestamp();
      if (false && nowTime - lastReportTime >= 1000 * 1000) {
         LOG(INFO) << "commitPoint=" << commitPoint_ << "\t"
                   << "syncPoint=" << nextSyncedLogId_ - 1 << "\t"
                   << "lastBroadcastSyncedLogId_=" << lastBroadcastSyncedLogId_
                   << "\t"
                   << "lastBroadcastCommittedLogId_="
                   << lastBroadcastCommittedLogId_ << "\t"
                   << "serverSignal=" << serverSignalToCoord_ << "\t"
                   << "earlyBufferSize=" << cnters_ << "\t"
                   << "lastReqId_=" << lastReqId_ << "\t"
                   << "lastDdlCheckReqId_=" << lastDdlCheckReqId_ << "\t"
                   << "lastDdlAgreedReqId_=" << lastDdlAgreedReqId_ << "\t"
                   << "lastExecReqId_=" << lastExecReqId_ << "\t"
                   << "lastRepliedReqId_=" << lastRepliedReqId_ << "\t"
                   << "dqReplyNum_=" << dqReplyNum_ << "\t"
                   << "replyNum=" << replyNum_ << "\t"
                   << "holdNum=" << holdNum_ << "\t"
                   << "releaseNum=" << releaseNum_ << "\t"
                   << "toExecuQU=" << toExecQu_.size_approx();

         lastReportTime = nowTime;
      }
#endif
#ifdef CREATE_FAILURE
      if (shardId_ == 0) {
         SendHeartBeat();
         if (cm_.actionStatus_ == CM_PREPARING_NEW_GVIEW) {
            assert(AmCMLeader());
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
                  LOG(INFO)
                      << "Rady Lview " << i << "--" << cm_.gInfo_.gVec_[i];
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

            if (AmCMLeader() && nextSyncedLogId_ >= 600000 && (!killed_)) {
               cm_.actionStatus_ = CM_KILLING;
               serverSignalToCoord_ = CSTATUS_SUSPEND;
               targetGView = cm_.gInfo_.gViewId_ + 1;
               LOG(INFO) << "Change to SUSPEND  nowTime=" << nowTime;
               killed_ = true;
               // Send fail signal to a leader
               if (shardNum_ > 1) {
                  TigaFailSignal fs;
                  fs.failedGView_ = gViewId_;
                  // hard code for now
                  fs.failedReplicaId_ = replicaIdToKill;
                  fs.failedShardId_ = shardIdToKill;
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
            if (cm_.serverStatus_[shardIdToKill][replicaIdToKill].status_ ==
                STATUS_FAILING) {
               // TODO: replace the condition with "Receiving failsignalack"
               cm_.prepareGInfo_.gViewId_++;
               // Here can be generalize and specify viewIds which are not
               // colocated in one region
               cm_.prepareGInfo_.gVec_.clear();
               cm_.prepareGInfo_.gVec_.resize(shardNum_,
                                              cm_.prepareGInfo_.gViewId_);
               for (uint32_t i = 0; i < shardNum_; i++) {
                  LOG(INFO) << "Prepare LView " << i << "\t"
                            << cm_.prepareGInfo_.gVec_[i];
               }
               BroadcastCMPrepare();
               cm_.actionStatus_ = CM_PREPARING_NEW_GVIEW;
            }
         }
      }
#endif

      ThreadSleepFor(10000);
      if (status_ == STATUS_TERMINATE) {
         LOG(INFO) << threadMap_.size() << " working threads ";
         for (auto& kv : threadMap_) {
            LOG(INFO) << "Wait for " << kv.first << " to complete";
            kv.second->join();
            delete kv.second;
            LOG(INFO) << kv.first << " completed";
         }
         break;
      }
   }

   LOG(INFO) << "Exit Main";
}

void TigaReplica::Stop() {
   status_ = SERVER_STATUS::STATUS_TERMINATE;
   LOG(INFO) << "Close Connections";

   for (uint32_t i = 0; i < shardNum_; i++) {
      if (localRPCClients_[i]) {
         localRPCClients_[i]->close_and_release();
         delete localProxies_[i];
      }
   }
   LOG(INFO) << "localRPC closed";

   for (uint32_t i = 0; i < replicaNum_; i++) {
      if (globalRPCClients_[i]) {
         globalRPCClients_[i]->close_and_release();
         delete globalProxies_[i];
      }
   }

   LOG(INFO) << "globalRPC closed";

   for (uint32_t i = 0; i < shardNum_; i++) {
      for (uint32_t j = 0; j < replicaNum_; j++) {
         if (vrRPCClients_[i][j]) {
            vrRPCClients_[i][j]->close_and_release();
            delete vrProxies_[i][j];
         }
      }
   }
   LOG(INFO) << "vrRPC closed";

   LOG(INFO) << "Terminating...";
   mainTd_->join();
   delete mainTd_;
}

void TigaReplica::ClearContext() {
   // Clean Deadline Quorum
   for (uint32_t i = 0; i < HASH_PARTITION_NUM; i++) {
      ddlMap_[i].deadlineQ_.clear();
   }

   // Clean HoldBuffer
   holdBuffer_.clear();
   holdBuffer2_.clear();

   // Clean execSequencer_
   execSequencer_.clear();

   TigaLogEntry* entry;
   while (toHoldAndReleaseQu_.try_dequeue(entry)) {
   }
   while (toDdlSyncQu_.try_dequeue(entry)) {
   }
   while (toExecCheckQu_.try_dequeue(entry)) {
   }
   while (toExecQu_.try_dequeue(entry)) {
   }
   std::pair<TigaLogEntry*, uint32_t> e;
   while (toExecQuF_.try_dequeue(e)) {
   }
   std::pair<LogInfo*, uint32_t> e2;
   while (toReplyQuF_.try_dequeue(e2)) {
   }
   CommitReplyInfo ce;
   while (toCommitRepyQu_.try_dequeue(ce)) {
   }
   for (uint32_t i = 0; i < HASH_PARTITION_NUM; i++) {
      pendingInfoToCommitReply_[i].clear();
      pendingCommitRepyHandlers_[i].clear();
   }

   LogInfo* info;
   while (syncedLogInfoQu_.try_dequeue(info)) {
      pendingLogInfo_[info->logId_] = info;
   }
   LOG(INFO) << "pendingLogInfoSize=" << pendingLogInfo_.size();
   while (!pendingLogInfo_.empty()) {
      if (pendingLogInfo_.begin()->first - 1 == syncedLogList_.size()) {
         syncedLogList_.push_back(pendingLogInfo_.begin()->second);
         pendingLogInfo_.erase(pendingLogInfo_.begin());
      }
   }
   LOG(INFO) << "pendingLogInfoSize Size2=" << pendingLogInfo_.size()
             << "--syncedLogSize=" << syncedLogList_.size()
             << "--nextSyncedLogId=" << nextSyncedLogId_;

   while (followerCommitExecuteQu_.try_dequeue(info)) {
      rebuffer_[info->logId_] = info;
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
   pendingLogInfo_.clear();
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

void TigaReplica::onNormalRequest(const TigaReq& req, TigaReply* rep,
                                  const std::function<void()>& cb) {

   ClientCommand* cmd = new ClientCommand(req.cmd_);
   uint64_t txnKey = cmd->TxnKey();
   ClientCommand* insertedCmd = InsertEntry(cmdMap_, txnKey, cmd);
   if (insertedCmd != cmd) {
      // duplicate one, delete the newly created cmd
      delete cmd;
   }

   TigaLogEntry* entry = new TigaLogEntry();
   entry->cmd_ = cmd;
   entry->sendTime_ = req.sendTime_;
   // make sure each txn has a different ddlRank
#if defined(USE_LOGICAL_TIME)
   // entry->localDdlRank_ =
   //     CONCAT_UINT32((100 - req.cmd_.clientId_), req.cmd_.reqId_);
   entry->localDdlRank_ = req.bound_ + req.cmd_.reqId_;
   // entry->localDdlRank_ = req.cmd_.reqId_;
#elif defined(USE_SKEEN)
   entry->localDdlRank_ = serverLogicalClock_.fetch_add(1);
#else
   entry->localDdlRank_ = req.sendTime_ + req.bound_;
   // if (req.cmd_.reqId_ % 1000 == 1) {
   //    LOG(INFO) << "reqId=" << req.cmd_.reqId_ << "--bound=" << req.bound_
   //              << "--sendTime=" << entry->sendTime_ << "\t"
   //              << "--localDdlRank=" << entry->localDdlRank_;
   // }
#endif

   sm_->InitializeRelatedShards(cmd->txnType_, &(cmd->ws_),
                                &(entry->shardKeyMap_));
   for (auto& key : entry->shardKeyMap_[shardId_]) {
      entry->localKeys_.push_back(key);
   }
   entry->replyHandler_ = [rep, cb](const TigaReply& r) {
      *rep = r;
      cb();
   };

   // LOG(INFO) << "reqId=" << entry->cmd_->reqId_;
   lastReqId_ = entry->cmd_->reqId_;
   // entry->eqTime_ = GetMicrosecondTimestamp();
   toHoldAndReleaseQu_.enqueue(entry);
   // toExecQu_.enqueue(entry);
}

void TigaReplica::onDispatchRequest(const TigaDispatchRequest& req,
                                    TigaDispatchReply* rep,
                                    const std::function<void()>& cb) {
   sm_->PreRead(req.txnType_, &(req.input_), &(rep->result_));
   rep->shardId_ = shardId_;
   cb();
}

void TigaReplica::onCommitRequest(const TigaCommitRequest& req,
                                  TigaCommitReply* rep,
                                  const std::function<void()>& cb) {}

void TigaReplica::onReconcliationRequest(const TigaReconcliationReq& req,
                                         TigaReply* rep,
                                         const std::function<void()>& cb) {
   // When receiving non-serializable results
   // Send commitRequests to trigger rollback, if it has not been triggered
   assert(specExec_);
   if (req.gViewId_ < gViewId_) {
      return;
   }
   uint64_t txnKey = CONCAT_UINT32(req.clientId_, req.reqId_);
   std::function<void(const TigaReply&)> repyHandler =
       [rep, cb](const TigaReply& r) {
          *rep = r;
          cb();
       };
   // LOG(INFO) << "Reconcliation " << req.clientId_ << ":" << req.reqId_;
   toCommitRepyQu_.enqueue({txnKey, repyHandler});
}

void TigaReplica::onDeadlineAgreementRequest(
    const TigaDeadlineAgreeRequest& req) {
   if (status_ == STATUS_FAILING) {
      return;
   }
   if (req.gViewId_ < gViewId_) {
      return;
   }
   // if (req.shardId_ == 2 && shardId_ == 0) {
   //    LOG(INFO) << "sz=" << req.deadlineRanks_.size();
   // }
   toDdlSyncRequestQu_.enqueue(req);
}

void TigaReplica::onExecAgreementRequest(const TigaExecAgreeRequest& req) {
   if (status_ == STATUS_FAILING) {
      return;
   }
   if (req.gViewId_ < gViewId_) {
      return;
   }
   toExecAgreeRequestQu_.enqueue(req);
}

void TigaReplica::onInquireServerSyncStatus(
    const TigaServerSyncStatusRequest& req, TigaServerSyncStatusReply* rep) {
   rep->gViewId_ = gViewId_;
   rep->viewId_ = viewId_;
   rep->latestSyncedLogId_ = nextSyncedLogId_ - 1;
   rep->shardId_ = shardId_;
   rep->replicaId_ = replicaId_;
   rep->status_ = status_;
   if (AmCMLeader()) {
      rep->signal_ = serverSignalToCoord_;
   } else {
      rep->signal_ = 0;
   }
   rep->token_ = req.token_;
}

void TigaReplica::LeaderHoldAndReleaseTdS() {
   TigaLogEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   uint32_t enqCnt = 0;
   uint64_t currentTime = GetMicrosecondTimestamp();
   bool debug = false;
   uint64_t lastCheckTime = 0;
   uint64_t checkInterval = 0;
   activeThreads_.fetch_add(1);
   std::default_random_engine generator;
   generator.seed(time(0));
   std::normal_distribution<double> distribution(clockOffsetMean_,
                                                 clockOffsetStd_);
   uint64_t lastReleaseTime = 0;
   std::vector<TigaLogEntry*> candidateEntries;
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      // debug = (GetMicrosecondTimestamp() - currentTime >= 31 * 1000ul *
      // 1000ul);
      while ((cnt = toHoldAndReleaseQu_.try_dequeue_bulk(entries, UINT8_MAX)) >
             0) {
         for (uint32_t i = 0; i < cnt; i++) {
            holdNum_++;
            TigaLogEntry* entry = entries[i];
            uint64_t txnKey = entry->cmd_->TxnKey();
            if (entry->deadlineAgreed_) {
               // Come from DdlAgreementTd
               assert(entry->shardKeyMap_.size() > 1);
               holdBuffer2_[{entry->agreedDdlRank_, txnKey}] = entry;
            } else {
               // Check whether the txn can enter the early buffer
               uint64_t nowTime =
                   GetMicrosecondTimestamp(int(distribution(generator)));
               if (nowTime < entry->sendTime_) {
                  entry->owd_ = 0;
               } else {
                  entry->owd_ = nowTime - entry->sendTime_;
               }
               uint64_t newDdl = entry->localDdlRank_;
               for (auto& key : entry->localKeys_) {
                  newDdl = std::max(newDdl, lastReleasedTxnDeadlines_[key] + 1);
               }
               entry->localDdlRank_ = newDdl;

               if (entry->shardKeyMap_.size() == 1) {
                  // single-shard txn
                  entry->agreedDdlRank_ = newDdl;
                  entry->deadlineAgreed_ = true;
                  entry->execAgreed_ = true;
                  entry->execStatus_ = FastExecuting;
                  holdBuffer_[{entry->agreedDdlRank_, txnKey}] = entry;
               } else {
                  entry->execStatus_ = ExecInit;
                  toDdlSyncQu_.enqueue(entry);
                  holdBuffer_[{entry->localDdlRank_, txnKey}] = entry;
               }
            }
         }
      }

#if !defined(USE_LOGICAL_TIME) && !defined(USE_SKEEN)
      uint64_t nowTime = GetMicrosecondTimestamp(int(distribution(generator)));
      // nowTime = std::max(nowTime, lastReleaseTime + 1);
#else
      // always release
      uint64_t nowTime = lastReleaseTime + 1;
      if (!holdBuffer_.empty()) {
         nowTime = std::max(nowTime, holdBuffer_.rbegin()->first.first + 1);
      }
#endif

      candidateEntries.clear();
      while ((!holdBuffer_.empty()) &&
             nowTime > holdBuffer_.begin()->first.first) {
         TigaLogEntry* entry = holdBuffer_.begin()->second;
         uint64_t ddlRank = holdBuffer_.begin()->first.first;
         for (auto& key : entry->localKeys_) {
            lastReleasedTxnDeadlines_[key] =
                std::max(lastReleasedTxnDeadlines_[key], ddlRank);
         }
         candidateEntries.push_back(entry);
         holdBuffer_.erase(holdBuffer_.begin());
      }

      while ((!holdBuffer2_.empty()) &&
             nowTime > holdBuffer2_.begin()->first.first) {
         TigaLogEntry* entry = holdBuffer2_.begin()->second;
         uint64_t ddlRank = holdBuffer2_.begin()->first.first;
         for (auto& key : entry->localKeys_) {
            lastReleasedTxnDeadlines_[key] =
                std::max(lastReleasedTxnDeadlines_[key], ddlRank);
         }
         candidateEntries.push_back(entry);
         holdBuffer2_.erase(holdBuffer2_.begin());
      }

      for (auto& e : candidateEntries) {
         enqCnt++;
         // if (enqCnt % 10000 == 1) {
         //    LOG(INFO) << "Enqueue Check enCnt=" << enqCnt;
         // }
         LeaderCheckForRelease(e);
         // toExecCheckQu_.enqueue(e);
      }
      lastReleaseTime = nowTime;
   }
   LOG(INFO) << "LeaderHoldAndReleaseTd Terminated";
   activeThreads_.fetch_sub(1);
}

void TigaReplica::LeaderHoldAndReleaseTd() {
   TigaLogEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   uint32_t enqCnt = 0;
   uint64_t currentTime = GetMicrosecondTimestamp();
   bool debug = false;
   uint64_t lastCheckTime = 0;
   uint64_t checkInterval = 0;
   activeThreads_.fetch_add(1);
   std::default_random_engine generator;
   generator.seed(time(0));
   std::normal_distribution<double> distribution(clockOffsetMean_,
                                                 clockOffsetStd_);
   uint64_t lastReleaseTime = 0;
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      // debug = (GetMicrosecondTimestamp() - currentTime >= 31 * 1000ul *
      // 1000ul);
      while ((cnt = toHoldAndReleaseQu_.try_dequeue_bulk(entries, UINT8_MAX)) >
             0) {
         for (uint32_t i = 0; i < cnt; i++) {
            holdNum_++;
            TigaLogEntry* entry = entries[i];
            uint64_t txnKey = entry->cmd_->TxnKey();
            if (entry->deadlineAgreed_) {
               // Come from DdlAgreementTd
               assert(entry->shardKeyMap_.size() > 1);
               if (entry->agreedDdlRank_ != entry->localDdlRank_) {
                  RemovePlaceHolder(entry, true);
                  InsertPlaceHolder(entry, false);
                  holdBuffer_.erase({entry->localDdlRank_, txnKey});
                  holdBuffer_[{entry->agreedDdlRank_, txnKey}] = entry;
               }
            } else {
               // Check whether the txn can enter the early buffer
               uint64_t nowTime =
                   GetMicrosecondTimestamp(int(distribution(generator)));

               if (nowTime < entry->sendTime_) {
                  entry->owd_ = 0;
               } else {
                  entry->owd_ = nowTime - entry->sendTime_;
               }
               uint64_t newDdl = entry->localDdlRank_;
               for (auto& key : entry->localKeys_) {
                  newDdl = std::max(newDdl, lastReleasedTxnDeadlines_[key] + 1);
               }
               entry->localDdlRank_ = newDdl;

               if (entry->shardKeyMap_.size() == 1) {
                  // single-shard txn
                  entry->agreedDdlRank_ = newDdl;
                  entry->deadlineAgreed_ = true;
                  entry->execAgreed_ = true;
                  entry->execStatus_ = FastExecuting;
                  InsertPlaceHolder(entry, false);
                  holdBuffer_[{entry->agreedDdlRank_, txnKey}] = entry;
               } else {
                  entry->execStatus_ = ExecInit;
                  toDdlSyncQu_.enqueue(entry);
                  InsertPlaceHolder(entry, true);
                  holdBuffer_[{entry->localDdlRank_, txnKey}] = entry;
               }
            }
         }
      }

      uint64_t nowTime = GetMicrosecondTimestamp(int(distribution(generator)));
      // nowTime = std::max(nowTime, lastReleaseTime + 1);
      std::vector<TigaLogEntry*> entriesToDel;
      for (auto& kv : holdBuffer_) {
         TigaLogEntry* entry = kv.second;
         uint64_t timeRank = kv.first.first;
#if !defined(USE_LOGICAL_TIME) && !defined(USE_SKEEN)
         if (nowTime <= timeRank) {
            break;
         }
#endif
         if (entry->deadlineAgreed_ == false) {
            continue;
         }
         if (CanRelease(entry, entry->agreedDdlRank_)) {
            // if (entry->cmd_->reqId_ % 1000 == 1) {
            //    uint64_t nt = GetMicrosecondTimestamp();
            //    LOG(INFO) << "Release " << entry->cmd_->reqId_
            //              << "--duration=" << nt - entry->sendTime_ <<
            //              "--bound="
            //              << entry->localDdlRank_ - entry->sendTime_
            //              << "--timeRank=" << timeRank << "\t"
            //              << "--sendTime=" << entry->sendTime_ << "\t"
            //              << "--localDdlRank=" << entry->localDdlRank_ << "\t"
            //              << "\tnt=" << nt << "\tnowTime=" << nowTime;
            // }

            if (entry->shardKeyMap_.size() > 1) {
               // multi-shard txn, require exec agreement
               toExecSyncQu_.enqueue(entry);
            } else {
               entry->execAgreed_ = true;
            }
            toExecCheckQu_.enqueue(entry);
            RemovePlaceHolder(entry, false);
            entry->execStatus_ = FastExecuted;
            entriesToDel.push_back(entry);
         }
      }
      for (auto& e : entriesToDel) {
         holdBuffer_.erase({e->agreedDdlRank_, e->cmd_->TxnKey()});
         // Update record after releasing every txns
         for (auto& key : e->localKeys_) {
            lastReleasedTxnDeadlines_[key] =
                std::max(lastReleasedTxnDeadlines_[key], e->agreedDdlRank_);
         }
      }
      // lastReleaseTime = nowTime;
   }
   LOG(INFO) << "LeaderHoldAndReleaseTd Terminated";
   activeThreads_.fetch_sub(1);
}

void TigaReplica::LeaderExecCheckTd() {
   TigaLogEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   uint64_t lastPrintTime = 0;
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toExecCheckQu_.try_dequeue_bulk(entries, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            TigaLogEntry* entry = entries[i];
            assert(entry->deadlineAgreed_);
            execEntryQu_.push(entry);
         }
      }

      // Try Dequeue
      while (!execEntryQu_.empty()) {
         TigaLogEntry* entry = execEntryQu_.front();
         if (entry->execAgreed_) {
            toExecQu_.enqueue(entry);
            execEntryQu_.pop();
         } else {
            break;
         }
      }
      if (toExecCheckQu_.size_approx() < UINT8_MAX) {
         ThreadSleepFor(1000);
      }
      // uint64_t nowTime = GetMicrosecondTimestamp();
      // if (nowTime - lastPrintTime >= 1000 * 1000ul) {
      //    LOG(INFO) << "execEntryQu_.size=" << execEntryQu_.size();
      //    lastPrintTime = nowTime;
      // }
   }
   LOG(INFO) << "LeaderExecCheckTd Terminated";
   activeThreads_.fetch_sub(1);
}

void TigaReplica::UpdateDeadlineRecord(const uint64_t txnKey,
                                       const uint32_t shardId,
                                       const uint64_t ddl,
                                       TigaLogEntry* entry) {
   // LOG(INFO) << HIGH_32BIT(txnKey) << ":" << LOW_32BIT(shardId) << "--" <<
   // ddl
   //           << "--local?--" << (entry != NULL);
   DeadlineQMap& dqm = ddlMap_[HASH_PARTITION_ID(txnKey)];
   // std::unique_lock lck(dqm.mtx_);
   DeadlineQItem& dqi = dqm.deadlineQ_[txnKey];
   dqi.shardIds_[dqi.itemCnt_] = shardId;
   dqi.ddls_[dqi.itemCnt_] = ddl;
   dqi.itemCnt_++;
   if (dqi.itemCnt_ == 1) {
      dqi.aggregatedDdl_ = ddl;
   } else {
      dqi.aggregatedDdl_ = std::max(ddl, dqi.aggregatedDdl_);
   }

   if (entry) {
      dqi.entry_ = entry;
   }
   if (dqi.entry_) {
      if (dqi.entry_->shardKeyMap_.size() == dqi.itemCnt_) {
         // dqi.entry_->owd_ = GetMicrosecondTimestamp() -
         // dqi.entry_->sendTime_;
         dqi.entry_->agreedDdlRank_ = dqi.aggregatedDdl_;
         dqi.entry_->deadlineAgreed_ = true;
         // dqi.entry_->ddlAgreeTime_ = GetMicrosecondTimestamp();
         assert(dqi.entry_->localDdlRank_ <= dqi.entry_->agreedDdlRank_);
         toHoldAndReleaseQu_.enqueue(dqi.entry_);
         dqm.deadlineQ_.erase(txnKey);
         // LOG(INFO) << "DDL agreed " << HIGH_32BIT(txnKey) << ":"
         //           << LOW_32BIT(shardId) << "--" << ddl << "--local?--"
         //           << (entry != NULL);
      }
   }
}

void TigaReplica::LeaderDeadlineAgreementTd() {
   TigaLogEntry* entries[UINT8_MAX];
   TigaDeadlineAgreeRequest reqEntries[UINT8_MAX];
   uint32_t cnt = 0;
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      // Broadcast DeadlineAgreement Request
      TigaDeadlineAgreeRequest reqs[MAX_SHARD_NUM];
      while ((cnt = toDdlSyncQu_.try_dequeue_bulk(entries, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            TigaLogEntry* entry = entries[i];
            // entry->ddlSyncTime_ = GetMicrosecondTimestamp();
            lastDdlCheckReqId_ = entry->cmd_->reqId_;
            uint64_t txnKey = entry->cmd_->TxnKey();
            UpdateDeadlineRecord(txnKey, shardId_, entry->localDdlRank_, entry);

            for (auto& kv : entry->shardKeyMap_) {
               uint32_t sid = kv.first;
               reqs[sid].txnKeys_.push_back(txnKey);
               reqs[sid].deadlineRanks_.push_back(entry->localDdlRank_);
            }
         }
      }
      for (uint32_t sid = 0; sid < shardNum_; sid++) {
         if (sid == shardId_) {
            continue;
         }
         if (reqs[sid].txnKeys_.empty()) {
            continue;
         }
         reqs[sid].replicaId_ = replicaId_;
         reqs[sid].shardId_ = shardId_;
         reqs[sid].viewId_ = viewId_;
         reqs[sid].gViewId_ = gViewId_;
         // LOG(INFO) << "Send ddl agreement to " << sid;
         Future::safe_release(
             localProxies_[sid]->async_DeadlineAgreeRequest(reqs[sid]));
      }
      while ((cnt = toDdlSyncRequestQu_.try_dequeue_bulk(reqEntries,
                                                         UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            const TigaDeadlineAgreeRequest& req = reqEntries[i];
            for (uint32_t j = 0; j < req.txnKeys_.size(); j++) {
               UpdateDeadlineRecord(req.txnKeys_[j], req.shardId_,
                                    req.deadlineRanks_[j]);
            }
         }
      }
      if (toDdlSyncQu_.size_approx() <= 50) {
         ThreadSleepFor(2000);
      }
   }
   LOG(INFO) << "LeaderDeadlineAgreementTd Terminated";
   activeThreads_.fetch_sub(1);
}

void TigaReplica::LeaderExecAgreementTd() {
   TigaLogEntry* entries[UINT8_MAX];
   TigaExecAgreeRequest reqEntries[MAX_SHARD_NUM];
   uint32_t cnt = 0;
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      // Broadcast ExecAgreement Request
      TigaExecAgreeRequest reqs[MAX_SHARD_NUM];
      while ((cnt = toExecSyncQu_.try_dequeue_bulk(entries, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            TigaLogEntry* entry = entries[i];
            uint64_t txnKey = entry->cmd_->TxnKey();
            ExecAgreeQItem& execQ =
                agreeQMap_[HASH_PARTITION_ID(txnKey)].agreeQ_[txnKey];
            execQ.entry_ = entry;
            execQ.itemCnt_++;
            for (auto& kv : entry->shardKeyMap_) {
               uint32_t sid = kv.first;
               reqs[sid].txnKeys_.push_back(txnKey);
            }
            if (execQ.entry_->shardKeyMap_.size() == execQ.itemCnt_) {
               // quroum established
               execQ.entry_->execAgreed_ = true;
               // Remove it from the map
               agreeQMap_[HASH_PARTITION_ID(txnKey)].agreeQ_.erase(txnKey);
            }
         }
      }
      for (uint32_t sid = 0; sid < shardNum_; sid++) {
         if (sid == shardId_) {
            continue;
         }
         if (reqs[sid].txnKeys_.empty()) {
            continue;
         }
         reqs[sid].replicaId_ = replicaId_;
         reqs[sid].shardId_ = shardId_;
         reqs[sid].viewId_ = viewId_;
         reqs[sid].gViewId_ = gViewId_;
         // LOG(INFO) << "Send ddl agreement to " << sid;
         Future::safe_release(
             localProxies_[sid]->async_ExecAgreeRequest(reqs[sid]));
      }
      while ((cnt = toExecAgreeRequestQu_.try_dequeue_bulk(reqEntries,
                                                           UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            const TigaExecAgreeRequest& req = reqEntries[i];
            for (uint32_t j = 0; j < req.txnKeys_.size(); j++) {
               uint64_t txnKey = req.txnKeys_[j];
               ExecAgreeQItem& execQ =
                   agreeQMap_[HASH_PARTITION_ID(txnKey)].agreeQ_[txnKey];
               execQ.itemCnt_++;
               if (execQ.entry_ &&
                   execQ.entry_->shardKeyMap_.size() == execQ.itemCnt_) {
                  // quroum established
                  execQ.entry_->execAgreed_ = true;
                  // Remove it from the map
                  agreeQMap_[HASH_PARTITION_ID(txnKey)].agreeQ_.erase(txnKey);
               }
            }
         }
      }
      ThreadSleepFor(2000);
   }
   LOG(INFO) << "LeaderExecAgreementTd Terminated";
   activeThreads_.fetch_sub(1);
}

void TigaReplica::LeaderCrossReplicaSyncTd() {
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      uint64_t startTime = GetMicrosecondTimestamp();
      BroadcastInterReplicaSync();
      uint64_t endTime = GetMicrosecondTimestamp();
      if (interDCYieldPeriodUs > 0 &&
          endTime - startTime < interDCYieldPeriodUs) {
         ThreadSleepFor(interDCYieldPeriodUs - (endTime - startTime));
      }
   }
   LOG(INFO) << "LeaderCrossReplicaSyncTd Terminated";
   activeThreads_.fetch_sub(1);
}

void TigaReplica::TryExecute(TigaLogEntry* entry) {
   assert(specExec_);
   std::vector<TigaLogEntry*> candidates;
   for (auto& key : entry->localKeys_) {
      TigaLogEntry* candidate = TrySpecExecuteByKey(key);
      if (candidate) {
         candidates.push_back(candidate);
      }
   }
   // candidates.push_back(entry);
   std::vector<TigaLogEntry*> newCandidates;
   do {
      newCandidates.clear();
      for (TigaLogEntry* e : candidates) {
         for (auto& key : e->localKeys_) {
            TigaLogEntry* candidate = TrySpecExecuteByKey(key);
            if (candidate) {
               newCandidates.push_back(candidate);
            }
         }
      }
      candidates = newCandidates;
   } while (!candidates.empty());
}

void TigaReplica::LeaderCheckForRelease(TigaLogEntry* entry) {
   uint64_t txnKey = entry->cmd_->TxnKey();
   // holdingTxns_[txnKey] = (entry);
   if (entry->shardKeyMap_.size() == 1) {
      assert(entry->deadlineAgreed_ &&
             entry->agreedDdlRank_ == entry->localDdlRank_);
      if (CanInitialRelease(entry, false)) {
         // Fast Exec
         toExecQuF_.enqueue({entry, FastExecuting});
         entry->execStatus_ = FastExecuted;
         // holdingTxns_.erase(txnKey);
      } else {
         entry->execStatus_ = FastExecuting;
         InsertPlaceHolder(entry, false);
      }
   } else {
      if (entry->execStatus_ == CommitExecuted ||
          entry->execStatus_ == CommitExecuting) {
         LOG(ERROR) << "Should Not come here " << entry->cmd_->clientId_ << ":"
                    << entry->cmd_->reqId_ << "--status=" << entry->execStatus_;
         assert(0);
      } else if (entry->execStatus_ == ExecInit) {
         // First Come
         if (CanInitialRelease(entry, false)) {
            LOG(INFO) << "Can InitiaExec " << entry->cmd_->clientId_ << ":"
                      << entry->cmd_->reqId_;
            toExecQuF_.enqueue({entry, PrepareExecuting});
            entry->execStatus_ = PrepareExecuted;
         } else {
            entry->execStatus_ = PrepareExecuting;
         }

         std::string keystr = "";
         for (auto& key : entry->localKeys_) {
            keystr += std::to_string(key) + ",";
         }
         LOG(INFO) << "Insert placeholder " << entry->cmd_->clientId_ << ":"
                   << entry->cmd_->reqId_ << "--keystr=" << keystr << "\t"
                   << "ddl=" << entry->localDdlRank_;

         InsertPlaceHolder(entry, true);
      } else if (entry->execStatus_ == PrepareExecuting) {
         // Second Come [Previous one has not been executed]
         assert(entry->deadlineAgreed_);
         entry->execStatus_ = FastExecuting;
         if (entry->agreedDdlRank_ != entry->localDdlRank_) {
            // No need to rollback, but need to clear the previous
            // PreExecuting placeholder
            LOG(INFO) << "Remove1=" << entry->cmd_->clientId_ << ":"
                      << entry->cmd_->reqId_;
            RemovePlaceHolder(entry, true);
            InsertPlaceHolder(entry, false);
            // Since we have removed some placeholder
            // it might enable some txns to be released
            TryExecute(entry);
         }
      } else if (entry->execStatus_ == PrepareExecuted) {
         // Previous one has been executed
         if (entry->agreedDdlRank_ != entry->localDdlRank_) {
            LOG(INFO) << "Remove2=" << entry->cmd_->clientId_ << ":"
                      << entry->cmd_->reqId_;
            // Need rollback
            RemovePlaceHolder(entry, true);
            // LOG(INFO) << "ExecQuF " << entry->cmd_->reqId_ <<
            // "\t"
            //           << "Rollbacking";
            toExecQuF_.enqueue({entry, Rollbacking});  // comment for
            // debug now
            entry->execStatus_ = FastExecuting;
            // Insert new placeholder
            InsertPlaceHolder(entry, false);
            TryExecute(entry);
         } else {
            // No need to rollback, the preExecute is valid
            // GJK: But need to wait until it is execAgreed
            toExecQuF_.enqueue({entry, CommitExecuting});
            entry->execStatus_ = CommitExecuted;
            for (auto& k : entry->localKeys_) {
               if (execSequencer_[k].begin()->second != entry) {
                  TigaLogEntry* e = execSequencer_[k].begin()->second;
                  LOG(ERROR) << "Ahead " << e->cmd_->clientId_ << ":"
                             << e->cmd_->reqId_ << "--status=" << e->execStatus_
                             << "--local=" << e->localDdlRank_
                             << "--agreed=" << e->agreedDdlRank_;
                  LOG(ERROR) << "My " << entry->cmd_->clientId_ << ":"
                             << entry->cmd_->reqId_
                             << "--status=" << entry->execStatus_
                             << "--local=" << entry->localDdlRank_
                             << "--agreed=" << entry->agreedDdlRank_;
               }
               assert(execSequencer_[k].begin()->second == entry);
               execSequencer_[k].erase(execSequencer_[k].begin());
            }
            // holdingTxns_.erase(txnKey);
            TryExecute(entry);
         }
      }
   }
}

TigaLogEntry* TigaReplica::TrySpecExecuteByKey(uint32_t key) {
   if (execSequencer_[key].empty()) {
      return NULL;
   }
   // TigaLogEntry* candidate = execSequencer_[key].begin()->second;
   // if ((candidate->execStatus_ == PrepareExecuting ||
   //      candidate->execStatus_ == CommitExecuting ||
   //      candidate->execStatus_ == FastExecuting) &&
   //     CanExec(candidate, true)) {
   //    // Can Execute
   //    if (candidate->execStatus_ == PrepareExecuting) {
   //       toExecQuF_.enqueue({candidate, PrepareExecuting});
   //       candidate->execStatus_ = PrepareExecuted;
   //       // LOG(INFO) << "Prepared " << candidate->cmd_->clientId_ << ":"
   //       //           << candidate->cmd_->reqId_;
   //       // preparedTxns_.insert(candidate);
   //    } else if (candidate->execStatus_ == CommitExecuting) {
   //       toExecQuF_.enqueue({candidate, CommitExecuting});
   //       candidate->execStatus_ = CommitExecuted;
   //       // Remove PlaceHolder
   //       for (auto& k : candidate->localKeys_) {
   //          execSequencer_[k].erase(execSequencer_[k].begin());
   //       }
   //       // holdingTxns_.erase(candidate->cmd_->TxnKey());

   //       return candidate;
   //    } else if (candidate->execStatus_ == FastExecuting) {
   //       toExecQuF_.enqueue({candidate, FastExecuting});
   //       candidate->execStatus_ = FastExecuted;
   //       // Remove PlaceHolder
   //       for (auto& k : candidate->localKeys_) {
   //          execSequencer_[k].erase(execSequencer_[k].begin());
   //       }
   //       // holdingTxns_.erase(candidate->cmd_->TxnKey());
   //       return candidate;
   //    } else {
   //       LOG(INFO) << "Unexpected status " << "(" <<
   //       candidate->cmd_->clientId_
   //                 << ":" << candidate->cmd_->reqId_ << ")\t"
   //                 << "execStatus=" << candidate->execStatus_;
   //       assert(0);
   //    }
   // }
   return NULL;
}

bool TigaReplica::CanRelease(TigaLogEntry* entry, uint64_t ddlRank,
                             bool debug) {
   for (auto& key : entry->localKeys_) {
      const auto& k = execSequencer_[key].begin()->first;
      if (k.first != ddlRank || k.second != entry->cmd_->TxnKey()) {
         return false;
      }
   }
   return true;
}

bool TigaReplica::CanInitialRelease(TigaLogEntry* entry, bool existPlaceHolder,
                                    bool debug) {
   for (auto& key : entry->localKeys_) {
      if (execSequencer_[key].empty()) {
         assert(!existPlaceHolder);
      } else {
         bool okay = false;
         if (existPlaceHolder) {
            auto& topEntry = execSequencer_[key].begin()->second;
            okay = (topEntry == entry);
         } else {
            auto& topEle = execSequencer_[key].begin()->first;
            okay = ((topEle.first > entry->localDdlRank_) ||
                    (topEle.first == entry->localDdlRank_ &&
                     topEle.second >= entry->cmd_->TxnKey()));
         }
         if (!okay) {
            if (debug) {
               auto& topEle = execSequencer_[key].begin()->first;
               auto& topEntry = execSequencer_[key].begin()->second;
               LOG(INFO) << "Cannot SpecExec reqId =(" << entry->cmd_->clientId_
                         << ":" << entry->cmd_->reqId_ << "\t"
                         << "localDdl=" << entry->localDdlRank_ << "\t"
                         << "agreedDDl=" << entry->agreedDdlRank_
                         << "top id=" << HIGH_32BIT(topEle.second) << ":"
                         << LOW_32BIT(topEle.second) << "\t"
                         << "topDdl=" << topEle.first << "\t"
                         << "topExecStatus=" << topEntry->execStatus_ << "\t"
                         << "topComes=" << topEntry->comeTimes_;
            }
            return false;
         }
      }
   }
   if (debug) {
      LOG(INFO) << "Can SpecExec reqId =(" << entry->cmd_->clientId_ << ":"
                << entry->cmd_->reqId_ << "\t";
   }
   return true;
}

void TigaReplica::InsertPlaceHolder(TigaLogEntry* entry, bool useLocalDdl) {
   uint64_t txnKey = entry->cmd_->TxnKey();
   std::pair<uint64_t, uint64_t> p;
   if (useLocalDdl) {
      p.first = entry->localDdlRank_;
      p.second = txnKey;
   } else {
      p.first = entry->agreedDdlRank_;
      p.second = txnKey;
   }
   for (auto& key : entry->localKeys_) {
      execSequencer_[key][p] = entry;
   }
}

void TigaReplica::RemovePlaceHolder(TigaLogEntry* entry, bool useLocalDdl) {
   uint64_t txnKey = entry->cmd_->TxnKey();
   std::pair<uint64_t, uint64_t> p;
   if (useLocalDdl) {
      p.first = entry->localDdlRank_;
      p.second = txnKey;
   } else {
      p.first = entry->agreedDdlRank_;
      p.second = txnKey;
   }

   for (auto& key : entry->localKeys_) {
      execSequencer_[key].erase(p);
   }
}

void TigaReplica::LeaderSpecExecCheckTd() {
   activeThreads_.fetch_add(1);
   // Hold txn
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
   }
   LOG(INFO) << "LeaderSpecExecCheckTd Terminated";
   activeThreads_.fetch_sub(1);
}

void TigaReplica::LeaderSpecExecTd() {
   std::pair<TigaLogEntry*, uint32_t> eles[UINT8_MAX];
   CommitReplyInfo cinfos[UINT8_MAX];
   uint32_t cnt = 0;
   uint32_t exeCnt = 0;
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toExecQuF_.try_dequeue_bulk(eles, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            exeCnt++;
            if (exeCnt % 100000 == 1) {
               LOG(INFO) << "exeCnt=" << exeCnt;
            }
            TigaLogEntry* entry = eles[i].first;
            uint32_t cmd = eles[i].second;
            // assert(entry != NULL);
            LOG(INFO) << "cmd=" << cmd << "--entry=" << entry->cmd_->clientId_
                      << ":" << entry->cmd_->reqId_;

            if (cmd == FastExecuting) {
               entry->execAgreed_ = true;
               // LOG(INFO) << "execAgreed_=true";
               execEntryQu_.push(entry);
               // LOG(INFO) << "Entry Queue";
            } else if (cmd == CommitExecuting) {
               // queuing them  until execAgreed = True
               // LOG(INFO) << "toExecSyncQu_";
               toExecSyncQu_.enqueue(entry);
               execEntryQu_.push(entry);
            } else if (cmd == PrepareExecuting) {
               LeaderPreExecute(entry);
            } else if (cmd == Rollbacking) {
               LeaderRollback(entry);
            } else {
               LOG(ERROR) << "Unexpected cmd " << cmd;
            }
         }
      }

      // Check whether the FastExecuting/CommitExecuting txns can be executed
      while (!execEntryQu_.empty()) {
         TigaLogEntry* entry = execEntryQu_.front();
         // assert(entry != NULL);
         if (entry->execAgreed_) {
            if (entry->execStatus_ == FastExecuting ||
                entry->execStatus_ == FastExecuted) {
               // LOG(INFO) << "Fast Executing " << entry->execStatus_;
               LeaderFastExecute(entry);
            } else if (entry->execStatus_ == CommitExecuting ||
                       entry->execStatus_ == CommitExecuted) {
               LeaderCommitExecute(entry);
            } else {
               LOG(ERROR) << "Unexpected execStatus "
                          << (uint32_t)(entry->execStatus_);
            }
            execEntryQu_.pop();
         } else {
            break;
         }
      }
   }
   LOG(INFO) << "LeaderSpecExecTd Terminated";
   activeThreads_.fetch_sub(1);
}

void TigaReplica::LeaderPreExecute(TigaLogEntry* entry) {
   uint64_t txnKey = entry->cmd_->TxnKey();
   LogInfo* info = new LogInfo();
   info->reply_ = new TigaReply();  // not necessary--duplicate below
   info->entry_ = entry;
   preparedLogInfo_[txnKey] = info;
   info->myHash_.CalculateHash(entry->localDdlRank_, entry->cmd_->TxnKey());
   for (auto& key : entry->localKeys_) {
      EntryQu<LogInfo>* entryQu =
          GetOrCreateEntryByKey(syncedEntries_, key, logMtx_);
      info->accumulativeHashByKey_[key] = info->myHash_;
      // Leader does not need locks when inserting a specific qu
      // (but follower does need)
      if (!entryQu->qu_.empty()) {
         LogInfo* syncedInfo = entryQu->qu_.back();
         info->accumulativeHashByKey_[key].XOR(
             syncedInfo->accumulativeHashByKey_[key]);
      }
   }
   info->reply_ = new TigaReply();
   sm_->SpecExecute(
       entry->cmd_->txnType_, &(entry->localKeys_), &(entry->cmd_->ws_),
       &(info->reply_->result_),
       entry->cmd_->TxnKey());  // should be reinterpret_cast<uint64_t>(info)
                                // rather than txnKey
   // reinterpret_cast<uint64_t>(info));
   toReplyQuF_.enqueue({info, PrepareExecuting});
}

void TigaReplica::LeaderRollback(TigaLogEntry* entry) {
   uint64_t txnKey = entry->cmd_->TxnKey();
   LogInfo* info = preparedLogInfo_[txnKey];
   assert(info != NULL);
   assert(entry == info->entry_);
   sm_->RollbackExecute(entry->cmd_->txnType_, &(entry->localKeys_),
                        &(entry->cmd_->ws_), &(info->reply_->result_),
                        reinterpret_cast<uint64_t>(info));
}

void TigaReplica::LeaderFastExecute(TigaLogEntry* entry) {
   uint64_t txnKey = entry->cmd_->TxnKey();
   LogInfo* info = new LogInfo();
   info->reply_ = new TigaReply();
   info->entry_ = entry;
   info->myHash_.CalculateHash(entry->agreedDdlRank_, entry->cmd_->TxnKey());
   for (auto& key : entry->localKeys_) {
      EntryQu<LogInfo>* entryQu =
          GetOrCreateEntryByKey(syncedEntries_, key, logMtx_);
      info->accumulativeHashByKey_[key] = info->myHash_;
      // Leader does not need locks when inserting a specific qu
      // (but follower does need)
      if (!entryQu->qu_.empty()) {
         LogInfo* syncedInfo = entryQu->qu_.back();
         info->accumulativeHashByKey_[key].XOR(
             syncedInfo->accumulativeHashByKey_[key]);
      }
      entryQu->qu_.push(info);  // Different from PreExecuting
   }
   info->logId_ = nextSyncedLogId_.fetch_add(1);
   syncedLogInfoQu_.enqueue(info);

   sm_->Execute(entry->cmd_->txnType_, &(entry->localKeys_),
                &(entry->cmd_->ws_), &(info->reply_->result_),
                reinterpret_cast<uint64_t>(info));
   toReplyQuF_.enqueue({info, FastExecuting});
   executedLogId_++;
   assert(executedLogId_ == info->logId_);
   syncedLogInfoQu_.enqueue(info);  // wrong, already synced
}

void TigaReplica::LeaderCommitExecute(TigaLogEntry* entry) {
   uint64_t txnKey = entry->cmd_->TxnKey();
   assert(entry->shardKeyMap_.size() > 1);
   LogInfo* info = preparedLogInfo_[txnKey];
   assert(info != NULL);
   assert(info->entry_ == entry);
   info->myHash_.CalculateHash(entry->agreedDdlRank_, entry->cmd_->TxnKey());
   for (auto& key : entry->localKeys_) {
      EntryQu<LogInfo>* entryQu =
          GetOrCreateEntryByKey(syncedEntries_, key, logMtx_);
      info->accumulativeHashByKey_[key] = info->myHash_;
      // Leader does not need locks when inserting a specific qu
      // (but follower does need)
      if (!entryQu->qu_.empty()) {
         LogInfo* syncedInfo = entryQu->qu_.back();
         info->accumulativeHashByKey_[key].XOR(
             syncedInfo->accumulativeHashByKey_[key]);
      }
      entryQu->qu_.push(info);
   }
   info->logId_ = nextSyncedLogId_.fetch_add(1);
   syncedLogInfoQu_.enqueue(info);
   sm_->CommitExecute(entry->cmd_->txnType_, &(entry->localKeys_),
                      &(entry->cmd_->ws_), &(info->reply_->result_),
                      info->entry_->cmd_->TxnKey());
   // reinterpret_cast<uint64_t>(info));
   toReplyQuF_.enqueue({info, CommitExecuting});
   executedLogId_++;
   assert(executedLogId_ == info->logId_);
   syncedLogInfoQu_.enqueue(info);  // wrong, duplicate
   // Erase the fingerprint of PreExec
   preparedLogInfo_.erase(txnKey);
}

void TigaReplica::LeaderReplyTd() {
   std::pair<LogInfo*, uint32_t> eles[UINT8_MAX];
   CommitReplyInfo cinfos[UINT8_MAX];
   uint32_t cnt = 0;
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toReplyQuF_.try_dequeue_bulk(eles, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            LogInfo* info = eles[i].first;
            TigaLogEntry* entry = info->entry_;
            uint32_t cmd = eles[i].second;
            TigaReply& reply = *(info->reply_);
            reply.gViewId_ = gViewId_;
            reply.viewId_ = viewId_;
            reply.clientId_ = entry->cmd_->clientId_;
            reply.reqId_ = entry->cmd_->reqId_;
            reply.shardId_ = shardId_;
            reply.replicaId_ = replicaId_;
            reply.hasHash_ = 1; /* fast reply, i.e., has hash */

            for (auto& kv : info->accumulativeHashByKey_) {
               TigaHash hsh = AddKeyToHash(
                   info->accumulativeHashByKey_[kv.first], kv.first);
               reply.hash_.XOR(hsh);
            }

            reply.owd_ = entry->owd_;
            reply.logId_ = info->logId_;
            reply.latestSyncedLogId_ = nextSyncedLogId_ - 1;

            if (cmd == CommitExecuting || cmd == FastExecuting) {
               reply.deadline_ = entry->agreedDdlRank_;
               if (entry->replyHandler_) {
                  entry->replyHandler_(reply);
                  // Once the handler is used, it cannot be reused
                  entry->replyHandler_ = NULL;
                  // LOG(INFO) << "reply txnId=" << reply.reqId_;
               }
               if (!specExec_) {
                  continue;
               }
               if (entry->commitReplyHandler_) {
                  entry->commitReplyHandler_(reply);
                  entry->commitReplyHandler_ = NULL;
               } else {
                  uint64_t txnKey = entry->cmd_->TxnKey();
                  auto iter =
                      pendingCommitRepyHandlers_[HASH_PARTITION_ID(txnKey)]
                          .find(txnKey);
                  if (iter !=
                      pendingCommitRepyHandlers_[HASH_PARTITION_ID(txnKey)]
                          .end()) {
                     // LOG(INFO) << "Commit1 " << entry->cmd_->clientId_ <<
                     // ":"
                     //           << entry->cmd_->reqId_;
                     entry->commitReplyHandler_ = iter->second;
                     entry->commitReplyHandler_(reply);
                     entry->commitReplyHandler_ = NULL;
                     pendingCommitRepyHandlers_[HASH_PARTITION_ID(txnKey)]
                         .erase(iter);
                  } else {
                     pendingInfoToCommitReply_[HASH_PARTITION_ID(txnKey)]
                                              [txnKey] = info;
                  }
               }
            } else if (cmd == PrepareExecuting) {
               reply.deadline_ = entry->localDdlRank_;
               entry->replyHandler_(reply);
               // Once the handler is used, it cannot be reused
               entry->replyHandler_ = NULL;
            } else {
               LOG(ERROR) << "Unexpected cmd in ReplyTd " << cmd;
            }
         }
      }
      if (!specExec_) {
         continue;
      }

      while ((cnt = toCommitRepyQu_.try_dequeue_bulk(cinfos, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            uint64_t txnKey = cinfos[i].txnKey_;
            auto iter =
                pendingInfoToCommitReply_[HASH_PARTITION_ID(txnKey)].find(
                    txnKey);
            if (iter !=
                pendingInfoToCommitReply_[HASH_PARTITION_ID(txnKey)].end()) {

               LogInfo* info = iter->second;
               TigaLogEntry* entry = info->entry_;
               // LOG(INFO) << "Commit2 " << entry->cmd_->clientId_ << ":"
               //           << entry->cmd_->reqId_;
               entry->commitReplyHandler_ = cinfos[i].commitReplyHandler_;
               entry->commitReplyHandler_(*(info->reply_));
               entry->commitReplyHandler_ = NULL;
               pendingInfoToCommitReply_[HASH_PARTITION_ID(txnKey)].erase(iter);
            } else {
               pendingCommitRepyHandlers_[HASH_PARTITION_ID(txnKey)][txnKey] =
                   cinfos[i].commitReplyHandler_;
            }
         }
      }
   }

   LOG(INFO) << "LeaderReplyTd Terminated";
   activeThreads_.fetch_sub(1);
}

void TigaReplica::LeaderExecuteTd() {
   TigaLogEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   uint32_t tmp = 0;
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toExecQu_.try_dequeue_bulk(entries, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            TigaLogEntry* entry = entries[i];
            // if (entry->cmd_->reqId_ % 1000 == 1) {
            //    LOG(INFO) << "reqId=" << entry->cmd_->reqId_ << "--owd="
            //              << GetMicrosecondTimestamp() - entry->sendTime_;
            // }
            // Prepare the LogInfo to insert
            LogInfo* info = new LogInfo();
            // is moving here affect the performance?
            info->reply_ = new TigaReply();
            info->entry_ = entry;

            LeaderNormalExecute(info);
         }
      }
   }
   LOG(INFO) << "LeaderExecuteTd Terminated";
   activeThreads_.fetch_sub(1);
}

void TigaReplica::LeaderNormalExecute(LogInfo* info) {
   TigaLogEntry* entry = info->entry_;
   info->myHash_.CalculateHash(entry->agreedDdlRank_, entry->cmd_->TxnKey());
   // Make Hash and send reply
   info->logId_ = nextSyncedLogId_.fetch_add(1);
   for (auto& key : entry->localKeys_) {
      EntryQu<LogInfo>* entryQu =
          GetOrCreateEntryByKey(syncedEntries_, key, logMtx_);
      info->accumulativeHashByKey_[key] = info->myHash_;
      // Leader does not need locks when inserting a specific qu
      // (but follower does need)
      if (!entryQu->qu_.empty()) {
         LogInfo* syncedInfo = entryQu->qu_.back();
         info->accumulativeHashByKey_[key].XOR(
             syncedInfo->accumulativeHashByKey_[key]);
      }
      entryQu->qu_.push(info);
   }
   syncedLogInfoQu_.enqueue(info);
   sm_->Execute(entry->cmd_->txnType_, &(entry->localKeys_),
                &(entry->cmd_->ws_), &(info->reply_->result_),
                reinterpret_cast<uint64_t>(info));
   toReplyQuF_.enqueue({info, FastExecuting});
   executedLogId_++;
   assert(executedLogId_ == info->logId_);
   replyNum_++;
}

void TigaReplica::FollowerHoldAndReleaseTd() {
   TigaLogEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   uint32_t enqCnt = 0;
   activeThreads_.fetch_add(1);
   uint64_t lastReleaseTime = 0;
   std::default_random_engine generator;
   generator.seed(time(0));
   std::normal_distribution<double> distribution(clockOffsetMean_,
                                                 clockOffsetStd_);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toHoldAndReleaseQu_.try_dequeue_bulk(entries, UINT8_MAX)) >
             0) {
         for (uint32_t i = 0; i < cnt; i++) {
            holdNum_++;
            TigaLogEntry* entry = entries[i];
            uint64_t txnKey = entry->cmd_->TxnKey();
#ifdef USE_SKEEN
            entry->localDdlRank_ = serverLogicalClock_.fetch_add(1);
#endif
            // Check whether the txn can enter the early buffer
            bool canEnterEarlyBuffer = true;
            for (auto& key : entry->localKeys_) {
               if (entry->localDdlRank_ <= lastReleasedTxnDeadlines_[key]) {
                  canEnterEarlyBuffer = false;
                  break;
               }
            }
            if (canEnterEarlyBuffer) {
               // Follower directly use localDdlRank as agreedDdl
               uint64_t nowTime =
                   GetMicrosecondTimestamp(int(distribution(generator)));
               if (nowTime < entry->sendTime_) {
                  // LOG(INFO) << entry->cmd_->clientId_ << "--nowTime=" <<
                  // nowTime
                  //           << "---sendTime=" << entry->sendTime_;
                  entry->owd_ = 0;
               } else {
                  entry->owd_ = nowTime - entry->sendTime_;
               }

               entry->agreedDdlRank_ = entry->localDdlRank_;
#if !defined(USE_LOGICAL_TIME) && !defined(USE_SKEEN)
               holdBuffer_[{entry->localDdlRank_, txnKey}] = entry;
#else
               toExecQu_.enqueue(entry);
               // Update lastRelease ddl
               for (auto& key : entry->localKeys_) {
                  lastReleasedTxnDeadlines_[key] = std::max(
                      lastReleasedTxnDeadlines_[key], entry->localDdlRank_);
               }
#endif
            }
         }
      }
#if !defined(USE_LOGICAL_TIME) && !defined(USE_SKEEN)
      uint64_t nowTime = GetMicrosecondTimestamp(int(distribution(generator)));
      // nowTime = std::max(nowTime, lastReleaseTime + 1);
      while ((!holdBuffer_.empty()) &&
             nowTime >= holdBuffer_.begin()->first.first) {
         TigaLogEntry* entry = holdBuffer_.begin()->second;
         uint64_t txnKey = holdBuffer_.begin()->first.second;
         releaseNum_++;

         // if (entry->cmd_->reqId_ % 1000 == 1) {
         //    LOG(INFO) << "Release " << entry->cmd_->reqId_ <<
         //    "--duration="
         //              << GetMicrosecondTimestamp() - entry->sendTime_
         //              << "--bound=" << entry->localDdlRank_ -
         //              entry->sendTime_;
         // }

         toExecQu_.enqueue(entry);

         // Update lastRelease ddl
         for (auto& key : entry->localKeys_) {
            lastReleasedTxnDeadlines_[key] =
                std::max(lastReleasedTxnDeadlines_[key], entry->localDdlRank_);
         }
         // This txn will no longer come back
         holdBuffer_.erase(holdBuffer_.begin());
      }
      // lastReleaseTime = nowTime;
#endif
   }
   LOG(INFO) << "FollowerHoldAndReleaseTd Terminated";
   activeThreads_.fetch_sub(1);
}

void TigaReplica::FollowerCrossReplicaSyncTd() {
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      uint64_t startTime = GetMicrosecondTimestamp();
      ProcessPendingInterReplicaSync();

      TigaSyncStatus report;
      report.gViewId_ = gViewId_;
      report.viewId_ = viewId_;
      report.shardId_ = shardId_;
      report.replicaId_ = replicaId_;
      report.syncPoint_ = nextSyncedLogId_ - 1;
      globalProxies_[viewId_ % replicaNum_]->async_SyncStatus(report);

      uint64_t endTime = GetMicrosecondTimestamp();
      if (interDCYieldPeriodUs > 0 &&
          endTime - startTime < interDCYieldPeriodUs) {
         ThreadSleepFor(interDCYieldPeriodUs - (endTime - startTime));
      }
   }
   LOG(INFO) << "FollowerCrossReplicaSyncTd Terminated";
   activeThreads_.fetch_sub(1);
}

void TigaReplica::FollowerExecuteTd() {
   TigaLogEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toExecQu_.try_dequeue_bulk(entries, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            dqReplyNum_++;
            TigaLogEntry* entry = entries[i];
            // Prepare the LogInfo to insert
            LogInfo* info = new LogInfo();
            info->entry_ = entry;
            info->myHash_.CalculateHash(entry->agreedDdlRank_,
                                        entry->cmd_->TxnKey());
            info->reply_ = new TigaReply();
            // Make Hash and send reply
            std::unordered_map<uint32_t, LogInfo*> boundaryLogInfos;
            std::unordered_map<uint32_t, SyncedHashItem>
                boundarySyncedHashItems;
            bool canSlowReply = false;
            for (auto& key : entry->localKeys_) {
               info->accumulativeHashByKey_[key] = info->myHash_;
               EntryQu<LogInfo>* entryQu =
                   GetOrCreateEntryByKey(unSyncedEntries_, key, logMtx_);
               // Get Qu and insert the entry
               {
                  std::unique_lock lk(entryQu->mtx_);
                  if (!entryQu->qu_.empty()) {
                     LogInfo* lastUnSyncedLogInfo = entryQu->qu_.back();
                     info->accumulativeHashByKey_[key].XOR(
                         lastUnSyncedLogInfo->accumulativeHashByKey_[key]);
                  }
                  entryQu->qu_.push(info);
               }
               SyncedHashItem* item =
                   GetOrCreateEntryByKey(latestSyncedHashes_, key, hashMtx_);
               {
                  std::shared_lock lck(item->mtx_);
                  // Thread-Safe copy
                  boundarySyncedHashItems[key].accumulativeHash_ =
                      item->accumulativeHash_;
                  boundarySyncedHashItems[key].deadlineRank_ =
                      item->deadlineRank_;
               }

               if (entry->agreedDdlRank_ <=
                   boundarySyncedHashItems[key].deadlineRank_) {
                  // Can send a slow reply
                  canSlowReply = true;
                  break;
               } else {
                  std::unique_lock lk(entryQu->mtx_);
                  while (!entryQu->qu_.empty()) {
                     if (entryQu->qu_.front()->entry_->agreedDdlRank_ >
                         boundarySyncedHashItems[key].deadlineRank_) {
                        boundaryLogInfos[key] = entryQu->qu_.front();
                        break;
                     } else {
                        entryQu->qu_.pop();
                     }
                  }

                  assert(boundaryLogInfos[key] != NULL);
               }
            }

            if (canSlowReply) {
               TigaReply& rep = *(info->reply_);
               rep.owd_ = entry->owd_;
               rep.hasHash_ = 0; /*slow reply*/
               rep.gViewId_ = gViewId_;
               rep.viewId_ = viewId_;
               rep.clientId_ = entry->cmd_->clientId_;
               rep.reqId_ = entry->cmd_->reqId_;
               rep.shardId_ = shardId_;
               rep.replicaId_ = replicaId_;
               rep.logId_ = 0; /* follower does not include logId*/
               rep.latestSyncedLogId_ = nextSyncedLogId_ - 1;
               /*But follower include latestSyncedLogId_, telling the
                     coordinator its synced progress*/
               entry->replyHandler_(rep);
               // toReplyQuF_.enqueue({info, 0});
            } else {
               TigaReply& rep = *(info->reply_);
               rep.gViewId_ = gViewId_;
               rep.viewId_ = viewId_;
               rep.clientId_ = entry->cmd_->clientId_;
               rep.reqId_ = entry->cmd_->reqId_;
               rep.shardId_ = shardId_;
               rep.replicaId_ = replicaId_;
               rep.owd_ = entry->owd_;
               rep.logId_ = 0;   /* follower does not include logId*/
               rep.hasHash_ = 1; /* fast reply, i.e., has hash */
               std::map<uint32_t, TigaHash> refinedHash;
               for (auto& key : entry->localKeys_) {
                  TigaHash hash = info->accumulativeHashByKey_[key];
                  // LOG(INFO) << "key=" << key
                  //           << "--hashBeforeB=" << hash.ToString();
                  if (boundaryLogInfos.find(key) != boundaryLogInfos.end()) {
                     LogInfo* boundaryLogInfo = boundaryLogInfos[key];
                     /* cut off the previous hashes*/
                     hash.XOR(boundaryLogInfo->accumulativeHashByKey_[key]);
                     // LOG(INFO) << "reqId=" << entry->cmd_->reqId_
                     //           << "\taccHash-afterXORBounday:"
                     //           << rep.hash_.ToString();
                     /* add back the self hash of this boundary entry */
                     hash.XOR(boundaryLogInfo->myHash_);
                     // LOG(INFO) << "reqId=" << entry->cmd_->reqId_
                     //           << "\taccHash-afterAddbackBounday:"
                     //           << rep.hash_.ToString();
                     hash.XOR(boundarySyncedHashItems[key].accumulativeHash_);
                  }
                  refinedHash[key] = hash;
                  // LOG(INFO) << "key=" << key
                  //           << "--hashBeforeB=" << hash.ToString();
               }

               for (auto& kv : refinedHash) {
                  kv.second = AddKeyToHash(kv.second, kv.first);
                  rep.hash_.XOR(kv.second);
               }
               // // [DEBUG]: erase hash for debug
               // rep.hash_.Reset();
               entry->replyHandler_(rep);
               // toReplyQuF_.enqueue({info, 0});
               replyNum_++;
            }
         }
      }
   }
   LOG(INFO) << "FollowerExecuteTd Terminated";
   activeThreads_.fetch_sub(1);
}

void TigaReplica::FollowerReplyTd() {
   uint32_t cnt = 0;
   std::pair<LogInfo*, uint32_t> eles[UINT8_MAX];
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toReplyQuF_.try_dequeue_bulk(eles, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            LogInfo* info = eles[i].first;
            info->entry_->replyHandler_(*(info->reply_));
         }
      }
   }
   LOG(INFO) << "FollowerReplyTd Terminated";
   activeThreads_.fetch_sub(1);
}

void TigaReplica::FollowerExecuteCommitTd() {
   LogInfo* entries[UINT8_MAX];
   uint32_t cnt = 0;
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = followerCommitExecuteQu_.try_dequeue_bulk(entries,
                                                              UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {

            // LOG(INFO) << "logId=" << entries[i]->logId_ << "--vs-- "
            //           << syncedLogList_.size();
            if (entries[i]->logId_ - 1 == syncedLogList_.size()) {
               syncedLogList_.push_back(entries[i]);
            } else {
               rebuffer_[entries[i]->logId_] = entries[i];
            }
            // assert(entries[i]->logId_ - 1 == syncedLogList_.size());
         }
         while (!rebuffer_.empty()) {
            if (rebuffer_.begin()->first - 1 == syncedLogList_.size()) {
               syncedLogList_.push_back(rebuffer_.begin()->second);
               rebuffer_.erase(rebuffer_.begin());
            } else {
               break;
            }
         }
      }
      uint32_t upTo = syncedLogList_.size();
      upTo = std::min(upTo, commitPoint_.load());
      ExecuteCommittedLogs(upTo);

      ThreadSleepFor(1000);
   }

   activeThreads_.fetch_sub(1);
}

void TigaReplica::ExecuteCommittedLogs(uint32_t upto) {
   while (executedLogId_ < upto) {
      uint32_t idx = executedLogId_;
      LogInfo* info = syncedLogList_[idx];
      TigaLogEntry* entry = info->entry_;
      if (!info->reply_) {
         info->reply_ = new TigaReply();
      }
      sm_->Execute(entry->cmd_->txnType_, &(entry->localKeys_),
                   &(entry->cmd_->ws_), &(info->reply_->result_),
                   reinterpret_cast<uint64_t>(info));
      executedLogId_++;
   }
}

void TigaReplica::BroadcastInterReplicaSync() {
   std::lock_guard<std::mutex> lock(syncBroadcastMtx_);  // not necessary
   if (syncedLogInfoQu_.size_approx() == 0 && pendingLogInfo_.empty() &&
       lastBroadcastCommittedLogId_ >= commitPoint_) {
      return;
   }
   LogInfo* info;
   while (syncedLogInfoQu_.try_dequeue(info)) {
      // LOG(INFO) << "info->logId=" << info->logId_
      //           << "--syncedLogList_ size=" << syncedLogList_.size();
      if (info->logId_ < syncedLogList_.size() + 1) {
         // logId starts from 1, and syncedLogList is empty initially
         continue;
      } else if (info->logId_ > syncedLogList_.size() + 1) {
         pendingLogInfo_[info->logId_] = info;
      } else {
         syncedLogList_.push_back(info);
      }
   }
   while (!pendingLogInfo_.empty()) {
      info = pendingLogInfo_.begin()->second;
      if (info->logId_ < syncedLogList_.size() + 1) {
         // logId starts from 1, and syncedLogList is empty initially
         pendingLogInfo_.erase(pendingLogInfo_.begin());
         continue;
      } else if (info->logId_ > syncedLogList_.size() + 1) {
         break;
      } else {
         syncedLogList_.push_back(info);
         pendingLogInfo_.erase(pendingLogInfo_.begin());
      }
   }

   uint32_t sz = syncedLogList_.size() - lastBroadcastSyncedLogId_;
   // LOG(INFO) << "syncedLogList_ size=" << syncedLogList_.size()
   //           << "--pendingLogInfo_.size()=" << pendingLogInfo_.size()
   //           << "--lastSyncedLogId=" << lastBroadcastSyncedLogId_;
   if (sz == 0 && lastBroadcastCommittedLogId_ >= commitPoint_) {
      // Nothing to broadcast
      return;
   }

   // has something to broadcast
   TigaInterReplicaSync sync;
   sync.viewId_ = viewId_;
   sync.shardId_ = shardId_;
   sync.replicaId_ = replicaId_;
   sync.commitPoint_ = commitPoint_;
   sync.logIdStart_ = lastBroadcastSyncedLogId_ + 1;
   if (sz > 0) {
      sync.deadlineRanks_.resize(sz);
      sync.txnKeys_.resize(sz);
      for (uint32_t i = sync.logIdStart_ - 1; i < syncedLogList_.size(); i++) {
         sync.deadlineRanks_[i - lastBroadcastSyncedLogId_] =
             syncedLogList_[i]->entry_->agreedDdlRank_;
         sync.txnKeys_[i - lastBroadcastSyncedLogId_] =
             syncedLogList_[i]->entry_->cmd_->TxnKey();
      }
   } else {
      sync.deadlineRanks_.clear();
      sync.txnKeys_.clear();
   }

   for (uint32_t rid = 0; rid < replicaNum_; rid++) {
      if (rid == replicaId_) {
         continue;
      }
      Future::safe_release(globalProxies_[rid]->async_InterReplicaSync(sync));
   }
   lastBroadcastSyncedLogId_ = syncedLogList_.size();
   lastBroadcastCommittedLogId_ = sync.commitPoint_;
   // LOG(INFO) << "sync " << sync.logIdStart_ << "--" <<
   // sync.txnKeys_.size(); LOG(INFO) << "lastBroadcastSyncedLogId_=" <<
   // lastBroadcastSyncedLogId_;
}

void TigaReplica::onInterReplicaSync(const TigaInterReplicaSync& req) {
   // LOG(INFO) << "logId=" << req.logIdStart_ << "\t" << req.txnKeys_.size()
   //           << "--nextSyncedLogId_=" << nextSyncedLogId_
   //           << "\t pendingSize=" << pendingTigaInterReplicaSyncs_.size();

   if (status_ == STATUS_FAILING) {
      return;
   }
   if (!CheckView(req.viewId_, req.shardId_, req.gViewId_)) {
      return;
   }

   {
      std::lock_guard<std::recursive_mutex> lock(interReplicaSyncMtx_);
      if (nextSyncedLogId_ < req.logIdStart_) {
         // some gap, just pending
         pendingTigaInterReplicaSyncs_[req.logIdStart_] = req;
      } else {
         bool okay = ProcessInterReplicaSync(req);
         if ((!okay) && pendingTigaInterReplicaSyncs_.find(req.logIdStart_) ==
                            pendingTigaInterReplicaSyncs_.end()) {
            pendingTigaInterReplicaSyncs_[req.logIdStart_] = req;
         }
      }
      ProcessPendingInterReplicaSync();
   }
}

void TigaReplica::ProcessPendingInterReplicaSync() {
   std::lock_guard<std::recursive_mutex> lock(interReplicaSyncMtx_);
   while (!pendingTigaInterReplicaSyncs_.empty()) {
      const TigaInterReplicaSync& syncReq =
          pendingTigaInterReplicaSyncs_.begin()->second;
      if (nextSyncedLogId_ < syncReq.logIdStart_) {
         break;
      } else {
         if (ProcessInterReplicaSync(syncReq)) {
            pendingTigaInterReplicaSyncs_.erase(
                pendingTigaInterReplicaSyncs_.begin());
         } else {
            break;
         }
      }
   }
}

bool TigaReplica::ProcessInterReplicaSync(const TigaInterReplicaSync& req) {
   std::lock_guard<std::recursive_mutex> lock(interReplicaSyncMtx_);
   // LOG(INFO) << "sync " << req.logIdStart_ << "--" << req.txnKeys_.size();
   for (uint32_t i = 0; i < req.txnKeys_.size(); i++) {
      if (req.logIdStart_ + i < nextSyncedLogId_) {
         continue;
      }
      // LOG(INFO) << "InProcessSync " << (req.logIdStart_ + i);
      uint64_t txnKey = req.txnKeys_[i];
      ClientCommand* cmd = GetEntry(cmdMap_, txnKey);
      if (cmd == NULL) {
         // this->missingTxnKey_ = req.txnKeys_[i];
         // LOG(INFO) << "missing txnKey " << missingTxnKey_
         //           << "--logId=" << req.logIdStart_ + i;
         return false;
      } else {
         if (this->missingTxnKey_ == req.txnKeys_[i]) {
            // no longer missing
            this->missingTxnKey_ = 0;
         }
      }

      LogInfo* info = new LogInfo();
      TigaLogEntry* entry = new TigaLogEntry();
      entry->agreedDdlRank_ = req.deadlineRanks_[i];
      entry->cmd_ = cmd;
      info->entry_ = entry;
      info->logId_ = nextSyncedLogId_.fetch_add(1);
      info->reply_ = new TigaReply();
      info->myHash_.CalculateHash(entry->agreedDdlRank_, entry->cmd_->TxnKey());
      sm_->InitializeRelatedShards(cmd->txnType_, &(cmd->ws_),
                                   &(entry->shardKeyMap_));

      // LOG(INFO) << "shardsSize=" << entry->shardKeyMap_.size();
      for (auto& key : entry->localKeys_) {
         info->accumulativeHashByKey_[key] = info->myHash_;
         // Update SyncedEntries
         EntryQu<LogInfo>* entryQu =
             GetOrCreateEntryByKey(syncedEntries_, key, logMtx_);
         // Leader does not need locks when inserting a specific qu (but
         // follower does need)
         {
            std::unique_lock lck(entryQu->mtx_);
            if (!entryQu->qu_.empty()) {
               info->accumulativeHashByKey_[key].XOR(
                   entryQu->qu_.back()->accumulativeHashByKey_[key]);
            }
            // LOG(INFO) << "InProcessSync logId" << info->logId_;
            entryQu->qu_.push(info);
         }

         // Update latestSyncedHashes_
         SyncedHashItem* item =
             GetOrCreateEntryByKey(latestSyncedHashes_, key, hashMtx_);
         {
            std::unique_lock lck(item->mtx_);
            // Thread-safe update
            item->deadlineRank_ = entry->agreedDdlRank_;
            item->accumulativeHash_ = info->accumulativeHashByKey_[key];
         }
      }
#ifdef ENABLE_CHECKPOINT
      // Disable Follower Periodic Sync when testing normal processing
      // LOG(INFO) << "Enque " << info->logId_;
      followerCommitExecuteQu_.enqueue(info);
#endif
   }

   if (commitPoint_ < req.commitPoint_) {
      commitPoint_ = req.commitPoint_;
   }
   return true;
}

void TigaReplica::onSyncStatus(const TigaSyncStatus& msg) {
   if (!CheckView(msg.viewId_, msg.shardId_, msg.gViewId_)) {
      return;
   }
   uint32_t tmp[MAX_REPLICA_NUM];
   std::unique_lock lck(syncQuorumMtx_);
   if (currentSyncPoints_[msg.replicaId_] < msg.syncPoint_) {
      currentSyncPoints_[msg.replicaId_] = msg.syncPoint_;
      currentSyncPoints_[replicaId_] = nextSyncedLogId_ - 1;
      uint32_t commitPt = 0;
      memcpy(tmp, currentSyncPoints_, sizeof(uint32_t) * replicaNum_);
      std::sort(tmp, tmp + replicaNum_);
      if (commitPoint_ < tmp[replicaNum_ / 2 + 1]) {
         commitPoint_ = tmp[replicaNum_ / 2 + 1];
      }
   }
}

void TigaReplica::onMissRequest(const MissRequestReq& req,
                                MissRequestRep* rep) {
   // TODO: Not implement so far
   /*If the coordinator does not crash --> incur partial multicast, then the
    * servers never need to worry about MissedRequst */
   if (!CheckView(req.viewId_, req.shardId_, req.gViewId_)) {
      return;
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
               LogInfo* info = recoveredSyncedLogs_[{myDeadline, txnKey}];
               assert(info != NULL);
               info->entry_->agreedDdlRank_ = msg.deadlines_[i];
               syncedTxnKeysToDeadlines_[txnKey] = msg.deadlines_[i];
               recoveredSyncedLogs_.erase({myDeadline, txnKey});
               recoveredSyncedLogs_[{msg.deadlines_[i], txnKey}] = info;
            }
         } else {
            LOG(INFO) << "Not Found";
            ClientCommand* cmd = GetEntry(cmdMap_, txnKey);
            if (cmd) {
               LOG(INFO) << "Fetch one";
               LogInfo* info = new LogInfo();
               info->entry_ = new TigaLogEntry();
               info->entry_->deadlineAgreed_ = true;
               info->entry_->agreedDdlRank_ = msg.deadlines_[i];
               info->entry_->cmd_ = cmd;
               syncedTxnKeysToDeadlines_[txnKey] = msg.deadlines_[i];
               recoveredSyncedLogs_[{msg.deadlines_[i], txnKey}] = info;
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
      TigaLogEntry* entry = kv.second->entry_;
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
         LogInfo* info = syncedLogList_[i];
         sv.deadlines_.push_back(info->entry_->agreedDdlRank_);
         sv.txns_.push_back(*(info->entry_->cmd_));
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
      LogInfo* info = iter->second;
      if (info->entry_->shardKeyMap_.empty()) {
         sm_->InitializeRelatedShards(info->entry_->cmd_->txnType_,
                                      &(info->entry_->cmd_->ws_),
                                      &(info->entry_->shardKeyMap_));
      }
      if (info->entry_->shardKeyMap_.find(targetShard) !=
          info->entry_->shardKeyMap_.end()) {
         rep.deadlines_.push_back(iter->first.first);
         rep.txnKeys_.push_back(iter->first.second);
      }

      iter++;
   }
   for (auto& kv : recoveredUnSyncedLogs_) {
      LogInfo* info = kv.second;
      if (info->entry_->shardKeyMap_.empty()) {
         sm_->InitializeRelatedShards(info->entry_->cmd_->txnType_,
                                      &(info->entry_->cmd_->ws_),
                                      &(info->entry_->shardKeyMap_));
      }
      if (info->entry_->shardKeyMap_.find(targetShard) !=
          info->entry_->shardKeyMap_.end()) {
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
      LogInfo* info = new LogInfo();
      TigaLogEntry* entry = new TigaLogEntry();
      entry->agreedDdlRank_ = startViewMsg_.deadlines_[i];
      entry->deadlineAgreed_ = true;
      entry->cmd_ = cmd;
      info->entry_ = entry;
      info->logId_ = nextSyncedLogId_.fetch_add(1);
      syncedLogList_.push_back(info);
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
         LogInfo* info = new LogInfo();
         info->logId_ = nextSyncedLogId_.fetch_add(1);
         syncedLogList_.push_back(info);
         TigaLogEntry* entry = new TigaLogEntry();
         info->entry_ = entry;
         entry->deadlineAgreed_ = true;
         entry->agreedDdlRank_ = rep.syncedTxnDeadlines_[i];
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
      syncedTxnKeysToDeadlines_[entry->entry_->cmd_->TxnKey()] =
          entry->entry_->agreedDdlRank_;
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
         LogInfo* info = new LogInfo();
         info->logId_ = nextSyncedLogId_.fetch_add(1);
         syncedLogList_.push_back(info);
         TigaLogEntry* entry = new TigaLogEntry();
         info->entry_ = entry;
         entry->deadlineAgreed_ = true;
         entry->agreedDdlRank_ = kv.first.first;
         entry->cmd_ = GetEntry(cmdMap_, kv.first.second);
         assert(entry->cmd_ != NULL);
      }
   }

   // reconstruct synced log list
   for (auto& entry : syncedLogList_) {
      uint64_t ddl = entry->entry_->agreedDdlRank_;
      uint64_t txnKey = entry->entry_->cmd_->TxnKey();
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
         LogInfo* info = syncedLogList_[i - 1];
         rep->syncedTxnDeadlines_.push_back(info->entry_->agreedDdlRank_);
         rep->syncedTxns_.push_back(*(info->entry_->cmd_));
      }
   }
   for (auto& kv : recoveredUnSyncedLogs_) {
      rep->unsyncedTxnDeadlines_.push_back(kv.first.first);
      rep->unsyncedTxns_.push_back(*(kv.second->entry_->cmd_));
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
      LogInfo* info = preparedLogInfo_.begin()->second;
      std::pair<uint64_t, uint64_t> p(info->entry_->localDdlRank_,
                                      info->entry_->cmd_->TxnKey());
      if (recoveredUnSyncedLogs_.find(p) == recoveredUnSyncedLogs_.end()) {
         recoveredUnSyncedLogs_[p] = info;
      }
      preparedLogInfo_.erase(preparedLogInfo_.begin());
   }
   for (auto& kv : unSyncedEntries_.entryMap_) {
      EntryQu<LogInfo>* qu = kv.second;
      while (!qu->qu_.empty()) {
         LogInfo* info = qu->qu_.front();
         std::pair<uint64_t, uint64_t> p(info->entry_->agreedDdlRank_,
                                         info->entry_->cmd_->TxnKey());
         if (recoveredUnSyncedLogs_.find(p) == recoveredUnSyncedLogs_.end()) {
            recoveredUnSyncedLogs_[p] = info;
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

   }  // else: not implemented yet, use typical VR to support configManager's
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

bool TigaReplica::AmLeader() { return (viewId_ % replicaNum_ == replicaId_); }

bool TigaReplica::AmCMLeader() {
   return (cm_.viewId_ % replicaNum_ == replicaId_ && shardId_ == 0);
}

bool TigaReplica::CheckView(uint32_t lView, uint32_t shardId, uint32_t gView) {
   if (shardId == shardId_) {
      // same shard
      return (lView == viewId_);
   } else {
      // cross shard
      if (gView != gViewId_) {
         return false;
      }
      if (gVec_[shardId] != lView) {
         return false;
      }
      return true;
   }
}

std::string TigaReplica::ServerAddrs(const uint32_t shardId,
                                     const uint32_t replicaId) {
   return serverAddrs_[shardId][replicaId];
}
std::string TigaReplica::LocalServerAddrs(const uint32_t shardId,
                                          const uint32_t replicaId) {
   return localServerAddrs_[shardId][replicaId];
}
std::string TigaReplica::GlobalServerAddrs(const uint32_t shardId,
                                           const uint32_t replicaId) {
   return globalServerAddrs_[shardId][replicaId];
}

std::string TigaReplica::VRServerAddrs(const uint32_t shardId,
                                       const uint32_t replicaId) {
   return vrServerAddrs_[shardId][replicaId];
}

std::string TigaReplica::MyServerAddr() {
   return serverAddrs_[shardId_][replicaId_];
}
std::string TigaReplica::MyLocalServerAddr() {
   return localServerAddrs_[shardId_][replicaId_];
}
std::string TigaReplica::MyGlobalServerAddr() {
   return globalServerAddrs_[shardId_][replicaId_];
}
std::string TigaReplica::MyVRServerAddr() {
   return vrServerAddrs_[shardId_][replicaId_];
}

void TigaReplica::ThreadSleepFor(const uint32_t sleepMicroSecond) {
   std::chrono::microseconds duration(sleepMicroSecond);
   std::this_thread::yield();
   std::this_thread::sleep_for(duration);
}