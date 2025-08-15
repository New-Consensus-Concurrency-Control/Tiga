#include "TigaReplica.h"

TigaReplica::TigaReplica(const std::string& serverName,
                         const YAML::Node& config) {
   lastPrintTime_ = GetMicrosecondTimestamp();
   shardNum_ = config["site"]["server"].size();
   replicaNum_ = config["site"]["server"][0].size();
   isPreventive_ = config["preventive"].as<bool>();
   if (config["owd_delta_us"].IsDefined()) {
      owdDeltaUs_ = config["owd_delta_us"].as<uint32_t>();
   } else {
      owdDeltaUs_ = 0;
   }

   reconcliationRequstNum_ = 0;
   normalRequestNum_ = 0;

   memset(localProxies_, '\0', sizeof(TigaLocalProxy*) * MAX_SHARD_NUM);
   memset(localRPCClients_, '\0', sizeof(rrr::Client*) * MAX_SHARD_NUM);
   memset(globalProxies_, '\0', sizeof(TigaGlobalProxy*) * MAX_REPLICA_NUM);
   memset(globalRPCClients_, '\0', sizeof(rrr::Client*) * MAX_REPLICA_NUM);
   memset(globalProxies_, '\0',
          sizeof(TigaViewChangeProxy*) * MAX_SHARD_NUM * MAX_REPLICA_NUM);

   shardId_ = replicaId_ = UINT32_MAX;
   activeThreads_ = 0;

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

         if (thisServerName == serverName) {
            shardId_ = sid;
            replicaId_ = rid;
         }
      }
   }

   useLogicalClock_ = false;
   useSkeen_ = false;
   if (config["clock_approach"].IsDefined()) {
      if (config["clock_approach"].as<std::string>() == "logical") {
         useLogicalClock_ = true;
      } else if (config["clock_approach"].as<std::string>() == "skeen") {
         useSkeen_ = true;
      } else {
         LOG(ERROR) << "Unrecognized approach "
                    << config["clock_approach"].as<std::string>();
      }
   }
   // To highlight the problem of logical timestamps, make every servers'
   // logical clock unsynced (i.e., with an offset)
   serverLogicalClock_ =
       (replicaId_ + shardId_ * replicaNum_) * 1000ul * 1000ul;

   LOG(INFO) << "my replicaId=" << replicaId_ << "\t shardId=" << shardId_
             << "replicaNum=" << replicaNum_ << "\t"
             << "shardNum=" << shardNum_ << "\t owdDeltaUs_=" << owdDeltaUs_
             << "\t useLogical=" << useLogicalClock_ << "\t"
             << "useSkeen=" << useSkeen_;

   FailureRecoveryInit(config);

   viewId_ = 0;
   lastNormalView_ = 0;
   status_ = SERVER_STATUS::STATUS_NORMAL;

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

   LOG(INFO) << "workload=" << workloadStr;
   lastReleasedTxnDeadlinesW_.resize(sm_->TotalNumberofKeys(), 0);
   lastReleasedTxnDeadlinesR_.resize(sm_->TotalNumberofKeys(), 0);
   execSequencers_.resize(sm_->TotalNumberofKeys());
   entriesInSpec_.resize(sm_->TotalNumberofKeys());

   boundaryLogInfos_.resize(sm_->TotalNumberofKeys(), NULL);
   boundarySyncedHashMarks_.resize(sm_->TotalNumberofKeys());

   // to change to global and local proxy
   // intra-DC needs more requests, to be optimized in the future
   localRpcPoll_ = new PollMgr(2);
   globalRpcPoll_ = new PollMgr(2);

   nextSyncedLogId_ = 1;
   nextSpecLogId_ = 1;
   commitPoint_ = 0;
   executedLogId_ = 0;

   syncedLogList_.reserve(1000ul * 1000ul * 100);
   lastBroadcastSyncedLogId_ = 0;
   lastBroadcastCommittedLogId_ = 0;
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

void TigaReplica::Connect() {
   LOG(INFO) << "Start Local Connection";
   this->ConnectToOtherLocalServers();

   LOG(INFO) << "Start Global Connection";
   this->ConnectToOtherGlobalServers();

   if (testFailureRecovery_) {
      LOG(INFO) << "Start VR Connection";
      this->ConnectToOtherVRServers();
   }
}

void TigaReplica::Run() {
   mainTd_ = new std::thread(&TigaReplica::MainTd, this);
}

void TigaReplica::MainTd() {
   uint64_t lastReportTime = GetMicrosecondTimestamp();
   uint64_t launchTime = GetMicrosecondTimestamp();
   std::string name;
   while (status_ != STATUS_TERMINATE) {
      if (status_ == STATUS_NORMAL) {
         if (threadMap_.empty()) {
            threadMap_["HoldReleaseTd"] =
                new std::thread(&TigaReplica::HoldReleaseTd, this);
            if (AmLeader()) {
               threadMap_["LeaderDdlSyncTd"] =
                   new std::thread(&TigaReplica::LeaderDdlSyncTd, this);
               threadMap_["LeaderPreExecTd"] =
                   new std::thread(&TigaReplica::LeaderPreExecTd, this);
               threadMap_["LeaderExecTd"] =
                   new std::thread(&TigaReplica::LeaderExecTd, this);
               threadMap_["LeaderCrossReplicaSyncTd"] = new std::thread(
                   &TigaReplica::LeaderCrossReplicaSyncTd, this);
               threadMap_["LeaderReplyTd"] =
                   new std::thread(&TigaReplica::LeaderReplyTd, this);
            } else {
               threadMap_["FollowerCrossReplicaSyncTd"] = new std::thread(
                   &TigaReplica::FollowerCrossReplicaSyncTd, this);
               threadMap_["FollowerExecTd"] =
                   new std::thread(&TigaReplica::FollowerExecTd, this);
               threadMap_["FollowerReplyTd"] =
                   new std::thread(&TigaReplica::FollowerReplyTd, this);
            }
         }
         lastNormalView_ = viewId_;
         if (testFailureRecovery_) {
            ActivateFailure();
         }
         ThreadSleepFor(10000);
      } else if (status_ == STATUS_TERMINATE) {
         LOG(INFO) << threadMap_.size() << " working threads ";
         for (auto& kv : threadMap_) {
            LOG(INFO) << "Wait for " << kv.first << " to complete";
            kv.second->join();
            delete kv.second;
            LOG(INFO) << kv.first << " completed";
         }
         break;
      } else {
         if (testFailureRecovery_) {
            FailureRecovery();
         }
      }

      if (testFailureRecovery_) {
         ConfigManagerAction();
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

void TigaReplica::HoldReleaseTd() {
   activeThreads_.fetch_add(1);
   TigaLogEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   uint64_t lastReleaseTime = 0;
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      uint64_t nowTime = GetMicrosecondTimestamp();
      while ((cnt = toHoldAndReleaseQu_.try_dequeue_bulk(entries, UINT8_MAX)) >
             0) {
         for (uint32_t i = 0; i < cnt; i++) {
            TigaLogEntry* entry = entries[i];
            uint64_t txnKey = entry->cmd_->TxnKey();
            uint32_t clientId = entry->cmd_->clientId_;

            if (entry->agreeStatus_ == AGREE_FLUSHING) {
               assert(AmLeader());
               assert(entry->agreedDdlRank_ > entry->localDdlRank_);
               holdBuffer_[{entry->agreedDdlRank_, txnKey}] = entry;
               continue;
            }
            // Check whether the txn is eligble to enter holdingBuffer
            uint64_t maxLastReleasedTime = 0;
            for (auto& k : entry->localKeys_) {
               if (maxLastReleasedTime < lastReleasedTxnDeadlinesW_[k]) {
                  maxLastReleasedTime = lastReleasedTxnDeadlinesW_[k];
               }
               if (entry->cmd_->isReadOnly_ == 0) {
                  // not read-only txn, should also check the conflict with
                  // read-only txns
                  if (maxLastReleasedTime < lastReleasedTxnDeadlinesR_[k]) {
                     maxLastReleasedTime = lastReleasedTxnDeadlinesR_[k];
                  }
               }
            }

            if ((maxLastReleasedTime >= entry->localDdlRank_) &&
                (!AmLeader())) {
               // If uses Skeen, followers do not know the timestamp agreed by
               // leader, they must wait until leader compeltes agreement and
               // tell the followers, i.e., the fast path becomes nullified

               // Not eligible to enter holdingBuffer
               // LOG(INFO) << "Not Elgible Enter " << entry->ID();
               entry->execStatus_ = EXEC_ABANDONED;
               toReplyQu_.enqueue(entry);
               continue;
            }

            // if (entry->localDdlRank_ <= maxLastReleasedTime) {
            //    LOG(INFO) << entry->ID() << "--updated timestamp";
            //    if (GetMicrosecondTimestamp() > entry->localDdlRank_) {
            //       LOG(INFO) << entry->ID() << "--gap="
            //                 << GetMicrosecondTimestamp() -
            //                 entry->localDdlRank_;
            //    }
            // }
            // (Leader) updates timestamp
            entry->localDdlRank_ =
                std::max(entry->localDdlRank_, maxLastReleasedTime + 1);
            holdBuffer_[{entry->localDdlRank_, txnKey}] = entry;
         }
      }

      // nowTime as the watermark
      while ((!holdBuffer_.empty()) &&
             (nowTime > /**owdDeltaUs_ +**/ holdBuffer_.begin()->first.first ||
              useLogicalClock_ ||
              useSkeen_ /** hold-and-release no longer valid: direct release**/
              )) {
         // nowTime has surpassed my deadline, release
         // Optimiaztion:  Add a delta here, nowTime - delta > xxx
         TigaLogEntry* entry = holdBuffer_.begin()->second;
         if (AmLeader()) {
            if (entry->agreeStatus_ == AGREE_FLUSHING) {
               // This txn has previously done timestamp agreement, but
               // encounters inconsistency
               toDdlSyncQu_.enqueue(entry);
            } else {
               // LOG(INFO) << "HR " << entry->ID() << "--owd=" << entry->owd_;
               if (entry->shardKeyMap_.size() == 1) {
                  entry->agreedDdlRank_ = entry->localDdlRank_;
                  entry->preAgreeStatus_ = AGREE_CHECK2;
                  entry->agreeStatus_ = AGREE_COMPLETE;
               } else  // Only multi-shard txns need timestamp agreement
                  toDdlSyncQu_.enqueue(entry);

               toExecCheckQu_.enqueue(entry);
            }

         } else {
            /** Follower: the second ele is not needed*/
            toExecQuF_.enqueue({entry, 0});
         }
         for (auto& k : entry->localKeys_) {
            if (entry->cmd_->isReadOnly_ == 1) {
               if (lastReleasedTxnDeadlinesR_[k] < entry->localDdlRank_)
                  lastReleasedTxnDeadlinesR_[k] = entry->localDdlRank_;
            } else {
               if (lastReleasedTxnDeadlinesW_[k] < entry->localDdlRank_)
                  lastReleasedTxnDeadlinesW_[k] = entry->localDdlRank_;
            }
         }
         holdBuffer_.erase(holdBuffer_.begin());
      }
      lastReleaseTime = GetMicrosecondTimestamp();
   }
   activeThreads_.fetch_sub(1);
}

void TigaReplica::LeaderDdlSyncTd() {
   activeThreads_.fetch_add(1);
   TigaLogEntry* entries[UINT8_MAX];
   TigaDeadlineAgreeRequest requests[UINT8_MAX];
   uint32_t cnt = 0;
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      TigaDeadlineAgreeRequest reqs[MAX_SHARD_NUM];
      while ((cnt = toDdlSyncQu_.try_dequeue_bulk(entries, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            TigaLogEntry* entry = entries[i];
            entry->comeTimes_++;
            uint64_t txnKey = entry->cmd_->TxnKey();
            if (entry->agreeStatus_ == AGREE_INIT) {
               for (auto& kv : entry->shardKeyMap_) {
                  uint32_t sid = kv.first;
                  reqs[sid].txnKeys_.push_back(txnKey);
                  reqs[sid].deadlineRanks_.push_back(entry->localDdlRank_);
               }
               UpdateDeadlineRecord(txnKey, shardId_, entry->localDdlRank_, 1,
                                    entry);
            } else if (entry->agreeStatus_ == AGREE_FLUSHING) {
               for (auto& kv : entry->shardKeyMap_) {
                  uint32_t sid = kv.first;
                  reqs[sid].txnKeys2_.push_back(txnKey);
                  reqs[sid].deadlineRanks2_.push_back(entry->agreedDdlRank_);
               }
               UpdateDeadlineRecord(txnKey, shardId_, entry->agreedDdlRank_, 2,
                                    entry);
            } else {
               LOG(ERROR) << "Unexpected agreeStatus =" << entry->agreeStatus_
                          << "\t"
                          << "entryID=" << entry->ID() << "\t"
                          << "comeTime=" << entry->comeTimes_
                          << "\t--shards=" << entry->shardKeyMap_.size();

               /****
               auto& logVec = logMap_[HASH_PARTITION_ID(txnKey)][txnKey];
               LOG(ERROR) << "Unexpected prev agreeStatus ="
                          << entry->preAgreeStatus_ << "\t"
                          << "logLen=" << logVec.size();

               for (uint32_t i = 0; i < logVec.size(); i++) {
                  LOG(ERROR) << logVec[i].ToString();
               }
               **/

               verify(0);
            }
         }
      }

      for (uint32_t sid = 0; sid < shardNum_; sid++) {
         if (sid == shardId_) {
            continue;
         }
         if (reqs[sid].txnKeys_.empty() && reqs[sid].txnKeys2_.empty()) {
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

      while ((cnt = toDdlSyncRequestQu_.try_dequeue_bulk(requests, UINT8_MAX)) >
             0) {
         for (uint32_t i = 0; i < cnt; i++) {
            TigaDeadlineAgreeRequest& req = requests[i];
            for (uint32_t j = 0; j < req.txnKeys_.size(); j++) {
               UpdateDeadlineRecord(req.txnKeys_[j], req.shardId_,
                                    req.deadlineRanks_[j], 1);
            }
            for (uint32_t j = 0; j < req.txnKeys2_.size(); j++) {
               UpdateDeadlineRecord(req.txnKeys2_[j], req.shardId_,
                                    req.deadlineRanks2_[j], 2);
            }
         }
      }
      // (to reduce CPU utilization)
      if (toDdlSyncQu_.size_approx() <= 50) {
         ThreadSleepFor(2000);
      }
   }
   activeThreads_.fetch_sub(1);
}

void TigaReplica::LeaderPreExecTd() {
   uint64_t debugTime = GetMicrosecondTimestamp();
   uint32_t checkedCnt = 0;
   activeThreads_.fetch_add(1);
   TigaLogEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   std::vector<TigaLogEntry*> candidateEntries;
   std::vector<TigaLogEntry*> newCandidateEntries;
   std::vector<uint32_t> candidateKeys;
   LOG(INFO) << "LeaderPreExecTd";
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toExecCheckQu_.try_dequeue_bulk(entries, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            TigaLogEntry* entry = entries[i];
            // LOG(INFO) << "Check ID=" << entry->ID() << "--" << entry->owd_;
            uint64_t txnKey = entry->cmd_->TxnKey();
            for (auto& k : entry->localKeys_) {
               execSequencers_[k][{entry->localDdlRank_, txnKey}] = entry;
            }
            if (AtTopofExecSequencer(entry) && CanSpecExec(entry)) {
               for (auto& k : entry->localKeys_) {
                  entriesInSpec_[k] = entry;
                  execSequencers_[k].erase(execSequencers_[k].begin());
               }
               candidateEntries.push_back(entry);
            }
         }
         checkedCnt++;
      }

      for (auto& candidate : candidateEntries) {
         // LOG(INFO) << "Check Candidate in " << candidateEntries.size()
         //           << "\tCandidate " << candidate->ID()
         //           << "--execStatus=" << candidate->execStatus_
         //           << "--agreeStatus=" << candidate->agreeStatus_;
         if (candidate->execStatus_ == EXEC_INIT) {
            if (candidate->agreeStatus_ == AGREE_COMPLETE) {
               candidate->execStatus_ = EXEC_DIRECT;
               toExecQuF_.enqueue({candidate, EXEC_DIRECT});
               for (auto& k : candidate->localKeys_) {
                  entriesInSpec_[k] = NULL;
                  candidateKeys.push_back(k);
               }
            } else {
               candidate->execStatus_ = EXEC_SPEC;
               toExecQuF_.enqueue({candidate, EXEC_SPEC});
               // Cannot remove entriesInSpec_ placeholders
               newCandidateEntries.push_back(candidate);
               // LOG(INFO) << "candidateSPEC=" << candidate->ID()
               //           << "--newCandidaSize=" <<
               //           newCandidateEntries.size();
            }
         } else if (candidate->execStatus_ == EXEC_SPEC) {
            if (candidate->agreeStatus_ == AGREE_COMPLETE) {
               if (candidate->localDdlRank_ == candidate->agreedDdlRank_) {
                  // The previous spec is correct --> commit
                  candidate->execStatus_ = EXEC_COMMITING;
                  toExecQuF_.enqueue({candidate, EXEC_COMMITING});
                  for (auto& k : candidate->localKeys_) {
                     entriesInSpec_[k] = NULL;
                     candidateKeys.push_back(k);
                  }
               } else {
                  // The previous spec is wrong --> rollback
                  candidate->execStatus_ = EXEC_ROLLBACK;
                  toExecQuF_.enqueue({candidate, EXEC_ROLLBACK});
                  // put it back (i.e., reposition) to execSequencer
                  uint64_t txnKey = candidate->cmd_->TxnKey();
                  // Reposition
                  for (auto& k : candidate->localKeys_) {
                     entriesInSpec_[k] = NULL;
                     candidateKeys.push_back(k);
                     execSequencers_[k][{candidate->agreedDdlRank_, txnKey}] =
                         candidate;
                  }
               }
            } else {
               // This one has SPEC_EXEC but has not completed timestamp
               // agreement Need to check it later
               newCandidateEntries.push_back(candidate);
               // Performance: maybe should use a map and incrementally
               // add/delete
            }
         } else if (candidate->execStatus_ == EXEC_ROLLBACK) {
            if (candidate->agreeStatus_ == AGREE_COMPLETE) {
               candidate->execStatus_ = EXEC_DIRECT;
               toExecQuF_.enqueue({candidate, EXEC_DIRECT});
               for (auto& k : candidate->localKeys_) {
                  entriesInSpec_[k] = NULL;
                  candidateKeys.push_back(k);
               }
            }
         } else {
            LOG(ERROR) << "Unexpected Exec Status " << candidate->execStatus_;
            verify(0);
         }
         // LOG(INFO) << "After CheckOp entryID=" << candidate->ID()
         //           << "\t execStatus=" << candidate->execStatus_
         //           << "\t agreeStatus=" << candidate->agreeStatus_
         //           << "\t newCandidateSize=" << newCandidateEntries.size();
      }

      AdvanceSpecExec(candidateKeys, &newCandidateEntries);
      candidateEntries = std::move(newCandidateEntries);
      newCandidateEntries.clear();
      candidateKeys.clear();
   }
   activeThreads_.fetch_sub(1);
}

void TigaReplica::LeaderExecTd() {
   activeThreads_.fetch_add(1);
   std::pair<TigaLogEntry*, uint32_t> eles[UINT8_MAX];
   uint32_t cnt = 0;
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toExecQuF_.try_dequeue_bulk(eles, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            TigaLogEntry* entry = eles[i].first;
            uint32_t cmd = eles[i].second;
            // LOG(INFO) << "EXEC " << entry->ID() << "\tcmd=" << cmd;
            if (isPreventive_) {
               PreventiveExec(entry, cmd);
            } else {
               DetectiveExec(entry, cmd);
            }
         }
      }
   }
   activeThreads_.fetch_sub(1);
}

void TigaReplica::PreventiveExec(TigaLogEntry* entry, uint32_t cmd) {
   uint64_t txnKey = entry->cmd_->TxnKey();
   if (cmd == EXEC_SPEC || cmd == EXEC_ROLLBACK) {
      return;
   } else if (cmd == EXEC_COMMITING || cmd == EXEC_DIRECT) {
      // Both commiting and direct are handled as direct,
      // because there is no spec execution in Preventive mode
      entry->reply_ = new TigaReply();
      entry->myHash_.CalculateHash(entry->agreedDdlRank_, txnKey);
      for (auto& key : entry->localKeys_) {
         EntryQu<TigaLogEntry>* entryQu =
             GetOrCreateEntryByKey(syncedEntries_, key, logMtx_);
         // Leader does not need locks when inserting a specific qu
         // (but follower does need)
         if (!entryQu->qu_.empty()) {
            // EXEC_COMMITTING case: it already initialized
            // accumulativeHash when the status is EXEC_SPEC
            TigaLogEntry* syncedInfo = entryQu->qu_.back();
            entry->accumulativeHashByKey_[key].XOR(
                syncedInfo->accumulativeHashByKey_[key]);
            // LOG(INFO) << "XORed syncedInfo=" << syncedInfo->ID();
            entry->accumulativeHashByKey_[key].XOR(syncedInfo->myHash_);
         }
         entryQu->qu_.push(entry);
      }
      entry->logId_ = nextSyncedLogId_.fetch_add(1);
      syncedLogInfoQu_.enqueue(entry);

      sm_->Execute(entry->cmd_->txnType_, &(entry->localKeys_),
                   &(entry->cmd_->ws_), &(entry->reply_->result_), txnKey);

      executedLogId_++;
      verify(executedLogId_ == entry->logId_);
      // LOG(INFO) << "DIRECT " << entry->ID();
      for (auto& key : entry->localKeys_) {
         TigaHash hsh = AddKeyToHash(entry->accumulativeHashByKey_[key], key);
         entry->reply_->hash_.XOR(hsh);
      }
      entry->reply_->hasHash_ = 1;
      entry->replyStatus_ = REPLY_AGREED;
      toReplyQu_.enqueue(entry);

   } else {
      LOG(ERROR) << "Unexpected cmd =" << cmd;
      verify(0);
   }
   // LOG(INFO) << "FIN-EXEC " << entry->ID() << "\tcmd=" << cmd;
}

void TigaReplica::DetectiveExec(TigaLogEntry* entry, uint32_t cmd) {
   uint64_t txnKey = entry->cmd_->TxnKey();
   if (cmd == EXEC_SPEC) {
      entry->specReply_ = new TigaReply();
      sm_->SpecExecute(entry->cmd_->txnType_, &(entry->localKeys_),
                       &(entry->cmd_->ws_), &(entry->specReply_->result_),
                       txnKey);
      entry->specLogId_ = nextSpecLogId_.fetch_add(1);
      for (auto& key : entry->localKeys_) {
         EntryQu<TigaLogEntry>* entryQu =
             GetOrCreateEntryByKey(syncedEntries_, key, logMtx_);
         // Leader does not need locks when inserting a specific qu
         // (but follower does need)
         if (!entryQu->qu_.empty()) {
            TigaLogEntry* syncedInfo = entryQu->qu_.back();
            entry->accumulativeHashByKey_[key].XOR(
                syncedInfo->accumulativeHashByKey_[key]);
            // LOG(INFO) << "XORed syncedInfo=" << syncedInfo->ID();
            // entry's accumulated hash does not include itself
            // In order to save the prefix, we need XOR syncedInfo
            // itself
            entry->accumulativeHashByKey_[key].XOR(syncedInfo->myHash_);
         }

         // LOG(INFO) << "key=" << key << "\t quLen=" << entryQu->qu_.size();
      }
      entry->replyMtx_.lock();
      for (auto& key : entry->localKeys_) {
         TigaHash hsh = AddKeyToHash(entry->accumulativeHashByKey_[key], key);
         entry->specReply_->hash_.XOR(hsh);
      }
      entry->specReply_->hasHash_ = 1;
      entry->replyMtx_.unlock();
      entry->replyStatus_ = REPLY_SPEC;
      toReplyQu_.enqueue(entry);
   } else if (cmd == EXEC_COMMITING || cmd == EXEC_DIRECT) {
      if (cmd == EXEC_DIRECT) {
         entry->reply_ = new TigaReply();
      }
      entry->myHash_.CalculateHash(entry->agreedDdlRank_, txnKey);
      for (auto& key : entry->localKeys_) {
         EntryQu<TigaLogEntry>* entryQu =
             GetOrCreateEntryByKey(syncedEntries_, key, logMtx_);
         // Leader does not need locks when inserting a specific qu
         // (but follower does need)
         if (cmd == EXEC_DIRECT && (!entryQu->qu_.empty())) {
            // EXEC_COMMITTING case: it already initialized
            // accumulativeHash when the status is EXEC_SPEC
            TigaLogEntry* syncedInfo = entryQu->qu_.back();
            entry->accumulativeHashByKey_[key].XOR(
                syncedInfo->accumulativeHashByKey_[key]);
            // LOG(INFO) << "XORed syncedInfo=" << syncedInfo->ID();
            entry->accumulativeHashByKey_[key].XOR(syncedInfo->myHash_);
         }
         entryQu->qu_.push(entry);
      }

      entry->logId_ = nextSyncedLogId_.fetch_add(1);
      syncedLogInfoQu_.enqueue(entry);
      entry->replyMtx_.lock();
      if (cmd == EXEC_COMMITING) {
         verify(entry->specReply_ != NULL);
         sm_->CommitExecute(entry->cmd_->txnType_, &(entry->localKeys_),
                            &(entry->cmd_->ws_), &(entry->specReply_->result_),
                            txnKey);
      } else {
         sm_->Execute(entry->cmd_->txnType_, &(entry->localKeys_),
                      &(entry->cmd_->ws_), &(entry->reply_->result_), txnKey);
      }
      entry->replyMtx_.unlock();

      executedLogId_++;
      verify(executedLogId_ == entry->logId_);

      // EXEC_COMMITING does not send second reply because the previous
      // (spec) one is already correct
      if (cmd == EXEC_DIRECT) {
         // LOG(INFO) << "DIRECT " << entry->ID();
         entry->replyMtx_.lock();
         for (auto& key : entry->localKeys_) {
            TigaHash hsh =
                AddKeyToHash(entry->accumulativeHashByKey_[key], key);
            entry->reply_->hash_.XOR(hsh);
         }
         entry->reply_->hasHash_ = 1;
         entry->replyMtx_.unlock();
         entry->replyStatus_ = REPLY_AGREED;
         toReplyQu_.enqueue(entry);
      }

   } else if (cmd == EXEC_ROLLBACK) {
      entry->replyMtx_.lock();
      // The ReplyTd might be serializing reply->result,  consider adding a
      // mutex
      sm_->RollbackExecute(entry->cmd_->txnType_, &(entry->localKeys_),
                           &(entry->cmd_->ws_), &(entry->specReply_->result_),
                           txnKey);
      entry->replyMtx_.unlock();
   } else {
      LOG(ERROR) << "Unexpected cmd =" << cmd;
      verify(0);
   }
   // LOG(INFO) << "FIN-EXEC " << entry->ID() << "\tcmd=" << cmd;
}

void TigaReplica::LeaderReplyTd() {
   activeThreads_.fetch_add(1);
   TigaLogEntry* entries[UINT8_MAX];
   CommitReplyInfo cinfos[UINT8_MAX];
   uint32_t cnt = 0;
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toReplyQu_.try_dequeue_bulk(entries, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            TigaLogEntry* entry = entries[i];
            TigaReply* replyPtr = NULL;
            verify(entry != NULL);
            if (entry->replyStatus_ == REPLY_SPEC) {
               // LOG(INFO) << "Spec " << entry->ID();
               replyPtr = entry->specReply_;
               replyPtr->deadline_ = entry->localDdlRank_;
            } else if (entry->replyStatus_ == REPLY_AGREED) {
               // LOG(INFO) << "Agreed " << entry->ID();
               verify(entry->reply_ != NULL);
               replyPtr = entry->reply_;
               replyPtr->deadline_ = entry->agreedDdlRank_;
            } else {
               LOG(ERROR) << "Unexpected status " << entry->replyStatus_;
               assert(0);
            }
            replyPtr->gViewId_ = gViewId_;
            replyPtr->viewId_ = viewId_;
            replyPtr->clientId_ = entry->cmd_->clientId_;
            replyPtr->reqId_ = entry->cmd_->reqId_;
            replyPtr->shardId_ = shardId_;
            replyPtr->replicaId_ = replicaId_;
            // // For debug
            // replyPtr->hasHash_ = 0;

            replyPtr->owd_ = entry->owd_;
            // LOG(INFO) << "ID=" << entry->ID() << "\t owd=" << entry->owd_;
            replyPtr->logId_ = entry->logId_;
            replyPtr->specLogId_ = entry->specLogId_;
            replyPtr->latestSyncedLogId_ = nextSyncedLogId_ - 1;
            replyPtr->latestSyncedSpecLogId_ = nextSpecLogId_ - 1;
            if (entry->replyHandler_) {
               entry->replyMtx_.lock();
               entry->replyHandler_(*replyPtr);
               // LOG(INFO) << "entryID=" << entry->ID()
               //           << "--key=" << entry->localKeys_[0] << "|"
               //           << "\tkeyNum=" << entry->localKeys_.size()
               //           << "--hash=" << replyPtr->hash_.ToString()
               //           << "--[acHashMapSize=]"
               //           << entry->accumulativeHashByKey_.size()
               //           << "--syncedLogId=" << replyPtr->latestSyncedLogId_
               //           << "--specSyncLogId="
               //           << replyPtr->latestSyncedSpecLogId_
               //           << "--hasHash=" << (uint32_t)(replyPtr->hasHash_);
               // Once the handler is used, it cannot be reused
               entry->replyHandler_ = NULL;
               entry->replyMtx_.unlock();
            } else {
               // This entry has sent a reply before.
               // now it wants to send the second reply
               // but we are not sure whether the client RPC has come.
               // Put it into pendingMap
               uint64_t txnKey = entry->cmd_->TxnKey();
               auto iter =
                   pendingCommitRepyHandlers_[HASH_PARTITION_ID(txnKey)].find(
                       txnKey);
               if (iter != pendingCommitRepyHandlers_[HASH_PARTITION_ID(txnKey)]
                               .end()) {
                  // send the second reply (with agreed timestamp)
                  entry->replyMtx_.lock();
                  (iter->second)(*(entry->reply_));
                  entry->replyMtx_.unlock();
               } else {
                  pendingEntriesToCommitReply_[HASH_PARTITION_ID(txnKey)]
                                              [txnKey] = entry;
               }
            }
         }
      }

      while ((cnt = toCommitRepyQu_.try_dequeue_bulk(cinfos, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            uint64_t txnKey = cinfos[i].txnKey_;
            auto iter =
                pendingEntriesToCommitReply_[HASH_PARTITION_ID(txnKey)].find(
                    txnKey);
            if (iter !=
                pendingEntriesToCommitReply_[HASH_PARTITION_ID(txnKey)].end()) {
               TigaLogEntry* entry = iter->second;
               entry->replyMtx_.lock();
               cinfos[i].commitReplyHandler_(*(entry->reply_));
               entry->replyMtx_.unlock();
               pendingEntriesToCommitReply_[HASH_PARTITION_ID(txnKey)].erase(
                   iter);
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

void TigaReplica::LeaderCrossReplicaSyncTd() {
   activeThreads_.fetch_add(1);
   TigaLogEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      uint64_t startTime = GetMicrosecondTimestamp();
      // Broadcast Inter-Replica Sync Msg
      while ((cnt = syncedLogInfoQu_.try_dequeue_bulk(entries, UINT8_MAX)) >
             0) {
         assert(syncedLogList_.size() + 1 == entries[0]->logId_);
         syncedLogList_.insert(syncedLogList_.end(), entries, entries + cnt);
         assert(syncedLogList_.size() == (*syncedLogList_.rbegin())->logId_);
      }

      uint32_t sz = syncedLogList_.size() - lastBroadcastSyncedLogId_;
      // LOG(INFO) << "syncedLogList_ size=" << syncedLogList_.size()
      //           << "--lastSyncedLogId=" << lastBroadcastSyncedLogId_
      //           << "--syncedLogInfoQu_=" << syncedLogInfoQu_.size_approx();
      if (sz == 0 && lastBroadcastCommittedLogId_ >= commitPoint_) {
         // Nothing to broadcast
         continue;
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
         sync.specLogIds_.resize(sz);
         for (uint32_t i = sync.logIdStart_ - 1; i < syncedLogList_.size();
              i++) {
            sync.deadlineRanks_[i - lastBroadcastSyncedLogId_] =
                syncedLogList_[i]->agreedDdlRank_;
            sync.txnKeys_[i - lastBroadcastSyncedLogId_] =
                syncedLogList_[i]->cmd_->TxnKey();
            sync.specLogIds_[i - lastBroadcastSyncedLogId_] =
                syncedLogList_[i]->specLogId_;
         }
      }

      // LOG(INFO) << "logIdStart_=" << sync.logIdStart_
      //           << "--txnKeys=" << sync.txnKeys_.size();
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         if (rid == replicaId_) {
            continue;
         }
         Future::safe_release(
             globalProxies_[rid]->async_InterReplicaSync(sync));
      }
      lastBroadcastSyncedLogId_ = syncedLogList_.size();
      lastBroadcastCommittedLogId_ = sync.commitPoint_;

      uint64_t endTime = GetMicrosecondTimestamp();
      if (interDCYieldPeriodUs > 0 &&
          endTime - startTime < interDCYieldPeriodUs) {
         ThreadSleepFor(interDCYieldPeriodUs - (endTime - startTime));
      }
   }
   LOG(INFO) << "LeaderCrossReplicaSyncTd Terminated";
   activeThreads_.fetch_sub(1);
}

void TigaReplica::FollowerExecTd() {
   activeThreads_.fetch_add(1);
   std::pair<TigaLogEntry*, uint32_t> eles[UINT8_MAX];
   uint32_t cnt = 0;
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toExecQuF_.try_dequeue_bulk(eles, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            TigaLogEntry* entry = eles[i].first;
            // LOG(INFO) << "Follower Append " << entry->ID();
            uint64_t txnKey = entry->cmd_->TxnKey();
            entry->myHash_.CalculateHash(entry->localDdlRank_, txnKey);
            entry->reply_ = new TigaReply();

            // Make Hash and send reply
            bool canSlowReply = false;

            for (auto& key : entry->localKeys_) {
               EntryQu<TigaLogEntry>* entryQu =
                   GetOrCreateEntryByKey(unSyncedEntries_, key, logMtx_);
               // Get Qu and insert the entry
               {
                  std::unique_lock lk(entryQu->mtx_);
                  if (!entryQu->qu_.empty()) {
                     TigaLogEntry* lastUnSyncedLogInfo = entryQu->qu_.back();
                     entry->accumulativeHashByKey_[key].XOR(
                         lastUnSyncedLogInfo->accumulativeHashByKey_[key]);
                     // Each entry's accumulative hash does not include itself
                     // To calculate the prefix, remember to XOR the pre-entry's
                     // own hash
                     entry->accumulativeHashByKey_[key].XOR(
                         lastUnSyncedLogInfo->myHash_);
                  }
                  entryQu->qu_.push(entry);
               }

               SyncedHashItem* item =
                   GetOrCreateEntryByKey(latestSyncedHashes_, key, hashMtx_);
               {
                  std::shared_lock lck(item->mtx_);
                  // Thread-Safe copy
                  // This accumulative hash includes the synced boundary itself
                  boundarySyncedHashMarks_[key].accumulativeHash_ =
                      item->accumulativeHash_;
                  boundarySyncedHashMarks_[key].deadlineRank_ =
                      item->deadlineRank_;
               }
               //////
               if (entry->localDdlRank_ <=
                   boundarySyncedHashMarks_[key].deadlineRank_) {
                  // Can send a slow reply
                  canSlowReply = true;
                  break;
               } else {
                  std::unique_lock lk(entryQu->mtx_);
                  while (!entryQu->qu_.empty()) {
                     if (entryQu->qu_.front()->localDdlRank_ >
                         boundarySyncedHashMarks_[key].deadlineRank_) {
                        boundaryLogInfos_[key] = entryQu->qu_.front();
                        break;
                     } else {
                        entryQu->qu_.pop();
                     }
                  }
                  verify(boundaryLogInfos_[key] != NULL);
               }
            }

            TigaReply* replyPtr = entry->reply_;

            if (canSlowReply) {
               replyPtr->owd_ = entry->owd_;
               replyPtr->hasHash_ = 0; /*slow reply*/
               // assert(entry->replyHandler_ != NULL);
               // entry->replyHandler_(*replyPtr);
            } else {
               replyPtr->hasHash_ = 1; /* fast reply, i.e., has hash */
               for (uint32_t i = 0; i < entry->localKeys_.size(); i++) {
                  uint32_t key = entry->localKeys_[i];
                  TigaHash hash = entry->accumulativeHashByKey_[key];
                  if (boundaryLogInfos_[key]) {
                     /* cut off the previous hashes*/
                     hash.XOR(
                         boundaryLogInfos_[key]->accumulativeHashByKey_[key]);
                     // Add the synced hashes
                     hash.XOR(boundarySyncedHashMarks_[key].accumulativeHash_);
                  }
                  hash = AddKeyToHash(hash, key);
                  replyPtr->hash_.XOR(hash);
               }

               // // // [DEBUG]: erase hash for debug
               // // rep.hash_.Reset();

               // verify(entry->replyHandler_ != NULL);
               // entry->replyHandler_(*replyPtr);
            }

            entry->execStatus_ = EXEC_COMPLETE;
            toReplyQu_.enqueue(entry);
         }
      }
   }
   LOG(INFO) << "FollowerExecTd Terminated";
   activeThreads_.fetch_sub(1);
}

void TigaReplica::FollowerReplyTd() {
   activeThreads_.fetch_add(1);
   TigaLogEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toReplyQu_.try_dequeue_bulk(entries, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            TigaLogEntry* entry = entries[i];
            if (entry->execStatus_ == EXEC_ABANDONED) {
               entry->reply_ = new TigaReply();
               TigaReply* replyPtr = entry->reply_;
               replyPtr->gViewId_ = gViewId_;
               replyPtr->viewId_ = viewId_;
               replyPtr->clientId_ = entry->cmd_->clientId_;
               replyPtr->reqId_ = entry->cmd_->reqId_;
               replyPtr->shardId_ = shardId_;
               replyPtr->replicaId_ = replicaId_;
               replyPtr->latestSyncedLogId_ = nextSyncedLogId_ - 1;
               replyPtr->latestSyncedSpecLogId_ = nextSpecLogId_ - 1;
               replyPtr->logId_ = 0; /* follower does not include logId*/
               replyPtr->deadline_ = UINT64_MAX; /**indicate this entry does
                                                    not enter HRbuffer */
               replyPtr->owd_ = entry->owd_;
               replyPtr->hasHash_ = 0;
               entry->replyHandler_(*replyPtr);
            } else if (entry->execStatus_ == EXEC_COMPLETE) {
               TigaReply* replyPtr = entry->reply_;
               replyPtr->gViewId_ = gViewId_;
               replyPtr->viewId_ = viewId_;
               replyPtr->clientId_ = entry->cmd_->clientId_;
               replyPtr->reqId_ = entry->cmd_->reqId_;
               replyPtr->shardId_ = shardId_;
               replyPtr->replicaId_ = replicaId_;
               replyPtr->latestSyncedLogId_ = nextSyncedLogId_ - 1;
               replyPtr->latestSyncedSpecLogId_ = nextSpecLogId_ - 1;
               replyPtr->logId_ = 0; /* follower does not include logId*/
               replyPtr->deadline_ = entry->localDdlRank_;
               replyPtr->owd_ = entry->owd_;
               verify(entry->replyHandler_ != NULL);
               entry->replyHandler_(*replyPtr);
            } else {
               LOG(ERROR) << "Unexpected execStatus=" << entry->execStatus_;
               verify(0);
            }
         }
      }
   }
   LOG(INFO) << "FollowerReplyTd Terminated";
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

void TigaReplica::FollowerExecuteCommitTd() {
   TigaLogEntry* entries[UINT8_MAX];
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

      ThreadSleepFor(checkpointYieldPeriodUs);
   }

   activeThreads_.fetch_sub(1);
}

/******** ***** ***** ***** ***** Message Handler ***** ***** ***** *****
 * *****/
void TigaReplica::onDispatchRequest(const TigaDispatchRequest& req,
                                    TigaDispatchReply* rep,
                                    const std::function<void()>& cb) {
   sm_->PreRead(req.txnType_, &(req.input_), &(rep->result_));
   rep->shardId_ = shardId_;
   cb();
}

void TigaReplica::onNormalRequest(const TigaReq& req, TigaReply* rep,
                                  const std::function<void()>& cb) {

   // uint32_t reqNum = normalRequestNum_.fetch_add(1);
   // if (reqNum % 10000 == 1) {
   //    LOG(INFO) << "normalReq=" << normalRequestNum_ << "\t"
   //              << "reconcilationReq=" << reconcliationRequstNum_;
   // }
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
   if (useLogicalClock_) {
      entry->localDdlRank_ = req.sendTime_ + req.bound_;
   } else if (useSkeen_) {
      // Only leader can update timestamp
      /**
       * What would happen if followers are allowed to modify timestamps?
       * Given a cross-shard txn T1 on two shards S1 and S2.
       * another single-shard txn T2 on Ss.
       * S1's leader and S2's all servers have the same timestamp for T1
       * But S1's followers has updated T1's timestamp to be larger than T2
       * S2' execute T1, and coordinator knows it is not committed
       * But S2 continues to execute T2, then coordinator consider it is
       * committed Then S1's leader fails, S1's follower becomes leader S2's
       * leader also fails, S2's follower becomes leader S1 all beleive T1's
       * committed timestamp should be larger than T2 Then the recovered logs
       * will have T2->T1 But before crash, it is T1->T2
       */
      if (AmLeader()) {
         // To highlight logical clocks, make them "unsynced" (i.e, with a
         // different starting point)
         entry->localDdlRank_ = serverLogicalClock_.fetch_add(1);
      } else {
         // followers cannot update timestamps
         entry->localDdlRank_ = 0;
      }
   } else {
      entry->localDdlRank_ = req.sendTime_ + req.bound_;
   }

   sm_->InitializeRelatedShards(cmd->txnType_, &(cmd->ws_),
                                &(entry->shardKeyMap_));
   for (auto& key : entry->shardKeyMap_[shardId_]) {
      entry->localKeys_.push_back(key);
   }
   entry->replyHandler_ = [rep, cb](const TigaReply& r) {
      *rep = r;
      cb();
   };
   entry->agreeStatus_ = AGREE_INIT;
   entry->execStatus_ = EXEC_INIT;

   uint64_t nowTime = GetMicrosecondTimestamp();
   if (nowTime > entry->sendTime_) {
      entry->owd_ = nowTime - entry->sendTime_;
   } else {
      // Too close
      entry->owd_ = 1000;  // 1ms as default
   }

   // if (shardId_ == 0 && replicaId_ == 1 && entry->cmd_->clientId_ == 2) {
   //    LOG(INFO) << "My OWD =" << entry->ID() << "--" << entry->owd_
   //              << "--sendTime=" << entry->sendTime_ << "--nowTime=" <<
   //              nowTime;
   // }
   // LOG(INFO) << "owd=" << entry->owd_ << "--nowTime=" << nowTime
   //           << "\t sendTime=" << entry->sendTime_;
   toHoldAndReleaseQu_.enqueue(entry);
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

void TigaReplica::onInquireServerSyncStatus(
    const TigaServerSyncStatusRequest& req, TigaServerSyncStatusReply* rep) {
   rep->gViewId_ = gViewId_;
   rep->viewId_ = viewId_;
   rep->latestSyncedLogId_ = nextSyncedLogId_ - 1;
   rep->latestSyncedSpecLogId_ = nextSpecLogId_ - 1;
   rep->shardId_ = shardId_;
   rep->replicaId_ = replicaId_;
   rep->status_ = status_;
   if (AmCMLeader()) {
      rep->signal_ = serverSignalToCoord_;
   } else {
      rep->signal_ = 0;
   }
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

void TigaReplica::onReconcliationRequest(const TigaReconcliationReq& req,
                                         TigaReply* rep,
                                         const std::function<void()>& cb) {
   // reconcliationRequstNum_.fetch_add(1);
   // When client receives non-serializable (speculative) results
   // Send commitRequests to trigger rollback, if it has not been triggered
   if (req.gViewId_ < gViewId_) {
      return;
   }
   uint64_t txnKey = CONCAT_UINT32(req.clientId_, req.reqId_);
   std::function<void(const TigaReply&)> repyHandler =
       [rep, cb](const TigaReply& r) {
          *rep = r;
          cb();
       };
   toCommitRepyQu_.enqueue({txnKey, repyHandler});
}

/******** ***** ***** ***** Thread Helper ***** ***** ***** ***** *****
 * *****/

bool TigaReplica::CanSpecExec(TigaLogEntry* entry) {
   for (auto& k : entry->localKeys_) {
      if (entriesInSpec_[k]) {
         return false;
      }
   }
   return true;
}

bool TigaReplica::NoBlockingAhead(TigaLogEntry* entry) {
   for (auto& k : entry->localKeys_) {
      if (entriesInSpec_[k]) {
         return false;
      }
   }
   return true;
}

bool TigaReplica::AtTopofExecSequencer(TigaLogEntry* entry) {
   for (auto& k : entry->localKeys_) {
      if (execSequencers_[k].empty() ||
          execSequencers_[k].begin()->second != entry) {
         return false;
      }
   }
   return true;
}

void TigaReplica::AdvanceSpecExec(
    const std::vector<uint32_t>& candidateKeys,
    std::vector<TigaLogEntry*>* newCandidateEntries) {
   for (auto& k : candidateKeys) {
      if (!execSequencers_[k].empty()) {
         TigaLogEntry* candidate = execSequencers_[k].begin()->second;
         if (AtTopofExecSequencer(candidate) && CanSpecExec(candidate)) {
            newCandidateEntries->push_back(candidate);
            for (auto& k : candidate->localKeys_) {
               execSequencers_[k].erase(execSequencers_[k].begin());
               entriesInSpec_[k] = candidate;
            }
         }
      }
   }
}

void TigaReplica::UpdateDeadlineRecord(const uint64_t txnKey,
                                       const uint32_t shardId,
                                       const uint64_t ddl, const uint32_t phase,
                                       TigaLogEntry* entry) {

   DeadlineQMap& dqm = ddlMap_[HASH_PARTITION_ID(txnKey)];
   DeadlineQItem& dqi = dqm.deadlineQ_[txnKey];

   // LOG(INFO) << HIGH_32BIT(txnKey) << ":" << LOW_32BIT(txnKey) << "--"
   //           << "shard-replica=" << shardId << ":" << replicaId_
   //           << "--ddl=" << ddl << "--phase=" << phase
   //           << "--entry=" << (entry != NULL) << "\t"
   //           << "--dqi-phase=" << dqi.phases_[shardId];
   // std::string ddlStr = "";
   // std::string phaseStr = "";
   // for (uint32_t s = 0; s < shardNum_; s++) {
   //    ddlStr += std::to_string(dqi.ddls_[s]) + "|";
   //    phaseStr += std::to_string(dqi.phases_[s]) + "|";
   // }
   // LOG(INFO) << "DDL=" << ddlStr;
   // LOG(INFO) << "phases=" << phaseStr;

   if (entry) {
      dqi.entry_ = entry;
   }
   if (dqi.phases_[shardId] == 0) {
      // no item has come
      dqi.ddls_[shardId] = ddl;
      dqi.phases_[shardId] = phase;
      dqi.itemCnt_++;
   } else if (dqi.phases_[shardId] == 1) {
      if (phase == 2) {
         dqi.ddls_[shardId] = ddl;
         dqi.phases_[shardId] = phase;
      } else {
         LOG(INFO) << "duplicate message ";
         return;
      }
   } else if (dqi.phases_[shardId] == 2) {
      // The incoming message is stale
      assert(phase == 1);
      return;
   } else {
      LOG(ERROR) << "Unexpected " << "shardId=" << shardId << "\t"
                 << dqi.phases_[shardId];
   }

   /**
   AgreeLog log;
   memcpy(log.ddls_, dqi.ddls_, sizeof(uint64_t) * MAX_SHARD_NUM);
   memcpy(log.phases_, dqi.phases_, sizeof(uint32_t) * MAX_SHARD_NUM);
   if (dqi.entry_) {
      log.comeTimes = dqi.entry_->comeTimes_;
      log.agreeStatus_ = dqi.entry_->agreeStatus_;
   } else {
      log.agreeStatus_ = 0;
   }
   logMap_[HASH_PARTITION_ID(txnKey)][txnKey].push_back(log);
   **/

   // LOG(INFO) << HIGH_32BIT(txnKey) << ":" << LOW_32BIT(txnKey)
   //           << "--from shard=" << shardId << "--ddl=" <<
   //           dqi.ddls_[shardId];
   if (dqi.entry_ && dqi.itemCnt_ == dqi.entry_->shardKeyMap_.size()) {
      // LOG(INFO) << "entryID=" << dqi.entry_->ID()
      //           << "--itmCnt=" << dqi.itemCnt_;
      // Check
      uint64_t agreedDdl = 0;
      for (auto& kv : dqi.entry_->shardKeyMap_) {
         uint32_t sid = kv.first;
         if (dqi.ddls_[sid] > agreedDdl) {
            agreedDdl = dqi.ddls_[sid];
         }
      }
      dqi.entry_->agreedDdlRank_ = agreedDdl;
      bool allAgreed = true;
      for (auto& kv : dqi.entry_->shardKeyMap_) {
         uint32_t sid = kv.first;
         if (dqi.ddls_[sid] != agreedDdl) {
            allAgreed = false;
            break;
         }
      }

      // LOG(INFO) << "All Agreed " << allAgreed;
      // std::string ddlStr = "";
      // std::string phaseStr = "";
      // for (uint32_t s = 0; s < shardNum_; s++) {
      //    ddlStr += std::to_string(dqi.ddls_[s]) + "|";
      //    phaseStr += std::to_string(dqi.phases_[s]) + "|";
      // }
      // LOG(INFO) << "DDL=" << ddlStr;
      // LOG(INFO) << "phases=" << phaseStr;
      // LOG(INFO) << "=============================";

      if (allAgreed) {
         dqi.entry_->preAgreeStatus_ = AGREE_CHECK3;
         dqi.entry_->agreeStatus_ = AGREE_COMPLETE;
      } else {
         if (dqi.ddls_[shardId_] != dqi.entry_->agreedDdlRank_) {
            if (dqi.entry_->agreeStatus_ == AGREE_INIT) {
               dqi.entry_->agreeStatus_ = AGREE_FLUSHING;
               toHoldAndReleaseQu_.enqueue(dqi.entry_);
            } else {
               assert(dqi.entry_->agreeStatus_ == AGREE_FLUSHING);
            }
         } else {
            // It is no longer my problem, I am waiting for the other
            // stragglers
            dqi.entry_->agreeStatus_ = AGREE_CONFIRMING;
         }
      }
   }
}

void TigaReplica::ProcessPendingInterReplicaSync() {
   std::lock_guard<std::recursive_mutex> lock(interReplicaSyncMtx_);
   while (!pendingTigaInterReplicaSyncs_.empty()) {
      const TigaInterReplicaSync& syncReq =
          pendingTigaInterReplicaSyncs_.begin()->second;
      // LOG(INFO) << "nextSyncedLogId=" << nextSyncedLogId_ << "\t"
      //           << "nextSpecLogId_=" << nextSpecLogId_
      //           << "\t syncedReq=" << syncReq.logIdStart_;
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

      if (req.specLogIds_[i] > 0) {
         // specLogId_==0: DIREC_EXEC
         pendingSpecLogIds_.insert(req.specLogIds_[i]);
      }

      if (req.logIdStart_ + i < nextSyncedLogId_) {
         continue;
      }
      // LOG(INFO) << "InProcessSync " << (req.logIdStart_ + i);
      uint64_t txnKey = req.txnKeys_[i];
      ClientCommand* cmd = GetEntry(cmdMap_, txnKey);
      if (cmd == NULL) {
         return false;
      }
      /** When missing entries, the replica should try to fetch from the
       * other replicas Here for simplicity, we assume clients will not die
       * with a partial multicast i.e., the request cmd will finally come
       */

      TigaLogEntry* entry = new TigaLogEntry();
      entry->localDdlRank_ = entry->agreedDdlRank_ = req.deadlineRanks_[i];
      entry->preAgreeStatus_ = AGREE_CHECK1;  // to debug
      entry->agreeStatus_ = AGREE_COMPLETE;
      entry->cmd_ = cmd;
      entry->logId_ = nextSyncedLogId_.fetch_add(1);
      // LOG(INFO) << "nextSyncedLogId_=" << nextSyncedLogId_.load();
      /** Follower does not need SpecReply */
      entry->reply_ = new TigaReply();
      entry->myHash_.CalculateHash(entry->agreedDdlRank_,
                                   entry->cmd_->TxnKey());
      sm_->InitializeRelatedShards(cmd->txnType_, &(cmd->ws_),
                                   &(entry->shardKeyMap_));

      // LOG(INFO) << "shardsSize=" << entry->shardKeyMap_.size();
      for (auto& key : entry->localKeys_) {
         // Update SyncedEntries
         EntryQu<TigaLogEntry>* entryQu =
             GetOrCreateEntryByKey(syncedEntries_, key, logMtx_);
         // Leader does not need locks when inserting a specific qu (but
         // follower does need)
         {
            std::unique_lock lck(entryQu->mtx_);
            if (!entryQu->qu_.empty()) {
               TigaLogEntry* lastSyncedLog = entryQu->qu_.back();
               entry->accumulativeHashByKey_[key].XOR(
                   lastSyncedLog->accumulativeHashByKey_[key]);
               // each entry's accumulated hash does not include itself
               // To calculate the prefix, we also need to XOR
               // lastSyncedLog's self hash
               entry->accumulativeHashByKey_[key].XOR(lastSyncedLog->myHash_);
            }
            // LOG(INFO) << "InProcessSync logId" << info->logId_;
            entryQu->qu_.push(entry);
         }

         // Update latestSyncedHashes_
         SyncedHashItem* item =
             GetOrCreateEntryByKey(latestSyncedHashes_, key, hashMtx_);
         {
            std::unique_lock lck(item->mtx_);
            // Thread-safe update
            item->deadlineRank_ = entry->agreedDdlRank_;
            item->accumulativeHash_ = entry->accumulativeHashByKey_[key];
            // upto this entry, the hash is correct
            // The hash include this entry's self hash
            item->accumulativeHash_.XOR(entry->myHash_);
         }
      }

      // Follower Periodic Sync when testing normal processing
      followerCommitExecuteQu_.enqueue(entry);
   }

   while ((!pendingSpecLogIds_.empty()) &&
          nextSpecLogId_ == (*pendingSpecLogIds_.begin())) {
      pendingSpecLogIds_.erase(pendingSpecLogIds_.begin());
      nextSpecLogId_++;
   }
   if (commitPoint_ < req.commitPoint_) {
      commitPoint_ = req.commitPoint_;
   }
   return true;
}

void TigaReplica::ExecuteCommittedLogs(uint32_t upto) {
   while (executedLogId_ < upto) {
      uint32_t idx = executedLogId_;
      TigaLogEntry* entry = syncedLogList_[idx];
      if (!entry->reply_) {
         entry->reply_ = new TigaReply();
      }
      uint64_t txnKey = entry->cmd_->TxnKey();
      entry->replyMtx_.lock();
      sm_->Execute(entry->cmd_->txnType_, &(entry->localKeys_),
                   &(entry->cmd_->ws_), &(entry->reply_->result_), txnKey);
      entry->replyMtx_.unlock();
      executedLogId_++;
   }
}

/******** ***** ***** ***** ***** Helper ***** ***** ***** ***** *****
 *****/
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