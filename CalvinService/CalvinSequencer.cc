#include "CalvinSequencer.h"

CalvinSequencer::CalvinSequencer(const std::string& serverName,
                                 StateMachine* sm)
    : sm_(sm) {
   lastPrintTime_ = GetMicrosecondTimestamp();
   shardNum_ = sm_->ShardNum();
   replicaNum_ = sm_->ReplicaNum();
   shardId_ = sm_->ShardId();
   replicaId_ = sm_->ReplicaId();
   YAML::Node config = sm_->Config();

   memset(sequencerProxies_, '\0',
          sizeof(CalvinSequencerProxy*) * MAX_REPLICA_NUM);
   memset(sequencerRPCClients_, '\0', sizeof(rrr::Client*) * MAX_REPLICA_NUM);

   memset(schedulerProxies_, '\0',
          sizeof(CalvinSchedulerProxy*) * MAX_SHARD_NUM);
   memset(schedulerRPCClients_, '\0', sizeof(rrr::Client*) * MAX_SHARD_NUM);

   memset(&masterProxy_, '\0', sizeof(CalvinProxy));
   masterRPCClient_ = NULL;

   memset(owdSampleNums_, '\0', sizeof(owdSampleNums_));

   LOG(INFO) << "shardNum=" << shardNum_ << "\t"
             << "replicaNum=" << replicaNum_;
   // Get my IDs <shardId, replicaId>
   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         std::string fullName =
             config["site"]["server"][sid][rid].as<std::string>();
         std::string thisServerName = fullName.substr(0, fullName.find(':'));
         std::string portName = fullName.substr(fullName.find(':') + 1);
         std::string ip = config["host"][thisServerName].as<std::string>();
         int port = std::stoi(portName);
         serverAddrs_[sid][rid] = ip + ":" + std::to_string(port);
         sequencerAddrs_[sid][rid] = ip + ":" + std::to_string(port + 1);
         schedulerAddrs_[sid][rid] = ip + ":" + std::to_string(port + 2);
         if (sid == 0 && rid == 0) {
            masterAddr_ = ip + ":" + std::to_string(port);
         }
      }
   }

   LOG(INFO) << "my replicaId=" << replicaId_ << "\t shardId=" << shardId_;

   // Initialize bound and cap
   owdCap_ = config["server_bound_cap"][replicaId_].as<uint32_t>();
   uint32_t initialOWD =
       config["server_initial_bound"][replicaId_].as<uint32_t>();
   for (uint32_t i = 0; i < replicaNum_; i++) {
      owds_[i] = initialOWD;
   }

   designateReplicaId_ =
       config["designate_replica_id"][shardId_].as<uint32_t>();

   // viewId_ = 0;
   // TODO: Replace 0, given each shard a different view so
   // that their leaders are distributed across different regions
   viewId_ = designateReplicaId_;

   activeThreads_ = 0;
   sm_ = sm;

   maxCommittedSeqNo_ = 0;
   lastReleasedTxnDeadlines_.resize(sm_->TotalNumberofKeys(), 0);

   for (uint32_t r = 0; r < replicaNum_; r++) {
      replicaLatestSyncPoints_[r] = 0;
   }

   sequencerRPCPoll_ = new PollMgr(2);
   schedulerRPCPoll_ = new PollMgr(2);
   masterRPCPoll_ = new PollMgr(1);

   epochSequenceNo_ = 1;
   nextSyncedLogId_ = 1;

   syncedLogList_.reserve(1000ul * 1000ul * 100);
   lastBroadcastSyncedLogId_ = 0;
   LOG(INFO) << "shardId=" << shardId_ << "\t" << "replicaId=" << replicaId_
             << "\t"
             << "viewId=" << viewId_;
}

CalvinSequencer::~CalvinSequencer() {}

void CalvinSequencer::ConnectToSequencers() {
   // only connects to the replicas belonging to the same shard
   uint32_t sid = shardId_;
   LOG(INFO) << "ConnectToSequencers sid= " << sid;
   for (uint32_t rid = 0; rid < replicaNum_; rid++) {
      int ret = -1;
      sequencerRPCClients_[rid] = new rrr::Client(sequencerRPCPoll_);
      do {
         LOG(INFO) << "Sequencer Connecting to sid=" << sid << "\t rid=" << rid
                   << "\t"
                   << "ip=" << sequencerAddrs_[sid][rid];
         ret = sequencerRPCClients_[rid]->connect(
             sequencerAddrs_[sid][rid].c_str());
         if (ret == 0) {
            // success
            sequencerProxies_[rid] =
                new CalvinSequencerProxy(sequencerRPCClients_[rid]);
            LOG(INFO) << "Sequencer Connected to sid=" << sid
                      << "\t rid=" << rid << "\t" << sequencerAddrs_[sid][rid];
         } else {
            ThreadSleepFor(1200000);
         }
      } while (ret != 0);
   }
}

void CalvinSequencer::ConnectToSchedulers() {
   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      int ret = -1;
      schedulerRPCClients_[sid] = new rrr::Client(schedulerRPCPoll_);
      do {
         LOG(INFO) << "Scheduler Connecting to sid=" << sid << "\t"
                   << schedulerAddrs_[sid][replicaId_];
         ret = schedulerRPCClients_[sid]->connect(
             schedulerAddrs_[sid][replicaId_].c_str());
         if (ret == 0) {
            // success
            schedulerProxies_[sid] =
                new CalvinSchedulerProxy(schedulerRPCClients_[sid]);
            LOG(INFO) << "Scheduler Connected to " << sid << "\t"
                      << schedulerAddrs_[sid][replicaId_];
         } else {
            ThreadSleepFor(1200000);  // 1200ms
         }
      } while (ret != 0);
   }
}

void CalvinSequencer::ConnectToMaster() {
   masterRPCClient_ = new rrr::Client(masterRPCPoll_);
   int ret = -1;
   do {
      LOG(INFO) << "Master Connecting to " << masterAddr_;
      ret = masterRPCClient_->connect(masterAddr_.c_str());
      if (ret == 0) {
         // success
         masterProxy_ = new CalvinProxy(masterRPCClient_);
         LOG(INFO) << "Master Connected to " << masterAddr_;
      } else {
         ThreadSleepFor(1200000);  // 1200ms
      }
   } while (ret != 0);
}

void CalvinSequencer::onMasterSyncRequest(const MasterSyncRequest& req,
                                          MasterSyncReply* rep,
                                          const std::function<void()>& cb) {
   std::unique_lock lck(masterSyncMtx_);
   masterReplyHdls_[{req.shardId_, req.replicaId_}] =
       [rep, cb](const MasterSyncReply& r) {
          *rep = r;
          cb();
       };
   if (masterReplyHdls_.size() == shardNum_ * replicaNum_) {
      LOG(INFO) << "All Collected ";
      MasterSyncReply reply;
      reply.startTime_ = GetMicrosecondTimestamp() + 5 * 1000ul * 1000ul;
      for (auto kv : masterReplyHdls_) {
         kv.second(reply);
      }
   }
}

void CalvinSequencer::Run() {
   status_ = STATUS_NORMAL;
   mainTd_ = new std::thread(&CalvinSequencer::MainTd, this);

   MasterSyncRequest req;
   req.shardId_ = shardId_;
   req.replicaId_ = replicaId_;
   rrr::FutureAttr fuattr;
   std::function<void(Future*)> cb = [this](Future* fu) {
      MasterSyncReply rep;
      fu->get_reply() >> rep;
      uint64_t ct = GetMicrosecondTimestamp();
      LOG(INFO) << "CurrentTime=" << ct << "--startTime=" << rep.startTime_
                << "-- gap = " << rep.startTime_ - ct;
      startTime_ = rep.startTime_;
   };
   fuattr.callback = cb;
   Future::safe_release(masterProxy_->async_MasterSync(req, fuattr));
}

void CalvinSequencer::MainTd() {
   std::string name;
   while (true) {
      if (status_ == STATUS_NORMAL) {
         if (threadMap_.empty()) {
            name = "QuorumCheckTd";
            threadMap_[name] =
                new std::thread(&CalvinSequencer::QuorumCheckTd, this);

            name = "EpochReportTd";
            threadMap_[name] =
                new std::thread(&CalvinSequencer::EpochReportTd, this);
            if (AmDesignateReplica()) {
               // In each shard, only one designated replica can do the batch
               name = "EpochBatchTd";
               threadMap_[name] =
                   new std::thread(&CalvinSequencer::EpochBatchTd, this);
            }

            if (AmLeader()) {
               name = "LeadererHoldAndReleaseTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] = new std::thread(
                   &CalvinSequencer::LeaderHoldAndReleaseTd, this);

               name = "LeaderExecuteTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] =
                   new std::thread(&CalvinSequencer::LeaderExecuteTd, this);

               name = "LeaderReplyTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] =
                   new std::thread(&CalvinSequencer::LeaderReplyTd, this);

            } else {
               name = "FollowerHoldAndReleaseTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] = new std::thread(
                   &CalvinSequencer::FollowerHoldAndReleaseTd, this);

               name = "FollowerExecuteTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] =
                   new std::thread(&CalvinSequencer::FollowerExecuteTd, this);
            }
            // sleep(1000);
         }
      } else if (status_ == STATUS_TERMINATE) {
         break;
      }

      ThreadSleepFor(10000);
   }

   LOG(INFO) << "Exit Main";
}

void CalvinSequencer::Stop() {
   status_ = SERVER_STATUS::STATUS_TERMINATE;
   LOG(INFO) << "Close Connections";

   for (uint32_t i = 0; i < replicaNum_; i++) {
      if (sequencerRPCClients_[i]) {
         sequencerRPCClients_[i]->close_and_release();
         delete sequencerProxies_[i];
      }
   }
   LOG(INFO) << "sequencerRPC closed";

   for (uint32_t i = 0; i < shardNum_; i++) {
      if (schedulerRPCClients_[i]) {
         schedulerRPCClients_[i]->close_and_release();
         delete schedulerProxies_[i];
      }
   }
   LOG(INFO) << "schedulerRPC closed";

   LOG(INFO) << "Terminating...";
   mainTd_->join();
   LOG(INFO) << "MainTd Terminated...";
   delete mainTd_;
}

void CalvinSequencer::onNormalRequest(const CalvinRequest& req,
                                      CalvinReply* rep,
                                      const std::function<void()>& cb) {

   CalvinRequest* cmd = new CalvinRequest(req);
   CalvinLogEntry* entry = new CalvinLogEntry();
   sm_->InitializeRelatedShards(req.txnType_, &(cmd->ws_),
                                &(entry->shardKeyMap_));
   entry->cmd_ = cmd;
   entry->replyHandler_ = [rep, cb](const CalvinReply& r) {
      *rep = r;
      cb();
   };
   // LOG(INFO) << "Client Req " << req.clientId_ << ":" << req.reqId_;
   epochBatchQu_.enqueue(entry);
}

void CalvinSequencer::LeaderHoldAndReleaseTd() {
   CalvinEpochEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   uint32_t enqCnt = 0;
   uint64_t currentTime = GetMicrosecondTimestamp();
   uint64_t lastCheckTime = 0;
   uint64_t checkInterval = 0;
   activeThreads_.fetch_add(1);
   uint64_t lastReleaseTime = 0;
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toHoldAndReleaseQu_.try_dequeue_bulk(entries, UINT8_MAX)) >
             0) {
         for (uint32_t i = 0; i < cnt; i++) {
            CalvinEpochEntry* entry = entries[i];
            uint64_t txnKey = entry->cmd_->TxnKey();
            // Check whether the txn can enter the early buffer
            uint64_t nowTime = GetMicrosecondTimestamp();
            if (nowTime < entry->cmd_->sendTime_) {
               entry->owd_ = 0;
            } else {
               entry->owd_ = nowTime - entry->cmd_->sendTime_;
            }

            uint64_t newDdl = entry->deadlineRank_;
            for (auto& key : entry->keys_) {
               newDdl = std::max(newDdl, lastReleasedTxnDeadlines_[key] + 1);
            }
            entry->deadlineRank_ = newDdl;
            holdBuffer_[{entry->deadlineRank_, txnKey}] = entry;
         }
      }

      uint64_t nowTime = GetMicrosecondTimestamp();
      nowTime = std::max(nowTime, lastReleaseTime + 1);
      while ((!holdBuffer_.empty()) &&
             nowTime >= holdBuffer_.begin()->first.first) {
         CalvinEpochEntry* entry = holdBuffer_.begin()->second;
         uint64_t txnKey = holdBuffer_.begin()->first.second;
         // if (entry->cmd_->sequenceNo_ % 100 == 1) {
         //    LOG(INFO) << "ToExecute " << entry->cmd_->shardId_ << ":"
         //              << entry->cmd_->replicaId_;
         // }
         toExecQu_.enqueue(entry);
         // Update lastRelease ddl
         for (auto& key : entry->keys_) {
            lastReleasedTxnDeadlines_[key] =
                std::max(lastReleasedTxnDeadlines_[key], entry->deadlineRank_);
         }
         // This txn will no longer come back
         holdBuffer_.erase(holdBuffer_.begin());
      }
   }
   LOG(INFO) << "LeaderHoldAndReleaseTd Terminated";
   activeThreads_.fetch_sub(1);
}

void CalvinSequencer::LeaderExecuteTd() {
   CalvinEpochEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   uint32_t tmp = 0;
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toExecQu_.try_dequeue_bulk(entries, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            CalvinEpochEntry* entry = entries[i];
            // is moving here affect the performance?
            entry->reply_ = new NezhaFastReply();

            LeaderNormalExecute(entry);
         }
      }
      BroadcastInterReplicaSync();
   }
   LOG(INFO) << "LeaderExecuteTd Terminated";
   activeThreads_.fetch_sub(1);
}

void CalvinSequencer::LeaderNormalExecute(CalvinEpochEntry* entry) {
   entry->myHash_.CalculateHash(entry->deadlineRank_, entry->cmd_->TxnKey());
   // Make Hash and send reply
   entry->logId_ = nextSyncedLogId_.fetch_add(1);
   for (auto& key : entry->keys_) {
      EntryQu<CalvinEpochEntry>* entryQu =
          GetOrCreateEntryByKey(syncedEntries_, key, logMtx_);
      entry->accumulativeHashByKey_[key] = entry->myHash_;
      // Leader does not need locks when inserting a specific qu
      // (but follower does need)
      if (!entryQu->qu_.empty()) {
         entryQu->qu_.back();
         CalvinEpochEntry* syncedInfo = entryQu->qu_.back();
         entry->accumulativeHashByKey_[key].XOR(
             syncedInfo->accumulativeHashByKey_[key]);
      }
      entryQu->qu_.push(entry);
   }

   // if (entry->cmd_->sequenceNo_ % 1000 == 1) {
   //    // if (entry->keys_.size() > 0) {
   //    //    std::string keysStr = "";
   //    //    for (auto& k : entry->keys_) {
   //    //       keysStr += std::to_string(k) + ",";
   //    //    }
   //    //    LOG(INFO) << " seqNo=" << entry->cmd_->sequenceNo_
   //    //              << "--keysSize=" << entry->keys_.size() << "--"
   //    //              << keysStr;
   //    // }
   //    LOG(INFO) << "Leader seqNo=" << entry->cmd_->sequenceNo_
   //              << "keySize=" << entry->keys_.size();
   // }
   syncedLogList_.push_back(entry);
   assert(entry->logId_ == syncedLogList_.size());
   toReplyQu_.enqueue(entry);
}

void CalvinSequencer::LeaderReplyTd() {
   CalvinEpochEntry* eles[UINT8_MAX];
   uint32_t cnt = 0;
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toReplyQu_.try_dequeue_bulk(eles, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            CalvinEpochEntry* entry = eles[i];
            NezhaFastReply& reply = *(entry->reply_);
            reply.viewId_ = viewId_;
            reply.sequenceNo_ = entry->cmd_->sequenceNo_;
            reply.shardId_ = entry->cmd_->shardId_;
            reply.shardId_ = shardId_;
            reply.replicaId_ = replicaId_;
            reply.hasHash_ = 1; /* fast reply, i.e., has hash */

            for (auto& kv : entry->accumulativeHashByKey_) {
               NezhaHash hsh = AddKeyToHash(
                   entry->accumulativeHashByKey_[kv.first], kv.first);
               reply.hash_.XOR(hsh);
            }
            reply.owd_ = entry->owd_;
            reply.designateShardId_ = entry->cmd_->shardId_;
            reply.designateReplicaId_ = entry->cmd_->replicaId_;
            // if (reply.sequenceNo_ % 100 == 1) {
            //    LOG(INFO) << "To Send Reply " << reply.designateShardId_ <<
            //    ":"
            //              << reply.designateReplicaId_;
            // }
            reply.logId_ = entry->logId_;
            reply.latestSyncedLogId_ = nextSyncedLogId_ - 1;

            for (int32_t rid = replicaNum_ - 1; rid >= 0; rid--) {
               // for (uint32_t rid = 0; rid < replicaNum_; rid++) {
               sequencerProxies_[rid]->async_CollectNezhaFastReply(reply);
            }
         }
      }
   }

   LOG(INFO) << "LeaderReplyTd Terminated";
   activeThreads_.fetch_sub(1);
}

void CalvinSequencer::FollowerHoldAndReleaseTd() {
   CalvinEpochEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   uint32_t enqCnt = 0;
   activeThreads_.fetch_add(1);
   uint64_t lastReleaseTime = 0;
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toHoldAndReleaseQu_.try_dequeue_bulk(entries, UINT8_MAX)) >
             0) {
         for (uint32_t i = 0; i < cnt; i++) {
            CalvinEpochEntry* entry = entries[i];
            uint64_t txnKey = entry->cmd_->TxnKey();
            uint64_t nowTime = GetMicrosecondTimestamp();
            if (nowTime < entry->cmd_->sendTime_) {
               entry->owd_ = 0;
            } else {
               entry->owd_ = nowTime - entry->cmd_->sendTime_;
            }

            // Check whether the txn can enter the early buffer
            bool canEnterEarlyBuffer = true;
            for (auto& key : entry->keys_) {
               if (entry->deadlineRank_ <= lastReleasedTxnDeadlines_[key]) {
                  canEnterEarlyBuffer = false;
                  break;
               }
            }
            if (canEnterEarlyBuffer) {
               // Follower directly use localDdlRank as agreedDdl
               toExecQu_.enqueue(entry);
               // Update lastRelease ddl
               for (auto& key : entry->keys_) {
                  lastReleasedTxnDeadlines_[key] = std::max(
                      lastReleasedTxnDeadlines_[key], entry->deadlineRank_);
               }
            }
         }
      }
      uint64_t nowTime = GetMicrosecondTimestamp();
      nowTime = std::max(nowTime, lastReleaseTime + 1);
      while ((!holdBuffer_.empty()) &&
             nowTime >= holdBuffer_.begin()->first.first) {
         CalvinEpochEntry* entry = holdBuffer_.begin()->second;
         uint64_t txnKey = holdBuffer_.begin()->first.second;
         toExecQu_.enqueue(entry);
         // Update lastRelease ddl
         for (auto& key : entry->keys_) {
            lastReleasedTxnDeadlines_[key] =
                std::max(lastReleasedTxnDeadlines_[key], entry->deadlineRank_);
         }
         // This txn will no longer come back
         holdBuffer_.erase(holdBuffer_.begin());
      }
   }
   LOG(INFO) << "FollowerHoldAndReleaseTd Terminated";
   activeThreads_.fetch_sub(1);
}

void CalvinSequencer::FollowerExecuteTd() {
   CalvinEpochEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toExecQu_.try_dequeue_bulk(entries, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            CalvinEpochEntry* entry = entries[i];
            entry->myHash_.CalculateHash(entry->deadlineRank_,
                                         entry->cmd_->TxnKey());
            // Make Hash and send reply
            std::unordered_map<uint32_t, CalvinEpochEntry*> boundaryLogInfos;
            std::unordered_map<uint32_t, SyncedHashItem>
                boundarySyncedHashItems;
            // if (entry->cmd_->sequenceNo_ % 1000 == 1) {
            //    // if (entry->keys_.size() > 0) {
            //    //    std::string keysStr = "";
            //    //    for (auto& k : entry->keys_) {
            //    //       keysStr += std::to_string(k) + ",";
            //    //    }
            //    //    LOG(INFO) << " seqNo=" << entry->cmd_->sequenceNo_
            //    //              << "--keysSize=" << entry->keys_.size() <<
            //    "--"
            //    //              << keysStr;
            //    // }
            //    LOG(INFO) << " seqNo=" << entry->cmd_->sequenceNo_
            //              << "keySize=" << entry->keys_.size();
            // }

            for (auto& key : entry->keys_) {
               entry->accumulativeHashByKey_[key] = entry->myHash_;
               EntryQu<CalvinEpochEntry>* entryQu =
                   GetOrCreateEntryByKey(unSyncedEntries_, key, logMtx_);
               // Get Qu and insert the entry
               {
                  std::unique_lock lk(entryQu->mtx_);
                  if (!entryQu->qu_.empty()) {
                     CalvinEpochEntry* lastUnSyncedLogInfo =
                         entryQu->qu_.back();
                     entry->accumulativeHashByKey_[key].XOR(
                         lastUnSyncedLogInfo->accumulativeHashByKey_[key]);
                  }
                  entryQu->qu_.push(entry);
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

               if (entry->deadlineRank_ <=
                   boundarySyncedHashItems[key].deadlineRank_) {
                  // This fast reply is not necessary
                  break;
               } else {
                  std::unique_lock lk(entryQu->mtx_);
                  while (!entryQu->qu_.empty()) {
                     if (entryQu->qu_.front()->deadlineRank_ >
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

            NezhaFastReply rep;
            rep.viewId_ = viewId_;
            rep.sequenceNo_ = entry->cmd_->sequenceNo_;
            rep.shardId_ = shardId_;
            rep.replicaId_ = replicaId_;
            rep.owd_ = entry->owd_;
            rep.designateShardId_ = entry->cmd_->shardId_;
            rep.designateReplicaId_ = entry->cmd_->replicaId_;
            rep.logId_ = 0;   /* follower does not include logId*/
            rep.hasHash_ = 1; /* fast reply, i.e., has hash */
            std::map<uint32_t, NezhaHash> refinedHash;
            for (auto& key : entry->keys_) {
               NezhaHash hash = entry->accumulativeHashByKey_[key];
               // LOG(INFO) << "key=" << key
               //           << "--hashBeforeB=" << hash.ToString();
               if (boundaryLogInfos.find(key) != boundaryLogInfos.end()) {
                  CalvinEpochEntry* boundaryLogInfo = boundaryLogInfos[key];
                  /* cut off the previous hashes*/
                  hash.XOR(boundaryLogInfo->accumulativeHashByKey_[key]);
                  /* add back the self hash of this boundary entry */
                  hash.XOR(boundaryLogInfo->myHash_);
                  hash.XOR(boundarySyncedHashItems[key].accumulativeHash_);
               }
               refinedHash[key] = hash;
            }

            for (auto& kv : refinedHash) {
               kv.second = AddKeyToHash(kv.second, kv.first);
               rep.hash_.XOR(kv.second);
            }

            // // [DEBUG]: erase hash for debug
            // rep.hash_.Reset();
            for (int32_t rid = replicaNum_ - 1; rid >= 0; rid--) {
               // for (uint32_t rid = 0; rid < replicaNum_; rid++) {
               sequencerProxies_[rid]->async_CollectNezhaFastReply(rep);
            }
         }
      }
   }
   LOG(INFO) << "FollowerSpecExecuteTd Terminated";
   activeThreads_.fetch_sub(1);
}

void CalvinSequencer::QuorumCheckTd() {
   uint32_t debugCnt = 0;
   uint32_t cnt;
   NezhaFastReply* eleVec[UINT8_MAX];
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      // Collect FastReplies
      while ((cnt = fastReplyQu_.try_dequeue_bulk(eleVec, UINT8_MAX)) > 0) {

         for (uint32_t i = 0; i < cnt; i++) {
            NezhaFastReply* rep = eleVec[i];
            if (replicatedTxnsBySeqNo_.find(rep->sequenceNo_) !=
                replicatedTxnsBySeqNo_.end()) {
               // already committed
               delete rep;
               continue;
            }

            uint64_t txnKey = rep->TxnKey();
            uint32_t replicaId = rep->replicaId_;
            incompleteReplicatedTxns_[rep->sequenceNo_] = txnKey;
            // if (rep->sequenceNo_ % 1000 == 1) {
            //    LOG(INFO) << "QC:\t" << rep->shardId_ << ":" <<
            //    rep->replicaId_
            //              << "\tdesignated=" << rep->designateShardId_ << ":"
            //              << rep->designateReplicaId_
            //              << "\t seqNo=" << rep->sequenceNo_
            //              << "\t logId=" << rep->logId_
            //              << "--hash=" << rep->hash_.ToString()
            //              << "--time=" << GetMicrosecondTimestamp();
            // }
            if (replicaId_ == rep->designateReplicaId_ &&
                shardId_ == rep->designateShardId_) {
               // Add OWD sample
               owdSampleNums_[replicaId]++;
               uint32_t sampleNo =
                   owdSampleNums_[replicaId] % OWD_SAMPLE_WINDOW_LENGTH;
               uint32_t oldSample = owdSamples_[replicaId][sampleNo];
               owdSamples_[replicaId][sampleNo] = rep->owd_;
               owdSampleQueues_[replicaId].erase({oldSample, sampleNo});
               owdSampleQueues_[replicaId].insert({rep->owd_, sampleNo});
               if (owdSampleQueues_[replicaId].size() >=
                   OWD_SAMPLE_WINDOW_LENGTH) {
                  auto median = std::next(owdSampleQueues_[replicaId].begin(),
                                          OWD_SAMPLE_WINDOW_LENGTH / 2);
                  owds_[replicaId] = median->first;
               }
               // if (true || rep->sequenceNo_ % 100 == 1) {
               //    LOG(INFO) << rep->shardId_ << ":" << rep->replicaId_
               //              << "\towd=" << rep->owd_
               //              << "--hash=" << rep->hash_.ToString();
               // }
            }

            replicaQuorum_[txnKey].fastReplies[replicaId] = 1;
            replicaQuorum_[txnKey].hashes_[replicaId] = rep->hash_;
            if (rep->viewId_ % replicaNum_ == replicaId) {
               // Update logId with leader's reply
               replicaQuorum_[txnKey].leaderLogId_ = rep->logId_;
            }

            if (QuorumOkay(txnKey)) {
               CalvinEpochEntry* entry = GetEntry(epochEntryMap_, txnKey);
               if (entry) {
                  // if (true || entry->cmd_->cmdVec_.size() > 0) {
                  //    LOG(INFO)
                  //        << "committed req seqNo=" <<
                  //        entry->cmd_->sequenceNo_
                  //        << " duration="
                  //        << GetMicrosecondTimestamp() -
                  //        entry->cmd_->sendTime_;
                  // }
                  replicatedTxnsBySeqNo_.insert(entry->cmd_->sequenceNo_);
                  incompleteReplicatedTxns_.erase(entry->cmd_->sequenceNo_);
                  replicaQuorum_.erase(txnKey);
                  toReportQu_.enqueue(entry);
                  maxCommittedSeqNo_ =
                      std::max(maxCommittedSeqNo_, entry->cmd_->sequenceNo_);
                  // debugCnt++;
                  // if (debugCnt % 100 == 1) {
                  //    LOG(INFO) << "CommittedCnt=" << debugCnt << "--entry "
                  //              << entry->cmd_->shardId_ << ":"
                  //              << entry->cmd_->replicaId_;
                  // }
               } else {
                  LOG(INFO) << "No Entry";
               }
            }

            delete rep;
         }
      }
      // Periodically Inquire SyncStatus
      InquireSyncStatus();
      CheckInCompleteTxns();
   }
   activeThreads_.fetch_sub(1);
}

void CalvinSequencer::CheckInCompleteTxns() {
   if (incompleteReplicatedTxns_.empty()) {
      return;
   }
   while (incompleteReplicatedTxns_.begin()->first < maxCommittedSeqNo_) {
      // Worth Check
      uint64_t txnKey = incompleteReplicatedTxns_.begin()->second;
      if (QuorumOkay(txnKey)) {
         CalvinEpochEntry* entry = GetEntry(epochEntryMap_, txnKey);
         if (entry) {
            replicatedTxnsBySeqNo_.insert(entry->cmd_->sequenceNo_);
            incompleteReplicatedTxns_.erase(incompleteReplicatedTxns_.begin());
            replicaQuorum_.erase(txnKey);
            toReportQu_.enqueue(entry);
            maxCommittedSeqNo_ =
                std::max(maxCommittedSeqNo_, entry->cmd_->sequenceNo_);
         } else {
            LOG(INFO) << "No Entry";
            break;
         }
      } else {
         break;
      }
   }
}

void CalvinSequencer::InquireSyncStatus() {
   uint64_t nowTime = GetMicrosecondTimestamp();
   if (nowTime < lastReplicaInquireTime_ + inquireIntervalUs) {
      return;
   }
   NezhaInquireRequest req;
   req.replicaId_ = replicaId_;
   req.shardId_ = shardId_;
   req.viewId_ = viewId_;

   for (uint32_t r = 0; r < replicaNum_; r++) {
      if (r != replicaId_) {
         rrr::FutureAttr fuattr;
         std::function<void(Future*)> cb = [this](Future* fu) {
            NezhaInquireReply rep;
            fu->get_reply() >> rep;
            if (CheckView(rep.viewId_)) {
               if (replicaLatestSyncPoints_[rep.replicaId_] <
                   rep.latestSyncedLogId_) {
                  replicaLatestSyncPoints_[rep.replicaId_] =
                      rep.latestSyncedLogId_;
               }
            }
         };
         fuattr.callback = cb;
         Future::safe_release(
             sequencerProxies_[r]->async_NezhaInquire(req, fuattr));
      }
   }

   lastReplicaInquireTime_ = nowTime;
}

void CalvinSequencer::onNezhaInquireRequest(const NezhaInquireRequest& req,
                                            NezhaInquireReply* rep) {

   if (!CheckView(req.viewId_)) {
      return;
   }
   rep->viewId_ = viewId_;
   rep->shardId_ = shardId_;
   rep->replicaId_ = replicaId_;
   rep->latestSyncedLogId_ = nextSyncedLogId_ - 1;
}

bool CalvinSequencer::QuorumOkay(uint64_t txnKey) {
   uint32_t shardId = HIGH_32BIT(txnKey);
   uint32_t seqNo = LOW_32BIT(txnKey);

   ReplicaQuorum& rq = replicaQuorum_[txnKey];
   uint32_t leaderReplicaId = viewId_ % replicaNum_;
   if (rq.fastReplies[leaderReplicaId] == 0) {
      return false;
   }
   uint32_t validFastReplyNum = 0;
   uint32_t validSlowReplyNum = 0;
   for (uint32_t rid = 0; rid < replicaNum_; rid++) {
      if (rq.fastReplies[rid] == 1 &&
              rq.hashes_[rid].Equal(rq.hashes_[leaderReplicaId]) ||
          replicaLatestSyncPoints_[rid] >= rq.leaderLogId_) {
         validFastReplyNum++;
      }
      if (replicaLatestSyncPoints_[rid] >= rq.leaderLogId_) {
         validSlowReplyNum++;
      }
      // LOG(INFO) << "rid=" << rid
      //           << "\treplicaSyncPoints = " << replicaLatestSyncPoints_[rid]
      //           << "--leaderLogId=" << rq.leaderLogId_;
   }
   uint32_t f = replicaNum_ / 2;
   uint32_t fastQuorum = f + (f + 1) / 2 + 1;
   uint32_t slowQuorum = f + 1;
   // LOG(INFO) << "validFast=" << validFastReplyNum
   //           << "--validSlow=" << validSlowReplyNum;
   if (validFastReplyNum >= fastQuorum || validSlowReplyNum + 1 >= slowQuorum) {
      // if (true || seqNo % 100 == 1) {
      //    LOG(INFO) << seqNo << ":" << "Committed";
      // }
      return true;
   } else {
      // if (seqNo % 100 == 1) {
      //    LOG(INFO) << seqNo << ":" << "Quorum Missing " << validFastReplyNum
      //              << "--" << validSlowReplyNum;
      // }
      return false;
   }
}

void CalvinSequencer::EpochBatchTd() {
   activeThreads_.fetch_add(1);
   CalvinLogEntry* ele;
   LOG(INFO) << "Run Epoch BatchTd";
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      // Batch every 10ms (according to Calvin paper)
      ThreadSleepFor(10 * 1000ul);
      EpochRequest* epochReq = new EpochRequest();
      while (epochBatchQu_.try_dequeue(ele)) {
         uint64_t txnKey = ele->cmd_->TxnKey();
         CalvinLogEntry* entry = InsertEntry(entryMap_, txnKey, ele);
         epochReq->cmdVec_.push_back(*(entry->cmd_));
         // if (epochReq->cmdVec_.size() >= 1000) {
         //    // Do not exceed the RPC message capacity
         //    break;
         // }
      }

      epochReq->sequenceNo_ = epochSequenceNo_.fetch_add(1);
      epochReq->shardId_ = shardId_;
      epochReq->replicaId_ = replicaId_;

      // if (epochSequenceNo_ == 10) {
      //    break;
      // }

      // if (epochReq->sequenceNo_ % 100 == 1) {
      //    LOG(INFO) << "Batch seqNo=" << epochReq->sequenceNo_
      //              << "--shardId=" << epochReq->shardId_;
      // }

      uint32_t bound = 0;
      for (uint32_t i = 0; i < replicaNum_; i++) {
         bound = std::max(bound, owds_[i].load());
      }

      epochReq->bound_ = bound;
      epochReq->sendTime_ = GetMicrosecondTimestamp();

      // LOG(INFO) << "Batch seqNo=" << epochReq->sequenceNo_
      //           << "--bound=" << bound << "--sendTime=" <<
      //           epochReq->sendTime_;

      for (int32_t r = replicaNum_ - 1; r >= 0; r--) {
         Future::safe_release(
             sequencerProxies_[r]->async_ReplicateEpoch(*epochReq));
      }
   }
   activeThreads_.fetch_sub(1);
}

void CalvinSequencer::EpochReportTd() {
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      ReportToSchedulers();
      ReportToClients();
   }
   activeThreads_.fetch_sub(1);
}

void CalvinSequencer::ReportToSchedulers() {
   CalvinEpochEntry* ele;
   while (toReportQu_.try_dequeue(ele)) {
      // if (ele->cmd_->sequenceNo_ % 100 == 1) {
      //    uint64_t endTme = GetMicrosecondTimestamp();
      //    uint64_t duration = endTme - ele->cmd_->sendTime_;

      //    LOG(INFO) << "Report " << ele->cmd_->sequenceNo_
      //              << "--shardId=" << ele->cmd_->shardId_
      //              << "--replicaId=" << ele->cmd_->replicaId_
      //              << "--duration=" << duration;
      // }
      for (int32_t sid = 0; sid < shardNum_; sid++) {
         rrr::FutureAttr fuattr;
         std::function<void(Future*)> cb = [this, ele](Future* fu) {
            EpochReply* rep = new EpochReply();
            fu->get_reply() >> (*rep);
            if (AmDesignateReplica()) {
               // only desginate replica does reply
               // if (rep->sequenceNo_ % 100 == 1) {
               //    LOG(INFO) << "Reply from Scheduler " << rep->sequenceNo_
               //              << "\tduration="
               //              << GetMicrosecondTimestamp() -
               //              ele->cmd_->sendTime_;
               // }
               epochReplyQu_.enqueue(rep);
            } else {
               delete rep;
            }
         };
         // if (ele->cmd_->cmdVec_.size() > 0) {
         //    LOG(INFO) << "Report to Schedler Seq= " << ele->cmd_->sequenceNo_
         //              << "--shardId=" << ele->cmd_->shardId_
         //              << "--replicaId=" << ele->cmd_->replicaId_;
         // }
         fuattr.callback = cb;
         Future::safe_release(
             schedulerProxies_[sid]->async_EpochReport(*(ele->cmd_), fuattr));
      }
   }
}

void CalvinSequencer::ReportToClients() {
   EpochReply* reply;
   while (epochReplyQu_.try_dequeue(reply)) {
      // if (reply->resultVec_.size() > 0) {
      //    LOG(INFO) << "Reply seqNo=" << reply->sequenceNo_
      //              << "--resultVec_=" << reply->resultVec_.size();
      // }
      for (uint32_t j = 0; j < reply->resultVec_.size(); j++) {
         uint64_t txnKey = reply->txnKeys_[j];
         // LOG(INFO) << "TxnKey=" << txnKey << "--" << HIGH_32BIT(txnKey) <<
         // ":"
         //           << LOW_32BIT(txnKey);
         CalvinLogEntry* entry = GetEntry(entryMap_, txnKey);

         assert(entry != NULL);
         ShardQuorum& sq = shardQuorum_[txnKey];
         sq.shardReplies[reply->shardId_] = 1;
         for (auto& kv : reply->resultVec_[j]) {
            sq.results_.insert(kv);
         }

         // Check whether shard complete
         bool shardComplete = true;
         for (auto& kv : entry->shardKeyMap_) {
            if (sq.shardReplies[kv.first] == 0) {
               shardComplete = false;
               break;
            }
         }
         if (shardComplete) {
            // LOG(INFO) << "Report to client";

            // Reply to client
            CalvinReply rep;
            rep.clientId_ = entry->cmd_->clientId_;
            rep.reqId_ = entry->cmd_->reqId_;
            rep.replicaId_ = replicaId_;
            rep.shardId_ = shardId_;
            rep.result_ = sq.results_;
            entry->replyHandler_(rep);
            entry->replyHandler_ = NULL;
            shardQuorum_.erase(txnKey);
         }
      }
      delete reply;
   }
}

void CalvinSequencer::BroadcastInterReplicaSync() {
   uint32_t sz = syncedLogList_.size() - lastBroadcastSyncedLogId_;
   if (sz == 0) {
      // Nothing to broadcast
      return;
   }
   // has something to broadcast
   NezhaInterReplicaSync sync;
   sync.viewId_ = viewId_;
   sync.shardId_ = shardId_;
   sync.replicaId_ = replicaId_;
   sync.logIdStart_ = lastBroadcastSyncedLogId_ + 1;
   sync.deadlineRanks_.resize(sz);
   sync.txnKeys_.resize(sz);
   for (uint32_t i = sync.logIdStart_ - 1; i < syncedLogList_.size(); i++) {
      sync.deadlineRanks_[i - lastBroadcastSyncedLogId_] =
          syncedLogList_[i]->deadlineRank_;
      sync.txnKeys_[i - lastBroadcastSyncedLogId_] =
          syncedLogList_[i]->cmd_->TxnKey();
   }

   for (uint32_t rid = 0; rid < replicaNum_; rid++) {
      if (rid == replicaId_) {
         continue;
      }
      Future::safe_release(
          sequencerProxies_[rid]->async_InterReplicaSync(sync));
   }
   lastBroadcastSyncedLogId_ = syncedLogList_.size();
}

void CalvinSequencer::onReplicateEpochRequest(const EpochRequest& req) {
   CalvinEpochEntry* entry = new CalvinEpochEntry();
   entry->cmd_ = new EpochRequest(req);
   // if (true || req.sequenceNo_ % 100 == 1) {
   //    LOG(INFO) << "OnReplca seqNo=" << req.sequenceNo_ << "\t" <<
   //    req.shardId_
   //              << ":" << req.replicaId_ << "--duration="
   //              << GetMicrosecondTimestamp() - entry->cmd_->sendTime_;
   // }

   // Initialize Key Map
   for (auto& cmd : entry->cmd_->cmdVec_) {
      std::map<uint32_t, std::set<int32_t>> shardKeyMap;
      sm_->InitializeRelatedShards(cmd.txnType_, &(cmd.ws_), &shardKeyMap);
      for (auto& kv : shardKeyMap) {
         for (auto& key : kv.second) {
            entry->keys_.insert(key);
         }
      }
   }
   // LOG(INFO) << "keysSize=" << entry->keys_.size();
   // if (entry->keys_.size() > 0) {
   //    std::string keysStr = "";
   //    for (auto& k : entry->keys_) {
   //       keysStr += std::to_string(k) + ",";
   //    }
   //    LOG(INFO) << "keys=" << keysStr;
   // }

   uint64_t txnKey = entry->cmd_->TxnKey();
   CalvinEpochEntry* insertEntry = InsertEntry(epochEntryMap_, txnKey, entry);
   insertEntry->deadlineRank_ =
       insertEntry->cmd_->sendTime_ + insertEntry->cmd_->bound_;
   // if (req.sequenceNo_ % 100 == 1) {
   //    LOG(INFO) << "insertEntry " << insertEntry->cmd_->shardId_ << ":"
   //              << insertEntry->cmd_->replicaId_;
   // }

   toHoldAndReleaseQu_.enqueue(insertEntry);
}

void CalvinSequencer::onNezhaFastReply(const NezhaFastReply& rep) {
   if (!CheckView(rep.viewId_)) {
      return;
   }
   NezhaFastReply* reply = new NezhaFastReply(rep);
   fastReplyQu_.enqueue(reply);
}

void CalvinSequencer::onInterReplicaSync(const NezhaInterReplicaSync& req) {
   // LOG(INFO) << "logId=" << req.logIdStart_ << "\t" << req.txnKeys_.size()
   //           << "--nextSyncedLogId_=" << nextSyncedLogId_
   //           << "\t pendingSize=" << pendingTigaInterReplicaSyncs_.size();

   if (!CheckView(req.viewId_)) {
      return;
   }
   std::lock_guard<std::recursive_mutex> lock(interReplicaSyncMtx_);
   if (nextSyncedLogId_ < req.logIdStart_) {
      // some gap, just pending
      pendingInterReplicaSyncs_[req.logIdStart_] = req;
   } else {
      bool okay = ProcessInterReplicaSync(req);
      if ((!okay) && pendingInterReplicaSyncs_.find(req.logIdStart_) ==
                         pendingInterReplicaSyncs_.end()) {
         pendingInterReplicaSyncs_[req.logIdStart_] = req;
      }
   }
   ProcessPendingInterReplicaSync();
}

void CalvinSequencer::ProcessPendingInterReplicaSync() {
   std::lock_guard<std::recursive_mutex> lock(interReplicaSyncMtx_);
   while (!pendingInterReplicaSyncs_.empty()) {
      const NezhaInterReplicaSync& syncReq =
          pendingInterReplicaSyncs_.begin()->second;
      if (nextSyncedLogId_ < syncReq.logIdStart_) {
         break;
      } else {
         if (ProcessInterReplicaSync(syncReq)) {
            pendingInterReplicaSyncs_.erase(pendingInterReplicaSyncs_.begin());
         } else {
            break;
         }
      }
   }
}

bool CalvinSequencer::ProcessInterReplicaSync(
    const NezhaInterReplicaSync& req) {
   std::lock_guard<std::recursive_mutex> lock(interReplicaSyncMtx_);
   // LOG(INFO) << "sync " << req.logIdStart_ << "--" << req.txnKeys_.size();
   for (uint32_t i = 0; i < req.txnKeys_.size(); i++) {
      if (req.logIdStart_ + i < nextSyncedLogId_) {
         continue;
      }
      // LOG(INFO) << "InProcessSync " << (req.logIdStart_ + i);
      uint64_t txnKey = req.txnKeys_[i];
      CalvinEpochEntry* cmd = GetEntry(epochEntryMap_, txnKey);
      if (cmd == NULL) {
         // this->missingTxnKey_ = req.txnKeys_[i];
         // LOG(INFO) << "missing txnKey " << missingTxnKey_
         //           << "--logId=" << req.logIdStart_ + i;
         return false;
      }

      CalvinEpochEntry* entry = new CalvinEpochEntry();
      entry->cmd_ = cmd->cmd_;
      entry->deadlineRank_ = req.deadlineRanks_[i];
      entry->logId_ = nextSyncedLogId_.fetch_add(1);
      entry->reply_ = new NezhaFastReply();
      entry->myHash_.CalculateHash(entry->deadlineRank_, entry->cmd_->TxnKey());
      // LOG(INFO) << "shardsSize=" << entry->shardKeyMap_.size();
      for (auto& key : entry->keys_) {
         entry->accumulativeHashByKey_[key] = entry->myHash_;
         // Update SyncedEntries
         EntryQu<CalvinEpochEntry>* entryQu =
             GetOrCreateEntryByKey(syncedEntries_, key, logMtx_);
         // Leader does not need locks when inserting a specific qu (but
         // follower does need)
         {
            std::unique_lock lck(entryQu->mtx_);
            if (!entryQu->qu_.empty()) {
               entry->accumulativeHashByKey_[key].XOR(
                   entryQu->qu_.back()->accumulativeHashByKey_[key]);
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
            item->deadlineRank_ = entry->deadlineRank_;
            item->accumulativeHash_ = entry->accumulativeHashByKey_[key];
         }
      }

      syncedLogList_.push_back(entry);
   }

   return true;
}

bool CalvinSequencer::AmLeader() {
   return (viewId_ % replicaNum_ == replicaId_);
}

bool CalvinSequencer::AmDesignateReplica() {
   return (replicaId_ == designateReplicaId_);
}

bool CalvinSequencer::CheckView(uint32_t viewId) { return viewId_ == viewId; }

std::string CalvinSequencer::ServerAddrs(const uint32_t shardId,
                                         const uint32_t replicaId) {
   return serverAddrs_[shardId][replicaId];
}

std::string CalvinSequencer::MyServerAddr() {
   return serverAddrs_[shardId_][replicaId_];
}

std::string CalvinSequencer::MySequencerAddr() {
   return sequencerAddrs_[shardId_][replicaId_];
}

std::string CalvinSequencer::MySchedulerAddr() {
   return schedulerAddrs_[shardId_][replicaId_];
}

void CalvinSequencer::ThreadSleepFor(const uint32_t sleepMicroSecond) {
   std::chrono::microseconds duration(sleepMicroSecond);
   std::this_thread::yield();
   std::this_thread::sleep_for(duration);
}