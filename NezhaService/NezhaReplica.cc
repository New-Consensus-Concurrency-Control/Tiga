#include "NezhaReplica.h"

NezhaReplica::NezhaReplica(const std::string& serverName,
                           const YAML::Node& config) {

   shardId_ = replicaId_ = UINT32_MAX;
   activeThreads_ = 0;
   reqIdNo_ = 0;
   // TODO: replace with replica
   shardNum_ = config["site"]["replica"].size();
   replicaNum_ = config["site"]["replica"][0].size();

   viewId_ = 0;

   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         std::string fullName =
             config["site"]["replica"][sid][rid].as<std::string>();
         LOG(INFO) << "fullName=" << fullName;
         std::string thisServerName = fullName.substr(0, fullName.find(':'));
         std::string portName = fullName.substr(fullName.find(':') + 1);
         std::string ip = config["host"][thisServerName].as<std::string>();
         int port = std::stoi(portName);
         serverAddrs_[sid][rid] = ip + ":" + std::to_string(port);
         if (thisServerName == serverName) {
            shardId_ = sid;
            replicaId_ = rid;
         }
      }
   }

   LOG(INFO) << "my ShardId=" << shardId_ << "\t my ReplicaId=" << replicaId_
             << "--myAddr=" << serverAddrs_[shardId_][replicaId_];

   uint32_t initialOWD =
       config["server_initial_bound"][replicaId_].as<uint32_t>();
   for (uint32_t i = 0; i < replicaNum_; i++) {
      owds_[i] = initialOWD;
   }
   LOG(INFO) << "owds init " << initialOWD;

   memset(replicaProxies_, '\0', sizeof(NezhaProxy*) * MAX_REPLICA_NUM);
   memset(replicaRPCClients_, '\0', sizeof(rrr::Client*) * MAX_REPLICA_NUM);

   lastReleasedTxnDeadlines_.resize(1000ul * 1000ul, 0);
   // to change to global and local proxy
   // intra-DC needs more requests, to be optimized in the future
   serverRPCPoll_ = new PollMgr(2);

   nextSyncedLogId_ = 1;

   syncedLogList_.reserve(1000ul * 1000ul * 100);
   lastBroadcastSyncedLogId_ = 0;

   status_ = STATUS_NORMAL;
}

NezhaReplica::~NezhaReplica() {}

void NezhaReplica::ConnectToReplicas() {
   // only connects to the replicas belonging to the same shard
   uint32_t sid = shardId_;
   LOG(INFO) << "ConnectToOtherSequencers sid= " << sid;
   for (uint32_t rid = 0; rid < replicaNum_; rid++) {
      int ret = -1;
      LOG(INFO) << "Try rid= " << rid;
      replicaRPCClients_[rid] = new rrr::Client(serverRPCPoll_);
      do {
         LOG(INFO) << "Connecting to sid=" << sid << "\t rid=" << rid << "\t"
                   << serverAddrs_[sid][rid];
         ret = replicaRPCClients_[rid]->connect(serverAddrs_[sid][rid].c_str());
         if (ret == 0) {
            // success
            replicaProxies_[rid] = new NezhaProxy(replicaRPCClients_[rid]);
            LOG(INFO) << "Connected to " << sid << "\t rid=" << rid << "\t"
                      << serverAddrs_[sid][rid];
         } else {
            ThreadSleepFor(1200000);
         }
      } while (ret != 0);
   }
}

void NezhaReplica::Run() {
   status_ = STATUS_NORMAL;
   mainTd_ = new std::thread(&NezhaReplica::MainTd, this);
}

void NezhaReplica::MainTd() {
   std::string name;
   while (true) {
      if (status_ == STATUS_NORMAL) {
         if (threadMap_.empty()) {
            if (AmLeader()) {
               name = "LeadererHoldAndReleaseTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] =
                   new std::thread(&NezhaReplica::LeaderHoldAndReleaseTd, this);

               name = "LeaderExecuteTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] =
                   new std::thread(&NezhaReplica::LeaderExecuteTd, this);

               name = "LeaderReplyTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] =
                   new std::thread(&NezhaReplica::LeaderReplyTd, this);

            } else {
               name = "FollowerHoldAndReleaseTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] = new std::thread(
                   &NezhaReplica::FollowerHoldAndReleaseTd, this);

               name = "FollowerExecuteTd";
               LOG(INFO) << "Run " << name;
               threadMap_[name] =
                   new std::thread(&NezhaReplica::FollowerExecuteTd, this);
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

void NezhaReplica::Stop() {
   status_ = SERVER_STATUS::STATUS_TERMINATE;
   LOG(INFO) << "Close Connections";

   for (uint32_t i = 0; i < replicaNum_; i++) {
      if (replicaRPCClients_[i]) {
         replicaRPCClients_[i]->close_and_release();
         delete replicaProxies_[i];
      }
   }
   LOG(INFO) << "replicaRPC closed";

   LOG(INFO) << "Terminating...";
   mainTd_->join();
   delete mainTd_;
   LOG(INFO) << "Terminated...";
}

void NezhaReplica::onNezhaReplicateRequest(const NezhaRequest& req,
                                           NezhaFastReply* rep,
                                           const std::function<void()>& cb) {
   NezhaLogEntry* entry = new NezhaLogEntry();
   entry->cmd_ = new NezhaRecordMessage(req.cmd_);
   entry->deadlineRank_ = req.sendTime_ + req.bound_;
   entry->sendTime_ = req.sendTime_;
   entry->clientId_ = req.clientId_;
   entry->reqId_ = req.reqId_;
   for (auto& k : req.cmd_.keys_) {
      entry->keys_.insert(k);
   }
   entry->replyHandler_ = [rep, cb](const NezhaFastReply& r) {
      *rep = r;
      cb();
   };
   uint64_t txnKey = entry->TxnKey();
   InsertEntry(entryMap_, txnKey, entry);
   // LOG(INFO) << "req " << req.clientId_ << ":" << req.reqId_
   //           << "--sendTIme=" << req.sendTime_ << "--bound=" << req.bound_;
   toHoldAndReleaseQu_.enqueue(entry);
}

void NezhaReplica::LeaderHoldAndReleaseTd() {
   NezhaLogEntry* entries[UINT8_MAX];
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
            NezhaLogEntry* entry = entries[i];
            uint64_t txnKey = entry->cmd_->txnKey_;
            // Check whether the txn can enter the early buffer
            uint64_t nowTime = GetMicrosecondTimestamp();
            if (nowTime < entry->sendTime_) {
               entry->owd_ = 0;
            } else {
               entry->owd_ = nowTime - entry->sendTime_;
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
         NezhaLogEntry* entry = holdBuffer_.begin()->second;
         // LOG(INFO) << "toExec " << entry->senderReplicaId_ << ":"
         //           << entry->reqId_;
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

void NezhaReplica::LeaderExecuteTd() {
   NezhaLogEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   uint32_t tmp = 0;
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toExecQu_.try_dequeue_bulk(entries, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            NezhaLogEntry* entry = entries[i];
            entry->reply_ = new NezhaFastReply();
            // LOG(INFO) << "Leader Execute " << entry->senderReplicaId_ << ":"
            //           << entry->reqId_;
            LeaderNormalExecute(entry);
         }
      }
      BroadcastInterReplicaSync();
   }
   LOG(INFO) << "LeaderExecuteTd Terminated";
   activeThreads_.fetch_sub(1);
}

void NezhaReplica::LeaderNormalExecute(NezhaLogEntry* entry) {
   entry->myHash_.CalculateHash(entry->deadlineRank_, entry->cmd_->txnKey_);
   // Make Hash and send reply
   entry->logId_ = nextSyncedLogId_.fetch_add(1);
   // std::string keyStr = "";
   for (auto& key : entry->keys_) {
      // keyStr += std::to_string(key) + ",";
      EntryQu<NezhaLogEntry>* entryQu =
          GetOrCreateEntryByKey(syncedEntries_, key, logMtx_);
      entry->accumulativeHashByKey_[key] = entry->myHash_;
      // Leader does not need locks when inserting a specific qu
      // (but follower does need)
      if (!entryQu->qu_.empty()) {
         entryQu->qu_.back();
         NezhaLogEntry* syncedInfo = entryQu->qu_.back();
         entry->accumulativeHashByKey_[key].XOR(
             syncedInfo->accumulativeHashByKey_[key]);
      }
      entryQu->qu_.push(entry);
   }
   syncedLogList_.push_back(entry);
   assert(entry->logId_ == syncedLogList_.size());
   // LOG(INFO) << "toReply " << entry->senderReplicaId_ << ":" <<
   // entry->reqId_;
   toReplyQu_.enqueue(entry);
}

void NezhaReplica::LeaderReplyTd() {
   NezhaLogEntry* eles[UINT8_MAX];
   uint32_t cnt = 0;
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toReplyQu_.try_dequeue_bulk(eles, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            NezhaLogEntry* entry = eles[i];
            NezhaFastReply& reply = *(entry->reply_);
            reply.viewId_ = viewId_;
            reply.replicaId_ = replicaId_;
            reply.token_ = entry->cmd_->txnKey_;
            reply.hasHash_ = 1; /* fast reply, i.e., has hash */

            for (auto& kv : entry->accumulativeHashByKey_) {
               NezhaHash hsh = AddKeyToHash(
                   entry->accumulativeHashByKey_[kv.first], kv.first);
               reply.hash_.XOR(hsh);
            }
            reply.owd_ = entry->owd_;
            reply.logId_ = entry->logId_;
            reply.clientId_ = entry->clientId_;
            reply.reqId_ = entry->reqId_;
            reply.latestSyncedLogId_ = nextSyncedLogId_ - 1;
            reply.shardId_ = shardId_;
            reply.replicaId_ = replicaId_;
            if (entry->replyHandler_) {
               entry->replyHandler_(reply);
               entry->replyHandler_ = NULL;
            } else {
               LOG(ERROR) << "Null Reply " << entry->clientId_ << ":"
                          << entry->reqId_;
            }
         }
      }
   }

   LOG(INFO) << "LeaderReplyTd Terminated";
   activeThreads_.fetch_sub(1);
}

void NezhaReplica::FollowerHoldAndReleaseTd() {
   NezhaLogEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   uint32_t enqCnt = 0;
   activeThreads_.fetch_add(1);
   uint64_t lastReleaseTime = 0;
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toHoldAndReleaseQu_.try_dequeue_bulk(entries, UINT8_MAX)) >
             0) {
         for (uint32_t i = 0; i < cnt; i++) {
            NezhaLogEntry* entry = entries[i];
            uint64_t txnKey = entry->cmd_->txnKey_;
            uint64_t nowTime = GetMicrosecondTimestamp();
            if (nowTime < entry->sendTime_) {
               entry->owd_ = 0;
            } else {
               entry->owd_ = nowTime - entry->sendTime_;
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
         NezhaLogEntry* entry = holdBuffer_.begin()->second;
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

void NezhaReplica::FollowerExecuteTd() {
   NezhaLogEntry* entries[UINT8_MAX];
   uint32_t cnt = 0;
   activeThreads_.fetch_add(1);
   while (status_ == SERVER_STATUS::STATUS_NORMAL) {
      while ((cnt = toExecQu_.try_dequeue_bulk(entries, UINT8_MAX)) > 0) {
         for (uint32_t i = 0; i < cnt; i++) {
            NezhaLogEntry* entry = entries[i];
            entry->myHash_.CalculateHash(entry->deadlineRank_,
                                         entry->cmd_->txnKey_);
            // Make Hash and send reply
            std::unordered_map<uint32_t, NezhaLogEntry*> boundaryLogInfos;
            std::unordered_map<uint32_t, SyncedHashItem>
                boundarySyncedHashItems;
            for (auto& key : entry->keys_) {
               entry->accumulativeHashByKey_[key] = entry->myHash_;
               EntryQu<NezhaLogEntry>* entryQu =
                   GetOrCreateEntryByKey(unSyncedEntries_, key, logMtx_);
               // Get Qu and insert the entry
               {
                  std::unique_lock lk(entryQu->mtx_);
                  if (!entryQu->qu_.empty()) {
                     NezhaLogEntry* lastUnSyncedLogInfo = entryQu->qu_.back();
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
            rep.shardId_ = shardId_;
            rep.replicaId_ = replicaId_;
            rep.clientId_ = entry->clientId_;
            rep.reqId_ = entry->reqId_;
            rep.owd_ = entry->owd_;
            rep.token_ = entry->cmd_->txnKey_;
            rep.logId_ = 0;   /* follower does not include logId*/
            rep.hasHash_ = 1; /* fast reply, i.e., has hash */
            std::map<uint32_t, NezhaHash> refinedHash;
            for (auto& key : entry->keys_) {
               NezhaHash hash = entry->accumulativeHashByKey_[key];
               // LOG(INFO) << "key=" << key
               //           << "--hashBeforeB=" << hash.ToString();
               if (boundaryLogInfos.find(key) != boundaryLogInfos.end()) {
                  NezhaLogEntry* boundaryLogInfo = boundaryLogInfos[key];
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
            if (entry->replyHandler_) {
               entry->replyHandler_(rep);
            } else {
               LOG(ERROR) << "Null reply Handler " << entry->clientId_ << ":"
                          << entry->reqId_;
            }
         }
      }
   }
   LOG(INFO) << "FollowerSpecExecuteTd Terminated";
   activeThreads_.fetch_sub(1);
}

void NezhaReplica::onNezhaInquireRequest(const NezhaInquireRequest& req,
                                         NezhaInquireReply* rep) {

   if (!CheckView(req.viewId_)) {
      return;
   }
   rep->viewId_ = viewId_;
   rep->shardId_ = shardId_;
   rep->replicaId_ = replicaId_;
   rep->latestSyncedLogId_ = nextSyncedLogId_ - 1;
}

void NezhaReplica::BroadcastInterReplicaSync() {
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
          syncedLogList_[i]->TxnKey();
   }

   for (uint32_t rid = 0; rid < replicaNum_; rid++) {
      if (rid == replicaId_) {
         continue;
      }
      Future::safe_release(replicaProxies_[rid]->async_InterReplicaSync(sync));
   }
   lastBroadcastSyncedLogId_ = syncedLogList_.size();
}

void NezhaReplica::onNezhaFastReply(const NezhaFastReply& rep) {
   if (!CheckView(rep.viewId_)) {
      return;
   }
   NezhaFastReply* reply = new NezhaFastReply(rep);

   fastReplyQu_.enqueue(reply);
}

void NezhaReplica::onInterReplicaSync(const NezhaInterReplicaSync& req) {
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

void NezhaReplica::ProcessPendingInterReplicaSync() {
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

bool NezhaReplica::ProcessInterReplicaSync(const NezhaInterReplicaSync& req) {
   std::lock_guard<std::recursive_mutex> lock(interReplicaSyncMtx_);
   // LOG(INFO) << "sync " << req.logIdStart_ << "--" << req.txnKeys_.size();
   for (uint32_t i = 0; i < req.txnKeys_.size(); i++) {
      if (req.logIdStart_ + i < nextSyncedLogId_) {
         continue;
      }
      // LOG(INFO) << "InProcessSync " << (req.logIdStart_ + i);
      uint64_t txnKey = req.txnKeys_[i];
      NezhaLogEntry* cmd = GetEntry(entryMap_, txnKey);
      if (cmd == NULL) {
         // this->missingTxnKey_ = req.txnKeys_[i];
         // LOG(INFO) << "missing txnKey " << missingTxnKey_
         //           << "--logId=" << req.logIdStart_ + i;
         return false;
      }

      NezhaLogEntry* entry = new NezhaLogEntry();
      entry->cmd_ = cmd->cmd_;
      entry->deadlineRank_ = req.deadlineRanks_[i];
      entry->logId_ = nextSyncedLogId_.fetch_add(1);
      entry->reply_ = new NezhaFastReply();
      entry->myHash_.CalculateHash(entry->deadlineRank_, entry->cmd_->txnKey_);
      // LOG(INFO) << "shardsSize=" << entry->shardKeyMap_.size();
      for (auto& key : entry->keys_) {
         entry->accumulativeHashByKey_[key] = entry->myHash_;
         // Update SyncedEntries
         EntryQu<NezhaLogEntry>* entryQu =
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

bool NezhaReplica::AmLeader() { return (viewId_ % replicaNum_ == replicaId_); }

bool NezhaReplica::CheckView(uint32_t viewId) { return viewId_ == viewId; }

std::string NezhaReplica::ServerAddrs(const uint32_t shardId,
                                      const uint32_t replicaId) {
   return serverAddrs_[shardId][replicaId];
}

std::string NezhaReplica::MyServerAddr() {
   return serverAddrs_[shardId_][replicaId_];
}

void NezhaReplica::ThreadSleepFor(const uint32_t sleepMicroSecond) {
   std::chrono::microseconds duration(sleepMicroSecond);
   std::this_thread::yield();
   std::this_thread::sleep_for(duration);
}