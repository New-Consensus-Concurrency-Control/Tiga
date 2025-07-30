#include "DetockExecutor.h"

DetockExecutor::DetockExecutor(const std::string& serverName, StateMachine* sm,
                               DetockLogManager* mgr,
                               ConcurrentQueue<DetockLocalLogSync>* qu)
    : serverName_(serverName), sm_(sm), logManager_(mgr), logSyncQu_(qu) {
   viewId_ = 0;
   lastBatchId_ = 0;
   lastLocalSyncMsgId_ = 0;
   graphSeqNoToProcess_ = 1;
   lastProcessedGraphSeqNo_ = 0;
   sequencerRPCPoll_ = new PollMgr(2);
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
         sequencerAddrs_[sid][rid] = ip + ":" + std::to_string(port + 1);
      }
   }

   gQu_.resize(8);
   execQu_.resize(8);
}

DetockExecutor::~DetockExecutor() {}

void DetockExecutor::ConnectToSequencers() {
   // only connects to the replicas belonging to the same shard
   for (uint32_t sid = 0; sid < sm_->ShardNum(); sid++) {
      int ret = -1;
      sequencerRPCClients_[sid] = new rrr::Client(sequencerRPCPoll_);
      do {
         uint32_t rid = LeaderReplicaId(viewId_, sid);
         LOG(INFO) << "Sequencer Connecting to sid=" << sid << "\t rid=" << rid
                   << "\t"
                   << "ip=" << sequencerAddrs_[sid][rid];
         ret = sequencerRPCClients_[sid]->connect(
             sequencerAddrs_[sid][rid].c_str());
         if (ret == 0) {
            // success
            sequencerProxies_[sid] =
                new DetockSequencerProxy(sequencerRPCClients_[sid]);
            LOG(INFO) << "Log Manager Connected to sid=" << sid
                      << "\t rid=" << rid << "\t" << sequencerAddrs_[sid][rid];
         } else {
            ThreadSleepFor(1200000);
         }
      } while (ret != 0);
   }
}

void DetockExecutor::Run() {
   status_ = STATUS_NORMAL;
   mainTd_ = new std::thread(&DetockExecutor::MainTd, this);
}

void DetockExecutor::Stop() {
   status_ = SERVER_STATUS::STATUS_TERMINATE;
   LOG(INFO) << "Terminating...";
   mainTd_->join();
   LOG(INFO) << "MainTd Terminated...";
   delete mainTd_;

   LOG(INFO) << "Close Connections";
   for (uint32_t i = 0; i < sm_->ShardNum(); i++) {
      if (sequencerRPCClients_[i]) {
         sequencerRPCClients_[i]->close_and_release();
         delete sequencerProxies_[i];
      }
   }
   LOG(INFO) << "sequencerRPC closed";
   logManager_->CloseConnection();
}

void DetockExecutor::MainTd() {
   std::string name;
   while (true) {
      if (status_ == STATUS_NORMAL) {
         if (threadMap_.empty()) {
            std::string name = "BatchTd";
            LOG(INFO) << "Run " << name;
            threadMap_[name] = new std::thread(&DetockExecutor::BatchTd, this);

            name = "HoldReleaseTd";
            LOG(INFO) << "Run " << name;
            threadMap_[name] =
                new std::thread(&DetockExecutor::HoldReleaseTd, this);

            name = "ParseDepTd";
            LOG(INFO) << "Run " << name;
            threadMap_[name] =
                new std::thread(&DetockExecutor::ParseDepTd, this);

            name = "OrderTd";
            LOG(INFO) << "Run " << name;
            threadMap_[name] = new std::thread(&DetockExecutor::OrderTd, this);

            name = "ExecuteTd";
            LOG(INFO) << "Run " << name;
            threadMap_[name] =
                new std::thread(&DetockExecutor::ExecuteTd, this);

            name = "ReplyTd";
            LOG(INFO) << "Run " << name;
            threadMap_[name] = new std::thread(&DetockExecutor::ReplyTd, this);

            for (uint32_t idx = 0; idx < gQu_.size(); idx++) {
               name = "GraphTd-" + std::to_string(idx);
               threadMap_[name] =
                   new std::thread(&DetockExecutor::GraphTd, this, idx);
            }
         }
      } else if (status_ == STATUS_TERMINATE) {
         break;
      }

      ThreadSleepFor(10000);
   }

   LOG(INFO) << "Exit Main";
}

void DetockExecutor::onNormalRequest(const DetockTxn& req, DetockReply* rep,
                                     const std::function<void()>& cb) {
   DetockEntry* entry = new DetockEntry();
   entry->txn_ = new DetockTxn(req);
   entry->replyHandler_ = [rep, cb](const DetockReply& r) {
      *rep = r;
      cb();
   };

   sm_->InitializeRelatedShards(entry->txn_->txnType_, &(entry->txn_->ws_),
                                &(entry->shardKeyMap_));
   for (auto& k : entry->shardKeyMap_[sm_->ShardId()]) {
      entry->localKeys_.push_back(k);
   }
   entry->owd_ = GetMicrosecondTimestamp() - entry->txn_->sendTime_;
   // LOG(INFO) << "Got " << entry->txn_->reqId_
   //           << "--owd=" << GetMicrosecondTimestamp() - req.sendTime_;
   // TODO: Add Hold Qu
   // localEntryQu_.enqueue(entry);
   uint64_t txnKey = entry->txn_->TxnKey();
   InsertEntry(localEntryMap_, txnKey, entry);
   holdReleaseQu_.enqueue(entry);
}

void DetockExecutor::onDispatchRequest(const DetockDispatchRequest& req,
                                       DetockDispatchReply* rep,
                                       const std::function<void()>& cb) {
   sm_->PreRead(req.txnType_, &(req.input_), &(rep->result_));
   rep->shardId_ = sm_->ShardId();
   cb();
}

DetockLogManager* DetockExecutor::GetLogManager() { return logManager_; }

void DetockExecutor::HoldReleaseTd() {
   DetockEntry* entry;
   while (status_ == STATUS_NORMAL) {
      while (holdReleaseQu_.try_dequeue(entry)) {
         uint64_t rank = entry->txn_->sendTime_ + entry->txn_->bound_;
         uint64_t txnKey = entry->txn_->TxnKey();
         holdReleaseBuffer_[{rank, txnKey}] = entry;
      }
      uint64_t currentTime = GetMicrosecondTimestamp();
      while (!holdReleaseBuffer_.empty()) {
         uint64_t t = holdReleaseBuffer_.begin()->first.first;
         if (t < currentTime) {
            entry = holdReleaseBuffer_.begin()->second;
            holdReleaseBuffer_.erase(holdReleaseBuffer_.begin());
            batchQu_.enqueue(entry);
         } else {
            break;
         }
      }
      ThreadSleepFor(2000);
   }
}

void DetockExecutor::BatchTd() {
   DetockEntry* entry;
   std::vector<DetockEntry*>* entries = NULL;
   DetockBatch* batch = NULL;
   uint64_t lastPrint = 0;
   uint32_t totalBroadcastSize = 0;
   uint64_t entryNum = 0;
   activeThreads_.fetch_add(1);
   while (status_ == STATUS_NORMAL) {
      // uint64_t nowTime = GetMicrosecondTimestamp();
      // if (nowTime - lastPrint >= 5000 * 1000ul) {
      //    LOG(INFO) << "totalBroadcastSize=" << totalBroadcastSize
      //              << "\tentryNum=" << entryNum;
      //    lastPrint = nowTime;
      // }
      // Only the home region for this shard runs batching and log sync
      if (batchQu_.size_approx() == 0) {
         ThreadSleepFor(5000);
      }
      while (batchQu_.try_dequeue(entry)) {
         entryNum++;
         if (batch == NULL) {
            batch = new DetockBatch();
            batch->batchId_ = lastBatchId_ + 1;
            entries = new std::vector<DetockEntry*>();
         }
         batch->batchContent_.push_back(*(entry->txn_));
         entries->push_back(entry);
         if (batch->batchContent_.size() == 50) {
            logManager_->AppendNewBatch(batch);
            pendingBatchToSync_.push({batch->batchId_, entries});
            lastBatchId_++;
            delete batch;
            batch = NULL;
            entries = NULL;
         }
      }
      if (batch) {
         logManager_->AppendNewBatch(batch);
         pendingBatchToSync_.push({batch->batchId_, entries});
         lastBatchId_++;
         delete batch;
         batch = NULL;
         entries = NULL;
      }
      // Check whether the batches have been persisted (so that we can do global
      // sync)
      uint32_t committedBatchId = logManager_->GetCommitedLogId();
      while (!pendingBatchToSync_.empty()) {
         if (pendingBatchToSync_.front().first <= committedBatchId) {
            // Can globally sync
            std::vector<DetockEntry*>* entries =
                pendingBatchToSync_.front().second;
            totalBroadcastSize += entries->size();
            BroadcastLogSync(*entries);
            delete entries;
            pendingBatchToSync_.pop();
         } else {
            break;
         }
      }
   }
   activeThreads_.fetch_sub(1);
}
void DetockExecutor::BroadcastLogSync(const std::vector<DetockEntry*> entries) {
   if (entries.empty()) {
      return;
   }
   lastLocalSyncMsgId_++;
   DetockLocalLogSync sync;
   sync.viewId_ = viewId_;
   sync.msgId_ = lastLocalSyncMsgId_;
   sync.shardId_ = sm_->ShardId();
   sync.replicaId_ = sm_->ReplicaId();
   sync.txnMetas_.resize(entries.size());

   for (uint32_t i = 0; i < entries.size(); i++) {
      sync.txnMetas_[i].txnKey_ = entries[i]->txn_->TxnKey();
      sync.txnMetas_[i].localKeys_ = entries[i]->localKeys_;
      for (auto& kv : entries[i]->shardKeyMap_) {
         sync.txnMetas_[i].shardIds_.push_back(kv.first);
      }
   }

   // LOG(INFO) << "Broadcast msgId= " << sync.msgId_
   //           << "--size=" << sync.txnMetas_.size();
   // Broadcast
   for (uint32_t sid = 0; sid < sm_->ShardNum(); sid++) {
      sequencerProxies_[sid]->async_SyncBatch(sync);
   }
}

void DetockExecutor::ParseDepTd() {
   DetockLocalLogSync sync;
   DetockEntry* entry;
   ConcurrentQueue<int> testQu_;
   uint32_t totalMetas = 0;
   uint32_t totalValidV = 0;
   uint32_t totalInvalidV = 0;
   uint64_t lastPrint = 0;
   activeThreads_.fetch_add(1);
   while (status_ == STATUS_NORMAL) {
      // uint64_t nowTime = GetMicrosecondTimestamp();
      // if (nowTime - lastPrint >= 5000 * 1000ul) {
      //    LOG(INFO) << "logSync sync " << sync.shardId_
      //              << "--msgId=" << sync.msgId_
      //              << "--metaDataSize=" << sync.txnMetas_.size() << "\t"
      //              << "--totalMetas=" << totalMetas
      //              << "--totalValidV=" << totalValidV
      //              << "--totalInvalidV=" << totalInvalidV
      //              << "--outstanding=" << txnMetaData_.size();
      //    lastPrint = nowTime;
      // }
      while (logSyncQu_->try_dequeue(sync)) {
         if (sync.shardId_ == 0) {
            totalMetas += sync.txnMetas_.size();
         }

         if (sync.viewId_ != viewId_) {
            continue;
         }
         // Update global logs
         for (uint32_t i = 0; i < sync.txnMetas_.size(); i++) {
            bool classified = false;
            uint64_t txnKey = sync.txnMetas_[i].txnKey_;
            // Filter out txns that does not involve my shard
            if (!ContainShard(sync.txnMetas_[i].shardIds_, sm_->ShardId())) {
               // LOG(INFO) << "Invalid V:" << HIGH_32BIT(txnKey) << ":"
               //           << LOW_32BIT(txnKey);
               if (sync.shardId_ == 0) {
                  totalInvalidV++;
               }
               continue;
            }
            // Get the edges
            for (auto& k : sync.txnMetas_[i].localKeys_) {
               if (prevTxns_[sync.shardId_].find(k) !=
                   prevTxns_[sync.shardId_].end()) {
                  // There is an edge
                  uint64_t prevTxnKey = prevTxns_[sync.shardId_][k];
                  GraphInfo info;
                  info.type_ = 0;
                  info.from_ = prevTxnKey;
                  info.to_ = txnKey;
                  // LOG(INFO) << "Enque edge " << info.from_ << "->" <<
                  // info.to_;
                  graphInfoQu_.enqueue(info);
               }
            }
            for (auto& k : sync.txnMetas_[i].localKeys_) {
               prevTxns_[sync.shardId_][k] = txnKey;
            }

            if (txnMetaData_.find(txnKey) == txnMetaData_.end()) {
               for (auto& sid : sync.txnMetas_[i].shardIds_) {
                  txnMetaData_[txnKey].involvedShardIds_.insert(sid);
               }
            }
            txnMetaData_[txnKey].collectedShardIds_.insert(sync.shardId_);
            if (txnMetaData_[txnKey].isComplete()) {
               GraphInfo info;
               info.type_ = 1;
               info.from_ = txnKey;
               // LOG(INFO) << "Enque Complete vertex " << info.from_ << "("
               //           << HIGH_32BIT(txnKey) << ":" << LOW_32BIT(txnKey)
               //           << ")";
               // LOG(INFO) << "Valid V:" << HIGH_32BIT(txnKey) << ":"
               //           << LOW_32BIT(txnKey);
               if (ContainShard(sync.txnMetas_[i].shardIds_, 0)) {
                  totalValidV++;
               }
               graphInfoQu_.enqueue(info);
               txnMetaData_.erase(txnKey);
            }
         }
      }
   }
   activeThreads_.fetch_sub(1);
}

void DetockExecutor::OrderTd() {
   GraphInfo info;
   activeThreads_.fetch_add(1);
   uint64_t lastPrint = 0;
   while (status_ == STATUS_NORMAL) {
      // if (graphInfoQu_.size_approx() == 0) {
      //    // usleep(40000);
      //    // sleep(1);
      //    usleep(1000);
      // }
      // uint64_t nowTime = GetMicrosecondTimestamp();
      // if (nowTime - lastPrint >= 5000 * 1000ul) {
      //    LOG(INFO) << "completeVerices =" << completeVertices_.size()
      //              << "--incomplete=" << incompleteVertices_.size()
      //              << "--unstableVertices_=" << unstableVertices_.size()
      //              << "--pendingTxnToExecute_=" <<
      //              pendingTxnToExecute_.size()
      //              << "--depGraph=" << depGraph_.size();
      //    std::string lg = "";
      //    for (auto& t : incompleteVertices_) {
      //       lg += std::to_string(HIGH_32BIT(t)) + ":" +
      //             std::to_string(LOW_32BIT(t)) + "\t";
      //    }
      //    LOG(INFO) << "incomplete " << lg;
      //    lastPrint = nowTime;
      // }
      while (graphInfoQu_.try_dequeue(info)) {
         if (info.type_ == 0) {
            // edge
            if (prunedVs_.find(info.from_) == prunedVs_.end()) {
               depGraph_[info.from_].insert(info.to_);
               reverseGraph_[info.to_].insert(info.from_);
               incompleteVertices_.insert(info.from_);
               incompleteVertices_.insert(info.to_);
            } else {
               // This edge is not meaningful, only need to record the end
               // vertex
               depGraph_[info.to_];
               incompleteVertices_.insert(info.to_);
            }

            // LOG(INFO) << "Edge " << info.from_ << "->" << info.to_;
         } else {
            // vertices (completed)
            // LOG(INFO) << "Vertex " << HIGH_32BIT(info.from_) << ":"
            //           << LOW_32BIT(info.from_);
            // make sure the vertex is in the stable graph even if it does not
            // have edges
            depGraph_[info.from_];
            completeVertices_.insert(info.from_);
         }
      }
      // Purify Incomplete vertices in the depGraph_
      std::unordered_set<uint64_t> purifiedInComplete;
      for (auto& txnKey : incompleteVertices_) {
         if (completeVertices_.find(txnKey) == completeVertices_.end()) {
            purifiedInComplete.insert(txnKey);
            incompleteVerticesQu_.push(txnKey);
         }
      }
      incompleteVertices_ = purifiedInComplete;

      /// 24ms
      //////////////////////////////////////////////////////////////////////
      // Get Unstable Vertices->BFS
      unstableVertices_.clear();
      std::unordered_set<uint64_t> visited;
      while (!incompleteVerticesQu_.empty()) {
         uint64_t txnKey = incompleteVerticesQu_.front();
         if (visited.find(txnKey) != visited.end()) {
            // already visited
            incompleteVerticesQu_.pop();
            continue;
         }
         incompleteVerticesQu_.pop();
         visited.insert(txnKey);
         unstableVertices_.insert(txnKey);
         for (auto& depNode : depGraph_[txnKey]) {
            if (visited.find(depNode) == visited.end()) {
               incompleteVerticesQu_.push(depNode);
            }
         }
      }
      // 23ms
      // Get a subgraph that only contains Stable Nodes
      // CompleteVertics-UnstableVertices

      DepGraph* stableGraph = new DepGraph();
      for (auto& kv : depGraph_) {
         uint64_t from = kv.first;
         if (completeVertices_.find(from) != completeVertices_.end() &&
             unstableVertices_.find(from) == unstableVertices_.end()) {
            // make sure the vertex is in the stable graph
            // even if it does not have edges
            // stableDepGraph_[from];
            stableVertices_.insert(from);
            stableGraph->insert({from, {}});
            // stable
            for (auto& to : kv.second) {
               if (completeVertices_.find(to) != completeVertices_.end() &&
                   unstableVertices_.find(to) == unstableVertices_.end()) {
                  // stableDepGraph_[from].insert(to);
                  (*stableGraph)[from].insert(to);
                  stableVertices_.insert(to);
               }
            }
         }
      }

      if (stableGraph->size() > 0) {
         // LOG(INFO) << "graphSeqNo=" << graphSeqNoToProcess_;
         gQu_[(graphSeqNoToProcess_) % gQu_.size()].enqueue(
             {graphSeqNoToProcess_, stableGraph});
         graphSeqNoToProcess_++;
         // Prune depgraph
         while (!stableVertices_.empty()) {
            auto& stableV = *stableVertices_.begin();
            depGraph_.erase(stableV);
            for (auto& from : reverseGraph_[stableV]) {
               if (depGraph_.find(from) != depGraph_.end()) {
                  depGraph_[from].erase(stableV);
               }
            }
            prunedVs_.insert(stableV);
            stableVertices_.erase(stableVertices_.begin());
         }
      } else {
         delete stableGraph;
      }
   }
   activeThreads_.fetch_sub(1);
}

void DetockExecutor::GraphTd(int idx) {
   DepGraphInfo info;
   while (status_ == STATUS_NORMAL) {
      while (gQu_[idx].try_dequeue(info)) {
         DepGraph& stableDepGraph = *(info.graph_);
         // Find SCC
         std::vector<std::set<uint64_t>>* sccsPtr = findSCCs(stableDepGraph);
         std::vector<std::set<uint64_t>>& sccs = (*sccsPtr);
         ExecSeqInfo execSeqInfo;
         execSeqInfo.graphSeqNo_ = info.graphSeqNo_;
         execSeqInfo.txnKeys_ = new std::vector<uint64_t>();
         // LOG(INFO) << "to exec " << execSeqInfo.graphSeqNo_;
         if (sccs.size() > 0) {
            // LOG(INFO) << "sccs " << sccs.size();
            // Establish condensation
            SCCGraph* condensationGraphPtr = new SCCGraph();
            SCCGraph& condensationGraph = (*condensationGraphPtr);
            for (uint32_t sccNo = 0; sccNo < sccs.size(); sccNo++) {
               // even if this scc does not have edges,
               // we need to make sure it is
               // included in condensationGraph_
               condensationGraph[sccNo];
               for (uint32_t sccNo2 = 0; sccNo2 < sccs.size(); sccNo2++) {
                  if (sccNo == sccNo2) continue;
                  if (isSCCConnected(sccs[sccNo], sccs[sccNo2],
                                     stableDepGraph)) {
                     condensationGraph[sccNo].insert(sccNo2);
                  }
               }
            }
            // Topological Sort
            std::vector<uint32_t> orderedSCCs =
                topologicalSort(condensationGraph);
            // LOG(INFO) << "orderedSCCs " << orderedSCCs.size();
            for (auto& sccNo : orderedSCCs) {
               auto& scc = sccs[sccNo];
               for (auto& txnKey : scc) {
                  // LOG(INFO) << "ToexecQu " << HIGH_32BIT(txnKey) << ":"
                  //           << LOW_32BIT(txnKey);
                  execSeqInfo.txnKeys_->push_back(txnKey);
               }
            }
            delete condensationGraphPtr;
         }

         execQu_[idx].enqueue(execSeqInfo);
         // execQu_[0].enqueue(execSeqInfo);
         delete info.graph_;
      }
      ThreadSleepFor(1000);
   }
}

void DetockExecutor::ExecuteTd() {
   DetockEntry* entry;
   ExecSeqInfo execSeqInfo;
   std::vector<ExecSeqInfo> evec;
   uint64_t lastPrint = 0;
   activeThreads_.fetch_add(1);
   while (status_ == STATUS_NORMAL) {
      uint32_t toProcessGraphSeqNo = lastProcessedGraphSeqNo_ + 1;
      uint32_t quIdx = (toProcessGraphSeqNo) % execQu_.size();
      // Round-Robin order
      while (execQu_[quIdx].try_dequeue(execSeqInfo) == false) {
         // ThreadSleepFor(1000);
      }
      // while (execQu_[0].try_dequeue(execSeqInfo))
      {
         auto& txnKeys = execSeqInfo.txnKeys_;
         for (auto& txnKey : *txnKeys) {
            // Executel
            DetockEntry* entry = GetEntry(localEntryMap_, txnKey);
            entry->reply_ = new DetockReply();
            entry->reply_->owd_ = entry->owd_;
            // LOG(INFO) << "entry owd =" << entry->owd_;
            entry->reply_->clientId_ = entry->txn_->clientId_;
            entry->reply_->reqId_ = entry->txn_->reqId_;
            entry->reply_->replicaId_ = sm_->ReplicaId();
            entry->reply_->shardId_ = sm_->ShardId();
            sm_->Execute(entry->txn_->txnType_, &(entry->localKeys_),
                         &(entry->txn_->ws_), &(entry->reply_->result_),
                         reinterpret_cast<uint64_t>(entry));
            replyQu_.enqueue(entry);
         }
         delete txnKeys;
         lastProcessedGraphSeqNo_++;
      }
   }
   activeThreads_.fetch_sub(1);
}

void DetockExecutor::ReplyTd() {
   DetockEntry* entry;
   uint64_t txnKey;
   activeThreads_.fetch_add(1);
   while (status_ == STATUS_NORMAL) {
      while (replyQu_.try_dequeue(entry)) {
         // assert(entry->replyHandler_ != NULL);
         // assert(entry->reply_ != NULL);
         // LOG(INFO) << "reply " << entry->reply_->clientId_ << ":"
         //           << entry->reply_->reqId_;
         entry->replyHandler_(*(entry->reply_));
         delete (entry->reply_);
         entry->reply_ = NULL;
         entry->replyHandler_ = NULL;
      }
   }
   activeThreads_.fetch_sub(1);
}

uint32_t DetockExecutor::LeaderReplicaId(const uint32_t viewId,
                                         const uint32_t shardId) {
   return ((viewId % sm_->ReplicaNum()) + shardId) % sm_->ReplicaNum();
}

bool DetockExecutor::ContainShard(const std::vector<uint32_t>& shardIds,
                                  const uint32_t shardId) const {
   for (const uint32_t& sid : shardIds) {
      if (sid == shardId) {
         return true;
      }
   }
   return false;
}