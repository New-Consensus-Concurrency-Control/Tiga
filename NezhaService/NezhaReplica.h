#pragma once
#include "NezhaService/NezhaMessage.h"
#include "NezhaService/NezhaService.h"
using namespace NezhaRPC;

struct ReplicaQuorum {
   NezhaHash hashes_[MAX_REPLICA_NUM];
   uint32_t fastReplies[MAX_REPLICA_NUM];
   uint32_t leaderLogId_;
   ReplicaQuorum() {
      memset(fastReplies, '\0', sizeof(fastReplies));
      leaderLogId_ = 0;
   }
};

class NezhaReplica {
  protected:
   uint64_t lastPrintTime_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t shardNum_;
   uint32_t replicaNum_;

   uint32_t status_;
   uint32_t viewId_;
   // const uint32_t interDCYieldPeriodUs = 2000;  // WAN
   // const uint32_t intraDCYieldPeriodUs = 2000;  // WAN

   const uint32_t interDCYieldPeriodUs = 1000;  // LAN
   const uint32_t intraDCYieldPeriodUs = 0;     // LAN

   std::atomic<uint32_t> reqIdNo_;
   std::string serverAddrs_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   PollMgr* serverRPCPoll_;
   NezhaProxy* replicaProxies_[MAX_SHARD_NUM];
   rrr::Client* replicaRPCClients_[MAX_SHARD_NUM];

   using ReplyHandler = std::function<void(const NezhaAck&)>;
   EntryMap<NezhaLogEntry> entryMap_[HASH_PARTITION_NUM];
   EntryMap<ReplyHandler> replyHandlerMap_[HASH_PARTITION_NUM];  //

   std::unordered_map<uint64_t, ReplicaQuorum> replicaQuorum_;
   std::unordered_set<uint64_t> replicatedTxns_;
   std::map<uint32_t, uint64_t> incompleteReplicatedTxns_;
   std::atomic<uint32_t> replicaLatestSyncPoints_[MAX_REPLICA_NUM];

   mutable std::shared_mutex logMtx_;
   EntryMap<EntryQu<NezhaLogEntry>> syncedEntries_;
   EntryMap<EntryQu<NezhaLogEntry>> unSyncedEntries_;

   std::atomic<uint32_t> nextSyncedLogId_;
   std::vector<NezhaLogEntry*> syncedLogList_;

   std::mutex syncBroadcastMtx_;
   uint32_t lastBroadcastSyncedLogId_;
   std::map<uint32_t, NezhaLogEntry*> pendingLogInfo_;

   /*only follower needs it*/
   mutable std::shared_mutex hashMtx_;
   EntryMap<SyncedHashItem> latestSyncedHashes_;

   std::recursive_mutex interReplicaSyncMtx_;
   std::map<uint32_t, NezhaInterReplicaSync> pendingInterReplicaSyncs_;

   std::map<std::pair<uint64_t, uint64_t>, NezhaLogEntry*> holdBuffer_;
   std::vector<uint64_t> lastReleasedTxnDeadlines_;

   ConcurrentQueue<NezhaLogEntry*> toHoldAndReleaseQu_;
   ConcurrentQueue<NezhaLogEntry*> toExecQu_;
   ConcurrentQueue<NezhaLogEntry*> toReplyQu_;
   ConcurrentQueue<NezhaFastReply*> fastReplyQu_;
   ConcurrentQueue<std::pair<uint64_t, uint64_t>> toReportQu_;

   std::atomic<uint32_t> owds_[MAX_REPLICA_NUM];
   int32_t owdSamples_[MAX_REPLICA_NUM][OWD_SAMPLE_WINDOW_LENGTH];
   int owdSampleNums_[MAX_REPLICA_NUM];
   std::set<std::pair<int32_t, int32_t>> owdSampleQueues_[MAX_REPLICA_NUM];
   uint32_t owdCap_;

   uint64_t lastReplicaInquireTime_;
   const uint64_t inquireIntervalUs = 10000;

   std::atomic<uint32_t> activeThreads_;
   std::unordered_map<std::string, std::thread*> threadMap_;
   std::thread* mainTd_;

  public:
   NezhaReplica(const std::string& serverName, const YAML::Node& config);
   ~NezhaReplica();
   void ConnectToReplicas();
   void Run();
   void Stop();

   void MainTd();

   // Leader Specific
   void LeaderHoldAndReleaseTd();
   void LeaderExecuteTd();
   void LeaderReplyTd();

   void FollowerHoldAndReleaseTd();
   void FollowerExecuteTd();

   std::mutex recordMtx_;
   void onNezhaFastReply(const NezhaFastReply& rep);
   void onNezhaReplicateRequest(const NezhaRequest& req, NezhaFastReply* rep,
                                const std::function<void()>& cb);
   void onNezhaInquireRequest(const NezhaInquireRequest& req,
                              NezhaInquireReply* rep);
   SyncedHashItem* GetOrCreateSyncedHashItemByKey(const uint32_t key);

   void LeaderNormalExecute(NezhaLogEntry* entry);

   bool CheckView(uint32_t lView, uint32_t shardId, uint32_t gView = 0);
   void BroadcastInterReplicaSync();
   void onInterReplicaSync(const NezhaInterReplicaSync& req);
   bool ProcessInterReplicaSync(const NezhaInterReplicaSync& req);
   void ProcessPendingInterReplicaSync();

   void InquireSyncStatus();

   void onNezhaInquireRequest(const NezhaInquireRequest& req,
                              NezhaInquireReply* rep,
                              const std::function<void()>& cb);

   bool AmLeader();
   bool CheckView(const uint32_t view);
   void ThreadSleepFor(const uint32_t sleepMicroSecond);
   std::string ServerAddrs(const uint32_t shardId, const uint32_t replicaId);
   std::string MyServerAddr();

   std::function<void(const NezhaAck&)> temp[1000];
};
