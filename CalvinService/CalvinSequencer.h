#pragma once
#include "CalvinService/CalvinMessage.h"
#include "CalvinService/CalvinService.h"
#include "StateMachine/MicroStateMachine.h"
#include "StateMachine/TPCCStateMachine.h"
using namespace CalvinRPC;
using namespace rrr;
using namespace std;

using MasterReplyHandler = std::function<void(const MasterSyncReply&)>;

struct ReplicaQuorum {
   NezhaHash hashes_[MAX_REPLICA_NUM];
   uint32_t fastReplies[MAX_REPLICA_NUM];
   uint32_t leaderLogId_;
   ReplicaQuorum() {
      memset(fastReplies, '\0', sizeof(fastReplies));
      leaderLogId_ = 0;
   }
};

struct ShardQuorum {
   uint32_t shardReplies[MAX_SHARD_NUM];
   std::map<int32_t, Value> results_;
   ShardQuorum() { memset(shardReplies, '\0', sizeof(shardReplies)); }
};

class CalvinSequencer {
  protected:
   uint64_t lastPrintTime_;  // debug
   uint32_t replicaId_;
   uint32_t shardId_;
   uint32_t replicaNum_;
   uint32_t shardNum_;

   std::atomic<uint64_t> startTime_;
   StateMachine* sm_;
   uint32_t status_;
   uint32_t viewId_;
   uint32_t designateReplicaId_;

   // clients interact with this addr
   std::string serverAddrs_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   std::string sequencerAddrs_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   std::string schedulerAddrs_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   std::string masterAddr_;
   PollMgr* sequencerRPCPoll_;
   CalvinSequencerProxy* sequencerProxies_[MAX_REPLICA_NUM];
   rrr::Client* sequencerRPCClients_[MAX_REPLICA_NUM];
   PollMgr* schedulerRPCPoll_;
   CalvinSchedulerProxy* schedulerProxies_[MAX_SHARD_NUM];
   rrr::Client* schedulerRPCClients_[MAX_SHARD_NUM];
   PollMgr* masterRPCPoll_;
   CalvinProxy* masterProxy_;
   rrr::Client* masterRPCClient_;

   EntryMap<CalvinLogEntry> entryMap_[HASH_PARTITION_NUM];
   EntryMap<CalvinEpochEntry> epochEntryMap_[HASH_PARTITION_NUM];

   std::unordered_map<uint64_t, ReplicaQuorum> replicaQuorum_;
   std::unordered_set<uint32_t> replicatedTxnsBySeqNo_;
   uint32_t maxCommittedSeqNo_;
   std::map<uint32_t, uint64_t> incompleteReplicatedTxns_;  // seqNo-->txnKey

   std::atomic<uint32_t> replicaLatestSyncPoints_[MAX_REPLICA_NUM];

   std::unordered_map<uint64_t, ShardQuorum> shardQuorum_;

   mutable std::shared_mutex logMtx_;
   EntryMap<EntryQu<CalvinEpochEntry>> syncedEntries_;
   EntryMap<EntryQu<CalvinEpochEntry>> unSyncedEntries_;

   std::atomic<uint32_t> nextSyncedLogId_;
   std::vector<CalvinEpochEntry*> syncedLogList_;

   std::mutex syncBroadcastMtx_;
   uint32_t lastBroadcastSyncedLogId_;
   std::map<uint32_t, CalvinEpochEntry*> pendingLogInfo_;

   /*only follower needs it*/
   mutable std::shared_mutex hashMtx_;
   EntryMap<SyncedHashItem> latestSyncedHashes_;

   std::recursive_mutex interReplicaSyncMtx_;
   std::map<uint32_t, NezhaInterReplicaSync> pendingInterReplicaSyncs_;

   std::map<std::pair<uint64_t, uint64_t>, CalvinEpochEntry*> holdBuffer_;
   std::vector<uint64_t> lastReleasedTxnDeadlines_;

   ConcurrentQueue<CalvinEpochEntry*> toHoldAndReleaseQu_;
   ConcurrentQueue<CalvinEpochEntry*> toExecQu_;
   ConcurrentQueue<CalvinEpochEntry*> toReplyQu_;
   ConcurrentQueue<NezhaFastReply*> fastReplyQu_;
   ConcurrentQueue<CalvinEpochEntry*> toReportQu_;
   ConcurrentQueue<EpochReply*> epochReplyQu_;
   ConcurrentQueue<CalvinLogEntry*> epochBatchQu_;
   std::atomic<uint32_t> epochSequenceNo_;

   // TODO: Collect OWDs stats
   int32_t owdSamples_[MAX_REPLICA_NUM][OWD_SAMPLE_WINDOW_LENGTH];
   int owdSampleNums_[MAX_REPLICA_NUM];
   std::set<std::pair<int32_t, int32_t>> owdSampleQueues_[MAX_REPLICA_NUM];
   std::atomic<uint32_t> owds_[MAX_REPLICA_NUM];
   uint32_t owdCap_;

   uint64_t lastReplicaInquireTime_;
   const uint64_t inquireIntervalUs = 10000;

   std::atomic<uint32_t> activeThreads_;
   std::unordered_map<std::string, std::thread*> threadMap_;
   std::thread* mainTd_;

   std::mutex masterSyncMtx_;
   std::map<std::pair<uint32_t, uint32_t>, MasterReplyHandler> masterReplyHdls_;

  public:
   CalvinSequencer(const std::string& serverName, StateMachine* sm);
   ~CalvinSequencer();
   void ConnectToSequencers();
   void ConnectToSchedulers();
   void ConnectToMaster();
   void Run();
   void Stop();

   void MainTd();
   void QuorumCheckTd();
   void EpochBatchTd();
   void EpochReportTd();

   // Leader Specific
   void LeaderHoldAndReleaseTd();
   void LeaderExecuteTd();
   void LeaderReplyTd();

   void FollowerHoldAndReleaseTd();
   void FollowerExecuteTd();

   void onNormalRequest(const CalvinRequest& req, CalvinReply* rep,
                        const std::function<void()>& cb);
   void onNezhaFastReply(const NezhaFastReply& rep);
   void onNezhaInquireRequest(const NezhaInquireRequest& req,
                              NezhaInquireReply* rep);
   void onReplicateEpochRequest(const EpochRequest& req);
   void onEpochReply(const EpochReply& rep);

   void onMasterSyncRequest(const MasterSyncRequest& req, MasterSyncReply* rep,
                            const std::function<void()>& cb);

   SyncedHashItem* GetOrCreateSyncedHashItemByKey(const uint32_t key);

   void LeaderNormalExecute(CalvinEpochEntry* entry);

   bool CheckView(uint32_t lView, uint32_t shardId, uint32_t gView = 0);
   void BroadcastInterReplicaSync();
   void onInterReplicaSync(const NezhaInterReplicaSync& req);
   bool ProcessInterReplicaSync(const NezhaInterReplicaSync& req);
   void ProcessPendingInterReplicaSync();

   void InquireSyncStatus();
   void onNezhaInquireRequest(const NezhaInquireRequest& req,
                              NezhaInquireReply* rep,
                              const std::function<void()>& cb);

   void ReportToSchedulers();
   void ReportToClients();

   bool AmLeader();
   bool AmDesignateReplica();
   bool CheckView(const uint32_t view);
   bool QuorumOkay(uint64_t txnKey);
   void CheckInCompleteTxns();
   void ThreadSleepFor(const uint32_t sleepMicroSecond);
   std::string ServerAddrs(const uint32_t shardId, const uint32_t replicaId);
   std::string MyServerAddr();
   std::string MySequencerAddr();
   std::string MySchedulerAddr();
};
