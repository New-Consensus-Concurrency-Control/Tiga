#pragma once
#include "Common.h"
#include "StateMachine/MicroStateMachine.h"
#include "StateMachine/TPCCStateMachine.h"
#include "TigaService/TigaMessage.h"
#include "TigaService/TigaService.h"
using namespace TigaRPC;
using namespace rrr;
using namespace std;

// #define CREATE_FAILURE
// #define ENABLE_CHECKPOINT
// #define ENABLE_FAILURE_RECOVERY

struct TigaLogEntryCmp {
   bool operator()(TigaLogEntry* x, TigaLogEntry* y) {
      uint64_t xRank, yRank, xTxnKey, yTxnKey;
      xRank = (x->deadlineAgreed_) ? x->agreedDdlRank_ : x->localDdlRank_;
      yRank = (y->deadlineAgreed_) ? y->agreedDdlRank_ : y->localDdlRank_;
      return (xRank < yRank ||
              (xRank == yRank && x->cmd_->TxnKey() <= y->cmd_->TxnKey()));
   }
};

typedef std::function<void(const TigaSlowReply&)> SlowReplyHandler;

#define HASH_PARTITION_ID(key) (key & 0x1f)
#define HASH_PARTITION_NUM (32)

struct DeadlineQItem {
   uint32_t shardIds_[MAX_SHARD_NUM];
   uint64_t ddls_[MAX_SHARD_NUM];
   TigaLogEntry* entry_;
   uint32_t itemCnt_;
   uint64_t aggregatedDdl_;
   DeadlineQItem() : entry_(NULL), itemCnt_(0) {}
};

struct DeadlineQMap {
   mutable std::shared_mutex mtx_;
   // <txnKey, ddlItem>
   std::unordered_map<uint64_t, DeadlineQItem> deadlineQ_;
};

struct ExecAgreeQItem {
   TigaLogEntry* entry_;
   uint32_t itemCnt_;
   ExecAgreeQItem() : entry_(NULL), itemCnt_(0) {}
};

struct ExecAgreeQMap {
   mutable std::shared_mutex mtx_;
   // <txnKey, execAgreeQItem>
   std::unordered_map<uint64_t, ExecAgreeQItem> agreeQ_;
};

struct GlobalViewInfo {
   uint32_t gViewId_;
   std::vector<uint32_t> gVec_;
};

struct ServerStatusInfo {
   uint32_t gViewId_;
   uint32_t viewId_;
   uint32_t status_;
   ServerStatusInfo() : gViewId_(0), viewId_(0), status_(STATUS_NORMAL) {}
};

struct configManager {
   std::shared_mutex mtx_;
   GlobalViewInfo prepareGInfo_;
   GlobalViewInfo gInfo_;
   uint32_t status_;
   uint32_t actionStatus_;
   uint32_t viewId_;
   std::map<uint32_t, TigaCMPrepareReply> vCMPrepareReps_;
   ServerStatusInfo serverStatus_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   void Initialize(uint32_t shardNum) {
      prepareGInfo_.gViewId_ = 0;
      prepareGInfo_.gVec_.resize(shardNum, 0);
      gInfo_.gViewId_ = 0;
      gInfo_.gVec_.resize(shardNum, 0);
      // always normal, CM is supported by traditional VR,
      // and we do not implement its failure recovery
      status_ = STATUS_NORMAL;
      actionStatus_ = CM_NO_ACTION;
      viewId_ = 0;
   }
};

enum SpecExecCmd {
   ExecInit = 0,
   PrepareExecuting,
   PrepareExecuted,
   Rollbacking,
   Rollbacked,
   CommitExecuting,
   CommitExecuted,
   FastExecuting,
   FastExecuted

};

class TigaReplica {
  protected:
   uint64_t lastPrintTime_;  // debug
   uint32_t replicaId_;
   uint32_t shardId_;
   uint32_t replicaNum_;
   uint32_t shardNum_;
   bool specExec_;
   int clockOffsetMean_;
   int clockOffsetStd_;
   int clockError_;
   std::atomic<uint64_t> releaseWaterMark_;

   mutable std::shared_mutex failedServerRecordMtx_;
   std::unordered_map<uint32_t, std::set<std::pair<uint32_t, uint32_t>>>
       failedServersByGView_;

   StateMachine* sm_;

   std::atomic<uint32_t> serverSignalToCoord_;
   mutable std::shared_mutex statusMtx_;
   std::atomic<uint32_t> status_;
   std::atomic<uint32_t> toFail_;

   mutable std::shared_mutex gNewsMtx_;
   GlobalViewInfo gNews_;

   configManager cm_;
   uint32_t gViewId_;
   std::vector<uint32_t> gVec_;
   uint32_t viewId_;
   uint32_t lastNormalView_;
   // const uint32_t interDCYieldPeriodUs = 2000;  // WAN
   // const uint32_t intraDCYieldPeriodUs = 2000;  // WAN

   const uint32_t interDCYieldPeriodUs = 1000;  // LAN
   const uint32_t intraDCYieldPeriodUs = 0;     // LAN
   PollMgr* localRpcPoll_;                      // to handle intra-DC messages
   PollMgr* globalRpcPoll_;                     // to handle inter-DC messages
   PollMgr* vrRpcPoll_;                         // to handle VR messages
   // clients interact with this addr
   std::string serverAddrs_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   std::string globalServerAddrs_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   std::string localServerAddrs_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   std::string vrServerAddrs_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   TigaLocalProxy* localProxies_[MAX_SHARD_NUM];
   rrr::Client* localRPCClients_[MAX_SHARD_NUM];
   TigaGlobalProxy* globalProxies_[MAX_REPLICA_NUM];
   rrr::Client* globalRPCClients_[MAX_REPLICA_NUM];
   TigaViewChangeProxy* vrProxies_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   rrr::Client* vrRPCClients_[MAX_SHARD_NUM][MAX_REPLICA_NUM];

   EntryMap<TigaLogEntry> entryMap_[HASH_PARTITION_NUM];
   EntryMap<ClientCommand> cmdMap_[HASH_PARTITION_NUM];
   DeadlineQMap ddlMap_[HASH_PARTITION_NUM];
   ExecAgreeQMap agreeQMap_[HASH_PARTITION_NUM];

   mutable std::shared_mutex logMtx_;
   EntryMap<EntryQu<LogInfo>> syncedEntries_;
   EntryMap<EntryQu<LogInfo>> unSyncedEntries_;

   std::atomic<uint32_t> nextSyncedLogId_;
   std::atomic<uint32_t> commitPoint_;
   std::atomic<uint32_t> executedLogId_;
   std::vector<LogInfo*> syncedLogList_;

   std::unordered_map<uint64_t, uint64_t> syncedTxnKeysToDeadlines_;
   // then number of servers that contains the log entry
   std::map<std::pair<uint64_t, uint64_t>, uint32_t> recoveryUnSyncedCntMap_;
   std::map<std::pair<uint64_t, uint64_t>, LogInfo*> recoveredSyncedLogs_;
   std::map<std::pair<uint64_t, uint64_t>, LogInfo*> recoveredUnSyncedLogs_;

   std::mutex syncBroadcastMtx_;
   ConcurrentQueue<LogInfo*> syncedLogInfoQu_;
   uint32_t lastBroadcastSyncedLogId_;
   std::map<uint32_t, LogInfo*> pendingLogInfo_;
   uint32_t lastBroadcastCommittedLogId_;

   /*only follower needs it*/
   mutable std::shared_mutex hashMtx_;
   EntryMap<SyncedHashItem> latestSyncedHashes_;

   std::recursive_mutex interReplicaSyncMtx_;
   std::map<uint32_t, TigaInterReplicaSync> pendingTigaInterReplicaSyncs_;
   std::atomic<uint64_t> missingTxnKey_;
   ConcurrentQueue<LogInfo*> followerCommitExecuteQu_;

   std::map<std::pair<uint64_t, uint64_t>, TigaLogEntry*> holdBuffer_;
   std::map<std::pair<uint64_t, uint64_t>, TigaLogEntry*> holdBuffer2_;
   std::vector<uint64_t> lastReleasedTxnDeadlines_;

   struct CommitReplyInfo {
      uint64_t txnKey_;
      std::function<void(const TigaReply&)> commitReplyHandler_;
   };
   std::unordered_map<uint64_t, LogInfo*>
       pendingInfoToCommitReply_[HASH_PARTITION_NUM];
   std::unordered_map<uint64_t, std::function<void(const TigaReply&)>>
       pendingCommitRepyHandlers_[HASH_PARTITION_NUM];

   std::unordered_set<uint64_t> execAgreedTxns_;
   std::queue<TigaLogEntry*> execEntryQu_;
   ConcurrentQueue<TigaLogEntry*> toHoldAndReleaseQu_;
   ConcurrentQueue<TigaLogEntry*> toDdlSyncQu_;
   ConcurrentQueue<TigaDeadlineAgreeRequest> toDdlSyncRequestQu_;
   ConcurrentQueue<TigaLogEntry*> toExecSyncQu_;
   ConcurrentQueue<TigaExecAgreeRequest> toExecAgreeRequestQu_;
   ConcurrentQueue<TigaLogEntry*> toExecCheckQu_;
   ConcurrentQueue<TigaLogEntry*> toExecQu_;
   ConcurrentQueue<std::pair<TigaLogEntry*, uint32_t>> toExecQuF_;
   ConcurrentQueue<std::pair<LogInfo*, uint32_t>> toReplyQuF_;
   ConcurrentQueue<CommitReplyInfo> toCommitRepyQu_;

   std::vector<std::map<std::pair<uint64_t, uint64_t>, TigaLogEntry*>>
       execSequencer_;

   std::unordered_map<uint64_t, LogInfo*> preparedLogInfo_;

   std::atomic<uint32_t> activeThreads_;
   std::unordered_map<std::string, std::thread*> threadMap_;
   std::thread* mainTd_;

   /////////////////////////////// ///////////////////////////////
   //////////////////////////////////
   mutable std::shared_mutex vcQuorumMtx_;
   struct VCQuorumInfo {
      uint32_t highestNormalView_;
      uint32_t highestSyncPoint_;
      uint32_t targetReplicaId_;
      std::map<uint32_t, TigaViewChange> quorum_;
      std::map<uint32_t, TigaViewChange> fullQuorum_;
   };
   VCQuorumInfo vcQuorum_;

   mutable std::shared_mutex syncQuorumMtx_;
   // <syncPoint, replicaId>
   uint32_t currentSyncPoints_[MAX_REPLICA_NUM];

   mutable std::shared_mutex stQuorumMtx_;
   struct StateQuorum {
      uint32_t targetViewToRecover_;
      uint32_t replicaIdForSyncedLogs_;
      std::vector<uint32_t> involvedReplicaIds_;
      std::map<uint32_t, TigaStateTransferReply> Quorum_;
   };
   StateQuorum stateTransferQuorum_;

   mutable std::shared_mutex crossShardConfirmQuorumMtx_;
   std::unordered_set<uint32_t> verifiedShards_;
   std::map<uint32_t, TigaCrossShardVerifyReq> crossShardVerifyReqs_;
   std::map<uint32_t, TigaCrossShardVerifyRep> crossShardVerifyReps_;

   std::map<uint32_t, LogInfo*> rebuffer_;

   mutable std::shared_mutex startViewMtx_;
   TigaStartView startViewMsg_;

   std::function<void(const TigaFailAck&)> failCb_;

#ifdef USE_SKEEN
   std::atomic<uint64_t> serverLogicalClock_;
#endif

  public:
   TigaReplica(const std::string& serverName, const YAML::Node& config);
   ~TigaReplica();
   void ConnectToOtherGlobalServers();
   void ConnectToOtherLocalServers();
   void ConnectToOtherVRServers();
   void ReConnectToOtherVRServers();
   void Run();
   void Stop();

   void MainTd();
   // Leader Specific
   // Preventive Strategy
   void LeaderHoldAndReleaseTd();
   void LeaderExecCheckTd();
   void LeaderExecuteTd();

   // Detective Strategy
   void LeaderHoldAndReleaseTdS();
   void LeaderSpecExecCheckTd();
   void LeaderSpecExecTd();

   // Common Threads
   void LeaderDeadlineAgreementTd();
   void LeaderExecAgreementTd();
   void LeaderCrossReplicaSyncTd();
   void LeaderReplyTd();

   void LeaderCheck(TigaLogEntry* entry);
   void LeaderNormalExecute(LogInfo* info);
   std::unordered_set<uint64_t> dupSet_;
   void LeaderCheckForRelease(TigaLogEntry* entry);
   void LeaderPreExecute(TigaLogEntry* entry);
   void LeaderRollback(TigaLogEntry* entry);
   void LeaderFastExecute(TigaLogEntry* entry);
   void LeaderCommitExecute(TigaLogEntry* entry);

   void FollowerHoldAndReleaseTd();
   void FollowerCrossReplicaSyncTd();
   void FollowerExecuteTd();
   void FollowerReplyTd();
   void FollowerExecuteCommitTd();

   void UpdateDeadlineRecord(const uint64_t txnKey, const uint32_t shardId,
                             const uint64_t ddl, TigaLogEntry* entry = NULL);

   bool CanExec(TigaLogEntry* entry, bool existPlaceHolder, bool debug = false);
   // Check whether the txn can be initially executed: (1) Single-shard txn is
   // FastExecte (2) Multi-Shard txn is PrepareExec
   bool CanInitialRelease(TigaLogEntry* entry, bool existPlaceHolder,
                          bool debug = false);
   bool CanRelease(TigaLogEntry* entry, uint64_t ddlRank, bool debug = false);

   void InsertPlaceHolder(TigaLogEntry* entry, bool useLocalDdl = true);
   void RemovePlaceHolder(TigaLogEntry* entry, bool useLocalDdl = true);
   void TryExecute(TigaLogEntry* entry);
   TigaLogEntry* TrySpecExecuteByKey(uint32_t key);

   void ViewChangeTd();
   bool CheckView(uint32_t lView, uint32_t shardId, uint32_t gView = 0);

   void BroadcastInterReplicaSync();
   void onNormalRequest(const TigaReq& req, TigaReply* rep,
                        const std::function<void()>& cb);
   void onCommitRequest(const TigaCommitRequest& req, TigaCommitReply* rep,
                        const std::function<void()>& cb);
   void onReconcliationRequest(const TigaReconcliationReq& req, TigaReply* rep,
                               const std::function<void()>& cb);
   void onDispatchRequest(const TigaDispatchRequest& req,
                          TigaDispatchReply* rep,
                          const std::function<void()>& cb);
   void onDeadlineAgreementRequest(const TigaDeadlineAgreeRequest& req);
   void onExecAgreementRequest(const TigaExecAgreeRequest& req);

   void onInterReplicaSync(const TigaInterReplicaSync& req);
   void onMissRequest(const MissRequestReq& req, MissRequestRep* rep);
   void onInquireServerSyncStatus(const TigaServerSyncStatusRequest& req,
                                  TigaServerSyncStatusReply* rep);
   bool ProcessInterReplicaSync(const TigaInterReplicaSync& req);
   void Execute(LogInfo* info);
   void ProcessPendingInterReplicaSync();
   void ExecuteCommittedLogs(uint32_t upto);
   void ClearContext();
   void onSyncStatus(const TigaSyncStatus& msg);

   // ViewChange Related
   void onViewChangeReq(const TigaViewChangeReq& msg);
   void onViewChange(const TigaViewChange& msg);
   void onHeartBeat(const TigaHeartBeat& req, TigaHeartBeatAck* ack);
   void onStateTransfer(const TigaStateTransferRequest& req,
                        TigaStateTransferReply* rep);
   void onCrossShardVerifyReq(const TigaCrossShardVerifyReq& req);
   void onCrossShardVerifyRep(const TigaCrossShardVerifyRep& rep);

   void onStartView(const TigaStartView& msg);
   void BroadcastCMPrepare();
   void onCMPrepare(const TigaCMPrepare& req);
   void onCMPrepareReply(const TigaCMPrepareReply& rep);
   void onCMCommit(const TigaCMCommit& msg);
   void BroadcastViewChangeReq();
   void onStateTransferReply(TigaStateTransferReply& rep);
   void SendHeartBeat();
   void SendViewChange();
   void CheckServerStatus();
   void onHeartBeatAck(const TigaHeartBeatAck& ack);
   bool CheckVCQuorum();
   void BuildUnSyncedLogList();
   void CollectLogs();
   void RebuildLogs();
   void ConfirmLogWithOtherLeaders();
   void LeaderFinalizeRecoveredLogs();
   void SendCrossShardVerifyReplyTo(uint32_t shardId);
   void FollowerFinalizeRecoveredLogs();
   void BroadcastStartView();
   void KillServer();
   void onFailSignal(const TigaFailSignal& msg, TigaFailAck* ack,
                     const std::function<void()>& cb);
   void onFailAck(const TigaFailAck& ack);

   uint64_t lastHeartBeatTime_;
   /////////////////////////////////////

   // helpers

   bool AmLeader();
   bool AmCMLeader();
   bool CheckView(const uint32_t view);
   void ThreadSleepFor(const uint32_t sleepMicroSecond);
   std::string ServerAddrs(const uint32_t shardId, const uint32_t replicaId);
   std::string LocalServerAddrs(const uint32_t shardId,
                                const uint32_t replicaId);
   std::string GlobalServerAddrs(const uint32_t shardId,
                                 const uint32_t replicaId);
   std::string VRServerAddrs(const uint32_t shardId, const uint32_t replicaId);
   std::string MyServerAddr();
   std::string MyLocalServerAddr();
   std::string MyGlobalServerAddr();
   std::string MyVRServerAddr();

   bool killed_;
   std::unordered_set<TigaLogEntry*> preparedTxns_;
   std::map<uint64_t, TigaLogEntry*> holdingTxns_;
   std::unordered_set<TigaLogEntry*> repliedEntries_;
   std::unordered_set<TigaLogEntry*> processedTxns_;
   uint32_t cnters_;  // for debug
   uint32_t lastReqId_;
   uint32_t lastRepliedReqId_;
   uint32_t lastDdlCheckReqId_;
   uint32_t lastDdlAgreedReqId_;
   uint32_t lastExecReqId_;
   uint32_t cnters2_;
   uint32_t holdNum_;
   uint32_t releaseNum_;
   uint64_t startTime;
   uint64_t dqReplyNum_;
   uint64_t replyNum_;
   bool turnOnDebug_;

   uint64_t lastUsedDdl_;
};
