#pragma once
#include "Common.h"
#include "StateMachine/MicroStateMachine.h"
#include "StateMachine/TPCCStateMachine.h"
#include "TigaService/TigaMessage.h"
#include "TigaService/TigaService.h"
using namespace TigaRPC;
using namespace rrr;
using namespace std;


typedef std::function<void(const TigaSlowReply&)> SlowReplyHandler;

#define HASH_PARTITION_ID(key) (key & 0x1f)
#define HASH_PARTITION_NUM (32)

struct DeadlineQItem {
   uint32_t shardIds_[MAX_SHARD_NUM] = {};
   uint32_t phases_[MAX_SHARD_NUM] = {};
   uint64_t ddls_[MAX_SHARD_NUM] = {};
   TigaLogEntry* entry_ = NULL;
   uint32_t itemCnt_ = 0;
   uint64_t aggregatedDdl_ = 0;
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
using SequencerBuffer = std::map<std::pair<uint64_t, uint64_t>, TigaLogEntry*>;
using SequencerFlag = std::set<std::pair<uint64_t, uint64_t>>;

class TigaReplica {
  protected:
   uint64_t lastPrintTime_;  // debug
   uint32_t replicaId_;
   uint32_t shardId_;
   uint32_t replicaNum_;
   uint32_t shardNum_;
   bool isPreventive_;
   int clockOffsetMean_;  // obsolete
   int clockOffsetStd_;   // obsolete
   int clockError_;       // obsolete
   std::atomic<uint64_t> releaseWaterMark_;

   mutable std::shared_mutex failedServerRecordMtx_;
   std::unordered_map<uint32_t, std::set<std::pair<uint32_t, uint32_t>>>
       failedServersByGView_;

   bool testFailureRecovery_;
   uint32_t syncedLogIdBeforeFailure_;
   StateMachine* sm_;
   std::atomic<uint32_t> serverSignalToCoord_;
   mutable std::shared_mutex statusMtx_;
   std::atomic<uint32_t> status_;
   std::atomic<uint32_t> toFail_;

   uint32_t targetGView_;
   uint32_t replicaIdToKill_;
   uint32_t shardIdToKill_;
   mutable std::shared_mutex gNewsMtx_;
   GlobalViewInfo gNews_;
   configManager cm_;
   uint32_t gViewId_;
   std::vector<uint32_t> gVec_;
   uint32_t viewId_;
   uint32_t lastNormalView_;
   uint32_t owdDeltaUs_;

   const uint32_t interDCYieldPeriodUs = 2000;
   const uint32_t checkpointYieldPeriodUs = 2000;
   PollMgr* localRpcPoll_;   // to handle intra-DC messages
   PollMgr* globalRpcPoll_;  // to handle inter-DC messages
   PollMgr* vrRpcPoll_;      // to handle VR messages
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

   EntryMap<ClientCommand> cmdMap_[HASH_PARTITION_NUM];
   DeadlineQMap ddlMap_[HASH_PARTITION_NUM];

   mutable std::shared_mutex logMtx_;
   EntryMap<EntryQu<TigaLogEntry>> syncedEntries_;
   EntryMap<EntryQu<TigaLogEntry>> unSyncedEntries_;

   std::atomic<uint32_t> nextSyncedLogId_;
   std::atomic<uint32_t> nextSpecLogId_;
   std::atomic<uint32_t> commitPoint_;
   std::atomic<uint32_t> executedLogId_;
   std::vector<TigaLogEntry*> syncedLogList_;

   std::unordered_map<uint64_t, uint64_t> syncedTxnKeysToDeadlines_;
   // then number of servers that contains the log entry
   std::map<std::pair<uint64_t, uint64_t>, uint32_t> recoveryUnSyncedCntMap_;
   std::map<std::pair<uint64_t, uint64_t>, TigaLogEntry*> recoveredSyncedLogs_;
   std::map<std::pair<uint64_t, uint64_t>, TigaLogEntry*>
       recoveredUnSyncedLogs_;

   std::mutex syncBroadcastMtx_;
   ConcurrentQueue<TigaLogEntry*> syncedLogInfoQu_;
   std::map<uint32_t, TigaLogEntry*> pendingEntries_;
   uint32_t lastBroadcastSyncedLogId_;
   uint32_t lastBroadcastCommittedLogId_;

   /*only follower needs it*/
   mutable std::shared_mutex hashMtx_;
   EntryMap<SyncedHashItem> latestSyncedHashes_;
   std::vector<TigaLogEntry*> boundaryLogInfos_;
   std::vector<SyncedHashMark> boundarySyncedHashMarks_;

   std::recursive_mutex interReplicaSyncMtx_;
   // followers use it to make continous history
   std::set<uint32_t> pendingSpecLogIds_;
   std::map<uint32_t, TigaInterReplicaSync> pendingTigaInterReplicaSyncs_;
   ConcurrentQueue<TigaLogEntry*> followerCommitExecuteQu_;

   SequencerBuffer holdBuffer_;
   std::vector<uint64_t> lastReleasedTxnDeadlinesW_; // write-txns
   std::vector<uint64_t> lastReleasedTxnDeadlinesR_; // read-only txns
   std::vector<SequencerBuffer> execSequencers_;
   std::vector<TigaLogEntry*> entriesInSpec_;

   struct CommitReplyInfo {
      uint64_t txnKey_;
      std::function<void(const TigaReply&)> commitReplyHandler_;
   };
   std::unordered_map<uint64_t, TigaLogEntry*>
       pendingEntriesToCommitReply_[HASH_PARTITION_NUM];
   std::unordered_map<uint64_t, std::function<void(const TigaReply&)>>
       pendingCommitRepyHandlers_[HASH_PARTITION_NUM];

   ConcurrentQueue<TigaLogEntry*> toDdlSyncQu_;
   ConcurrentQueue<TigaDeadlineAgreeRequest> toDdlSyncRequestQu_;

   ConcurrentQueue<TigaLogEntry*> toHoldAndReleaseQu_;
   // HoldReleaseTd transfers entry to ExecCheck, for specExec
   // AgreeTd and ExecTd communicates via the atomic member in entry
   ConcurrentQueue<TigaLogEntry*> toExecCheckQu_;
   ConcurrentQueue<std::pair<TigaLogEntry*, uint32_t>> toExecQuF_;
   ConcurrentQueue<TigaLogEntry*> toReplyQu_;
   ConcurrentQueue<CommitReplyInfo> toCommitRepyQu_;

   std::atomic<uint32_t> activeThreads_;
   std::unordered_map<std::string, std::thread*> threadMap_;
   std::thread* mainTd_;

   ///////////////////////////////////////////////////////////////
   uint64_t lastHeartBeatTime_;
   std::unordered_map<uint64_t, TigaLogEntry*> preparedLogInfo_;
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
   std::map<uint32_t, TigaLogEntry*> rebuffer_;
   mutable std::shared_mutex startViewMtx_;
   TigaStartView startViewMsg_;
   std::function<void(const TigaFailAck&)> failCb_;

   // clock test
   std::atomic<uint64_t> serverLogicalClock_;
   bool useLogicalClock_;
   bool useSkeen_;

  public:
   TigaReplica(const std::string& serverName, const YAML::Node& config);
   ~TigaReplica();
   void ConnectToOtherGlobalServers();
   void ConnectToOtherLocalServers();
   void ConnectToOtherVRServers();
   void Connect();
   void Run();
   void Stop();

   void MainTd();
   void HoldReleaseTd();
   void LeaderDdlSyncTd();
   void LeaderPreExecTd();
   void LeaderExecTd();
   void LeaderReplyTd();
   void LeaderCrossReplicaSyncTd();

   void FollowerExecTd();
   void FollowerCrossReplicaSyncTd();
   void FollowerReplyTd();
   void FollowerExecuteCommitTd();

   void onDispatchRequest(const TigaDispatchRequest& req,
                          TigaDispatchReply* rep,
                          const std::function<void()>& cb);
   void onNormalRequest(const TigaReq& req, TigaReply* rep,
                        const std::function<void()>& cb);
   void onDeadlineAgreementRequest(const TigaDeadlineAgreeRequest& req);
   void onInterReplicaSync(const TigaInterReplicaSync& req);
   void onInquireServerSyncStatus(const TigaServerSyncStatusRequest& req,
                                  TigaServerSyncStatusReply* rep);
   void onSyncStatus(const TigaSyncStatus& msg);
   void onReconcliationRequest(const TigaReconcliationReq& req, TigaReply* rep,
                               const std::function<void()>& cb);
   // Thread Helpers
   bool CanSpecExec(TigaLogEntry* entry);
   bool NoBlockingAhead(TigaLogEntry* entry);
   bool AtTopofExecSequencer(TigaLogEntry* entry);
   void AdvanceSpecExec(const std::vector<uint32_t>& candidateKeys,
                        std::vector<TigaLogEntry*>* newCandidateEntries);
   void PreventiveExec(TigaLogEntry* entry, uint32_t cmd);
   void DetectiveExec(TigaLogEntry* entry, uint32_t cmd);
   void UpdateDeadlineRecord(const uint64_t txnKey, const uint32_t shardId,
                             const uint64_t ddl, const uint32_t phase,
                             TigaLogEntry* entry = NULL);
   void ProcessPendingInterReplicaSync();
   bool ProcessInterReplicaSync(const TigaInterReplicaSync& req);
   void ExecuteCommittedLogs(uint32_t upto);

   //  ViewChange and FailureRecovery
   // For modularity, these functions are defined separately in TigaReplicaFR.cc
   void FailureRecoveryInit(const YAML::Node& config);
   void ConfigManagerAction();
   void ActivateFailure();
   void FailureRecovery();
   void ClearContext();
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

   // helpers
   bool AmLeader();
   bool AmCMLeader();
   bool CheckView(const uint32_t view);
   bool CheckView(uint32_t lView, uint32_t shardId, uint32_t gView);
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

   std::atomic<uint32_t> normalRequestNum_;
   std::atomic<uint32_t> reconcliationRequstNum_;

   bool killed_;
};
