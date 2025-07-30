#pragma once
// clang-format off
#include "DetockService/DetockLogManager.h"
#include "DetockService/SCCFinder.h"

using namespace DetockRPC;
using namespace rrr;
using namespace std;

// clang-format on

using DepGraph = std::unordered_map<uint64_t, std::unordered_set<uint64_t>>;
using SCCGraph = std::unordered_map<uint32_t, std::unordered_set<uint32_t>>;
class DetockExecutor {
  protected:
   std::string serverName_;
   uint32_t viewId_;
   StateMachine* sm_;
   DetockLogManager* logManager_;
   uint32_t status_;
   uint32_t lastBatchId_;

   rrr::PollMgr* sequencerRPCPoll_;
   rrr::Client* sequencerRPCClients_[MAX_SHARD_NUM];
   DetockSequencerProxy* sequencerProxies_[MAX_SHARD_NUM];
   std::string sequencerAddrs_[MAX_SHARD_NUM][MAX_REPLICA_NUM];

   std::unordered_map<std::string, std::thread*> threadPool_;

   uint32_t lastLocalSyncMsgId_;
   // key --> txnKey
   std::unordered_map<uint32_t, uint64_t> prevTxns_[MAX_SHARD_NUM];
   DepGraph depGraph_;
   DepGraph reverseGraph_;
   std::unordered_set<uint64_t> prunedVs_;
   std::unordered_set<uint64_t> completeVertices_;
   std::unordered_set<uint64_t> incompleteVertices_;
   std::queue<uint64_t> incompleteVerticesQu_;
   std::unordered_set<uint64_t> unstableVertices_;
   std::unordered_set<uint64_t> stableVertices_;
   DepGraph stableDepGraph_;
   SCCGraph condensationGraph_;
   uint32_t graphSeqNoToProcess_;
   uint32_t lastProcessedGraphSeqNo_;

   struct DepGraphInfo {
      uint32_t graphSeqNo_;
      DepGraph* graph_;
   };

   struct ExecSeqInfo {
      uint32_t graphSeqNo_;
      std::vector<uint64_t>* txnKeys_;
   };

   std::queue<std::pair<uint32_t, std::vector<DetockEntry*>*>>
       pendingBatchToSync_;
   ConcurrentQueue<DetockEntry*> holdReleaseQu_;
   std::map<std::pair<uint64_t, uint64_t>, DetockEntry*> holdReleaseBuffer_;
   ConcurrentQueue<DetockEntry*> batchQu_;
   std::unordered_map<uint64_t, DetockEntry*> localEntries_;
   EntryMap<DetockEntry> localEntryMap_[HASH_PARTITION_NUM];
   std::map<uint32_t, ExecSeqInfo> pendingTxnToExecute_;
   std::vector<ConcurrentQueue<DepGraphInfo>> gQu_;
   std::vector<ConcurrentQueue<ExecSeqInfo>> execQu_;
   ConcurrentQueue<DetockLocalLogSync>* logSyncQu_;
   ConcurrentQueue<DetockEntry*> replyQu_;

   struct GraphInfo {
      uint32_t type_;  // 0 is edge, 1 is vertice (complete)
      uint64_t from_;
      uint64_t to_;
   };
   ConcurrentQueue<GraphInfo> graphInfoQu_;

   struct TxnMetaData {
      std::unordered_set<uint32_t> involvedShardIds_;
      std::unordered_set<uint32_t> collectedShardIds_;
      bool isComplete() {
         return (involvedShardIds_.size() > 0 &&
                 involvedShardIds_.size() == collectedShardIds_.size());
      }
   };
   std::unordered_map<uint64_t, TxnMetaData> txnMetaData_;

   std::atomic<uint32_t> activeThreads_;
   std::unordered_map<std::string, std::thread*> threadMap_;
   std::thread* mainTd_;

  public:
   DetockExecutor(const std::string& serverName, StateMachine* sm,
                  DetockLogManager* mgr,
                  ConcurrentQueue<DetockLocalLogSync>* qu);
   ~DetockExecutor();
   void ConnectToSequencers();
   void Run();
   void Stop();
   void onNormalRequest(const DetockTxn& req, DetockReply* rep,
                        const std::function<void()>& cb);
   void onDispatchRequest(const DetockDispatchRequest& req,
                          DetockDispatchReply* rep,
                          const std::function<void()>& cb);
   void onLogSync(const DetockLocalLogSync& req);
   void BroadcastLogSync(const std::vector<DetockEntry*> entries);

   DetockLogManager* GetLogManager();

   // Thread

   void MainTd();

   void HoldReleaseTd();

   // Peridically batching the txns and sync with the other txn holders
   void BatchTd();

   void ParseDepTd();

   // Find the stable SCC and decide a deterministic order
   void OrderTd();

   void GraphTd(int idx);

   // Execute txns and send the reply to coord
   void ExecuteTd();

   void ReplyTd();

   uint32_t LeaderReplicaId(const uint32_t viewId, const uint32_t shardId);
   bool ContainShard(const std::vector<uint32_t>& shardIds,
                     const uint32_t shardId) const;
};
