#pragma once
#include "PaxosMessage.h"
#include "PaxosService.h"
using namespace PaxosRPC;

class PaxosReplica {
  protected:
   uint64_t lastPrintTime_;
   YAML::Node config_;
   uint32_t shardId_;
   uint32_t replicaId_;
   uint32_t shardNum_;
   uint32_t replicaNum_;
   uint32_t status_;
   uint32_t viewId_;

   std::string serverAddrs_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   rrr::PollMgr* replicaRPCPoll_;
   rrr::Client* replicaRPCClients_[MAX_REPLICA_NUM];
   PaxosProxy* replicaProxies_[MAX_REPLICA_NUM];

   std::atomic<uint32_t> activeThreads_;
   std::unordered_map<std::string, std::thread*> threadMap_;
   std::thread* mainTd_;
   std::mutex recordMtx_;

   using ClientReplyHandler = std::function<void(const ClientRecordRep&)>;
   struct EntryInfo {
      PaxosAppend* msg;
      ClientReplyHandler cb_;
   };

   ConcurrentQueue<EntryInfo> entryQu_;
   ConcurrentQueue<EntryInfo> replyQu_;
   ConcurrentQueue<PaxosAppendRep> qcQu_;
   std::map<uint32_t, std::unordered_set<uint32_t>> quorum_;

   std::atomic<uint32_t> toCommitPoint_;
   std::atomic<uint32_t> committedLogId_;
   std::atomic<uint32_t> nextLogId_;
   std::vector<PaxosAppend*> logList_;
   std::map<uint32_t, EntryInfo> clientReplyCallBacks_;
   std::map<uint32_t, PaxosAppend*> pendingLogs_;

  public:
   PaxosReplica(const std::string& serverName, const YAML::Node& config,
                const uint32_t shardId, const uint32_t replicaId,
                const uint32_t shardNum, const uint32_t replicaNum);
   ~PaxosReplica();
   void ConnectToReplicas();
   void Run();
   void Stop();

   void onClientRecord(const ClientRecord& req, ClientRecordRep* rep,
                       const std::function<void()>& cb);
   void onPaxosAppend(const PaxosAppend& req);
   void onPaxosAppendRep(const PaxosAppendRep& rep);
   void onPaxosCommitReq(const PaxosCommitReq& req);

   void MainTd();
   void RequestMulticastTd();
   void QuorumCheckTd();
   void ReplyTd();

   bool AmLeader();
   bool CheckView(const uint32_t view);

   std::string ServerAddrs(const uint32_t shardId, const uint32_t replicaId);
   std::string MyServerAddr();
};
