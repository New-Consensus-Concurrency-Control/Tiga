#pragma once
// clang-format off
#include "CalvinService/CalvinMessage.h"
#include "CalvinService/CalvinService.h"
#include "StateMachine/MicroStateMachine.h"
#include "StateMachine/TPCCStateMachine.h"
using namespace CalvinRPC;

// clang-format on

struct EpochEntry {
   EpochRequest req_;
   EpochReply* rep_;
   std::function<void()> cb_;
   EpochEntry(const EpochRequest& req, EpochReply* rep,
              const std::function<void()>& cb)
       : req_(req), rep_(rep), cb_(cb) {}
};

class CalvinScheduler {
  protected:
   std::mutex executionMtx_;
   std::map<std::pair<uint32_t, uint32_t>, EpochEntry*> pendingEpochEntries_;
   std::pair<uint32_t, uint32_t> nextEpochKey_;  // <seqNo, shardId>
   void ProcessPendingEpochEntries();
   void ProcessEpoch(const EpochRequest& req, EpochReply* rep,
                     const std::function<void()>& cb);

  public:
   CalvinScheduler(const std::string& serverName, StateMachine* sm);

   void onEpochRequest(const EpochRequest& req, EpochReply* rep,
                       const std::function<void()>& cb);

   void onDispatchRequest(const CalvinDispatchRequest req,
                          CalvinDispatchReply* rep,
                          const std::function<void()>& cb);
   ~CalvinScheduler();
   StateMachine* sm_;
   uint32_t maxSeqNoByShard_[MAX_SHARD_NUM];
};
