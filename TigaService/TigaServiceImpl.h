// clang-format off
#pragma once
#include "TigaReplica.h"

// clang-format on
// optional %%: marks header section, code above will be copied into begin of
// generated C++ header
namespace TigaRPC {

class TigaServiceImpl : public TigaService {
  protected:
   TigaReplica* replica_;
   bool hrRuning_;
   std::vector<std::thread*> hrHandlers_;
   bool replyRunning_;
   std::thread* replyTdHandler_;

  public:
   TigaServiceImpl(TigaReplica* r);
   void RunReplicaProcessingThreads();
   void StopReplicaProcessingThreads();
   // these RPC handler functions need to be implemented by user
   // for 'raw' handlers, remember to reply req, delete req, and
   // sconn->release(); use sconn->run_async for heavy job
   void NormalRequest(const TigaReq& req, TigaReply* rep,
                      rrr::DeferredReply* defer) override;
   void InquireServerSyncStatus(const TigaServerSyncStatusRequest& req,
                                TigaServerSyncStatusReply* rep) override;
   void Reconcliation(const TigaReconcliationReq& req, TigaReply* rep,
                      rrr::DeferredReply* defer) override;
   void DispatchRequest(const TigaDispatchRequest& req, TigaDispatchReply* rep,
                        rrr::DeferredReply* defer) override;
};

}  // namespace TigaRPC
