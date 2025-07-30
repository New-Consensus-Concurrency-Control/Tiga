// clang-format off
#pragma once
#include "NezhaReplica.h"

// clang-format on
// optional %%: marks header section, code above will be copied into begin of
// generated C++ header
namespace NezhaRPC {

class NezhaServiceImpl : public NezhaService {
  protected:
   NezhaReplica* replica_;

  public:
   NezhaServiceImpl(NezhaReplica* r);
   void RunReplicaThreads();
   void StopReplicaThreads();
   // these RPC handler functions need to be implemented by user
   // for 'raw' handlers, remember to reply req, delete req, and
   // sconn->release(); use sconn->run_async for heavy job

   void NezhaReplicateRequest(const NezhaRequest& req, NezhaFastReply* rep,
                              rrr::DeferredReply* defer) override;
   void NezhaInquire(const NezhaInquireRequest& req, NezhaInquireReply* rep,
                     rrr::DeferredReply* defer) override;
   void InterReplicaSync(const NezhaInterReplicaSync& req,
                         rrr::DeferredReply* defer) override;
};

}  // namespace NezhaRPC
