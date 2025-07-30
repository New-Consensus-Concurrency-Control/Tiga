// clang-format off
#pragma once
#include "PaxosReplica.h"

// clang-format on
// optional %%: marks header section, code above will be copied into begin of
// generated C++ header
namespace PaxosRPC {

class PaxosServiceImpl : public PaxosService {
  protected:
   PaxosReplica* replica_;

  public:
   PaxosServiceImpl(PaxosReplica* r);
   void RunReplicaThreads();
   void StopReplicaThreads();
   // these RPC handler functions need to be implemented by user
   // for 'raw' handlers, remember to reply req, delete req, and
   // sconn->release(); use sconn->run_async for heavy job

   void RecordRequest(const ClientRecord& req, ClientRecordRep* rep,
                      rrr::DeferredReply* defer) override;
   void PaxosAppendRequest(const PaxosAppend& req,
                           rrr::DeferredReply* defer) override;
   void PaxosAppendResponse(const PaxosAppendRep& rep,
                            rrr::DeferredReply* defer) override;
   void PaxosCommitRequest(const PaxosCommitReq& cmt,
                           rrr::DeferredReply* defer) override;
};

}  // namespace PaxosRPC
