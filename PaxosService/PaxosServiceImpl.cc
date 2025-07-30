#include "PaxosServiceImpl.h"

namespace PaxosRPC {

PaxosServiceImpl::PaxosServiceImpl(PaxosReplica* r) : replica_(r) {}

void PaxosServiceImpl::RunReplicaThreads() { replica_->Run(); }

void PaxosServiceImpl::StopReplicaThreads() { replica_->Stop(); }

void PaxosServiceImpl::RecordRequest(const ClientRecord& req,
                                     ClientRecordRep* rep,
                                     rrr::DeferredReply* defer) {
   replica_->onClientRecord(req, rep, [defer]() { defer->reply(); });
}
void PaxosServiceImpl::PaxosAppendRequest(const PaxosAppend& req,
                                          rrr::DeferredReply* defer) {
   replica_->onPaxosAppend(req);
   defer->reply();
}
void PaxosServiceImpl::PaxosAppendResponse(const PaxosAppendRep& rep,
                                           rrr::DeferredReply* defer) {
   replica_->onPaxosAppendRep(rep);
   defer->reply();
}
void PaxosServiceImpl::PaxosCommitRequest(const PaxosCommitReq& cmt,
                                          rrr::DeferredReply* defer) {
   replica_->onPaxosCommitReq(cmt);
   defer->reply();
}

}  // namespace PaxosRPC