#include "NezhaServiceImpl.h"

namespace NezhaRPC {

NezhaServiceImpl::NezhaServiceImpl(NezhaReplica* r) : replica_(r) {}

void NezhaServiceImpl::RunReplicaThreads() { replica_->Run(); }

void NezhaServiceImpl::StopReplicaThreads() { replica_->Stop(); }

void NezhaServiceImpl::NezhaReplicateRequest(const NezhaRequest& req,
                                             NezhaFastReply* rep,
                                             rrr::DeferredReply* defer) {
   replica_->onNezhaReplicateRequest(req, rep, [defer]() {
      // LOG(INFO) << "reply called";
      defer->reply();
   });
}

void NezhaServiceImpl::NezhaInquire(const NezhaInquireRequest& req,
                                    NezhaInquireReply* rep,
                                    rrr::DeferredReply* defer) {
   replica_->onNezhaInquireRequest(req, rep);
   defer->reply();
}

void NezhaServiceImpl::InterReplicaSync(const NezhaInterReplicaSync& req,
                                        rrr::DeferredReply* defer) {
   replica_->onInterReplicaSync(req);
   defer->reply();
}
}  // namespace NezhaRPC