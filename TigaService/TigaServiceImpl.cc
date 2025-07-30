#include "TigaServiceImpl.h"

namespace TigaRPC {

TigaServiceImpl::TigaServiceImpl(TigaReplica* r) : replica_(r) {}

void TigaServiceImpl::RunReplicaProcessingThreads() { replica_->Run(); }

void TigaServiceImpl::StopReplicaProcessingThreads() { replica_->Stop(); }

void TigaServiceImpl::NormalRequest(const TigaReq& req, TigaReply* rep,
                                    rrr::DeferredReply* defer) {
   replica_->onNormalRequest(req, rep, [defer]() {
      // LOG(INFO) << "reply called";
      defer->reply();
   });
}

void TigaServiceImpl::InquireServerSyncStatus(
    const TigaServerSyncStatusRequest& req, TigaServerSyncStatusReply* rep) {
   replica_->onInquireServerSyncStatus(req, rep);
}

void TigaServiceImpl::Reconcliation(const TigaReconcliationReq& req,
                                    TigaReply* rep, rrr::DeferredReply* defer) {
   replica_->onReconcliationRequest(req, rep, [defer]() { defer->reply(); });
}

void TigaServiceImpl::DispatchRequest(const TigaDispatchRequest& req,
                                      TigaDispatchReply* rep,
                                      rrr::DeferredReply* defer) {
   replica_->onDispatchRequest(req, rep, [defer]() { defer->reply(); });
}
}  // namespace TigaRPC