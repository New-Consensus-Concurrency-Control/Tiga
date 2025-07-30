#include "TigaViewChangeServiceImpl.h"

namespace TigaRPC {
TigaViewChangeServiceImpl::TigaViewChangeServiceImpl(TigaReplica* r)
    : replica_(r) {}

void TigaViewChangeServiceImpl::StartConnection() {
   replica_->ConnectToOtherVRServers();
}

void TigaViewChangeServiceImpl::ViewChangeReq(const TigaViewChangeReq& msg,
                                              rrr::DeferredReply* defer) {
   replica_->onViewChangeReq(msg);
   defer->reply();
}

void TigaViewChangeServiceImpl::ViewChange(const TigaViewChange& msg,
                                           rrr::DeferredReply* defer) {
   replica_->onViewChange(msg);
   defer->reply();
}

void TigaViewChangeServiceImpl::HeartBeat(const TigaHeartBeat& req,
                                          TigaHeartBeatAck* ack,
                                          rrr::DeferredReply* defer) {
   replica_->onHeartBeat(req, ack);
   defer->reply();
}
void TigaViewChangeServiceImpl::StateTransfer(
    const TigaStateTransferRequest& req, TigaStateTransferReply* rep,
    rrr::DeferredReply* defer) {
   replica_->onStateTransfer(req, rep);
   defer->reply();
}

void TigaViewChangeServiceImpl::CrossShardVerifyReq(
    const TigaCrossShardVerifyReq& req, rrr::DeferredReply* defer) {
   replica_->onCrossShardVerifyReq(req);
   defer->reply();
}

void TigaViewChangeServiceImpl::CrossShardVerifyRep(
    const TigaCrossShardVerifyRep& rep, rrr::DeferredReply* defer) {
   replica_->onCrossShardVerifyRep(rep);
   defer->reply();
}

void TigaViewChangeServiceImpl::StartView(const TigaStartView& msg,
                                          rrr::DeferredReply* defer) {
   replica_->onStartView(msg);
   defer->reply();
}

void TigaViewChangeServiceImpl::CMPrepare(const TigaCMPrepare& msg,
                                          rrr::DeferredReply* defer) {
   replica_->onCMPrepare(msg);
   defer->reply();
}
void TigaViewChangeServiceImpl::CMPrepareReply(const TigaCMPrepareReply& msg,
                                               rrr::DeferredReply* defer) {
   replica_->onCMPrepareReply(msg);
   defer->reply();
}
void TigaViewChangeServiceImpl::CMCommit(const TigaCMCommit& msg,
                                         rrr::DeferredReply* defer) {
   replica_->onCMCommit(msg);
   defer->reply();
}
void TigaViewChangeServiceImpl::FailSignal(const TigaFailSignal& msg,
                                           TigaFailAck* ack,
                                           rrr::DeferredReply* defer) {
   replica_->onFailSignal(msg, ack, [defer]() {
      // LOG(INFO) << "reply called";
      defer->reply();
   });
}

}  // namespace TigaRPC
