#include "TigaLocalServiceImpl.h"

namespace TigaRPC {

TigaLocalServiceImpl::TigaLocalServiceImpl(TigaReplica* r) : replica_(r) {}

void TigaLocalServiceImpl::StartConnection() {
   replica_->ConnectToOtherLocalServers();
}

void TigaLocalServiceImpl::DeadlineAgreeRequest(
    const TigaDeadlineAgreeRequest& req, rrr::DeferredReply* defer) {
   replica_->onDeadlineAgreementRequest(req);
   defer->reply();
}

}  // namespace TigaRPC