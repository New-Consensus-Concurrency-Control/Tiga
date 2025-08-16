#include "TigaGlobalServiceImpl.h"

namespace TigaRPC {

TigaGlobalServiceImpl::TigaGlobalServiceImpl(TigaReplica* r) : replica_(r) {}

void TigaGlobalServiceImpl::StartConnection() {
   replica_->ConnectToOtherGlobalServers();
}

void TigaGlobalServiceImpl::InterReplicaSync(const TigaInterReplicaSync& req,
                                             rrr::DeferredReply* defer) {
   replica_->onInterReplicaSync(req);
   defer->reply();
}

void TigaGlobalServiceImpl::SyncStatus(const TigaSyncStatus& msg,
                                       rrr::DeferredReply* defer) {
   replica_->onSyncStatus(msg);
   defer->reply();
}

void TigaGlobalServiceImpl::GuardNotify(const TigaGuard& msg, TigaGuardAck* ack,
                                        rrr::DeferredReply* defer) {
   replica_->onGuardNotify(msg, ack);
   defer->reply();
}

void TigaGlobalServiceImpl::PromiseNotify(const TigaPromise& msg,
                                          TigaPromiseAck* ack,
                                          rrr::DeferredReply* defer) {
   replica_->onPromiseNotify(msg, ack);
   defer->reply();
}

void TigaGlobalServiceImpl::PromiseRevoke(const TigaPromiseRevoke& msg,
                                          TigaPromiseRevokeAck* ack,
                                          rrr::DeferredReply* defer) {
   replica_->onPromiseRevoke(msg, ack);
   defer->reply();
}
}  // namespace TigaRPC