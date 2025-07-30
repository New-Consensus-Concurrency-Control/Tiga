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

}  // namespace TigaRPC