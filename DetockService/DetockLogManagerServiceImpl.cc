#include "DetockLogManagerServiceImpl.h"

namespace DetockRPC {

DetockLogManagerServiceImpl::DetockLogManagerServiceImpl(
    DetockLogManager* logmgr)
    : logManager_(logmgr) {}

void DetockLogManagerServiceImpl::ReplicateBatch(const DetockPaxosAppend& req,
                                                 rrr::DeferredReply* defer) {
   logManager_->onDetockPaxosAppend(req);
   defer->reply();
}

void DetockLogManagerServiceImpl::ReplicateBatchReply(
    const DetockPaxosAppendReply& rep, rrr::DeferredReply* defer) {
   logManager_->onDetockPaxosAppendReply(rep);
   defer->reply();
}

void DetockLogManagerServiceImpl::CommitBatch(const DetockPaxosCommit& req,
                                              rrr::DeferredReply* defer) {
   logManager_->onDetockPaxosCommit(req);
   defer->reply();
}

}  // namespace DetockRPC