#include "DetockSequencerServiceImpl.h"

namespace DetockRPC {

DetockSequencerServiceImpl::DetockSequencerServiceImpl(
    ConcurrentQueue<DetockLocalLogSync>* qu)
    : logSyncQu_(qu) {}

void DetockSequencerServiceImpl::SyncBatch(const DetockLocalLogSync& req,
                                           rrr::DeferredReply* defer) {
   //    LOG(INFO) << "Called SyncBatch";
   logSyncQu_->enqueue(req);
   defer->reply();
}

}  // namespace DetockRPC