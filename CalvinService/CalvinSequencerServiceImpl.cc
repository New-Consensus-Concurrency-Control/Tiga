#include "CalvinSequencerServiceImpl.h"

namespace CalvinRPC {

CalvinSequencerServiceImpl::CalvinSequencerServiceImpl(CalvinSequencer* seq)
    : sequencer_(seq) {}

void CalvinSequencerServiceImpl::ReplicateEpoch(const EpochRequest& req,
                                                rrr::DeferredReply* defer) {
   sequencer_->onReplicateEpochRequest(req);
   defer->reply();
}

void CalvinSequencerServiceImpl::InterReplicaSync(
    const NezhaInterReplicaSync& req, rrr::DeferredReply* defer) {
   sequencer_->onInterReplicaSync(req);
   defer->reply();
}

void CalvinSequencerServiceImpl::CollectNezhaFastReply(
    const NezhaFastReply& rep, rrr::DeferredReply* defer) {
   sequencer_->onNezhaFastReply(rep);
   defer->reply();
}
void CalvinSequencerServiceImpl::NezhaInquire(const NezhaInquireRequest& req,
                                              NezhaInquireReply* rep,
                                              rrr::DeferredReply* defer) {
   sequencer_->onNezhaInquireRequest(req, rep);
   defer->reply();
}

}  // namespace CalvinRPC