#include "CalvinServiceImpl.h"

namespace CalvinRPC {

CalvinServiceImpl::CalvinServiceImpl(CalvinSequencer* seq,
                                     CalvinScheduler* schdl)
    : sequencer_(seq), scheduler_(schdl) {}

void CalvinServiceImpl::RunSequencerThreads() { sequencer_->Run(); }

void CalvinServiceImpl::StopSequencerThreads() { sequencer_->Stop(); }

void CalvinServiceImpl::NormalRequest(const CalvinRequest& req,
                                      CalvinReply* rep,
                                      rrr::DeferredReply* defer) {
   sequencer_->onNormalRequest(req, rep, [defer]() {
      // LOG(INFO) << "reply called";
      defer->reply();
   });
}

void CalvinServiceImpl::MasterSync(const MasterSyncRequest& req,
                                   MasterSyncReply* rep,
                                   rrr::DeferredReply* defer) {
   sequencer_->onMasterSyncRequest(req, rep, [defer]() {
      // LOG(INFO) << "reply called";
      defer->reply();
   });
}

void CalvinServiceImpl::InquireSeqNoStatus(const InquireSeqNoRequest& req,
                                           InquireSeqNoReply* rep,
                                           rrr::DeferredReply* defer) {

   rep->shardId_ = scheduler_->sm_->ShardId();
   rep->replicaId_ = scheduler_->sm_->ReplicaId();
   rep->maxSeqNos_.resize(scheduler_->sm_->ShardNum());
   for (uint32_t i = 0; i < rep->maxSeqNos_.size(); i++) {
      rep->maxSeqNos_[i] = scheduler_->maxSeqNoByShard_[i];
   }
   defer->reply();
}

void CalvinServiceImpl::DispatchRequest(const CalvinDispatchRequest& req,
                                        CalvinDispatchReply* rep,
                                        rrr::DeferredReply* defer) {
   scheduler_->onDispatchRequest(req, rep, [defer]() { defer->reply(); });
}

}  // namespace CalvinRPC