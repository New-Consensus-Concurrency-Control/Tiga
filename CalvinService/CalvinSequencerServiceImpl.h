// clang-format off
#pragma once
#include "CalvinSequencer.h"

// clang-format on
// optional %%: marks header section, code above will be copied into begin of
// generated C++ header
namespace CalvinRPC {

class CalvinSequencerServiceImpl : public CalvinSequencerService {
  protected:
   CalvinSequencer* sequencer_;

  public:
   CalvinSequencerServiceImpl(CalvinSequencer* seq);

   void ReplicateEpoch(const EpochRequest& req,
                       rrr::DeferredReply* defer) override;
   void InterReplicaSync(const NezhaInterReplicaSync& req,
                         rrr::DeferredReply* defer) override;
   void CollectNezhaFastReply(const NezhaFastReply& rep,
                              rrr::DeferredReply* defer) override;
   void NezhaInquire(const NezhaInquireRequest& req, NezhaInquireReply* rep,
                     rrr::DeferredReply* defer) override;
};

}  // namespace CalvinRPC
