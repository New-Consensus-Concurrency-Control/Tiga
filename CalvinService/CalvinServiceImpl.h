// clang-format off
#pragma once
#include "CalvinSequencer.h"
#include "CalvinScheduler.h"

// clang-format on
// optional %%: marks header section, code above will be copied into begin of
// generated C++ header
namespace CalvinRPC {

class CalvinServiceImpl : public CalvinService {
  protected:
   CalvinSequencer* sequencer_;
   CalvinScheduler* scheduler_;

  public:
   CalvinServiceImpl(CalvinSequencer* seq, CalvinScheduler* schdl);
   void RunSequencerThreads();
   void StopSequencerThreads();
   // these RPC handler functions need to be implemented by user
   // for 'raw' handlers, remember to reply req, delete req, and
   // sconn->release(); use sconn->run_async for heavy job
   void NormalRequest(const CalvinRequest& req, CalvinReply* rep,
                      rrr::DeferredReply* defer) override;
   void MasterSync(const MasterSyncRequest& req, MasterSyncReply* rep,
                   rrr::DeferredReply* defer) override;

   void InquireSeqNoStatus(const InquireSeqNoRequest& req,
                           InquireSeqNoReply* rep,
                           rrr::DeferredReply* defer) override;
   void DispatchRequest(const CalvinDispatchRequest& req,
                        CalvinDispatchReply* rep,
                        rrr::DeferredReply* defer) override;
};

}  // namespace CalvinRPC
