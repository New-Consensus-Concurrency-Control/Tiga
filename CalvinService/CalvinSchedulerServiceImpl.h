// clang-format off
#pragma once
#include "CalvinScheduler.h"

// clang-format on
// optional %%: marks header section, code above will be copied into begin of
// generated C++ header
namespace CalvinRPC {

class CalvinSchedulerServiceImpl : public CalvinSchedulerService {
  protected:
   CalvinScheduler* scheduler_;

  public:
   CalvinSchedulerServiceImpl(CalvinScheduler* schd);

   void EpochReport(const EpochRequest& req, EpochReply* rep,
                    rrr::DeferredReply* defer) override;
};

}  // namespace CalvinRPC
