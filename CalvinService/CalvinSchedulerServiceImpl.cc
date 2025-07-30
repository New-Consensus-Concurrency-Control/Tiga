#include "CalvinSchedulerServiceImpl.h"

namespace CalvinRPC {

CalvinSchedulerServiceImpl::CalvinSchedulerServiceImpl(CalvinScheduler* schd)
    : scheduler_(schd) {}

void CalvinSchedulerServiceImpl::EpochReport(const EpochRequest& req,
                                             EpochReply* rep,
                                             rrr::DeferredReply* defer) {

   scheduler_->onEpochRequest(req, rep, [defer]() {
      // LOG(INFO) << "reply called";
      defer->reply();
   });
}

}  // namespace CalvinRPC