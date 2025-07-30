#include "DetockServiceImpl.h"

namespace DetockRPC {

DetockServiceImpl::DetockServiceImpl(DetockExecutor* e) : exec_(e) {}

void DetockServiceImpl::NormalRequest(const DetockTxn& req, DetockReply* rep,
                                      rrr::DeferredReply* defer) {
   exec_->onNormalRequest(req, rep, [defer]() { defer->reply(); });
}
void DetockServiceImpl::DispatchRequest(const DetockDispatchRequest& req,
                                        DetockDispatchReply* rep,
                                        rrr::DeferredReply* defer) {
   exec_->onDispatchRequest(req, rep, [defer]() { defer->reply(); });
}

}  // namespace DetockRPC