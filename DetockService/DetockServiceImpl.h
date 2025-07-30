// clang-format off
#pragma once
#include "DetockService/DetockExecutor.h"

// clang-format on
// optional %%: marks header section, code above will be copied into begin of
// generated C++ header
namespace DetockRPC {

class DetockServiceImpl : public DetockService {
  protected:
   DetockExecutor* exec_;

  public:
   DetockServiceImpl(DetockExecutor* e);
   void NormalRequest(const DetockTxn& req, DetockReply* rep,
                      rrr::DeferredReply* defer) override;
   void DispatchRequest(const DetockDispatchRequest& req,
                        DetockDispatchReply* rep,
                        rrr::DeferredReply* defer) override;
};

}  // namespace DetockRPC
