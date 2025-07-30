// clang-format off
#pragma once
#include "DetockService/DetockLogManager.h"

// clang-format on
// optional %%: marks header section, code above will be copied into begin of
// generated C++ header
namespace DetockRPC {

class DetockLogManagerServiceImpl : public DetockLogManagerService {
  protected:
   DetockLogManager* logManager_;

  public:
   DetockLogManagerServiceImpl(DetockLogManager* logManager);

   void ReplicateBatch(const DetockPaxosAppend& req,
                       rrr::DeferredReply* defer) override;
   void ReplicateBatchReply(const DetockPaxosAppendReply& rep,
                            rrr::DeferredReply* defer) override;
   void CommitBatch(const DetockPaxosCommit& req,
                    rrr::DeferredReply* defer) override;
};

}  // namespace DetockRPC
