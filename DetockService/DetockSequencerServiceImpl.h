// clang-format off
#pragma once
#include "DetockService/DetockMessage.h"
#include "DetockService/DetockService.h"

// clang-format on
// optional %%: marks header section, code above will be copied into begin of
// generated C++ header
namespace DetockRPC {

class DetockSequencerServiceImpl : public DetockSequencerService {
  protected:
   ConcurrentQueue<DetockLocalLogSync>* logSyncQu_;

  public:
   DetockSequencerServiceImpl(ConcurrentQueue<DetockLocalLogSync>* qu);

   void SyncBatch(const DetockLocalLogSync& req,
                  rrr::DeferredReply* defer) override;
};

}  // namespace DetockRPC
