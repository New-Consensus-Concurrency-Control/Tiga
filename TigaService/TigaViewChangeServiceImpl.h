// clang-format off
#pragma once
#include "TigaReplica.h"

// clang-format on
// optional %%: marks header section, code above will be copied into begin of
// generated C++ header
namespace TigaRPC {
class TigaViewChangeServiceImpl : public TigaViewChangeService {

  protected:
   TigaReplica* replica_;

  public:
   TigaViewChangeServiceImpl(TigaReplica* r);
   void StartConnection();

   void ViewChangeReq(const TigaViewChangeReq&,
                      rrr::DeferredReply* defer) override;
   void ViewChange(const TigaViewChange&, rrr::DeferredReply* defer) override;
   void HeartBeat(const TigaHeartBeat&, TigaHeartBeatAck*,
                  rrr::DeferredReply* defer) override;
   void StateTransfer(const TigaStateTransferRequest&, TigaStateTransferReply*,
                      rrr::DeferredReply* defer) override;

   void CrossShardVerifyReq(const TigaCrossShardVerifyReq&,
                            rrr::DeferredReply* defer) override;
   void CrossShardVerifyRep(const TigaCrossShardVerifyRep&,
                            rrr::DeferredReply* defer) override;

   void StartView(const TigaStartView&, rrr::DeferredReply* defer) override;

   void CMPrepare(const TigaCMPrepare&, rrr::DeferredReply* defer) override;
   void CMPrepareReply(const TigaCMPrepareReply&,
                       rrr::DeferredReply* defer) override;
   void CMCommit(const TigaCMCommit&, rrr::DeferredReply* defer) override;

   void FailSignal(const TigaFailSignal&, TigaFailAck*,
                   rrr::DeferredReply* defer) override;
};
}  // namespace TigaRPC
