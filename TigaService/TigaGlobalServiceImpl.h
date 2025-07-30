// clang-format off
#pragma once
#include "TigaReplica.h"

// clang-format on
// optional %%: marks header section, code above will be copied into begin of
// generated C++ header
namespace TigaRPC {

class TigaGlobalServiceImpl : public TigaGlobalService {
  protected:
   TigaReplica* replica_;

  public:
   TigaGlobalServiceImpl(TigaReplica* r);
   void StartConnection();

   void InterReplicaSync(const TigaInterReplicaSync&,
                         rrr::DeferredReply* defer) override;
   void SyncStatus(const TigaSyncStatus&, rrr::DeferredReply* defer) override;
};

}  // namespace TigaRPC
