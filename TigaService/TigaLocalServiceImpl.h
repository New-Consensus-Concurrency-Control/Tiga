// clang-format off
#pragma once
#include "TigaReplica.h"

// clang-format on
// optional %%: marks header section, code above will be copied into begin of
// generated C++ header
namespace TigaRPC {

class TigaLocalServiceImpl : public TigaLocalService {
  protected:
   TigaReplica* replica_;

  public:
   TigaLocalServiceImpl(TigaReplica* r);
   void StartConnection();

   void DeadlineAgreeRequest(const TigaDeadlineAgreeRequest& req,
                             rrr::DeferredReply* defer) override;
};

}  // namespace TigaRPC
