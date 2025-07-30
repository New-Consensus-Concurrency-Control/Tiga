// clang-format off
#pragma once
#include "NezhaService/NezhaMessage.h"
#include "NezhaService/nezha_rpc.h"

// clang-format on
// optional %%: marks header section, code above will be copied into begin of
// generated C++ header
namespace nezha {

struct BridgeStruct {
   NezhaRecordMessage msg_;
   std::function<void(const NezhaAck&)> replyHandler_;
};

class NezhaBridgeServiceImpl : public NezhaBridgeService {
  protected:
   std::atomic<uint64_t> seqNo_;
   ConcurrentQueue<BridgeStruct>* qu_;

  public:
   NezhaBridgeServiceImpl(ConcurrentQueue<BridgeStruct>* qu);
   void Persist(const NezhaRecordMessage& msg, NezhaAck* res,
                rrr::DeferredReply* defer) override;
};

}  // namespace nezha
