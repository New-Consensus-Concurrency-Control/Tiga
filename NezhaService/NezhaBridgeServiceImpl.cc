#include "NezhaBridgeServiceImpl.h"

namespace nezha {

NezhaBridgeServiceImpl::NezhaBridgeServiceImpl(
    ConcurrentQueue<BridgeStruct>* qu)
    : qu_(qu) {
   seqNo_ = 1;
}
void NezhaBridgeServiceImpl::Persist(const NezhaRecordMessage& msg,
                                     NezhaAck* res, rrr::DeferredReply* defer) {

   // LOG(INFO) << "Persist " << msg.cmdId_;
   // res->token_ = random();
   // defer->reply();
   std::function<void(const NezhaAck&)> replyHandler =
       std::function<void(const NezhaAck&)>([res, defer](const NezhaAck& r) {
          *res = r;
          defer->reply();
       });
   qu_->enqueue({msg, replyHandler});
}

}  // namespace nezha