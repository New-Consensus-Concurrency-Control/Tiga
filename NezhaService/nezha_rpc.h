#pragma once

#include <errno.h>
#include "rrr/rrr.hpp"

namespace nezha {

class NezhaBridgeService : public rrr::Service {
  public:
   enum {
      PERSIST = 0x6b7cda25,
   };
   int __reg_to__(rrr::Server* svr) {
      int ret = 0;
      if ((ret = svr->reg(PERSIST, this,
                          &NezhaBridgeService::__Persist__wrapper__)) != 0) {
         goto err;
      }
      return 0;
   err:
      svr->unreg(PERSIST);
      return ret;
   }
   // these RPC handler functions need to be implemented by user
   // for 'raw' handlers, remember to reply req, delete req, and
   // sconn->release(); use sconn->run_async for heavy job
   virtual void Persist(const NezhaRecordMessage& msg, NezhaAck* res,
                        rrr::DeferredReply* defer) = 0;

  private:
   void __Persist__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
      NezhaRecordMessage* in_0 = new NezhaRecordMessage;
      req->m >> *in_0;
      NezhaAck* out_0 = new NezhaAck;
      auto __marshal_reply__ = [=] { *sconn << *out_0; };
      auto __cleanup__ = [=] {
         delete in_0;
         delete out_0;
      };
      rrr::DeferredReply* __defer__ =
          new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
      this->Persist(*in_0, out_0, __defer__);
   }
};

class NezhaBridgeProxy {
  protected:
   rrr::Client* __cl__;

  public:
   NezhaBridgeProxy(rrr::Client* cl) : __cl__(cl) {}
   rrr::Future* async_Persist(
       const NezhaRecordMessage& msg,
       const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
      rrr::Future* __fu__ =
          __cl__->begin_request(NezhaBridgeService::PERSIST, __fu_attr__);
      if (__fu__ != nullptr) {
         *__cl__ << msg;
      }
      __cl__->end_request();
      return __fu__;
   }
   rrr::i32 Persist(const NezhaRecordMessage& msg, NezhaAck* res) {
      rrr::Future* __fu__ = this->async_Persist(msg);
      if (__fu__ == nullptr) {
         return ENOTCONN;
      }
      rrr::i32 __ret__ = __fu__->get_error_code();
      if (__ret__ == 0) {
         __fu__->get_reply() >> *res;
      }
      __fu__->release();
      return __ret__;
   }
};

}  // namespace nezha
