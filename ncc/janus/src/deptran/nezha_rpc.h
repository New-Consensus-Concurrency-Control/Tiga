#pragma once

#include "rrr.hpp"

#include <errno.h>


namespace PaxosRPC {

class PaxosService: public rrr::Service {
public:
    enum {
        RECORDREQUEST = 0x60a6d5df,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(RECORDREQUEST, this, &PaxosService::__RecordRequest__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(RECORDREQUEST);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void RecordRequest(const ClientRecord& req, ClientRecordRep* rep, rrr::DeferredReply* defer) = 0;
private:
    void __RecordRequest__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        ClientRecord* in_0 = new ClientRecord;
        req->m >> *in_0;
        ClientRecordRep* out_0 = new ClientRecordRep;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->RecordRequest(*in_0, out_0, __defer__);
    }
};

class PaxosProxy {
protected:
    rrr::Client* __cl__;
public:
    PaxosProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_RecordRequest(const ClientRecord& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(PaxosService::RECORDREQUEST, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 RecordRequest(const ClientRecord& req, ClientRecordRep* rep) {
        rrr::Future* __fu__ = this->async_RecordRequest(req);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *rep;
        }
        __fu__->release();
        return __ret__;
    }
};

} // namespace PaxosRPC



