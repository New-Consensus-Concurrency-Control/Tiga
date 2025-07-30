#pragma once

#include "rrr.hpp"

#include <errno.h>

// clang-format off

// optional %%: marks header section, code above will be copied into begin of generated C++ header
namespace NezhaRPC {

class NezhaService: public rrr::Service {
public:
    enum {
        NEZHAREPLICATEREQUEST = 0x40e38c3b,
        NEZHAINQUIRE = 0x6c5db62d,
        INTERREPLICASYNC = 0x561f9a4f,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(NEZHAREPLICATEREQUEST, this, &NezhaService::__NezhaReplicateRequest__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(NEZHAINQUIRE, this, &NezhaService::__NezhaInquire__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(INTERREPLICASYNC, this, &NezhaService::__InterReplicaSync__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(NEZHAREPLICATEREQUEST);
        svr->unreg(NEZHAINQUIRE);
        svr->unreg(INTERREPLICASYNC);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void NezhaReplicateRequest(const NezhaRequest& req, NezhaFastReply* rep, rrr::DeferredReply* defer) = 0;
    virtual void NezhaInquire(const NezhaInquireRequest& req, NezhaInquireReply* rep, rrr::DeferredReply* defer) = 0;
    virtual void InterReplicaSync(const NezhaInterReplicaSync& req, rrr::DeferredReply* defer) = 0;
private:
    void __NezhaReplicateRequest__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        NezhaRequest* in_0 = new NezhaRequest;
        req->m >> *in_0;
        NezhaFastReply* out_0 = new NezhaFastReply;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->NezhaReplicateRequest(*in_0, out_0, __defer__);
    }
    void __NezhaInquire__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        NezhaInquireRequest* in_0 = new NezhaInquireRequest;
        req->m >> *in_0;
        NezhaInquireReply* out_0 = new NezhaInquireReply;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->NezhaInquire(*in_0, out_0, __defer__);
    }
    void __InterReplicaSync__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        NezhaInterReplicaSync* in_0 = new NezhaInterReplicaSync;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->InterReplicaSync(*in_0, __defer__);
    }
};

class NezhaProxy {
protected:
    rrr::Client* __cl__;
public:
    NezhaProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_NezhaReplicateRequest(const NezhaRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(NezhaService::NEZHAREPLICATEREQUEST, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 NezhaReplicateRequest(const NezhaRequest& req, NezhaFastReply* rep) {
        rrr::Future* __fu__ = this->async_NezhaReplicateRequest(req);
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
    rrr::Future* async_NezhaInquire(const NezhaInquireRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(NezhaService::NEZHAINQUIRE, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 NezhaInquire(const NezhaInquireRequest& req, NezhaInquireReply* rep) {
        rrr::Future* __fu__ = this->async_NezhaInquire(req);
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
    rrr::Future* async_InterReplicaSync(const NezhaInterReplicaSync& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(NezhaService::INTERREPLICASYNC, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 InterReplicaSync(const NezhaInterReplicaSync& req) {
        rrr::Future* __fu__ = this->async_InterReplicaSync(req);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
};

} // namespace NezhaRPC


// optional %%: marks footer section, code below will be copied into end of generated C++ header


