#pragma once

#include <errno.h>
#include "rrr.hpp"

// clang-format off

// optional %%: marks header section, code above will be copied into begin of generated C++ header
namespace PaxosRPC {

class PaxosService: public rrr::Service {
public:
    enum {
        RECORDREQUEST = 0x60a6d5df,
        PAXOSAPPENDREQUEST = 0x1d441ee6,
        PAXOSAPPENDRESPONSE = 0x2406ceea,
        PAXOSCOMMITREQUEST = 0x6c2b3636,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(RECORDREQUEST, this, &PaxosService::__RecordRequest__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(PAXOSAPPENDREQUEST, this, &PaxosService::__PaxosAppendRequest__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(PAXOSAPPENDRESPONSE, this, &PaxosService::__PaxosAppendResponse__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(PAXOSCOMMITREQUEST, this, &PaxosService::__PaxosCommitRequest__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(RECORDREQUEST);
        svr->unreg(PAXOSAPPENDREQUEST);
        svr->unreg(PAXOSAPPENDRESPONSE);
        svr->unreg(PAXOSCOMMITREQUEST);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void RecordRequest(const ClientRecord& req, ClientRecordRep* rep, rrr::DeferredReply* defer) = 0;
    virtual void PaxosAppendRequest(const PaxosAppend& req, rrr::DeferredReply* defer) = 0;
    virtual void PaxosAppendResponse(const PaxosAppendRep& rep, rrr::DeferredReply* defer) = 0;
    virtual void PaxosCommitRequest(const PaxosCommitReq& cmt, rrr::DeferredReply* defer) = 0;
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
    void __PaxosAppendRequest__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        PaxosAppend* in_0 = new PaxosAppend;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->PaxosAppendRequest(*in_0, __defer__);
    }
    void __PaxosAppendResponse__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        PaxosAppendRep* in_0 = new PaxosAppendRep;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->PaxosAppendResponse(*in_0, __defer__);
    }
    void __PaxosCommitRequest__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        PaxosCommitReq* in_0 = new PaxosCommitReq;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->PaxosCommitRequest(*in_0, __defer__);
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
    rrr::Future* async_PaxosAppendRequest(const PaxosAppend& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(PaxosService::PAXOSAPPENDREQUEST, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 PaxosAppendRequest(const PaxosAppend& req) {
        rrr::Future* __fu__ = this->async_PaxosAppendRequest(req);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_PaxosAppendResponse(const PaxosAppendRep& rep, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(PaxosService::PAXOSAPPENDRESPONSE, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << rep;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 PaxosAppendResponse(const PaxosAppendRep& rep) {
        rrr::Future* __fu__ = this->async_PaxosAppendResponse(rep);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_PaxosCommitRequest(const PaxosCommitReq& cmt, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(PaxosService::PAXOSCOMMITREQUEST, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << cmt;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 PaxosCommitRequest(const PaxosCommitReq& cmt) {
        rrr::Future* __fu__ = this->async_PaxosCommitRequest(cmt);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
};

} // namespace PaxosRPC


// optional %%: marks footer section, code below will be copied into end of generated C++ header


