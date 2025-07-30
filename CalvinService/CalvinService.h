#pragma once

#include "rrr.hpp"

#include <errno.h>

// clang-format off

// optional %%: marks header section, code above will be copied into begin of generated C++ header
namespace CalvinRPC {

class CalvinService: public rrr::Service {
public:
    enum {
        NORMALREQUEST = 0x2f453068,
        MASTERSYNC = 0x1256cc36,
        INQUIRESEQNOSTATUS = 0x25bc5af9,
        DISPATCHREQUEST = 0x39376cf3,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(NORMALREQUEST, this, &CalvinService::__NormalRequest__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(MASTERSYNC, this, &CalvinService::__MasterSync__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(INQUIRESEQNOSTATUS, this, &CalvinService::__InquireSeqNoStatus__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(DISPATCHREQUEST, this, &CalvinService::__DispatchRequest__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(NORMALREQUEST);
        svr->unreg(MASTERSYNC);
        svr->unreg(INQUIRESEQNOSTATUS);
        svr->unreg(DISPATCHREQUEST);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void NormalRequest(const CalvinRequest& req, CalvinReply* rep, rrr::DeferredReply* defer) = 0;
    virtual void MasterSync(const MasterSyncRequest& req, MasterSyncReply* rep, rrr::DeferredReply* defer) = 0;
    virtual void InquireSeqNoStatus(const InquireSeqNoRequest& req, InquireSeqNoReply* rep, rrr::DeferredReply* defer) = 0;
    virtual void DispatchRequest(const CalvinDispatchRequest& req, CalvinDispatchReply* rep, rrr::DeferredReply* defer) = 0;
private:
    void __NormalRequest__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        CalvinRequest* in_0 = new CalvinRequest;
        req->m >> *in_0;
        CalvinReply* out_0 = new CalvinReply;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->NormalRequest(*in_0, out_0, __defer__);
    }
    void __MasterSync__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        MasterSyncRequest* in_0 = new MasterSyncRequest;
        req->m >> *in_0;
        MasterSyncReply* out_0 = new MasterSyncReply;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->MasterSync(*in_0, out_0, __defer__);
    }
    void __InquireSeqNoStatus__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        InquireSeqNoRequest* in_0 = new InquireSeqNoRequest;
        req->m >> *in_0;
        InquireSeqNoReply* out_0 = new InquireSeqNoReply;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->InquireSeqNoStatus(*in_0, out_0, __defer__);
    }
    void __DispatchRequest__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        CalvinDispatchRequest* in_0 = new CalvinDispatchRequest;
        req->m >> *in_0;
        CalvinDispatchReply* out_0 = new CalvinDispatchReply;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->DispatchRequest(*in_0, out_0, __defer__);
    }
};

class CalvinProxy {
protected:
    rrr::Client* __cl__;
public:
    CalvinProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_NormalRequest(const CalvinRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(CalvinService::NORMALREQUEST, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 NormalRequest(const CalvinRequest& req, CalvinReply* rep) {
        rrr::Future* __fu__ = this->async_NormalRequest(req);
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
    rrr::Future* async_MasterSync(const MasterSyncRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(CalvinService::MASTERSYNC, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 MasterSync(const MasterSyncRequest& req, MasterSyncReply* rep) {
        rrr::Future* __fu__ = this->async_MasterSync(req);
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
    rrr::Future* async_InquireSeqNoStatus(const InquireSeqNoRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(CalvinService::INQUIRESEQNOSTATUS, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 InquireSeqNoStatus(const InquireSeqNoRequest& req, InquireSeqNoReply* rep) {
        rrr::Future* __fu__ = this->async_InquireSeqNoStatus(req);
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
    rrr::Future* async_DispatchRequest(const CalvinDispatchRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(CalvinService::DISPATCHREQUEST, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 DispatchRequest(const CalvinDispatchRequest& req, CalvinDispatchReply* rep) {
        rrr::Future* __fu__ = this->async_DispatchRequest(req);
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

class CalvinSequencerService: public rrr::Service {
public:
    enum {
        REPLICATEEPOCH = 0x637670a0,
        INTERREPLICASYNC = 0x5246277a,
        COLLECTNEZHAFASTREPLY = 0x1cf3cdb4,
        NEZHAINQUIRE = 0x4c37e742,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(REPLICATEEPOCH, this, &CalvinSequencerService::__ReplicateEpoch__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(INTERREPLICASYNC, this, &CalvinSequencerService::__InterReplicaSync__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(COLLECTNEZHAFASTREPLY, this, &CalvinSequencerService::__CollectNezhaFastReply__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(NEZHAINQUIRE, this, &CalvinSequencerService::__NezhaInquire__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(REPLICATEEPOCH);
        svr->unreg(INTERREPLICASYNC);
        svr->unreg(COLLECTNEZHAFASTREPLY);
        svr->unreg(NEZHAINQUIRE);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void ReplicateEpoch(const EpochRequest& req, rrr::DeferredReply* defer) = 0;
    virtual void InterReplicaSync(const NezhaInterReplicaSync& req, rrr::DeferredReply* defer) = 0;
    virtual void CollectNezhaFastReply(const NezhaFastReply& rep, rrr::DeferredReply* defer) = 0;
    virtual void NezhaInquire(const NezhaInquireRequest& req, NezhaInquireReply* rep, rrr::DeferredReply* defer) = 0;
private:
    void __ReplicateEpoch__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        EpochRequest* in_0 = new EpochRequest;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->ReplicateEpoch(*in_0, __defer__);
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
    void __CollectNezhaFastReply__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        NezhaFastReply* in_0 = new NezhaFastReply;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->CollectNezhaFastReply(*in_0, __defer__);
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
};

class CalvinSequencerProxy {
protected:
    rrr::Client* __cl__;
public:
    CalvinSequencerProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_ReplicateEpoch(const EpochRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(CalvinSequencerService::REPLICATEEPOCH, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 ReplicateEpoch(const EpochRequest& req) {
        rrr::Future* __fu__ = this->async_ReplicateEpoch(req);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_InterReplicaSync(const NezhaInterReplicaSync& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(CalvinSequencerService::INTERREPLICASYNC, __fu_attr__);
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
    rrr::Future* async_CollectNezhaFastReply(const NezhaFastReply& rep, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(CalvinSequencerService::COLLECTNEZHAFASTREPLY, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << rep;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 CollectNezhaFastReply(const NezhaFastReply& rep) {
        rrr::Future* __fu__ = this->async_CollectNezhaFastReply(rep);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_NezhaInquire(const NezhaInquireRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(CalvinSequencerService::NEZHAINQUIRE, __fu_attr__);
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
};

class CalvinSchedulerService: public rrr::Service {
public:
    enum {
        EPOCHREPORT = 0x659f1f29,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(EPOCHREPORT, this, &CalvinSchedulerService::__EpochReport__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(EPOCHREPORT);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void EpochReport(const EpochRequest& req, EpochReply* rep, rrr::DeferredReply* defer) = 0;
private:
    void __EpochReport__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        EpochRequest* in_0 = new EpochRequest;
        req->m >> *in_0;
        EpochReply* out_0 = new EpochReply;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->EpochReport(*in_0, out_0, __defer__);
    }
};

class CalvinSchedulerProxy {
protected:
    rrr::Client* __cl__;
public:
    CalvinSchedulerProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_EpochReport(const EpochRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(CalvinSchedulerService::EPOCHREPORT, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 EpochReport(const EpochRequest& req, EpochReply* rep) {
        rrr::Future* __fu__ = this->async_EpochReport(req);
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

} // namespace CalvinRPC


// optional %%: marks footer section, code below will be copied into end of generated C++ header


