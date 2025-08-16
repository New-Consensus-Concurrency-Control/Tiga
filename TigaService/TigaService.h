#pragma once

#include "rrr.hpp"

#include <errno.h>

// clang-format off

// optional %%: marks header section, code above will be copied into begin of generated C++ header
namespace TigaRPC {

class TigaService: public rrr::Service {
public:
    enum {
        NORMALREQUEST = 0x460fb1dc,
        INQUIRESERVERSYNCSTATUS = 0x528d7946,
        RECONCLIATION = 0x3d59b1b2,
        DISPATCHREQUEST = 0x5d8c251a,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(NORMALREQUEST, this, &TigaService::__NormalRequest__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(INQUIRESERVERSYNCSTATUS, this, &TigaService::__InquireServerSyncStatus__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(RECONCLIATION, this, &TigaService::__Reconcliation__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(DISPATCHREQUEST, this, &TigaService::__DispatchRequest__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(NORMALREQUEST);
        svr->unreg(INQUIRESERVERSYNCSTATUS);
        svr->unreg(RECONCLIATION);
        svr->unreg(DISPATCHREQUEST);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void NormalRequest(const TigaReq& req, TigaReply* rep, rrr::DeferredReply* defer) = 0;
    virtual void InquireServerSyncStatus(const TigaServerSyncStatusRequest&, TigaServerSyncStatusReply*) = 0;
    virtual void Reconcliation(const TigaReconcliationReq& req, TigaReply* rep, rrr::DeferredReply* defer) = 0;
    virtual void DispatchRequest(const TigaDispatchRequest& req, TigaDispatchReply* rep, rrr::DeferredReply* defer) = 0;
private:
    void __NormalRequest__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaReq* in_0 = new TigaReq;
        req->m >> *in_0;
        TigaReply* out_0 = new TigaReply;
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
    void __InquireServerSyncStatus__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        auto f = [=] {
            TigaServerSyncStatusRequest in_0;
            req->m >> in_0;
            TigaServerSyncStatusReply out_0;
            this->InquireServerSyncStatus(in_0, &out_0);
            sconn->begin_reply(req);
            *sconn << out_0;
            sconn->end_reply();
            delete req;
            sconn->release();
        };
        sconn->run_async(f);
    }
    void __Reconcliation__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaReconcliationReq* in_0 = new TigaReconcliationReq;
        req->m >> *in_0;
        TigaReply* out_0 = new TigaReply;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->Reconcliation(*in_0, out_0, __defer__);
    }
    void __DispatchRequest__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaDispatchRequest* in_0 = new TigaDispatchRequest;
        req->m >> *in_0;
        TigaDispatchReply* out_0 = new TigaDispatchReply;
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

class TigaProxy {
protected:
    rrr::Client* __cl__;
public:
    TigaProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_NormalRequest(const TigaReq& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaService::NORMALREQUEST, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 NormalRequest(const TigaReq& req, TigaReply* rep) {
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
    rrr::Future* async_InquireServerSyncStatus(const TigaServerSyncStatusRequest& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaService::INQUIRESERVERSYNCSTATUS, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 InquireServerSyncStatus(const TigaServerSyncStatusRequest& in_0, TigaServerSyncStatusReply* out_0) {
        rrr::Future* __fu__ = this->async_InquireServerSyncStatus(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *out_0;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_Reconcliation(const TigaReconcliationReq& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaService::RECONCLIATION, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 Reconcliation(const TigaReconcliationReq& req, TigaReply* rep) {
        rrr::Future* __fu__ = this->async_Reconcliation(req);
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
    rrr::Future* async_DispatchRequest(const TigaDispatchRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaService::DISPATCHREQUEST, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 DispatchRequest(const TigaDispatchRequest& req, TigaDispatchReply* rep) {
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

class TigaLocalService: public rrr::Service {
public:
    enum {
        DEADLINEAGREEREQUEST = 0x64f53bcf,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(DEADLINEAGREEREQUEST, this, &TigaLocalService::__DeadlineAgreeRequest__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(DEADLINEAGREEREQUEST);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void DeadlineAgreeRequest(const TigaDeadlineAgreeRequest& req, rrr::DeferredReply* defer) = 0;
private:
    void __DeadlineAgreeRequest__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaDeadlineAgreeRequest* in_0 = new TigaDeadlineAgreeRequest;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->DeadlineAgreeRequest(*in_0, __defer__);
    }
};

class TigaLocalProxy {
protected:
    rrr::Client* __cl__;
public:
    TigaLocalProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_DeadlineAgreeRequest(const TigaDeadlineAgreeRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaLocalService::DEADLINEAGREEREQUEST, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 DeadlineAgreeRequest(const TigaDeadlineAgreeRequest& req) {
        rrr::Future* __fu__ = this->async_DeadlineAgreeRequest(req);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
};

class TigaGlobalService: public rrr::Service {
public:
    enum {
        INTERREPLICASYNC = 0x57740c3f,
        SYNCSTATUS = 0x59980735,
        GUARDNOTIFY = 0x6401a1eb,
        PROMISENOTIFY = 0x6e3b3da8,
        PROMISEREVOKE = 0x39806d69,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(INTERREPLICASYNC, this, &TigaGlobalService::__InterReplicaSync__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(SYNCSTATUS, this, &TigaGlobalService::__SyncStatus__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(GUARDNOTIFY, this, &TigaGlobalService::__GuardNotify__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(PROMISENOTIFY, this, &TigaGlobalService::__PromiseNotify__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(PROMISEREVOKE, this, &TigaGlobalService::__PromiseRevoke__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(INTERREPLICASYNC);
        svr->unreg(SYNCSTATUS);
        svr->unreg(GUARDNOTIFY);
        svr->unreg(PROMISENOTIFY);
        svr->unreg(PROMISEREVOKE);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void InterReplicaSync(const TigaInterReplicaSync&, rrr::DeferredReply* defer) = 0;
    virtual void SyncStatus(const TigaSyncStatus&, rrr::DeferredReply* defer) = 0;
    virtual void GuardNotify(const TigaGuard&, TigaGuardAck*, rrr::DeferredReply* defer) = 0;
    virtual void PromiseNotify(const TigaPromise&, TigaPromiseAck*, rrr::DeferredReply* defer) = 0;
    virtual void PromiseRevoke(const TigaPromiseRevoke&, TigaPromiseRevokeAck*, rrr::DeferredReply* defer) = 0;
private:
    void __InterReplicaSync__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaInterReplicaSync* in_0 = new TigaInterReplicaSync;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->InterReplicaSync(*in_0, __defer__);
    }
    void __SyncStatus__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaSyncStatus* in_0 = new TigaSyncStatus;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->SyncStatus(*in_0, __defer__);
    }
    void __GuardNotify__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaGuard* in_0 = new TigaGuard;
        req->m >> *in_0;
        TigaGuardAck* out_0 = new TigaGuardAck;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->GuardNotify(*in_0, out_0, __defer__);
    }
    void __PromiseNotify__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaPromise* in_0 = new TigaPromise;
        req->m >> *in_0;
        TigaPromiseAck* out_0 = new TigaPromiseAck;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->PromiseNotify(*in_0, out_0, __defer__);
    }
    void __PromiseRevoke__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaPromiseRevoke* in_0 = new TigaPromiseRevoke;
        req->m >> *in_0;
        TigaPromiseRevokeAck* out_0 = new TigaPromiseRevokeAck;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->PromiseRevoke(*in_0, out_0, __defer__);
    }
};

class TigaGlobalProxy {
protected:
    rrr::Client* __cl__;
public:
    TigaGlobalProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_InterReplicaSync(const TigaInterReplicaSync& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaGlobalService::INTERREPLICASYNC, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 InterReplicaSync(const TigaInterReplicaSync& in_0) {
        rrr::Future* __fu__ = this->async_InterReplicaSync(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_SyncStatus(const TigaSyncStatus& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaGlobalService::SYNCSTATUS, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 SyncStatus(const TigaSyncStatus& in_0) {
        rrr::Future* __fu__ = this->async_SyncStatus(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_GuardNotify(const TigaGuard& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaGlobalService::GUARDNOTIFY, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 GuardNotify(const TigaGuard& in_0, TigaGuardAck* out_0) {
        rrr::Future* __fu__ = this->async_GuardNotify(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *out_0;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_PromiseNotify(const TigaPromise& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaGlobalService::PROMISENOTIFY, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 PromiseNotify(const TigaPromise& in_0, TigaPromiseAck* out_0) {
        rrr::Future* __fu__ = this->async_PromiseNotify(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *out_0;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_PromiseRevoke(const TigaPromiseRevoke& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaGlobalService::PROMISEREVOKE, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 PromiseRevoke(const TigaPromiseRevoke& in_0, TigaPromiseRevokeAck* out_0) {
        rrr::Future* __fu__ = this->async_PromiseRevoke(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *out_0;
        }
        __fu__->release();
        return __ret__;
    }
};

class TigaViewChangeService: public rrr::Service {
public:
    enum {
        VIEWCHANGEREQ = 0x37552d67,
        VIEWCHANGE = 0x60d32e0e,
        HEARTBEAT = 0x66b13316,
        STATETRANSFER = 0x22e0f589,
        CMPREPARE = 0x2416a60e,
        CMPREPAREREPLY = 0x41f0a264,
        CMCOMMIT = 0x2c800a42,
        FAILSIGNAL = 0x3e7b0558,
        CROSSSHARDVERIFYREQ = 0x1ab32b69,
        CROSSSHARDVERIFYREP = 0x4f6f6901,
        STARTVIEW = 0x4d48c288,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(VIEWCHANGEREQ, this, &TigaViewChangeService::__ViewChangeReq__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(VIEWCHANGE, this, &TigaViewChangeService::__ViewChange__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(HEARTBEAT, this, &TigaViewChangeService::__HeartBeat__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(STATETRANSFER, this, &TigaViewChangeService::__StateTransfer__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(CMPREPARE, this, &TigaViewChangeService::__CMPrepare__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(CMPREPAREREPLY, this, &TigaViewChangeService::__CMPrepareReply__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(CMCOMMIT, this, &TigaViewChangeService::__CMCommit__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(FAILSIGNAL, this, &TigaViewChangeService::__FailSignal__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(CROSSSHARDVERIFYREQ, this, &TigaViewChangeService::__CrossShardVerifyReq__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(CROSSSHARDVERIFYREP, this, &TigaViewChangeService::__CrossShardVerifyRep__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(STARTVIEW, this, &TigaViewChangeService::__StartView__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(VIEWCHANGEREQ);
        svr->unreg(VIEWCHANGE);
        svr->unreg(HEARTBEAT);
        svr->unreg(STATETRANSFER);
        svr->unreg(CMPREPARE);
        svr->unreg(CMPREPAREREPLY);
        svr->unreg(CMCOMMIT);
        svr->unreg(FAILSIGNAL);
        svr->unreg(CROSSSHARDVERIFYREQ);
        svr->unreg(CROSSSHARDVERIFYREP);
        svr->unreg(STARTVIEW);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void ViewChangeReq(const TigaViewChangeReq&, rrr::DeferredReply* defer) = 0;
    virtual void ViewChange(const TigaViewChange&, rrr::DeferredReply* defer) = 0;
    virtual void HeartBeat(const TigaHeartBeat&, TigaHeartBeatAck*, rrr::DeferredReply* defer) = 0;
    virtual void StateTransfer(const TigaStateTransferRequest&, TigaStateTransferReply*, rrr::DeferredReply* defer) = 0;
    virtual void CMPrepare(const TigaCMPrepare&, rrr::DeferredReply* defer) = 0;
    virtual void CMPrepareReply(const TigaCMPrepareReply&, rrr::DeferredReply* defer) = 0;
    virtual void CMCommit(const TigaCMCommit&, rrr::DeferredReply* defer) = 0;
    virtual void FailSignal(const TigaFailSignal&, TigaFailAck*, rrr::DeferredReply* defer) = 0;
    virtual void CrossShardVerifyReq(const TigaCrossShardVerifyReq&, rrr::DeferredReply* defer) = 0;
    virtual void CrossShardVerifyRep(const TigaCrossShardVerifyRep&, rrr::DeferredReply* defer) = 0;
    virtual void StartView(const TigaStartView&, rrr::DeferredReply* defer) = 0;
private:
    void __ViewChangeReq__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaViewChangeReq* in_0 = new TigaViewChangeReq;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->ViewChangeReq(*in_0, __defer__);
    }
    void __ViewChange__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaViewChange* in_0 = new TigaViewChange;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->ViewChange(*in_0, __defer__);
    }
    void __HeartBeat__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaHeartBeat* in_0 = new TigaHeartBeat;
        req->m >> *in_0;
        TigaHeartBeatAck* out_0 = new TigaHeartBeatAck;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->HeartBeat(*in_0, out_0, __defer__);
    }
    void __StateTransfer__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaStateTransferRequest* in_0 = new TigaStateTransferRequest;
        req->m >> *in_0;
        TigaStateTransferReply* out_0 = new TigaStateTransferReply;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->StateTransfer(*in_0, out_0, __defer__);
    }
    void __CMPrepare__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaCMPrepare* in_0 = new TigaCMPrepare;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->CMPrepare(*in_0, __defer__);
    }
    void __CMPrepareReply__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaCMPrepareReply* in_0 = new TigaCMPrepareReply;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->CMPrepareReply(*in_0, __defer__);
    }
    void __CMCommit__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaCMCommit* in_0 = new TigaCMCommit;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->CMCommit(*in_0, __defer__);
    }
    void __FailSignal__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaFailSignal* in_0 = new TigaFailSignal;
        req->m >> *in_0;
        TigaFailAck* out_0 = new TigaFailAck;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->FailSignal(*in_0, out_0, __defer__);
    }
    void __CrossShardVerifyReq__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaCrossShardVerifyReq* in_0 = new TigaCrossShardVerifyReq;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->CrossShardVerifyReq(*in_0, __defer__);
    }
    void __CrossShardVerifyRep__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaCrossShardVerifyRep* in_0 = new TigaCrossShardVerifyRep;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->CrossShardVerifyRep(*in_0, __defer__);
    }
    void __StartView__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        TigaStartView* in_0 = new TigaStartView;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->StartView(*in_0, __defer__);
    }
};

class TigaViewChangeProxy {
protected:
    rrr::Client* __cl__;
public:
    TigaViewChangeProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_ViewChangeReq(const TigaViewChangeReq& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaViewChangeService::VIEWCHANGEREQ, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 ViewChangeReq(const TigaViewChangeReq& in_0) {
        rrr::Future* __fu__ = this->async_ViewChangeReq(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_ViewChange(const TigaViewChange& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaViewChangeService::VIEWCHANGE, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 ViewChange(const TigaViewChange& in_0) {
        rrr::Future* __fu__ = this->async_ViewChange(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_HeartBeat(const TigaHeartBeat& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaViewChangeService::HEARTBEAT, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 HeartBeat(const TigaHeartBeat& in_0, TigaHeartBeatAck* out_0) {
        rrr::Future* __fu__ = this->async_HeartBeat(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *out_0;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_StateTransfer(const TigaStateTransferRequest& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaViewChangeService::STATETRANSFER, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 StateTransfer(const TigaStateTransferRequest& in_0, TigaStateTransferReply* out_0) {
        rrr::Future* __fu__ = this->async_StateTransfer(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *out_0;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_CMPrepare(const TigaCMPrepare& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaViewChangeService::CMPREPARE, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 CMPrepare(const TigaCMPrepare& in_0) {
        rrr::Future* __fu__ = this->async_CMPrepare(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_CMPrepareReply(const TigaCMPrepareReply& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaViewChangeService::CMPREPAREREPLY, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 CMPrepareReply(const TigaCMPrepareReply& in_0) {
        rrr::Future* __fu__ = this->async_CMPrepareReply(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_CMCommit(const TigaCMCommit& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaViewChangeService::CMCOMMIT, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 CMCommit(const TigaCMCommit& in_0) {
        rrr::Future* __fu__ = this->async_CMCommit(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_FailSignal(const TigaFailSignal& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaViewChangeService::FAILSIGNAL, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 FailSignal(const TigaFailSignal& in_0, TigaFailAck* out_0) {
        rrr::Future* __fu__ = this->async_FailSignal(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *out_0;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_CrossShardVerifyReq(const TigaCrossShardVerifyReq& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaViewChangeService::CROSSSHARDVERIFYREQ, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 CrossShardVerifyReq(const TigaCrossShardVerifyReq& in_0) {
        rrr::Future* __fu__ = this->async_CrossShardVerifyReq(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_CrossShardVerifyRep(const TigaCrossShardVerifyRep& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaViewChangeService::CROSSSHARDVERIFYREP, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 CrossShardVerifyRep(const TigaCrossShardVerifyRep& in_0) {
        rrr::Future* __fu__ = this->async_CrossShardVerifyRep(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_StartView(const TigaStartView& in_0, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(TigaViewChangeService::STARTVIEW, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << in_0;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 StartView(const TigaStartView& in_0) {
        rrr::Future* __fu__ = this->async_StartView(in_0);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
};

} // namespace TigaRPC


// optional %%: marks footer section, code below will be copied into end of generated C++ header


