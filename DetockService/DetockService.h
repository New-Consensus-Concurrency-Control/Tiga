#pragma once

#include "rrr.hpp"

#include <errno.h>

// clang-format off

// optional %%: marks header section, code above will be copied into begin of generated C++ header
namespace DetockRPC {

class DetockService: public rrr::Service {
public:
    enum {
        NORMALREQUEST = 0x6eab1be4,
        DISPATCHREQUEST = 0x51047914,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(NORMALREQUEST, this, &DetockService::__NormalRequest__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(DISPATCHREQUEST, this, &DetockService::__DispatchRequest__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(NORMALREQUEST);
        svr->unreg(DISPATCHREQUEST);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void NormalRequest(const DetockTxn& req, DetockReply* rep, rrr::DeferredReply* defer) = 0;
    virtual void DispatchRequest(const DetockDispatchRequest& req, DetockDispatchReply* rep, rrr::DeferredReply* defer) = 0;
private:
    void __NormalRequest__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        DetockTxn* in_0 = new DetockTxn;
        req->m >> *in_0;
        DetockReply* out_0 = new DetockReply;
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
    void __DispatchRequest__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        DetockDispatchRequest* in_0 = new DetockDispatchRequest;
        req->m >> *in_0;
        DetockDispatchReply* out_0 = new DetockDispatchReply;
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

class DetockProxy {
protected:
    rrr::Client* __cl__;
public:
    DetockProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_NormalRequest(const DetockTxn& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(DetockService::NORMALREQUEST, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 NormalRequest(const DetockTxn& req, DetockReply* rep) {
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
    rrr::Future* async_DispatchRequest(const DetockDispatchRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(DetockService::DISPATCHREQUEST, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 DispatchRequest(const DetockDispatchRequest& req, DetockDispatchReply* rep) {
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

class DetockSequencerService: public rrr::Service {
public:
    enum {
        SYNCBATCH = 0x50ae0cb1,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(SYNCBATCH, this, &DetockSequencerService::__SyncBatch__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(SYNCBATCH);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void SyncBatch(const DetockLocalLogSync& req, rrr::DeferredReply* defer) = 0;
private:
    void __SyncBatch__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        DetockLocalLogSync* in_0 = new DetockLocalLogSync;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->SyncBatch(*in_0, __defer__);
    }
};

class DetockSequencerProxy {
protected:
    rrr::Client* __cl__;
public:
    DetockSequencerProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_SyncBatch(const DetockLocalLogSync& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(DetockSequencerService::SYNCBATCH, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 SyncBatch(const DetockLocalLogSync& req) {
        rrr::Future* __fu__ = this->async_SyncBatch(req);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
};

class DetockLogManagerService: public rrr::Service {
public:
    enum {
        REPLICATEBATCH = 0x56629e9e,
        REPLICATEBATCHREPLY = 0x1c9f446f,
        COMMITBATCH = 0x2359eab3,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(REPLICATEBATCH, this, &DetockLogManagerService::__ReplicateBatch__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(REPLICATEBATCHREPLY, this, &DetockLogManagerService::__ReplicateBatchReply__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(COMMITBATCH, this, &DetockLogManagerService::__CommitBatch__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(REPLICATEBATCH);
        svr->unreg(REPLICATEBATCHREPLY);
        svr->unreg(COMMITBATCH);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void ReplicateBatch(const DetockPaxosAppend& req, rrr::DeferredReply* defer) = 0;
    virtual void ReplicateBatchReply(const DetockPaxosAppendReply& rep, rrr::DeferredReply* defer) = 0;
    virtual void CommitBatch(const DetockPaxosCommit& req, rrr::DeferredReply* defer) = 0;
private:
    void __ReplicateBatch__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        DetockPaxosAppend* in_0 = new DetockPaxosAppend;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->ReplicateBatch(*in_0, __defer__);
    }
    void __ReplicateBatchReply__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        DetockPaxosAppendReply* in_0 = new DetockPaxosAppendReply;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->ReplicateBatchReply(*in_0, __defer__);
    }
    void __CommitBatch__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        DetockPaxosCommit* in_0 = new DetockPaxosCommit;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->CommitBatch(*in_0, __defer__);
    }
};

class DetockLogManagerProxy {
protected:
    rrr::Client* __cl__;
public:
    DetockLogManagerProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_ReplicateBatch(const DetockPaxosAppend& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(DetockLogManagerService::REPLICATEBATCH, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 ReplicateBatch(const DetockPaxosAppend& req) {
        rrr::Future* __fu__ = this->async_ReplicateBatch(req);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_ReplicateBatchReply(const DetockPaxosAppendReply& rep, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(DetockLogManagerService::REPLICATEBATCHREPLY, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << rep;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 ReplicateBatchReply(const DetockPaxosAppendReply& rep) {
        rrr::Future* __fu__ = this->async_ReplicateBatchReply(rep);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_CommitBatch(const DetockPaxosCommit& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(DetockLogManagerService::COMMITBATCH, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 CommitBatch(const DetockPaxosCommit& req) {
        rrr::Future* __fu__ = this->async_CommitBatch(req);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
};

} // namespace DetockRPC


// optional %%: marks footer section, code below will be copied into end of generated C++ header


