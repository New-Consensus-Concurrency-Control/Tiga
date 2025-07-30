#pragma once

#include "Common.h"
class TxnGenerator {
  protected:
   uint32_t shardNum_;
   uint32_t replicaNum_;
   YAML::Node config_;

  public:
   TxnGenerator(const uint32_t shardNum, const uint32_t replicaNum,
                const YAML::Node &config)
       : shardNum_(shardNum), replicaNum_(replicaNum), config_(config) {};
   ~TxnGenerator() {};
   virtual std::string RTTI() = 0;
   virtual void GetTxnReq(ClientRequest *req, uint32_t reqId, uint32_t cid) = 0;
   virtual bool NeedDisPatch(const ClientRequest &req) { return false; }
   virtual void GetInquireKeys(const uint32_t txnType,
                               std::map<int32_t, mdb::Value> *existing,
                               std::map<int32_t, mdb::Value> *input) {}
};
