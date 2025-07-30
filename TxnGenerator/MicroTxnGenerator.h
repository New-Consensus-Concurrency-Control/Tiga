#pragma once

#include "TxnGenerator.h"
#include "ZipfDist.h"
class MicroTxnGenerator : public TxnGenerator {
  private:
   std::vector<ZipfDist *> zDists;
   uint32_t keyNum_;
   double alpha_;
   boost::random::mt19937 rand_gen_;

  public:
   MicroTxnGenerator(const uint32_t shardNum, const uint32_t replicaNum,
                     const YAML::Node &config);
   ~MicroTxnGenerator();
   std::string RTTI();
   // tpcc
   void GetTxnReq(ClientRequest *req, uint32_t reqId, uint32_t cid) override;
};
