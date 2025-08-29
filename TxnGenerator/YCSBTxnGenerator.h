#pragma once

#include "TxnGenerator.h"
#include "ZipfDist.h"

// YCSB-Workload-b
class YCSBTxnGenerator : public TxnGenerator {
  private:
   ZipfDist *zDist_;
   uint32_t keyNum_;
   uint32_t fieldNum_;
   uint32_t fieldLength_;
   double alpha_;
   double readProportion_;
   boost::random::mt19937 rand_gen_;

  public:
   YCSBTxnGenerator(const uint32_t shardNum, const uint32_t replicaNum,
                    const YAML::Node &config);
   ~YCSBTxnGenerator();
   std::string RTTI();

   void GetTxnReq(ClientRequest *req, uint32_t reqId, uint32_t cid) override;
};
