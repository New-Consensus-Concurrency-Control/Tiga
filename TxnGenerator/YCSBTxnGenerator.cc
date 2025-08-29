#include "YCSBTxnGenerator.h"

YCSBTxnGenerator::YCSBTxnGenerator(const uint32_t shardNum,
                                   const uint32_t replicaNum,
                                   const YAML::Node& config)
    : TxnGenerator(shardNum, replicaNum, config) {
   keyNum_ = config_["bench"]["record-count"].as<uint32_t>();
   fieldNum_ = config_["bench"]["field-num"].as<uint32_t>();
   fieldLength_ = config_["bench"]["field-length"].as<uint32_t>();
   readProportion_ = config_["bench"]["read-proportion"].as<double>();
   alpha_ = config_["bench"]["coefficient"].as<double>();
   LOG(INFO) << "keyNum=" << keyNum_ << "--alpha=" << alpha_;
   zDist_ = new ZipfDist(alpha_, keyNum_);
   rand_gen_.seed((int)std::time(0) + (uint64_t)pthread_self());
   // rand_gen_.seed(1);  // for debug
}

std::string YCSBTxnGenerator::RTTI() { return "YCSBTxnGenerator"; }

void YCSBTxnGenerator::GetTxnReq(ClientRequest* req, uint32_t reqId,
                                 uint32_t cid) {
   // decide this req is rotxn or read-write txn
   std::uniform_real_distribution<> dis(0.0, 1.0);
   // get a token: a random double between 0 and 1
   double token = dis(rand_gen_);
   // Pick a key
   int key = (*zDist_)(rand_gen_);
   // every key has 10 fields
   if (token <= readProportion_) {
      req->cmd_.txnType_ = TXN_TYPE::YCSB_READ_TXN;
      for (uint32_t fno = 0; fno < fieldNum_; fno++) {
         int k = key * fieldNum_ + fno;
         req->cmd_.ws_[k].set_i32(0);
         req->targetShards_.insert(k % shardNum_);
      }
   } else {  // this is an update txn
      req->cmd_.txnType_ = TXN_TYPE::YCSB_UPDATE_TXN;
      for (uint32_t fno = 0; fno < fieldNum_; fno++) {
         int k = key * fieldNum_ + fno;
         std::string fieldValue = "";
         for (uint32_t f = 0; f < fieldLength_; f++) {
            fieldValue += random() % 26 + 'A';
         }
         req->cmd_.ws_[k].set_str(fieldValue);
         req->targetShards_.insert(k % shardNum_);
      }
   }
}