#include "MicroTxnGenerator.h"

MicroTxnGenerator::MicroTxnGenerator(const uint32_t shardNum,
                                     const uint32_t replicaNum,
                                     const YAML::Node& config)
    : TxnGenerator(shardNum, replicaNum, config) {
   keyNum_ = config_["bench"]["population"]["customer"].as<uint32_t>();
   alpha_ = config_["bench"]["coefficient"].as<double>();
   LOG(INFO) << "keyNum=" << keyNum_ << "--alpha=" << alpha_;
   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      ZipfDist* zDist = new ZipfDist(alpha_, keyNum_);
      zDists.push_back(zDist);
   }
   rand_gen_.seed((int)std::time(0) + (uint64_t)pthread_self());
   // rand_gen_.seed(1);  // for debug
}

std::string MicroTxnGenerator::RTTI() { return "MicroTxnGenerator"; }

void MicroTxnGenerator::GetTxnReq(ClientRequest* req, uint32_t reqId,
                                  uint32_t cid) {
   req->cmd_.txnType_ = TXN_TYPE::MICRO_TXN;
   req->cmd_.reqId_ = reqId;
   req->cmd_.clientId_ = cid;
   std::unordered_set<int32_t> keySet;  // de-duplicate
   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      int32_t k;
      do {
         k = (*zDists[sid % shardNum_])(rand_gen_);
         // k = keyCnter_.fetch_add(1) % keyNum;
      } while (keySet.find(k) != keySet.end());
      // k = (k / shardNum_) * shardNum_;  // for debug
      // // int k = cmd.reqId_ % keyNum;
      // int k = (keyCnter_.fetch_add(1)) % keyNum;
      req->cmd_.ws_[k].set_i32(
          0);  // make sure keys are unique, value has no use
      req->targetShards_.insert(k % shardNum_);

      // req->cmd_.ws_[sid + 1].set_i32(0);
      // req->targetShards_.insert(sid);
   }

   // for (uint32_t sid = 0; sid < shardNum_; sid++) {
   //    req->cmd_.ws_[sid + 1].set_i32(0);
   //    req->targetShards_.insert((sid + 1) % shardNum_);
   // }
   ///////////////////////////////////////////////////////////////
   // req->cmd_.ws_[1].set_i32(0);
   // req->cmd_.ws_[3].set_i32(0);
   // req->targetShards_.insert(1);
}