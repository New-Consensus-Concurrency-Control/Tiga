#include "CalvinCoordinator.h"

CalvinCoordinator::CalvinCoordinator(const uint32_t coordinatorId,
                                     const uint32_t shardNum,
                                     const uint32_t replicaNum,
                                     const int32_t designateShardId,
                                     const int32_t designateReplicaId,
                                     CalvinCommunicator* comm,
                                     const YAML::Node& config)
    : shardNum_(shardNum),
      replicaNum_(replicaNum),
      designatedShardId_(designateShardId),
      designatedReplicaId_(designateReplicaId),
      comm_(comm),
      config_(config) {}

void CalvinCoordinator::DoOne(const ClientRequest& creq, TxnGenerator* txnGen) {
   // LOG(INFO) << "Reseting..";
   Reset();
   // LOG(INFO) << "Resetted..";
   std::lock_guard<std::recursive_mutex> lock(this->mtx_);
   phase_++;  // Reset, i.e., upgrade phase counter
   sendTime_ = GetMicrosecondTimestamp();
   // Store on coordinator side and use coordiantor's id and requestId
   targetShards_ = creq.targetShards_;
   callback_ = creq.callback_;
   reqInProcess_.clientId_ = creq.cmd_.clientId_;
   reqInProcess_.reqId_ = creq.cmd_.reqId_;
   reqInProcess_.txnType_ = creq.cmd_.txnType_;
   reqInProcess_.ws_ = creq.cmd_.ws_;
   txnGen_ = txnGen;
   if (designatedShardId_ < 0 || designatedReplicaId_ < 0) {
      // Choose the least loaded
      // uint32_t rid = 0;
      // int32_t maxOutstanding = 0;
      // int32_t minOutstanding = INT32_MAX;
      // for (uint32_t r = 0; r < replicaNum_; r++) {
      //    if (outstandingTxnNum_[r] < minOutstanding) {
      //       rid = r;
      //       minOutstanding = outstandingTxnNum_[r];
      //    }
      //    if (outstandingTxnNum_[r] > maxOutstanding) {
      //       maxOutstanding = outstandingTxnNum_[r];
      //    }
      // }
      // if (maxOutstanding - minOutstanding < 1000) {
      //    reqInProcess_.designateShardId_ = creq.cmd_.reqId_ % replicaNum_;
      //    reqInProcess_.designateReplicaId_ = creq.cmd_.reqId_ % replicaNum_;
      // } else {
      //    reqInProcess_.designateShardId_ = rid;
      //    reqInProcess_.designateReplicaId_ = rid;
      // }

      reqInProcess_.designateShardId_ =
          totalSubmittedTxn_ % std::min(replicaNum_, shardNum_);
      reqInProcess_.designateReplicaId_ =
          totalSubmittedTxn_ % std::min(replicaNum_, shardNum_);
   } else {
      reqInProcess_.designateShardId_ = designatedShardId_;
      reqInProcess_.designateReplicaId_ = designatedReplicaId_;
   }

   if (txnGen_->NeedDisPatch(creq)) {
      stage_ = STAGE::Dispatching;
      Dispatch();
   } else {
      stage_ = STAGE::Commiting;
      Launch();
   }

   // if (totalSubmittedTxn_ % 10000 == 1) {
   //    for (uint32_t i = 0; i < replicaNum_; i++) {
   //       InquireSeqNoRequest req;
   //       req.clientId_ = coordinatorId_;
   //       rrr::FutureAttr fuattr;
   //       std::function<void(Future*)> cb = [this](Future* fu) {
   //          InquireSeqNoReply rep;
   //          fu->get_reply() >> rep;
   //          for (uint32_t s = 0; s < rep.maxSeqNos_.size(); s++) {
   //             maxSeqNos_[rep.replicaId_][s] = rep.maxSeqNos_[s];
   //          }
   //       };
   //       fuattr.callback = cb;
   //       Future::safe_release(
   //           comm_->ProxyAt(i, i)->async_InquireSeqNoStatus(req, fuattr));
   //    }
   // }
}

void CalvinCoordinator::Launch() {
   // LOG(INFO) << "Submit " << reqInProcess_.reqId_;
   uint32_t currentPhase = phase_;

   outstandingTxnNum_[reqInProcess_.designateReplicaId_]++;
   submittedTxnNUm_[reqInProcess_.designateReplicaId_]++;
   totalSubmittedTxn_++;

   rrr::FutureAttr fuattr;
   std::function<void(Future*)> cb =
       [this, currentPhase /**pass by value */](Future* fu) {
          CalvinReply rep;
          fu->get_reply() >> rep;
          this->onReply(currentPhase, rep);
       };
   fuattr.callback = cb;
   Future::safe_release(comm_
                            ->ProxyAt(reqInProcess_.designateShardId_,
                                      reqInProcess_.designateReplicaId_)
                            ->async_NormalRequest(reqInProcess_, fuattr));
}

void CalvinCoordinator::Dispatch() {
   uint32_t currentPhase = phase_;
   CalvinDispatchRequest dispatchReq;
   dispatchReq.txnType_ = reqInProcess_.txnType_;
   txnGen_->GetInquireKeys(dispatchReq.txnType_, &(reqInProcess_.ws_),
                           &(dispatchReq.input_));
   // LOG(INFO) << "DisPatching " << reqInProcess_.cmd_.reqId_;
   for (auto& sid : targetShards_) {
      rrr::FutureAttr fuattr;
      std::function<void(Future*)> cb =
          [this, currentPhase /**pass by value */](Future* fu) {
             // LOG(INFO) << "COme to reply";
             CalvinDispatchReply rep;
             fu->get_reply() >> rep;
             this->OnDispatchReply(currentPhase, rep);
          };
      fuattr.callback = cb;
      Future::safe_release(
          comm_->ProxyAt(sid, reqInProcess_.designateReplicaId_)
              ->async_DispatchRequest(dispatchReq, fuattr));
   }
}

void CalvinCoordinator::OnDispatchReply(const uint32_t phase,
                                        const CalvinDispatchReply& rep) {
   std::lock_guard<std::recursive_mutex> guard(mtx_);
   assert(phase == phase_);
   assert(stage_ == STAGE::Dispatching);
   if (!rep.result_.empty()) {
      reqInProcess_.ws_.insert(rep.result_.begin(), rep.result_.end());
   }
   dispatchShards_.insert(rep.shardId_);
   if (dispatchShards_.size() == targetShards_.size()) {
      stage_ = STAGE::Commiting;
      // LOG(INFO) << "DisPatch reply" << rep.shardId_
      //           << " my reqId = " << reqInProcess_.cmd_.reqId_;
      Launch();
   }
}

void CalvinCoordinator::Reset() {
   std::lock_guard<std::recursive_mutex> guard(mtx_);
   txnGen_ = NULL;
   // LOG(INFO) << "Before Reset requestIdByClient_=" << requestIdByClient_;
   targetShards_.clear();
   dispatchShards_.clear();
   phase_++;
}

void CalvinCoordinator::onReply(const uint32_t phase, const CalvinReply& rep) {
   std::lock_guard<std::recursive_mutex> guard(mtx_);
   if (phase != phase_) return;
   // if (true || rep.reqId_ % 1000 == 1) {
   //    LOG(INFO) << "reqId=" << rep.reqId_ << "--resSize=" <<
   //    rep.result_.size();
   // }
   ClientReply crep;
   crep.clientId_ = reqInProcess_.clientId_;
   crep.reqId_ = reqInProcess_.reqId_;
   crep.result_ = rep.result_;
   outstandingTxnNum_[rep.replicaId_]--;
   repliedTxnNum_[rep.replicaId_]++;
   if (crep.reqId_ % 10000 == 1) {
      for (uint32_t r = 0; r < replicaNum_; r++) {
         LOG(INFO) << "r=" << r << "\t" << outstandingTxnNum_[r] << "\t"
                   << submittedTxnNUm_[r] << "--" << repliedTxnNum_[r];
      }

      // for (uint32_t r = 0; r < replicaNum_; r++) {
      //    std::string line = "";
      //    for (uint32_t s = 0; s < shardNum_; s++) {
      //       line += std::to_string(maxSeqNos_[r][s].load()) + "\t";
      //    }
      //    LOG(INFO) << "r=" << r << ":\t" << line;
      // }
   }
   phase_++;
   callback_(crep);
}

CalvinCoordinator::~CalvinCoordinator() {}

std::atomic<int> CalvinCoordinator::outstandingTxnNum_[MAX_REPLICA_NUM];
std::atomic<int> CalvinCoordinator::submittedTxnNUm_[MAX_REPLICA_NUM];
std::atomic<int> CalvinCoordinator::repliedTxnNum_[MAX_REPLICA_NUM];
std::atomic<int32_t> CalvinCoordinator::totalSubmittedTxn_;
std::atomic<uint32_t> CalvinCoordinator::maxSeqNos_[MAX_REPLICA_NUM]
                                                   [MAX_SHARD_NUM];
void CalvinCoordinator::initialize() {
   for (size_t i = 0; i < MAX_REPLICA_NUM; ++i) {
      outstandingTxnNum_[i] = 0;  // Initialize each element to 0
      submittedTxnNUm_[i] = 0;
      repliedTxnNum_[i] = 0;
   }
   totalSubmittedTxn_ = 0;
   for (uint32_t r = 0; r < MAX_REPLICA_NUM; r++) {
      for (uint32_t s = 0; s < MAX_SHARD_NUM; s++) {
         maxSeqNos_[r][s] = 0;
      }
   }
}
