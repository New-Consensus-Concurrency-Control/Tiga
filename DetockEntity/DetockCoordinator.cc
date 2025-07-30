#include "DetockCoordinator.h"

DetockCoordinator::DetockCoordinator(const uint32_t coordinatorId,
                                     const uint32_t shardNum,
                                     const uint32_t replicaNum,
                                     DetockCommunicator* comm,
                                     const YAML::Node& config)
    : shardNum_(shardNum),
      replicaNum_(replicaNum),
      comm_(comm),
      config_(config) {
   viewId_ = 0;
}

GlobalInfo* DetockCoordinator::gInfo_ = NULL;
void DetockCoordinator::DoOne(const ClientRequest& creq, TxnGenerator* txnGen) {
   Reset();
   std::lock_guard<std::recursive_mutex> lock(this->mtx_);
   phase_++;  // Reset, i.e., upgrade phase counter
   txnGen_ = txnGen;
   targetShards_ = creq.targetShards_;
   sendTime_ = GetMicrosecondTimestamp();
   // Store on coordinator side and use coordiantor's id and requestId
   callback_ = creq.callback_;
   reqInProcess_.clientId_ = creq.cmd_.clientId_;
   reqInProcess_.reqId_ = creq.cmd_.reqId_;
   reqInProcess_.txnType_ = creq.cmd_.txnType_;
   reqInProcess_.ws_ = creq.cmd_.ws_;
   reqInProcess_.shardIds_.clear();
   reqInProcess_.replicaIds_.clear();
   uint32_t owd = 0;
   for (auto& sid : targetShards_) {
      reqInProcess_.shardIds_.push_back(sid);
      reqInProcess_.replicaIds_.push_back(LeaderReplicaId(viewId_, sid));
      owd = std::max(owd, gInfo_->owdByShard_[sid].load());
   }
   reqInProcess_.sendTime_ = sendTime_;
   reqInProcess_.bound_ = owd;
   // LOG(INFO) << "reqId=" << reqInProcess_.reqId_ << ":--owd=" << owd;

   if (txnGen_->NeedDisPatch(creq)) {
      stage_ = STAGE::Dispatching;
      Dispatch();
   } else {
      stage_ = STAGE::Commiting;
      Launch();
   }
}

void DetockCoordinator::Dispatch() {
   uint32_t currentPhase = phase_;
   DetockDispatchRequest dispatchReq;
   dispatchReq.txnType_ = reqInProcess_.txnType_;
   txnGen_->GetInquireKeys(dispatchReq.txnType_, &(reqInProcess_.ws_),
                           &(dispatchReq.input_));
   // LOG(INFO) << "DisPatching " << reqInProcess_.cmd_.reqId_;
   for (uint32_t i = 0; i < reqInProcess_.shardIds_.size(); i++) {
      uint32_t sid = reqInProcess_.shardIds_[i];
      uint32_t rid = reqInProcess_.replicaIds_[i];
      rrr::FutureAttr fuattr;
      std::function<void(Future*)> cb =
          [this, currentPhase /**pass by value */](Future* fu) {
             // LOG(INFO) << "COme to reply";
             DetockDispatchReply rep;
             fu->get_reply() >> rep;
             this->OnDispatchReply(currentPhase, rep);
          };
      fuattr.callback = cb;
      Future::safe_release(
          comm_->ProxyAt(sid, rid)->async_DispatchRequest(dispatchReq, fuattr));
   }
}

void DetockCoordinator::Launch() {
   // LOG(INFO) << "Submit " << reqInProcess_.reqId_;
   uint32_t currentPhase = phase_;
   rrr::FutureAttr fuattr;
   std::function<void(Future*)> cb =
       [this, currentPhase /**pass by value */](Future* fu) {
          DetockReply rep;
          fu->get_reply() >> rep;
          this->onReply(currentPhase, rep);
       };
   fuattr.callback = cb;
   for (uint32_t i = 0; i < reqInProcess_.shardIds_.size(); i++) {
      // LOG(INFO) << "ReqId=" << reqInProcess_.reqId_ << "\tSend to "
      //           << reqInProcess_.shardIds_[i] << ":"
      //           << reqInProcess_.replicaIds_[i];
      Future::safe_release(comm_
                               ->ProxyAt(reqInProcess_.shardIds_[i],
                                         reqInProcess_.replicaIds_[i])
                               ->async_NormalRequest(reqInProcess_, fuattr));
   }
}

void DetockCoordinator::OnDispatchReply(const uint32_t phase,
                                        const DetockDispatchReply& rep) {
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

void DetockCoordinator::onReply(const uint32_t phase, const DetockReply& rep) {
   std::lock_guard<std::recursive_mutex> guard(mtx_);
   if (phase != phase_) return;
   // if (true || rep.reqId_ % 1000 == 1) {
   //    LOG(INFO) << "reqId=" << rep.reqId_ << "--resSize=" <<
   //    rep.result_.size();
   // }
   gInfo_->owdQu_.enqueue({rep.shardId_, rep.owd_});
   ClientReply crep;
   crep.clientId_ = reqInProcess_.clientId_;
   crep.reqId_ = reqInProcess_.reqId_;
   crep.result_ = rep.result_;
   phase_++;
   callback_(crep);
}

DetockCoordinator::~DetockCoordinator() {}

void DetockCoordinator::Reset() {
   std::lock_guard<std::recursive_mutex> guard(mtx_);
   txnGen_ = NULL;
   // LOG(INFO) << "Before Reset requestIdByClient_=" << requestIdByClient_;
   targetShards_.clear();
   dispatchShards_.clear();
   phase_++;
}

uint32_t DetockCoordinator::LeaderReplicaId(const uint32_t viewId,
                                            const uint32_t shardId) {
   return ((viewId % replicaNum_) + shardId) % replicaNum_;
}

GlobalInfo::GlobalInfo() {
   for (uint32_t i = 0; i < MAX_SHARD_NUM; i++) {
      owdByShard_[i] = 0;
      owdSampleCnt_[i] = 0;
   }
   owdTd_ = new std::thread(&GlobalInfo::OWDCalc, this);
}
void GlobalInfo::OWDCalc() {
   OWDSample sample;
   while (true) {
      while (owdQu_.try_dequeue(sample)) {
         if (sample.owd_ >= 400000) {
            // filter annomaly value
            continue;
         }
         uint32_t prevOwd = owdByShard_[sample.shardId_];
         uint32_t prevOwdCnt = owdSampleCnt_[sample.shardId_];
         // if (prevOwdCnt % 1000 == 1) {
         //    LOG(INFO) << "shardId=" << sample.shardId_
         //              << "--owd=" << sample.owd_;
         // }
         owdSampleCnt_[sample.shardId_]++;
         owdByShard_[sample.shardId_] = (prevOwd * prevOwdCnt + sample.owd_) /
                                        owdSampleCnt_[sample.shardId_];
      }
   }
}