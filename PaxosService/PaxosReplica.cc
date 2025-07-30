#include "PaxosReplica.h"

PaxosReplica::PaxosReplica(const std::string& serverName,
                           const YAML::Node& config, const uint32_t shardId,
                           const uint32_t replicaId, const uint32_t shardNum,
                           const uint32_t replicaNum)
    : config_(config),
      shardId_(shardId),
      replicaId_(replicaId),
      shardNum_(shardNum),
      replicaNum_(replicaNum) {
   // Get my IDs <shardId, replicaId>
   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         std::string fullName =
             config["site"]["replica"][sid][rid].as<std::string>();
         std::string thisServerName = fullName.substr(0, fullName.find(':'));
         std::string portName = fullName.substr(fullName.find(':') + 1);
         std::string ip = config["host"][thisServerName].as<std::string>();
         int port = std::stoi(portName);
         serverAddrs_[sid][rid] = ip + ":" + std::to_string(port + 5000);
      }
   }
   replicaRPCPoll_ = new PollMgr(2);

   viewId_ = 0;
   toCommitPoint_ = 0;
   committedLogId_ = 0;
   nextLogId_ = 1;
   logList_.reserve(1000000ul);
   logList_.push_back(NULL);
}
PaxosReplica::~PaxosReplica() {}
void PaxosReplica::ConnectToReplicas() {
   for (uint32_t rid = 0; rid < replicaNum_; rid++) {
      int ret = -1;
      replicaRPCClients_[rid] = new rrr::Client(replicaRPCPoll_);
      do {

         LOG(INFO) << "Replica Connecting to sid=" << shardId_
                   << "\t rid=" << rid << "\t"
                   << "ip=" << serverAddrs_[shardId_][rid];
         ret = replicaRPCClients_[rid]->connect(
             serverAddrs_[shardId_][rid].c_str());
         if (ret == 0) {
            // success
            replicaProxies_[rid] = new PaxosProxy(replicaRPCClients_[rid]);
            LOG(INFO) << "Replica Connected to sid=" << shardId_
                      << "\t rid=" << rid << "\t"
                      << serverAddrs_[shardId_][rid];
         } else {
            ThreadSleepFor(1200000);
         }
      } while (ret != 0);
   }
}
void PaxosReplica::Run() {
   status_ = STATUS_NORMAL;
   mainTd_ = new std::thread(&PaxosReplica::MainTd, this);
}
void PaxosReplica::Stop() {
   status_ = SERVER_STATUS::STATUS_TERMINATE;
   LOG(INFO) << "Terminating...";
   mainTd_->join();
   LOG(INFO) << "MainTd Terminated...";
   delete mainTd_;

   LOG(INFO) << "Close Connections";
   for (uint32_t i = 0; i < replicaNum_; i++) {
      if (replicaRPCClients_[i]) {
         replicaRPCClients_[i]->close_and_release();
         delete replicaProxies_[i];
      }
   }
   LOG(INFO) << "PaxosRPC closed";
}
void PaxosReplica::onClientRecord(const ClientRecord& req, ClientRecordRep* rep,
                                  const std::function<void()>& cb) {
   if (!AmLeader()) {
      LOG(ERROR) << "Non Leader should not "
                    "handle client record";
      return;
   }
   EntryInfo info;
   info.cb_ = [rep, cb](const ClientRecordRep& r) {
      *rep = r;
      cb();
   };
   info.msg = new PaxosAppend();
   info.msg->record_ = req;
   info.msg->replicaId_ = replicaId_;
   info.msg->shardId_ = shardId_;
   info.msg->viewId_ = viewId_;
   entryQu_.enqueue(info);
}
void PaxosReplica::onPaxosAppend(const PaxosAppend& req) {
   if (!CheckView(req.viewId_)) {
      LOG(ERROR) << "View mismatch";
      return;
   }
   std::lock_guard<std::mutex> lock(recordMtx_);
   // LOG(INFO) << "Append " << req.logId_;
   // if (req.logId_ % 10000 == 1) {
   //    LOG(INFO) << "Append " << req.logId_;
   // }
   if (logList_.size() == req.logId_) {
      PaxosAppend* e = new PaxosAppend(req);
      logList_.push_back(e);
      // Respond to leader
      PaxosAppendRep rep;
      rep.logId_ = e->logId_;
      rep.replicaId_ = replicaId_;
      rep.shardId_ = shardId_;
      rep.viewId_ = viewId_;
      replicaProxies_[viewId_ % replicaNum_]->async_PaxosAppendResponse(rep);
   } else if (logList_.size() < req.logId_) {
      pendingLogs_[req.logId_] = new PaxosAppend(req);
   }
   // Process pending
   while (!pendingLogs_.empty()) {
      PaxosAppend* e = pendingLogs_.begin()->second;
      if (logList_.size() == e->logId_) {
         logList_.push_back(e);
         // Respond to leader
         PaxosAppendRep rep;
         rep.logId_ = e->logId_;
         rep.replicaId_ = replicaId_;
         rep.shardId_ = shardId_;
         rep.viewId_ = viewId_;
         replicaProxies_[viewId_ % replicaNum_]->async_PaxosAppendResponse(rep);
      } else if (logList_.size() < e->logId_) {
         break;
      }
   }
   committedLogId_ =
       std::min(toCommitPoint_.load(), (uint32_t)logList_.size() - 1);
}
void PaxosReplica::onPaxosAppendRep(const PaxosAppendRep& rep) {
   if (!CheckView(rep.viewId_)) {
      LOG(ERROR) << "View mismatch";
      return;
   }
   qcQu_.enqueue(rep);
}
void PaxosReplica::onPaxosCommitReq(const PaxosCommitReq& req) {
   if (!CheckView(req.viewId_)) {
      LOG(ERROR) << "View mismatch";
      return;
   }
   toCommitPoint_ = std::max(toCommitPoint_.load(), req.logId_);
   committedLogId_ =
       std::min(toCommitPoint_.load(), (uint32_t)logList_.size() - 1);
}

void PaxosReplica::MainTd() {
   std::string name;
   while (true) {
      if (status_ == STATUS_NORMAL) {
         if (threadMap_.empty()) {
            std::string name = "RequestMulticastTd";
            LOG(INFO) << "Run " << name;
            threadMap_[name] =
                new std::thread(&PaxosReplica::RequestMulticastTd, this);

            name = "QuorumCheckTd";
            LOG(INFO) << "Run " << name;
            threadMap_[name] =
                new std::thread(&PaxosReplica::QuorumCheckTd, this);

            name = "ReplyTd";
            LOG(INFO) << "Run " << name;
            threadMap_[name] = new std::thread(&PaxosReplica::ReplyTd, this);
         }
      } else if (status_ == STATUS_TERMINATE) {
         break;
      }

      ThreadSleepFor(10000);
   }

   LOG(INFO) << "Exit Main";
}
void PaxosReplica::RequestMulticastTd() {
   if (!AmLeader()) {
      return;
   }
   EntryInfo info;
   while (status_ == STATUS_NORMAL) {
      while (entryQu_.try_dequeue(info)) {
         info.msg->logId_ = nextLogId_.fetch_add(1);
         replyQu_.enqueue(info);
         for (uint32_t r = 0; r < replicaNum_; r++) {
            replicaProxies_[r]->async_PaxosAppendRequest(*(info.msg));
         }
      }
      ThreadSleepFor(1000);
   }
}
void PaxosReplica::QuorumCheckTd() {
   if (!AmLeader()) {
      return;
   }
   EntryInfo info;
   PaxosAppendRep rep;
   while (status_ == STATUS_NORMAL) {
      while (qcQu_.try_dequeue(rep)) {
         quorum_[rep.logId_].insert(rep.replicaId_);
         if (quorum_[rep.logId_].size() >= replicaNum_ / 2 + 1) {
            committedLogId_ = std::max(committedLogId_.load(), rep.logId_);
            while (!quorum_.empty()) {
               if (quorum_.begin()->first <= committedLogId_) {
                  quorum_.erase(quorum_.begin());
               } else {
                  break;
               }
            }
            // Broadcast to followers
            for (uint32_t r = 0; r < replicaNum_; r++) {
               if (r != replicaId_) {
                  PaxosCommitReq cmt;
                  cmt.viewId_ = viewId_;
                  cmt.logId_ = rep.logId_;
                  replicaProxies_[r]->async_PaxosCommitRequest(cmt);
               }
            }
         }
      }
   }
}

void PaxosReplica::ReplyTd() {
   if (!AmLeader()) {
      return;
   }
   EntryInfo info;
   while (status_ == STATUS_NORMAL) {
      while (replyQu_.try_dequeue(info)) {
         clientReplyCallBacks_[info.msg->logId_] = info;
      }
      while (!clientReplyCallBacks_.empty()) {
         if (clientReplyCallBacks_.begin()->first <= committedLogId_) {
            info = clientReplyCallBacks_.begin()->second;
            ClientRecordRep rep;
            rep.cmdId_ = info.msg->record_.cmdId_;
            info.cb_(rep);
            clientReplyCallBacks_.erase(clientReplyCallBacks_.begin());
         } else {
            break;
         }
      }
   }
}

bool PaxosReplica::AmLeader() { return (viewId_ % replicaNum_ == replicaId_); }

bool PaxosReplica::CheckView(uint32_t viewId) { return viewId_ == viewId; }

std::string PaxosReplica::ServerAddrs(const uint32_t shardId,
                                      const uint32_t replicaId) {
   return serverAddrs_[shardId][replicaId];
}

std::string PaxosReplica::MyServerAddr() {
   return serverAddrs_[shardId_][replicaId_];
}
