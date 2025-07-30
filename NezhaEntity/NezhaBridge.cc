#include "Common.h"
#include "NezhaService/NezhaBridgeServiceImpl.h"
#include "NezhaService/NezhaService.h"
DEFINE_string(serverName, "localhost", "The serverName");
DEFINE_int32(ioThreads, 1, "The number of IO(epoll) threads used by server");
DEFINE_int32(workerNum, 1, "The number of worker threads");
DEFINE_string(config, "config-tpl-local.yml", "Config file");
DEFINE_string(myAddr, "127.0.0.1:41237", "Bridge Addr");
DEFINE_int32(reqNum, 1000, "Number of Request");
DEFINE_int32(shardId, 1, "Shard ID");
DEFINE_int32(clientId, 1, "Client ID");
#define WINDOW_LEN (1000)
using namespace nezha;
using namespace NezhaRPC;
YAML::Node config_;
uint32_t replicaNum_;
std::string serverAddrs_[MAX_REPLICA_NUM];
rrr::PollMgr* rpcPgrs_[MAX_REPLICA_NUM];
rrr::Client* rpcClients_[MAX_REPLICA_NUM];
NezhaProxy* nezhaProxies_[MAX_REPLICA_NUM];
std::atomic<uint32_t> owds_[MAX_REPLICA_NUM];
std::set<uint32_t> owdSampleVec_[MAX_REPLICA_NUM];
std::atomic<uint32_t> replicaLatestSyncPoints_[MAX_REPLICA_NUM];
ConcurrentQueue<BridgeStruct> bridgeQu_;
std::map<std::string, std::thread*> threadMap_;
std::unordered_set<uint32_t> committedReqIds_;
struct AckStruct {
   uint32_t reqId_;
   std::function<void(const NezhaAck&)> replyHandler_;
};
ConcurrentQueue<NezhaFastReply> replyQu_;
ConcurrentQueue<AckStruct> ackQu_;
ConcurrentQueue<uint32_t> commitQu_;
std::unordered_map<uint32_t, std::function<void(const NezhaAck&)>> ackMap_;
struct OWDSample {
   uint32_t replicaId_;
   uint32_t owd_;
};
ConcurrentQueue<OWDSample> owdQu_;

uint32_t viewId_ = 0;  // For simplicity, do not consider replica failure
struct ReplicaQuorum {
   NezhaHash hashes_[MAX_REPLICA_NUM];
   uint32_t fastReplies[MAX_REPLICA_NUM];
   uint32_t leaderLogId_;
   ReplicaQuorum() {
      memset(fastReplies, '\0', sizeof(fastReplies));
      leaderLogId_ = 0;
   }
};
std::unordered_map<uint32_t, ReplicaQuorum> replicaQuorum_;
std::map<uint32_t, uint32_t> incompleteReplicatedTxns_;

bool running_ = true;
pthread_mutex_t gStopMtx_;
pthread_cond_t gStopCond_;
std::atomic<uint32_t> reqIdNo_ = 1;

static void signalHandler(int sig) {
   Log_info("caught signal %d, stopping server now", sig);
   running_ = false;
   Pthread_mutex_lock(&gStopMtx_);
   Pthread_cond_signal(&gStopCond_);
   Pthread_mutex_unlock(&gStopMtx_);
}

void Initialize() {
   // InitializeServerAddrs
   for (uint32_t rid = 0; rid < replicaNum_; rid++) {
      std::string fullName =
          config_["site"]["replica"][FLAGS_shardId][rid].as<std::string>();
      LOG(INFO) << "fullName=" << fullName;
      std::string thisServerName = fullName.substr(0, fullName.find(':'));
      std::string portName = fullName.substr(fullName.find(':') + 1);
      std::string ip = config_["host"][thisServerName].as<std::string>();
      int port = std::stoi(portName);
      serverAddrs_[rid] = ip + ":" + std::to_string(port);
   }
   // Initialize OWDs
   for (uint32_t rid = 0; rid < replicaNum_; rid++) {
      uint32_t initialOWD = config_["server_initial_bound"][rid].as<uint32_t>();
      owds_[rid] = initialOWD;
   }
}

void ConnectToReplicas() {
   for (uint32_t rid = 0; rid < replicaNum_; rid++) {
      rpcPgrs_[rid] = new rrr::PollMgr(1);
      rpcClients_[rid] = new rrr::Client(rpcPgrs_[rid]);
   }
   // Connect to all replicas
   for (uint32_t rid = 0; rid < replicaNum_; rid++) {
      int ret = -1;
      LOG(INFO) << "Try rid= " << rid;
      do {
         LOG(INFO) << "Connecting to sid=" << "\t rid=" << rid << "\t"
                   << serverAddrs_[rid];

         ret = rpcClients_[rid]->connect(serverAddrs_[rid].c_str());
         if (ret == 0) {
            // success
            nezhaProxies_[rid] = new NezhaProxy(rpcClients_[rid]);
            LOG(INFO) << "Connected to" << "\trid=" << rid << "\t"
                      << serverAddrs_[rid];
         } else {
            ThreadSleepFor(1200000);
         }
      } while (ret != 0);
   }
}

void RequestMulticastTd() {
   BridgeStruct e;
   while (running_) {
      if (!bridgeQu_.try_dequeue(e)) {
         continue;
      }
      NezhaRequest req;
      req.clientId_ = FLAGS_clientId;
      req.reqId_ = reqIdNo_.fetch_add(1);
      req.cmd_ = e.msg_;
      req.sendTime_ = GetMicrosecondTimestamp();
      req.bound_ = 0;
      ackQu_.enqueue({req.reqId_, e.replyHandler_});
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         req.bound_ = std::max(owds_[rid].load(), req.bound_);
      }
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         // LOG(INFO) << "Replicate Requests rid=" << rid;
         rrr::FutureAttr fuattr;
         std::function<void(Future*)> cb = [](Future* fu) {
            NezhaFastReply rep;
            fu->get_reply() >> rep;
            replyQu_.enqueue(rep);
         };
         fuattr.callback = cb;
         Future::safe_release(
             nezhaProxies_[rid]->async_NezhaReplicateRequest(req, fuattr));
      }
   }
}

bool QuorumOkay(uint32_t reqId, uint32_t debug = false) {
   ReplicaQuorum& rq = replicaQuorum_[reqId];
   uint32_t leaderReplicaId = viewId_ % replicaNum_;
   if (rq.fastReplies[leaderReplicaId] == 0) {
      if (debug > 0) {
         LOG(INFO) << "Debug-" << debug << "\t" << reqId << " no leader Reply";
      }
      return false;
   }
   uint32_t validFastReplyNum = 0;
   uint32_t validSlowReplyNum = 0;
   for (uint32_t rid = 0; rid < replicaNum_; rid++) {
      if ((rq.fastReplies[rid] == 1 &&
           rq.hashes_[rid].Equal(rq.hashes_[leaderReplicaId])) ||
          replicaLatestSyncPoints_[rid] >= rq.leaderLogId_) {
         if (debug) {
            LOG(INFO) << "Debug-" << debug << "\t" << "rid=" << rid << "\t"
                      << "syncedLogId " << replicaLatestSyncPoints_[rid] << "\t"
                      << "leaderLogId=" << rq.leaderLogId_;
         }

         validFastReplyNum++;
      }
      if (replicaLatestSyncPoints_[rid] >= rq.leaderLogId_) {
         validSlowReplyNum++;
      }
   }
   uint32_t f = replicaNum_ / 2;
   uint32_t fastQuorum = f + (f + 1) / 2 + 1;
   uint32_t slowQuorum = f + 1;
   if (debug) {
      LOG(INFO) << "Debug-" << debug << "\t"
                << "validFastReplyNum=" << validFastReplyNum
                << "\t validSlowReplyNum=" << validSlowReplyNum;
   }

   if (validFastReplyNum >= fastQuorum || validSlowReplyNum + 1 >= slowQuorum) {
      if (debug) {
         LOG(INFO) << "Debug-" << debug << "\t" << " Return True " << reqId;
      }
      return true;
   } else {
      if (debug) {
         LOG(INFO) << "Debug-" << debug << "\t" << " Return False " << reqId;
      }
      return false;
   }
}

void CheckInCompleteTxns() {
   while (!incompleteReplicatedTxns_.empty()) {
      uint32_t reqId = incompleteReplicatedTxns_.begin()->second;
      if (QuorumOkay(reqId)) {
         incompleteReplicatedTxns_.erase(incompleteReplicatedTxns_.begin());
         committedReqIds_.erase(reqId);
      } else {
         break;
      }
   }
}

void QuorumCheckTd() {
   NezhaFastReply rep;

   while (running_) {
      committedReqIds_.clear();
      while (replyQu_.try_dequeue(rep)) {
         uint32_t reqId = rep.reqId_;
         uint32_t replicaId = rep.replicaId_;
         if (rep.owd_ > 0) {
            owdQu_.enqueue({rep.replicaId_, rep.owd_});
         }

         replicaQuorum_[reqId].fastReplies[replicaId] = 1;
         replicaQuorum_[reqId].hashes_[replicaId] = rep.hash_;
         if (rep.replicaId_ == viewId_ % replicaNum_) {
            // // // Leader
            replicaQuorum_[reqId].leaderLogId_ = rep.logId_;
            incompleteReplicatedTxns_[rep.logId_] = reqId;
         }

         if (QuorumOkay(reqId)) {
            uint32_t leaderLogId = replicaQuorum_[reqId].leaderLogId_;
            replicaQuorum_.erase(reqId);
            incompleteReplicatedTxns_.erase(leaderLogId);
            commitQu_.enqueue(reqId);
         }
      }

      CheckInCompleteTxns();
   }
}

void SendAckTd() {
   AckStruct e;
   uint32_t commitReqId;
   std::vector<uint32_t> committedReqIdVec;
   NezhaAck dummyAck;
   while (running_) {
      committedReqIdVec.clear();
      while (commitQu_.try_dequeue(commitReqId)) {
         committedReqIdVec.push_back(commitReqId);
      }
      while (ackQu_.try_dequeue(e)) {
         ackMap_[e.reqId_] = e.replyHandler_;
      }
      for (auto& reqId : committedReqIdVec) {
         auto iter = ackMap_.find(reqId);
         assert(iter != ackMap_.end());
         dummyAck.token_ = reqId;
         (iter->second)(dummyAck);
         ackMap_.erase(iter);
      }
   }
}

void DaemonWorkTd() {
   OWDSample owdSample;
   uint64_t lastInquireTime = 0;
   while (running_) {
      while (owdQu_.try_dequeue(owdSample)) {
         owdSampleVec_[owdSample.replicaId_].insert(owdSample.owd_);
         if (owdSampleVec_[owdSample.replicaId_].size() == WINDOW_LEN) {
            auto begin = owdSampleVec_[owdSample.replicaId_].begin();
            std::advance(begin, WINDOW_LEN / 2);
            owds_[owdSample.replicaId_] = *begin;
            owdSampleVec_[owdSample.replicaId_].clear();
         }
      }
      uint64_t nowTime = GetMicrosecondTimestamp();
      if (nowTime - lastInquireTime >= 10ul * 1000ul * 1000ul) {
         // every 10ms
         for (uint32_t rid = 0; rid < replicaNum_; rid++) {
            NezhaInquireRequest req;
            req.replicaId_ = rid;
            req.shardId_ = FLAGS_shardId;
            req.viewId_ = viewId_;
            rrr::FutureAttr fuattr;
            std::function<void(Future*)> cb = [](Future* fu) {
               NezhaInquireReply rep;
               fu->get_reply() >> rep;
               if (replicaLatestSyncPoints_[rep.replicaId_] <
                   rep.latestSyncedLogId_) {
                  replicaLatestSyncPoints_[rep.replicaId_] =
                      rep.latestSyncedLogId_;
               }
            };
            fuattr.callback = cb;
            Future::safe_release(
                nezhaProxies_[rid]->async_NezhaInquire(req, fuattr));
         }
      }
   }
}

int main(int argc, char** argv) {
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   google::InitGoogleLogging(argv[0]);
   LOG(INFO) << "myAddr =" << FLAGS_myAddr;
   config_ = YAML::LoadFile(FLAGS_config);
   LOG(INFO) << "Load File Success";
   replicaNum_ = config_["site"]["replica"][0].size();
   Initialize();
   ConnectToReplicas();

   NezhaBridgeServiceImpl* svc = new NezhaBridgeServiceImpl(&bridgeQu_);
   LOG(INFO) << "BridgeService Creeated";
   PollMgr* pgr = new PollMgr(FLAGS_ioThreads);
   ThreadPool* thpool = new ThreadPool(FLAGS_workerNum);
   rrr::Server* svr = new rrr::Server(pgr, thpool);
   svr->reg(svc);
   svr->start(FLAGS_myAddr.c_str());
   threadMap_["RequestMulticastTd"] = new std::thread(RequestMulticastTd);
   threadMap_["QuorumCheckTd"] = new std::thread(QuorumCheckTd);
   threadMap_["SendAckTd"] = new std::thread(SendAckTd);
   threadMap_["DaemonWorkTd"] = new std::thread(DaemonWorkTd);

   Pthread_mutex_init(&gStopMtx_, nullptr);
   Pthread_cond_init(&gStopCond_, nullptr);
   signal(SIGPIPE, SIG_IGN);
   signal(SIGHUP, SIG_IGN);
   signal(SIGCHLD, SIG_IGN);
   signal(SIGALRM, signalHandler);
   signal(SIGINT, signalHandler);
   signal(SIGQUIT, signalHandler);
   signal(SIGTERM, signalHandler);
   Pthread_mutex_lock(&gStopMtx_);
   while (running_) {
      Pthread_cond_wait(&gStopCond_, &gStopMtx_);
   }
   Pthread_mutex_unlock(&gStopMtx_);
   LOG(INFO) << "thpool releasing";
   thpool->release();
   LOG(INFO) << "thpool released";
   delete svr;
   LOG(INFO) << "svr deleted";
}