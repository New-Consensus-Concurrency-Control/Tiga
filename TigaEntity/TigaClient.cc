#include "TigaCoordinator.h"

DEFINE_int32(threadPoolSize, 8, "The number of threads in the thread pool");
DEFINE_int32(runTimeSec, 30, "The totl run time (in seconds)");
DEFINE_string(config, "config-tpl-local.yml", "Config file");
DEFINE_string(serverName, "tiga-lan-proxy-0000", "serverName ");
DEFINE_int32(initBound, 60000, "The init estiamtion used by Tiga (us) ");
DEFINE_int32(yieldPeriodUs, 10000,
             "The yieldPeriod for Tiga to ask for Slow Replies from shards");
DEFINE_int32(cap, 400000, "The cap for Tiga to chunk its bound estimation");
DEFINE_int32(mcap, 15000, "The cap for outstanding txns");
DEFINE_int32(logPrintUnit, 10000, "Print a log for every x completed txns");
DEFINE_int32(maxReqNum, -1, "The number of max requests to send");

std::atomic<uint32_t> keyCnter_ = 1;
TxnGenerator* txnGen;
TigaCommunicator* comm_;
GlobalInfo* info_;
uint32_t clientGroupNum;
uint32_t clientVMNumber;
uint32_t coordinatorIdOffset;
ThreadPool* thrpool;
YAML::Node config;
uint32_t shardNum;
uint32_t replicaNum;
bool isOpenLoop;
uint32_t rate;
std::atomic<uint32_t> concurrentClientNum;
std::mutex coordinatorMtx;
std::vector<TigaCoordinator*> createdCoordinators;
std::vector<TigaCoordinator*> freeCoordinators;

std::mutex requestGenMtx;
std::atomic<uint32_t> nextRequestIdByClient[MAX_CLIENT_NUM_PER_VM];
std::mutex commitTimesMtxesByClient[MAX_CLIENT_NUM_PER_VM];
struct PerfSample {
   uint64_t sendTime_;
   uint64_t commitTime_;
   uint32_t txnType_;
   bool detectReplicationInconsistency_;
   bool detectNonSerial_;
   int32_t bound_;
};
std::unordered_map<uint32_t, PerfSample>
    commitTimesByClient_[MAX_CLIENT_NUM_PER_VM];
uint64_t startTime;
uint64_t endTime;
std::atomic<int32_t> outstandingRequests;

uint64_t profilingStartTime_[MAX_CLIENT_NUM_PER_VM];
uint32_t profilingCommittedTxns_[MAX_CLIENT_NUM_PER_VM];

void DispatchRequest(TigaCoordinator* coo, uint32_t clientId);
void RequestDone(TigaCoordinator* coo, const ClientReply& rep);

TigaCoordinator* FindOrCreateCoordinator() {
   TigaCoordinator* coo = NULL;
   std::lock_guard<std::mutex> lock(coordinatorMtx);
   if (freeCoordinators.size() > 0) {
      coo = freeCoordinators.back();
      freeCoordinators.pop_back();
      return coo;
   } else {
      // create
      // coordinatorId starts from 1
      coo = new TigaCoordinator(
          createdCoordinators.size() + 1 + coordinatorIdOffset, config);

      coo->SetGlobalInfo(info_);
      createdCoordinators.push_back(coo);
      return coo;
   }
}

void RequestDone(TigaCoordinator* coo, const ClientReply& rep) {
   outstandingRequests.fetch_sub(1);
   uint32_t myLocalClientId = rep.clientId_ - coordinatorIdOffset;
   uint64_t completeTime = GetMicrosecondTimestamp();
   if (profilingStartTime_[myLocalClientId] == 0) {
      profilingStartTime_[myLocalClientId] = GetMicrosecondTimestamp();
   }

   // LOG(INFO) << "DoneONe " << coo->requestIdByClient_;
   // if (rep.clientId_ == 1 && rep.reqId_ % 100 == 1) {
   if (coo->reqInProcess_.cmd_.reqId_ % FLAGS_logPrintUnit == 0) {
      uint64_t nowTime = GetMicrosecondTimestamp();
      uint64_t elapsed = nowTime - profilingStartTime_[myLocalClientId];
      if (elapsed == 0) elapsed = 1;
      profilingStartTime_[myLocalClientId] = nowTime;
      uint32_t reqCnt = commitTimesByClient_[myLocalClientId].size() -
                        profilingCommittedTxns_[myLocalClientId];
      profilingCommittedTxns_[myLocalClientId] =
          commitTimesByClient_[myLocalClientId].size();
      LOG(INFO) << "Done clientId=" << myLocalClientId
                << " reqId=" << rep.reqId_
                << "--totalReqId=" << coo->reqInProcess_.cmd_.reqId_
                << "--latency=" << completeTime - coo->reqInProcess_.sendTime_
                << "--resultSize=" << rep.result_.size()
                << "-- bound=" << coo->reqInProcess_.bound_
                << "--mysize=" << commitTimesByClient_[myLocalClientId].size()
                << "--mytp=" << reqCnt * 1000.0 * 1000 / elapsed << " txns/sec"
                << "--outstanding=" << outstandingRequests;
   }
   // if (coo->reqInProcess_.cmd_.reqId_ > 100) {
   //    exit(0);
   // }
   // if (nextRequestIdByClient[1] > nextRequestIdByClient[2] + 200 ||
   //     nextRequestIdByClient[1] + 200 < nextRequestIdByClient[2]) {
   //    LOG(INFO) << "--next1=" << nextRequestIdByClient[1]
   //              << "--next2=" << nextRequestIdByClient[2];
   //    exit(0);
   // }

   // if (rep.reqId_ == 5) {
   //    exit(0);
   // }

   // record time
   {
      std::lock_guard<std::mutex> lock(
          commitTimesMtxesByClient[myLocalClientId]);
      commitTimesByClient_[myLocalClientId][rep.reqId_] = {
          coo->sendTime_, completeTime, coo->reqInProcess_.cmd_.txnType_,
          coo->detectReplicationInconsistency_, coo->detectNonSerial_,
          coo->reqInProcess_.bound_};
      // LOG(INFO) << "clientId=" << rep.clientId_
      //           << "--size=" << commitTimesByClient_[rep.clientId_].size();
   }

   // closed-loop. continue to submit request
   bool hasMoreTime = (GetMicrosecondTimestamp() - startTime <
                       FLAGS_runTimeSec * 1000ul * 1000ul);
   if (hasMoreTime && isOpenLoop) {
      // put coo to free list
      std::lock_guard<std::mutex> lock(coordinatorMtx);
      // LOG(INFO) << "freecoo " << coo->requestIdByClient_;
      freeCoordinators.push_back(coo);
   } else if (hasMoreTime && (!isOpenLoop)) {
      DispatchRequest(coo, myLocalClientId);
   } else {
      concurrentClientNum--;
   }
}

void DispatchRequest(TigaCoordinator* coo, uint32_t clientId) {
   outstandingRequests.fetch_add(1);
   std::function<void()> task = [=]() {
      ClientRequest req;
      uint32_t reqId = nextRequestIdByClient[clientId].fetch_add(1);
      if (FLAGS_maxReqNum > 0 && reqId >= FLAGS_maxReqNum) {
         sleep(210);
         LOG(INFO) << "Sleep and Exit";
         exit(0);
      }
      {
         std::lock_guard<std::mutex> lock(requestGenMtx);
         // LOG(INFO) << "clientId=" << clientId
         //           << "--coordinatorIdOffset=" << coordinatorIdOffset;
         // LOG(INFO) << "reqClientId=" << clientId + coordinatorIdOffset;
         txnGen->GetTxnReq(&req, reqId, clientId + coordinatorIdOffset);
      }
      req.callback_ = std::bind(RequestDone, coo, std::placeholders::_1);
      coo->DoOne(req, txnGen);
   };
   // LOG(INFO) << " run async ";
   thrpool->run_async(task);
}

void ClientWorker(uint32_t clientId) {
   if (isOpenLoop) {
      double tps = 0;
      uint64_t txnCnt = 0;
      while (true) {
         if (info_->serverSignal_ != CSTATUS_RUN) {
            LOG(INFO) << "Come to suspend " << info_->serverSignal_;
            while (info_->serverSignal_ != CSTATUS_RUN) {
               usleep(10000);
            }
            LOG(INFO) << "Exit while suspend " << info_->serverSignal_;
            sleep(3);
            LOG(INFO) << "Start Resubmitting";
            // reset context
            tps = 0;
            startTime = GetMicrosecondTimestamp();
            endTime = startTime + FLAGS_runTimeSec * 1000ul * 1000ul;
            txnCnt = 0;
         }
         uint64_t elapsed = 0;
         while (tps < rate) {
            auto coo = FindOrCreateCoordinator();
            if (coo != NULL) {
               if (outstandingRequests < rate * 2 &&
                   outstandingRequests < FLAGS_mcap) {
                  DispatchRequest(coo, clientId);
               }
               txnCnt++;
               // if (txnCnt >= 100) {
               //    sleep(10);
               //    exit(0);
               // }
               elapsed = GetMicrosecondTimestamp() - startTime;
               tps = (double)(txnCnt * 1000000.0) / elapsed;
            }
         }
         // if (txnCnt % 1000 == 1) {
         //    LOG(INFO) << "txnCnt=" << txnCnt;
         // }
         // usleep(1000);
         uint64_t nowTime = GetMicrosecondTimestamp();
         elapsed = nowTime - startTime;
         tps = (double)(txnCnt * 1000000.0) / elapsed;

         if (nowTime >= endTime) {
            LOG(INFO) << "OKay, to terminate";
            break;
         }
      }

   } else {
      LOG(INFO) << "clientId=" << clientId
                << "\tconcurrentClientNum=" << concurrentClientNum;
      TigaCoordinator* coo = FindOrCreateCoordinator();
      DispatchRequest(coo, clientId);
   }
}

int main(int argc, char* argv[]) {
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   google::InitGoogleLogging(argv[0]);
   thrpool = new ThreadPool();
   LOG(INFO) << "config=" << FLAGS_config;
   config = YAML::LoadFile(FLAGS_config);
   shardNum = config["site"]["server"].size();
   replicaNum = config["site"]["server"][0].size();
   LOG(INFO) << "shardNum=" << shardNum << "\treplicaNum=" << replicaNum;
   isOpenLoop = (config["client"]["type"].as<std::string>() == "open");
   LOG(INFO) << "isOpenLoop=" << isOpenLoop;
   if (config["bench"]["workload"].as<std::string>() == "tpca") {
      txnGen = new MicroTxnGenerator(shardNum, replicaNum, config);

   } else if (config["bench"]["workload"].as<std::string>() == "tpcc") {
      txnGen = new TPCCTxnGenerator(shardNum, replicaNum, config);

   } else {
      LOG(ERROR) << "Not implemented yet "
                 << config["bench"]["workload"].as<std::string>();
      exit(0);
   }

   outstandingRequests = 0;
   concurrentClientNum = config["n_concurrent"].as<uint32_t>();
   int clientNum = concurrentClientNum;  // this variable will be constant
   rate = config["client"]["rate"].as<int>();
   LOG(INFO) << "workload=" << config["bench"]["workload"].as<std::string>()
             << "\tconcurrentClientNum=" << concurrentClientNum;

   for (uint32_t i = 0; i < MAX_CLIENT_NUM_PER_VM; i++) {
      nextRequestIdByClient[i] = 1;
      profilingStartTime_[i] = 0;
   }
   clientGroupNum = config["site"]["client"].size();
   LOG(INFO) << "clientGroupNum=" << clientGroupNum;
   int myIdx = 0;
   int coordIdx = 0;
   for (uint32_t i = 0; i < clientGroupNum; i++) {
      for (uint32_t j = 0; j < config["site"]["client"][i].size(); j++) {
         LOG(INFO) << "i=" << i << "--j=" << j << "---name="
                   << config["site"]["client"][i][j].as<std::string>()
                   << "--myIdx=" << myIdx;
         LOG(INFO) << "serverNme=" << FLAGS_serverName;

         if (config["site"]["client"][i][j].as<std::string>() ==
             FLAGS_serverName) {
            myIdx = coordIdx;
            break;
         }
         coordIdx++;
      }
   }
   LOG(INFO) << "Final MyIdx=" << myIdx;
   if (myIdx < 0) {
      LOG(ERROR) << "The clientVM is not found " << FLAGS_serverName;
      exit(0);
   }
   LOG(INFO) << "myIdx=" << myIdx;
   coordinatorIdOffset = myIdx * concurrentClientNum;
   LOG(INFO) << "coordinatorIdOffset=" << coordinatorIdOffset;
   comm_ = new TigaCommunicator(myIdx, config);
   LOG(INFO) << "Communicator idx= " << myIdx;
   comm_->Connect();
   LOG(INFO) << "Connected";
   LOG(INFO) << "CoordinatorId=" << myIdx + 1;
   info_ =
       new GlobalInfo(myIdx + 1 /**coordinatorId*/, shardNum, replicaNum,
                      FLAGS_cap, FLAGS_initBound, FLAGS_yieldPeriodUs, comm_);

   std::vector<std::thread*> workerTd;
   workerTd.reserve(clientNum);
   startTime = GetMicrosecondTimestamp();
   endTime = startTime + FLAGS_runTimeSec * 1000ul * 1000ul;
   for (int i = 1; i <= clientNum; i++) {
      workerTd.push_back(new std::thread(ClientWorker, i));
   }
   while (concurrentClientNum > 0) {
      usleep(1000);
      bool hasMoreTime =
          (GetMicrosecondTimestamp() - startTime <
           FLAGS_runTimeSec * 1000ul * 1000ul + 5 * 1000ul * 1000ul);
      if (!hasMoreTime) {
         break;
      }
   }
   LOG(INFO) << "Terminate";
   thrpool->release();
   LOG(INFO) << "ThreadPool Released";

   std::ofstream ofs(FLAGS_serverName + ".csv");
   std::vector<uint32_t> latencyStats;
   ofs << "TxnId,SendTime,CommitTime,TxnType,RepSlow,NonSerial,Bound" << std::endl;
   for (int cid = 1; cid <= clientNum; cid++) {
      for (auto& kv : commitTimesByClient_[cid]) {
         uint32_t l = kv.second.commitTime_ - kv.second.sendTime_;
         ofs << kv.first << "," << kv.second.sendTime_ << ","
             << kv.second.commitTime_ << "," << kv.second.txnType_ << ","
             << kv.second.detectReplicationInconsistency_ << ","
             << kv.second.detectNonSerial_<<","<<kv.second.bound_ << std::endl;
         latencyStats.push_back(l);
      }
   }
   sort(latencyStats.begin(), latencyStats.end());
   LOG(INFO) << "Number:\t" << latencyStats.size();
   LOG(INFO) << "50p:\t" << latencyStats[latencyStats.size() * 50 / 100];
   LOG(INFO) << "90p:\t" << latencyStats[latencyStats.size() * 90 / 100];
   LOG(INFO) << "Sleep...";
   // sleep(100);
   for (int i = 0; i < clientNum; i++) {
      workerTd[i]->join();
      LOG(INFO) << "Client " << i + 1 << " joined";
      delete workerTd[i];
      LOG(INFO) << " Client WorkerTd Freed " << i + 1;
   }
   // TODO: There should be more graceful reclaimation and exit
   exit(0);
}