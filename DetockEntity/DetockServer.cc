#include "DetockService/DetockLogManagerServiceImpl.h"
#include "DetockService/DetockSequencerServiceImpl.h"
#include "DetockService/DetockServiceImpl.h"

DEFINE_string(serverName, "localhost", "The serverName");
DEFINE_int32(ioThreads, 1, "The number of IO(epoll) threads used by server");
DEFINE_int32(workerNum, 1, "The number of worker threads");
DEFINE_string(config, "config-tpl-local.yml", "Config file");

bool should_stop = false;
pthread_mutex_t g_stop_mutex;
pthread_cond_t g_stop_cond;

static void signal_handler(int sig) {
   Log_info("caught signal %d, stopping server now", sig);
   should_stop = true;
   Pthread_mutex_lock(&g_stop_mutex);
   Pthread_cond_signal(&g_stop_cond);
   Pthread_mutex_unlock(&g_stop_mutex);
}

int main(int argc, char** argv) {
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   google::InitGoogleLogging(argv[0]);
   LOG(INFO) << "Start";

   YAML::Node config = YAML::LoadFile(FLAGS_config);
   uint32_t shardId, replicaId, shardNum, replicaNum;
   shardNum = config["site"]["server"].size();
   replicaNum = config["site"]["server"][0].size();
   shardId = replicaId = UINT32_MAX;
   std::string myServerAddr, mySequencerAddr, myLogManagerAddr;
   for (uint32_t sid = 0; sid < shardNum; sid++) {
      for (uint32_t rid = 0; rid < replicaNum; rid++) {
         std::string fullName =
             config["site"]["server"][sid][rid].as<std::string>();
         std::string thisServerName = fullName.substr(0, fullName.find(':'));
         std::string portName = fullName.substr(fullName.find(':') + 1);
         std::string ip = config["host"][thisServerName].as<std::string>();
         int port = std::stoi(portName);

         if (thisServerName == FLAGS_serverName) {
            shardId = sid;
            replicaId = rid;
            myServerAddr = ip + ":" + std::to_string(port);
            mySequencerAddr = ip + ":" + std::to_string(port + 1);
            myLogManagerAddr = ip + ":" + std::to_string(port + 2);
            break;
         }
      }
   }

   std::string workloadStr = config["bench"]["workload"].as<std::string>();
   StateMachine* stateMachine = NULL;
   if (workloadStr == "tpca") {
      stateMachine = new MicroStateMachine(shardId, replicaId, shardNum,
                                           replicaNum, config);
   } else if (workloadStr == "tpcc") {
      stateMachine = new TPCCStateMachine(shardId, replicaId, shardNum,
                                          replicaNum, config);
   } else {
      LOG(ERROR) << workloadStr << "--not implemented yet";
      assert(0);
   }

   DetockLogManager* logMgr =
       new DetockLogManager(FLAGS_serverName, stateMachine);

   ConcurrentQueue<DetockLocalLogSync>* logSyncQu =
       new ConcurrentQueue<DetockLocalLogSync>();

   DetockExecutor* exec =
       new DetockExecutor(FLAGS_serverName, stateMachine, logMgr, logSyncQu);

   // Handle client requests
   PollMgr* poll = new PollMgr(FLAGS_ioThreads);
   ThreadPool* thrpool = new ThreadPool(FLAGS_workerNum);
   rrr::Server* svr = new rrr::Server(poll, thrpool);
   DetockServiceImpl* svc = new DetockServiceImpl(exec);
   svr->reg(svc);
   LOG(INFO) << "Start Service serverAddr=" << myServerAddr;
   svr->start(myServerAddr.c_str());

   //////////////////////////////
   // Handle Sequencer messages
   PollMgr* seqPoll = new PollMgr(FLAGS_ioThreads);
   ThreadPool* seqThrpool = new ThreadPool(FLAGS_workerNum);
   rrr::Server* seqSvr = new rrr::Server(seqPoll, seqThrpool);
   DetockSequencerServiceImpl* seqSvc =
       new DetockSequencerServiceImpl(logSyncQu);
   seqSvr->reg(seqSvc);
   seqSvr->start(mySequencerAddr.c_str());

   //////////////////////////////
   // Handle LogManager messages
   PollMgr* logMgrPoll = new PollMgr(FLAGS_ioThreads);
   ThreadPool* logMgrThrpool = new ThreadPool(FLAGS_workerNum);
   rrr::Server* logMgrSvr = new rrr::Server(logMgrPoll, logMgrThrpool);
   DetockLogManagerServiceImpl* logMgrSvc =
       new DetockLogManagerServiceImpl(logMgr);
   logMgrSvr->reg(logMgrSvc);
   logMgrSvr->start(myLogManagerAddr.c_str());

   sleep(5);
   // Connect
   LOG(INFO) << "Start Connect LogManager";
   logMgr->ConnectOtherLogManager();

   LOG(INFO) << "Run Sequencer";
   exec->ConnectToSequencers();

   exec->Run();

   Pthread_mutex_init(&g_stop_mutex, nullptr);
   Pthread_cond_init(&g_stop_cond, nullptr);

   signal(SIGPIPE, SIG_IGN);
   signal(SIGHUP, SIG_IGN);
   signal(SIGCHLD, SIG_IGN);

   signal(SIGALRM, signal_handler);
   signal(SIGINT, signal_handler);
   signal(SIGQUIT, signal_handler);
   signal(SIGTERM, signal_handler);

   Pthread_mutex_lock(&g_stop_mutex);
   while (should_stop == false) {
      Pthread_cond_wait(&g_stop_cond, &g_stop_mutex);
   }
   Pthread_mutex_unlock(&g_stop_mutex);

   exec->Run();

   LOG(INFO) << "Server To Stop";
   poll->release();
   LOG(INFO) << "poll released";
   seqPoll->release();
   LOG(INFO) << "seqPoll released";
   logMgrPoll->release();
   LOG(INFO) << "logMgrPoll released";

   thrpool->release();
   LOG(INFO) << "thrpool released";
   seqThrpool->release();
   LOG(INFO) << "seqThrpool released";
   logMgrThrpool->release();
   LOG(INFO) << "logMgrThrpool released";

   delete seqSvr;
   LOG(INFO) << "seqSvr deleted";
   delete logMgrSvr;
   LOG(INFO) << "logMgrSvr deleted";
   delete svr;
   LOG(INFO) << "svr deleted";
   return 0;
}
