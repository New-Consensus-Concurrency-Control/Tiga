
#include "TigaService/TigaGlobalServiceImpl.h"
#include "TigaService/TigaLocalServiceImpl.h"
#include "TigaService/TigaServiceImpl.h"
#include "TigaService/TigaViewChangeServiceImpl.h"

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
   // uint32_t shardNum = config["site"]["server"].size();
   // uint32_t replicaNum = config["site"]["server"][0].size();

   TigaReplica* replica = new TigaReplica(FLAGS_serverName, config);

   // Handle client requests
   PollMgr* poll = new PollMgr(FLAGS_ioThreads);
   ThreadPool* thrpool = new ThreadPool(FLAGS_workerNum);
   rrr::Server* svr = new rrr::Server(poll, thrpool);
   TigaServiceImpl* svc = new TigaServiceImpl(replica);
   svr->reg(svc);
   LOG(INFO) << "Start Service serverAddr=" << replica->MyServerAddr();
   svr->start(replica->MyServerAddr().c_str());

   //////////////////////////////
   // Handle intra-DC messages
   PollMgr* lpoll = new PollMgr(FLAGS_ioThreads);
   ThreadPool* lthrpool = new ThreadPool(FLAGS_workerNum);
   rrr::Server* lsvr = new rrr::Server(lpoll, lthrpool);
   TigaLocalServiceImpl* lsvc = new TigaLocalServiceImpl(replica);
   lsvr->reg(lsvc);
   lsvr->start(replica->MyLocalServerAddr().c_str());

   //////////////////////////////
   // Handle inter-DC messages
   PollMgr* gpoll = new PollMgr(FLAGS_ioThreads);
   ThreadPool* gthrpool = new ThreadPool(FLAGS_workerNum);
   rrr::Server* gsvr = new rrr::Server(gpoll, gthrpool);
   TigaGlobalServiceImpl* gsvc = new TigaGlobalServiceImpl(replica);
   gsvr->reg(gsvc);
   gsvr->start(replica->MyGlobalServerAddr().c_str());

   // Handle ViewChange-related messages
   PollMgr* vrpoll = new PollMgr(FLAGS_ioThreads);
   ThreadPool* vrthrpool = new ThreadPool(FLAGS_workerNum);
   rrr::Server* vrsvr = new rrr::Server(vrpoll, vrthrpool);
   TigaViewChangeServiceImpl* vrsvc = new TigaViewChangeServiceImpl(replica);
   vrsvr->reg(vrsvc);
   vrsvr->start(replica->MyVRServerAddr().c_str());

   LOG(INFO) << "Replica Connect";
   replica->Connect();

   LOG(INFO) << "Run Replias";
   replica->Run();

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

   svc->StopReplicaProcessingThreads();
   LOG(INFO) << "Server To Stop";
   poll->release();
   LOG(INFO) << "poll released";
   lpoll->release();
   LOG(INFO) << "lpoll released";
   gpoll->release();
   LOG(INFO) << "gpoll released";
   vrpoll->release();
   LOG(INFO) << "vrpoll released";

   thrpool->release();
   LOG(INFO) << "thrpool released";
   lthrpool->release();
   LOG(INFO) << "lthrpool released";
   gthrpool->release();
   LOG(INFO) << "gthrpool released";
   vrthrpool->release();
   LOG(INFO) << "vrthrpool released";

   delete lsvr;
   LOG(INFO) << "lsvr deleted";
   delete gsvr;
   LOG(INFO) << "gsvr deleted";
   delete vrsvr;
   LOG(INFO) << "vrsvr deleted";
   delete svr;
   LOG(INFO) << "svr deleted";
   return 0;
}
