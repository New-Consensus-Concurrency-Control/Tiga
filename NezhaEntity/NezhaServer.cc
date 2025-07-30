#include "NezhaService/NezhaServiceImpl.h"
DEFINE_string(serverName, "localhost", "The serverName");
DEFINE_int32(ioThreads, 1, "The number of IO(epoll) threads used by server");
DEFINE_int32(workerNum, 1, "The number of worker threads");
DEFINE_string(config, "config-tpl-local.yml", "Config file");
DEFINE_string(myAddr, "127.0.0.1:41237", "Bridge Addr");
using namespace NezhaRPC;

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
   LOG(INFO) << "Load File Success";

   NezhaReplica* replica = new NezhaReplica(FLAGS_serverName, config);
   LOG(INFO) << "Replica Created";

   // Replica
   NezhaServiceImpl* replicaService = new NezhaServiceImpl(replica);
   PollMgr* replicaPgr = new PollMgr(FLAGS_ioThreads);
   ThreadPool* replicaThPool = new ThreadPool(FLAGS_workerNum);
   rrr::Server* replicaSvr = new rrr::Server(replicaPgr, replicaThPool);
   replicaSvr->reg(replicaService);
   LOG(INFO) << "Replica ServerAddr = " << replica->MyServerAddr();
   replicaSvr->start(replica->MyServerAddr().c_str());

   replica->ConnectToReplicas();
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

   LOG(INFO) << "Stopping Replica Threads";
   replicaService->StopReplicaThreads();
   LOG(INFO) << "Server To Stop";

   replicaThPool->release();
   LOG(INFO) << "replicaThPool released";

   delete replicaSvr;
   LOG(INFO) << "replicaSvr deleted";
   return 0;
}
