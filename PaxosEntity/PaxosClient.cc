#include "PaxosService/PaxosServiceImpl.h"
DEFINE_string(targetAddr, "127.0.0.1:41237", "Bridge Addr");
DEFINE_int32(rate, 1000, "submission rate");
DEFINE_int32(runTime, 30, "Run time ");
DEFINE_int32(clientId, 1, "Client ID");
std::recursive_mutex mtx;
std::unordered_map<uint32_t, uint64_t> sendTimes;
std::unordered_map<uint32_t, uint64_t> completeTimes;
std::atomic<int32_t> ackNum = 0;
int main(int argc, char** argv) {
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   google::InitGoogleLogging(argv[0]);
   LOG(INFO) << "targetAddr =" << FLAGS_targetAddr;

   rrr::PollMgr* rpcPgr = new rrr::PollMgr(1);
   auto rpcClient = new rrr::Client(rpcPgr);
   int ret = -1;
   do {
      ret = rpcClient->connect(FLAGS_targetAddr.c_str());
      if (ret == 0) {
         // success
         LOG(INFO) << "Connected to " << FLAGS_targetAddr;
         break;
      } else {
         sleep(1);
      }
   } while (ret != 0);

   PaxosProxy* proxy = new PaxosProxy(rpcClient);

   uint32_t id = FLAGS_clientId;
   uint64_t startTime = GetMicrosecondTimestamp();
   uint32_t nextReqId = 1;
   uint64_t interval_us = 1000000ul / FLAGS_rate;

   while (true) {
      ClientRecord msg;
      uint32_t reqId = nextReqId;
      msg.cmdId_ = reqId;
      msg.ssidHigh_ = random();
      msg.ssidLow_ = random();
      msg.ssidNew_ = random();

      rrr::FutureAttr fuattr;
      std::function<void(Future*)> cb = [reqId](Future* fu) {
         std::lock_guard<std::recursive_mutex> lock(mtx);
         completeTimes[reqId] = GetMicrosecondTimestamp();
         ClientRecordRep ack;
         fu->get_reply() >> ack;
         // LOG(INFO) << "ack " << ack.cmdId_;
         ackNum++;
      };
      fuattr.callback = cb;
      sendTimes[msg.cmdId_] = GetMicrosecondTimestamp();
      Future::safe_release(proxy->async_RecordRequest(msg, fuattr));
      // LOG(INFO) << "Record " << msg.cmdId_;
      nextReqId++;
      if (nextReqId % 20000 == 1) {
         LOG(INFO) << "nextReqId=" << nextReqId;
      }
      usleep(interval_us);
      uint64_t nowTime = GetMicrosecondTimestamp();
      if (nowTime - startTime >= FLAGS_runTime * 1000ul * 1000ul) {
         break;
      }
   }

   while (ackNum != nextReqId - 1) {
      sleep(1);
      LOG(INFO) << "ackNum=" << ackNum << "\t" << "nextReqId=" << nextReqId;
   }
   std::vector<uint64_t> latency;
   for (uint32_t reqId = 1; reqId < nextReqId; reqId++) {
      if (sendTimes.count(reqId) > 0 && completeTimes.count(reqId) > 0) {
         uint64_t l = completeTimes[reqId] - sendTimes[reqId];
         latency.push_back(l);
      }
   }
   sort(latency.begin(), latency.end());
   LOG(INFO) << "Number " << latency.size();
   LOG(INFO) << "50p " << latency[latency.size() / 2];
   LOG(INFO) << "90p " << latency[latency.size() * 9 / 10];
   LOG(INFO) << "99p " << latency[latency.size() * 99 / 100];
}