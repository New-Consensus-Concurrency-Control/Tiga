#include "CalvinCommunicator.h"

CalvinCommunicator::CalvinCommunicator(const uint32_t id,
                                       const YAML::Node& config)
    : id_(id), config_(config) {
   rpcPoll_ = new PollMgr(1);
   shardNum_ = config["site"]["server"].size();
   replicaNum_ = config["site"]["server"][0].size();
}

void CalvinCommunicator::Connect() {
   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         std::string fullName =
             config_["site"]["server"][sid][rid].as<std::string>();
         std::string thisServerName = fullName.substr(0, fullName.find(':'));
         std::string portName = fullName.substr(fullName.find(':') + 1);
         std::string ip = config_["host"][thisServerName].as<std::string>();
         serverAddrs_[sid][rid] = ip + ":" + portName;
      }
   }
   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         rrr::Client* cli = new rrr::Client(rpcPoll_);
         int ret = -1;
         LOG(INFO) << "Connect to sid=" << sid << "\trid=" << rid << ":"
                   << serverAddrs_[sid][rid];
         do {
            ret = cli->connect(serverAddrs_[sid][rid].c_str());
            if (ret != 0) {
               usleep(100000);
            }
         } while (ret != 0);
         proxies_[sid][rid] = new CalvinProxy(cli);
      }
   }
   LOG(INFO) << "All Connected";
}

CalvinProxy* CalvinCommunicator::ProxyAt(const uint32_t shardId,
                                         const uint32_t replicaId) {
   return proxies_[shardId][replicaId];
}

CalvinCommunicator::~CalvinCommunicator() {}