
#include "StateMachine/TPCCStateMachine.h"
#include "TigaService/TigaMessage.h"
#include "TxnGenerator/TPCCTxnGenerator.h"

using namespace rrr;
DEFINE_string(config, "config-local-tpcc.yml", "Config file");
DEFINE_string(serverName, "tiga-lan-proxy-0000", "serverName ");
uint32_t shardId = 0;
uint32_t replicaId = 0;
uint32_t shardNum = 3;
uint32_t replicaNum = 3;
TPCCStateMachine* tcms[6];
uint32_t txnCnt = 100000;
std::vector<LogInfo*> txnCmds;
std::vector<LogInfo*> localCmds[6];
std::thread* tdVec[6];
std::vector<std::set<uint32_t>> targetShards;
YAML::Node config;
TPCCTxnGenerator* gen;

void InitTd(uint32_t shardId) {
   tcms[shardId] =
       new TPCCStateMachine(shardId, replicaId, shardNum, replicaNum, config);
   LOG(INFO) << "Load Fine\t" << shardId;
}

void ExecuteTd(uint32_t shardId) {
   uint64_t startTime = GetMicrosecondTimestamp();
   for (auto& logInfo : localCmds[shardId]) {
      tcms[shardId]->Execute(
          logInfo->entry_->cmd_->txnType_, &(logInfo->entry_->localKeys_),
          &(logInfo->entry_->cmd_->ws_), &(logInfo->reply_->result_));
      delete logInfo->reply_;
      delete logInfo->entry_->cmd_;
      delete logInfo->entry_;
      delete logInfo;
   }
   uint64_t endTime = GetMicrosecondTimestamp();
   LOG(INFO) << shardId << "\tDuration=" << (endTime - startTime) * 1e-6;
   LOG(INFO) << shardId << "\tTest Done tp(txn/s)="
             << txnCnt / ((endTime - startTime) * 1e-6);
}

int main(int argc, char const* argv[]) {
   //    mdb::IndexedTable* t = new mdb::IndexedTable("ddd", NULL);
   config = YAML::LoadFile(FLAGS_config);
   gen = new TPCCTxnGenerator(shardNum, replicaNum, config);

   uint32_t totalShardTouch = 0;
   for (uint32_t i = 0; i < txnCnt; i++) {
      ClientRequest creq;
      gen->GetTxnReq(&creq, i, random() % 1000);
      targetShards.push_back(creq.targetShards_);
      totalShardTouch += creq.targetShards_.size();
      for (auto& sid : targetShards[i]) {
         LogInfo* info = new LogInfo();
         info->entry_ = new TigaLogEntry();
         info->entry_->cmd_ = new ClientCommand();
         info->entry_->cmd_->ws_ = creq.cmd_.ws_;
         info->entry_->cmd_->txnType_ = creq.cmd_.txnType_;
         info->reply_ = new TigaReply();
         localCmds[sid].push_back(info);
      }
      if (txnCnt % 1000 == 1) {
         LOG(INFO) << "Generating txn " << txnCnt;
      }
   }
   LOG(INFO) << "avgTouch " << (1.0 * totalShardTouch) / txnCnt;
   for (uint32_t i = 0; i < shardNum; i++) {
      tdVec[i] = new std::thread(InitTd, i);
   }
   for (uint32_t i = 0; i < shardNum; i++) {
      tdVec[i]->join();
      delete tdVec[i];
   }

   for (uint32_t i = 0; i < shardNum; i++) {
      LOG(INFO) << "sid=" << i
                << "\ttableSize=" << tcms[i]->tbl_customer_->size()
                << "--indexSize=" << tcms[i]->sharding_.g_c_last2id.size();
   }
   // for (auto& kv : tcms[0]->sharding_.g_c_last2id) {
   //    int32_t* v0 = (int*)(void*)(kv.first.c_index_smk.mb_[0].data);
   //    int32_t* v1 = (int*)(void*)(kv.first.c_index_smk.mb_[1].data);
   //    int32_t* v2 = (int*)(void*)(kv.first.c_index_smk.mb_[2].data);
   //    LOG(INFO) << "v0=" << (*v0) << "--"
   //              << "v1=" << (*v1) << "--"
   //              << "v2=" << (*v2) << "--";
   // }

   // exit(0);

   // for (uint32_t i = 0; i < txnCnt; i++) {
   //    ClientRequest creq;
   //    gen->GetTxnReq(&creq, i, random() % 1000);
   //    targetShards.push_back(creq.targetShards_);
   //    for (auto& sid : targetShards[i]) {
   //       LogInfo* info = new LogInfo();
   //       info->entry_ = new TigaLogEntry();
   //       info->entry_->cmd_ = new TigaCommand();
   //       info->entry_->cmd_->ws_ = creq.cmd_.ws_;
   //       info->entry_->cmd_->txnType_ = creq.cmd_.txnType_;
   //       info->reply_ = new TigaReply();
   //       tcms[sid]->ExecutePaymentTxn(info);
   //    }
   // }

   LOG(INFO) << "Start Execute";
   for (uint32_t i = 0; i < shardNum; i++) {
      tdVec[i] = new std::thread(ExecuteTd, i);
   }
   for (uint32_t i = 0; i < shardNum; i++) {
      tdVec[i]->join();
      delete tdVec[i];
   }

   return 0;
}
