#pragma once
#include "StateMachine/TPCCSharding.h"
#include "TxnGenerator.h"

class TPCCTxnGenerator : public TxnGenerator {
  private:
   TPCCSharding sharding_;

  public:
   int fix_id_ = -1;
   typedef struct {
      int n_w_id_;
      int n_d_id_;
      int n_c_id_;
      int n_i_id_;
      int const_home_w_id_;
      int delivery_d_id_;
   } tpcc_para_t;
   tpcc_para_t tpcc_para_;

  public:
   TPCCTxnGenerator(const uint32_t shardNum, const uint32_t replicaNum,
                    const YAML::Node &config);
   ~TPCCTxnGenerator();
   std::string RTTI();
   // tpcc
   void GetTxnReq(ClientRequest *req, uint32_t reqId, uint32_t cid) override;
   // tpcc new_order
   void GetNewOrderTxn(ClientRequest *req);
   // tpcc payment
   void GetPaymentTxn(ClientRequest *req);
   // tpcc stock_level
   void GetStockLevelTxn(ClientRequest *req);
   // tpcc delivery
   void GetDeliveryTxn(ClientRequest *req);
   // tpcc order_status
   void GetOrderStatusTxn(ClientRequest *req);
   bool NeedDisPatch(const ClientRequest &req) override;
   void GetInquireKeys(const uint32_t txnType,
                       std::map<int32_t, mdb::Value> *existing,
                       std::map<int32_t, mdb::Value> *input) override;
};
