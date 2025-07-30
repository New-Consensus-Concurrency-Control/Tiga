#include "TPCCTxnGenerator.h"

TPCCTxnGenerator::TPCCTxnGenerator(const uint32_t shardNum,
                                   const uint32_t replicaNum,
                                   const YAML::Node &config)
    : TxnGenerator(shardNum, replicaNum, config),
      sharding_(0, 0, shardNum, replicaNum, config, false) {

   std::map<std::string, uint64_t> table_num_rows;
   sharding_.get_number_rows(table_num_rows);

   std::vector<unsigned int> partitions;
   sharding_.GetTablePartitions(TPCC_TB_WAREHOUSE, partitions);

   uint64_t tb_w_rows = table_num_rows[std::string(TPCC_TB_WAREHOUSE)];
   tpcc_para_.n_w_id_ = (int)tb_w_rows * partitions.size();

   tpcc_para_.const_home_w_id_ =
       RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1);
   uint64_t tb_d_rows = table_num_rows[std::string(TPCC_TB_DISTRICT)];
   tpcc_para_.n_d_id_ = (int)tb_d_rows;
   uint64_t tb_c_rows = table_num_rows[std::string(TPCC_TB_CUSTOMER)];
   tpcc_para_.n_c_id_ = (int)tb_c_rows / tb_d_rows;
   tpcc_para_.n_i_id_ = (int)table_num_rows[std::string(TPCC_TB_ITEM)];
   tpcc_para_.delivery_d_id_ = RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1);
}

TPCCTxnGenerator::~TPCCTxnGenerator() {}

std::string TPCCTxnGenerator::RTTI() { return "TPCCTxnGenerator"; }

void TPCCTxnGenerator::GetTxnReq(ClientRequest *req, uint32_t reqId,
                                 uint32_t cid) {
   req->cmd_.reqId_ = reqId;
   req->cmd_.clientId_ = cid;
   if (sharding_.txn_weight_.size() != 5) {
      verify(0);
      GetNewOrderTxn(req);
   } else {
      // GetPaymentTxn(req);
      // return;
      switch (RandomGenerator::weighted_select(sharding_.txn_weight_)) {
         case 0:
            GetNewOrderTxn(req);
            break;
         case 1:
            GetPaymentTxn(req);
            break;
         case 2:
            GetOrderStatusTxn(req);
            break;
         case 3:
            GetDeliveryTxn(req);
            break;
         case 4:
            GetStockLevelTxn(req);
            break;
         default:
            verify(0);
      }
   }
}

void TPCCTxnGenerator::GetNewOrderTxn(ClientRequest *req) {
   req->cmd_.txnType_ = TPCC_TXN_NEW_ORDER;
   std::map<int32_t, Value> &ws = req->cmd_.ws_;
   std::set<uint32_t> &targetShards = req->targetShards_;
   targetShards.clear();
   // int home_w_id = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1);
   int home_w_id = req->cmd_.clientId_ % tpcc_para_.n_w_id_;
   Value w_id((i32)home_w_id);
   // Value d_id((i32)RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1));
   Value d_id((i32)(req->cmd_.clientId_ / tpcc_para_.n_w_id_) %
              tpcc_para_.n_d_id_);
   Value c_id((i32)RandomGenerator::nu_rand(1022, 0, tpcc_para_.n_c_id_ - 1));
   int ol_cnt = RandomGenerator::rand(6, 15);
   //  int ol_cnt = 0;

   rrr::i32 i_id_buf[ol_cnt];
   ws[TPCC_VAR_W_ID] = w_id;
   ws[TPCC_VAR_D_ID] = d_id;
   ws[TPCC_VAR_C_ID] = c_id;
   ws[TPCC_VAR_OL_CNT] = Value((i32)ol_cnt);
   ws[TPCC_VAR_O_CARRIER_ID] = Value((int32_t)0);

   targetShards.insert(PartitionFromKey(
       w_id, &(sharding_.tb_infos_[TPCC_TB_WAREHOUSE]), shardNum_));
   bool all_local = true;
   // Log_info("ol_cnt=%d", ol_cnt);
   for (int i = 0; i < ol_cnt; i++) {
      // req->ws_[4 + 3 * i] = Value((i32)RandomGenerator::nu_rand(8191, 0,
      // tpcc_para_.n_i_id_ - 1)); XXX nurand is the standard
      rrr::i32 tmp_i_id =
          (i32)RandomGenerator::rand(0, tpcc_para_.n_i_id_ - 1 - i);
      // Log_info("n_i_id_=%d", tpcc_para_.n_i_id_);
      int pre_n_less = 0, n_less = 0;
      while (true) {
         n_less = 0;
         for (int j = 0; j < i; j++)
            if (i_id_buf[j] <= tmp_i_id) n_less++;
         if (n_less == pre_n_less) break;
         tmp_i_id += (n_less - pre_n_less);
         pre_n_less = n_less;
      }

      i_id_buf[i] = tmp_i_id;
      ws[TPCC_VAR_I_ID(i)] = Value(tmp_i_id);
      // Item table is fully replicated
      ws[TPCC_VAR_OL_NUMBER(i)] = i;
      ws[TPCC_VAR_OL_DELIVER_D(i)] = std::string();
      ws[TPCC_VAR_OL_DIST_INFO(i)] = std::string();

      if (tpcc_para_.n_w_id_ > 1 &&  // warehouse more than one, can do remote
          RandomGenerator::percentage_true(1)) {  // XXX 1% REMOTE_RATIO
         int remote_w_id = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 2);
         remote_w_id = remote_w_id >= home_w_id ? remote_w_id + 1 : remote_w_id;
         ws[TPCC_VAR_S_W_ID(i)] = Value((i32)remote_w_id);
         ws[TPCC_VAR_S_REMOTE_CNT(i)] = Value((i32)1);
         //      verify(req->ws_[TPCC_VAR_S_W_ID(i)].get_i32() < 3);
         targetShards.insert(PartitionFromKey(
             ws[TPCC_VAR_S_W_ID(i)], &(sharding_.tb_infos_[TPCC_TB_WAREHOUSE]),
             shardNum_));
         all_local = false;
      } else {
         ws[TPCC_VAR_S_W_ID(i)] = ws[TPCC_VAR_W_ID];
         ws[TPCC_VAR_S_REMOTE_CNT(i)] = Value((i32)0);
         //      verify(req->ws_[TPCC_VAR_S_W_ID(i)].get_i32() < 3);
      }
      ws[TPCC_VAR_OL_QUANTITY(i)] = Value((i32)RandomGenerator::rand(0, 10));

      targetShards.insert(PartitionFromKey(
          ws[TPCC_VAR_S_W_ID(i)], &(sharding_.tb_infos_[TPCC_TB_WAREHOUSE]),
          shardNum_));
   }
   ws[TPCC_VAR_O_ALL_LOCAL] = all_local ? Value((i32)1) : Value((i32)0);

   // LOG(INFO) << "Homeshards=" << home_w_id;
   // for (auto &kv : ws) {
   //    if (IS_S_W_ID(kv.first)) {
   //       LOG(INFO) << "swid=" << kv.second;
   //    }
   // }
}

void TPCCTxnGenerator::GetPaymentTxn(ClientRequest *req) {
   req->cmd_.txnType_ = TPCC_TXN_PAYMENT;
   std::map<int32_t, Value> &ws = req->cmd_.ws_;
   std::set<uint32_t> &targetShards = req->targetShards_;
   targetShards.clear();
   // int home_w_id = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1);
   int home_w_id = req->cmd_.clientId_ % tpcc_para_.n_w_id_;
   Value c_w_id, c_d_id;
   Value w_id((i32)home_w_id);
   targetShards.insert(PartitionFromKey(
       w_id, &(sharding_.tb_infos_[TPCC_TB_WAREHOUSE]), shardNum_));
   // Value d_id((i32)RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1));
   Value d_id((i32)(req->cmd_.clientId_ / tpcc_para_.n_w_id_) %
              tpcc_para_.n_d_id_);
   if (RandomGenerator::percentage_true(60)) {  // XXX query by last name 60%
      ws[TPCC_VAR_C_LAST] = Value(
          RandomGenerator::int2str_n(RandomGenerator::nu_rand(255, 0, 999), 3));
      ;
      // Log_info("c_last=%s", req->ws_[TPCC_VAR_C_LAST].get_str().c_str());
   } else {
      ws[TPCC_VAR_C_ID] =
          Value((i32)RandomGenerator::nu_rand(1022, 0, tpcc_para_.n_c_id_ - 1));
      // Log_info("c_id=%d", req->ws_[TPCC_VAR_C_ID].get_i32());
   }
   if (tpcc_para_.n_w_id_ > 1 &&  // warehouse more than one, can do remote
       RandomGenerator::percentage_true(
           15)) {  // XXX 15% pay through remote warehouse, 85 home REMOTE_RATIO
      int c_w_id_int = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 2);
      c_w_id_int = c_w_id_int >= home_w_id ? c_w_id_int + 1 : c_w_id_int;
      c_w_id = Value((i32)c_w_id_int);
      c_d_id = Value((i32)RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1));
   } else {
      c_w_id = w_id;
      c_d_id = d_id;
   }
   Value h_amount(RandomGenerator::rand_double(1.00, 5000.00));
   //  req->ws_.resize(7);
   ws[TPCC_VAR_W_ID] = w_id;
   ws[TPCC_VAR_D_ID] = d_id;
   ws[TPCC_VAR_C_W_ID] = c_w_id;
   ws[TPCC_VAR_C_D_ID] = c_d_id;
   ws[TPCC_VAR_H_AMOUNT] = h_amount;
   ws[TPCC_VAR_H_KEY] = Value((i32)RandomGenerator::rand());  // h_key
   //  req->ws_[TPCC_VAR_W_NAME] = Value();
   //  req->ws_[TPCC_VAR_D_NAME] = Value();
   targetShards.insert(PartitionFromKey(
       c_w_id, &(sharding_.tb_infos_[TPCC_TB_WAREHOUSE]), shardNum_));
}

void TPCCTxnGenerator::GetStockLevelTxn(ClientRequest *req) {
   req->cmd_.txnType_ = TPCC_TXN_STOCK_LEVEL;
   std::set<uint32_t> &targetShards = req->targetShards_;
   targetShards.clear();
   std::map<int32_t, Value> &ws = req->cmd_.ws_;
   ws[TPCC_VAR_W_ID] = Value((i32)(req->cmd_.clientId_ % tpcc_para_.n_w_id_));
   ws[TPCC_VAR_D_ID] = Value((i32)(req->cmd_.clientId_ / tpcc_para_.n_w_id_) %
                             tpcc_para_.n_d_id_);
   ws[TPCC_VAR_THRESHOLD] = Value((i32)RandomGenerator::rand(10, 20));

   targetShards.insert(
       PartitionFromKey(ws[TPCC_VAR_W_ID],
                        &(sharding_.tb_infos_[TPCC_TB_WAREHOUSE]), shardNum_));
}

void TPCCTxnGenerator::GetDeliveryTxn(ClientRequest *req) {
   req->cmd_.txnType_ = TPCC_TXN_DELIVERY;
   std::set<uint32_t> &targetShards = req->targetShards_;
   targetShards.clear();
   std::map<int32_t, Value> &ws = req->cmd_.ws_;
   ws[TPCC_VAR_W_ID] = Value((i32)(req->cmd_.clientId_ % tpcc_para_.n_w_id_));
   ws[TPCC_VAR_O_CARRIER_ID] = Value((i32)RandomGenerator::rand(1, 10));
   ws[TPCC_VAR_D_ID] = Value((i32)(req->cmd_.clientId_ / tpcc_para_.n_w_id_) %
                             tpcc_para_.n_d_id_);
   targetShards.insert(
       PartitionFromKey(ws[TPCC_VAR_W_ID],
                        &(sharding_.tb_infos_[TPCC_TB_WAREHOUSE]), shardNum_));
}

void TPCCTxnGenerator::GetOrderStatusTxn(ClientRequest *req) {
   req->cmd_.txnType_ = TPCC_TXN_ORDER_STATUS;
   std::set<uint32_t> &targetShards = req->targetShards_;
   targetShards.clear();
   std::map<int32_t, Value> &ws = req->cmd_.ws_;
   if (RandomGenerator::percentage_true(60)) {  // XXX 60% by c_last
      ws[TPCC_VAR_C_LAST] = Value(
          RandomGenerator::int2str_n(RandomGenerator::nu_rand(255, 0, 999), 3));
   } else {
      ws[TPCC_VAR_C_ID] =
          Value((i32)RandomGenerator::nu_rand(1022, 0, tpcc_para_.n_c_id_ - 1));
   }
   // This is the customer's wid and did
   ws[TPCC_VAR_W_ID] = Value((i32)(req->cmd_.clientId_ % tpcc_para_.n_w_id_));
   ws[TPCC_VAR_D_ID] = Value(
       (i32)((req->cmd_.clientId_ / tpcc_para_.n_w_id_) % tpcc_para_.n_d_id_));

   targetShards.insert(
       PartitionFromKey(ws[TPCC_VAR_W_ID],
                        &(sharding_.tb_infos_[TPCC_TB_WAREHOUSE]), shardNum_));
}

bool TPCCTxnGenerator::NeedDisPatch(const ClientRequest &req) {
   if (req.cmd_.txnType_ == TPCC_TXN_PAYMENT ||
       req.cmd_.txnType_ == TPCC_TXN_ORDER_STATUS) {
      if (req.cmd_.ws_.count(TPCC_VAR_C_ID) == 0) {
         return true;
      }
   }
   return false;
}

void TPCCTxnGenerator::GetInquireKeys(const uint32_t txnType,
                                      std::map<int32_t, mdb::Value> *existing,
                                      std::map<int32_t, mdb::Value> *input) {
   if (txnType == TPCC_TXN_PAYMENT || txnType == TPCC_TXN_ORDER_STATUS) {
      if (existing->count(TPCC_VAR_C_ID) == 0 &&
          existing->count(TPCC_VAR_C_LAST) > 0) {
         (*input)[TPCC_VAR_C_LAST] = (*existing)[TPCC_VAR_C_LAST];
         if (txnType == TPCC_TXN_PAYMENT) {
            (*input)[TPCC_VAR_C_W_ID] = (*existing)[TPCC_VAR_C_W_ID];
            (*input)[TPCC_VAR_C_D_ID] = (*existing)[TPCC_VAR_C_D_ID];
         } else if (txnType == TPCC_TXN_ORDER_STATUS) {
            (*input)[TPCC_VAR_C_W_ID] = (*existing)[TPCC_VAR_W_ID];
            (*input)[TPCC_VAR_C_D_ID] = (*existing)[TPCC_VAR_D_ID];
         }
         (*input)[TPCC_VAR_W_ID] = (*existing)[TPCC_VAR_W_ID];
      } else {
         LOG(INFO) << "ce1=" << existing->count(TPCC_VAR_C_ID)
                   << "\t ce2=" << existing->count(TPCC_VAR_C_LAST);
      }
   }
}
