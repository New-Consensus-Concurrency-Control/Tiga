
#include "TPCCStateMachine.h"

TPCCStateMachine::TPCCStateMachine(const uint32_t shardId,
                                   const uint32_t replicaId,
                                   const uint32_t shardNum,
                                   const uint32_t replicaNum,
                                   const YAML::Node& config)
    : StateMachine(shardId, replicaId, shardNum, replicaNum, config),
      sharding_(shardId, replicaId, shardNum, replicaNum, config, true),
      dbHandler_(NULL, 0) {
   // // fully replicate customer's index (simulate)
   // for (uint32_t sid = 0; sid < shardNum; sid++) {
   //    if (sid == shardId) {
   //       // Home shard has already been populated
   //       continue;
   //    }
   //    for (auto& kv : sharding_.g_c_last2id) {
   //       const auto& k = kv.first;
   //       int32_t dId = *(int32_t*)(void*)(k.c_index_smk.mb_[0].data);
   //       int32_t wId = *(int32_t*)(void*)(k.c_index_smk.mb_[1].data);
   //       int32_t cId = *(int32_t*)(void*)(k.c_index_smk.mb_[2].data);
   //       // TO Check:??
   //    }
   //    // only extract its lastName2c_Id
   // }

   tbl_dist_ = sharding_.GetTxnMgr()->get_table(TPCC_TB_DISTRICT);
   tbl_warehouse_ = sharding_.GetTxnMgr()->get_table(TPCC_TB_WAREHOUSE);
   tbl_customer_ = sharding_.GetTxnMgr()->get_table(TPCC_TB_CUSTOMER);
   tbl_order_ = sharding_.GetTxnMgr()->get_table(TPCC_TB_ORDER);
   tbl_neworder_ = sharding_.GetTxnMgr()->get_table(TPCC_TB_NEW_ORDER);
   tbl_item_ = sharding_.GetTxnMgr()->get_table(TPCC_TB_ITEM);
   tbl_stock_ = sharding_.GetTxnMgr()->get_table(TPCC_TB_STOCK);
   tbl_orderline_ = sharding_.GetTxnMgr()->get_table(TPCC_TB_ORDER_LINE);
   tbl_history_ = sharding_.GetTxnMgr()->get_table(TPCC_TB_HISTORY);
   tbl_order_cid_secondary_ =
       sharding_.GetTxnMgr()->get_table(TPCC_TB_ORDER_C_ID_SECONDARY);
   tb_info_warehouse_ = sharding_.tb_infos_[TPCC_TB_WAREHOUSE];
   tb_info_item_ = sharding_.tb_infos_[TPCC_TB_ITEM];
   tb_info_stock_ = sharding_.tb_infos_[TPCC_TB_STOCK];
   // only this key's version needs to be tracked
   version_track_[TPCC_VAR_C_LAST_VERSION] = 1;
}

std::string TPCCStateMachine::RTTI() { return "TPCCStateMachine"; }

uint32_t TPCCStateMachine::TotalNumberofKeys() { return 200000u; }

void TPCCStateMachine::InitializeRelatedShards(
    const uint32_t txnType, std::map<int32_t, Value>* input,
    std::map<uint32_t, std::set<int32_t>>* shardKeyMap) {
   switch (txnType) {
      case TPCC_TXN_NEW_ORDER:
         InitializeShardsForNewOrder(txnType, input, shardKeyMap);
         break;
      case TPCC_TXN_PAYMENT:
         InitializeShardsForPayment(txnType, input, shardKeyMap);
         break;
      case TPCC_TXN_ORDER_STATUS:
         InitializeShardsForOrderStatus(txnType, input, shardKeyMap);
         break;
      case TPCC_TXN_DELIVERY:
         InitializeShardsForDelivery(txnType, input, shardKeyMap);
         break;
      case TPCC_TXN_STOCK_LEVEL:
         InitializeShardsForStockLevel(txnType, input, shardKeyMap);
         break;
      default:
         LOG(ERROR) << "Unrecognized txn type " << txnType;
         break;
   }
}

void TPCCStateMachine::InitializeShardsForNewOrder(
    const uint32_t txnType, std::map<int32_t, Value>* input,
    std::map<uint32_t, std::set<int32_t>>* shardKeyMap) {
   std::map<int32_t, Value>& ws = (*input);
   // item table should be fully replicated
   uint32_t targetShard =
       PartitionFromKey(ws[TPCC_VAR_W_ID], &tb_info_warehouse_, shardNum_);
   (*shardKeyMap)[targetShard].insert({TPCC_ORDER_ROW, TPCC_NEW_ORDER_ROW,
                                       TPCC_ORDER_LINE_ROW,
                                       TPCC_VAR_D_NEXT_O_ID});

   // LOG(INFO) << "home shard " << targetShard << "---"
   //           << entry->shardKeyMap_.size();
   for (auto& kv : ws) {
      if (IS_S_W_ID(kv.first)) {
         uint32_t targetShard =
             PartitionFromKey(kv.second, &tb_info_warehouse_, shardNum_);
         (*shardKeyMap)[targetShard].insert(
             {TPCC_VAR_S_QUANTITY, TPCC_VAR_S_YTD, TPCC_VAR_S_ORDER_COUNT,
              TPCC_VAR_S_REMOTE_COUNT});
         // LOG(INFO) << "s shard " << targetShard << "---"
         //           << entry->shardKeyMap_.size();
      }
   }
   // LOG(INFO) << "shardsSz=" << entry->shardKeyMap_.size();
}

void TPCCStateMachine::InitializeShardsForPayment(
    const uint32_t txnType, std::map<int32_t, Value>* input,
    std::map<uint32_t, std::set<int32_t>>* shardKeyMap) {
   std::map<int32_t, Value>& ws = (*input);
   uint32_t targetShard =
       PartitionFromKey(ws[TPCC_VAR_W_ID], &tb_info_warehouse_, shardNum_);
   (*shardKeyMap)[targetShard].insert({TPCC_VAR_D_YTD});

   uint32_t targetShard2 =
       PartitionFromKey(ws[TPCC_VAR_C_W_ID], &tb_info_warehouse_, shardNum_);
   (*shardKeyMap)[targetShard2].insert({TPCC_HISTORY_ROW, TPCC_VAR_C_BALANCE,
                                        TPCC_VAR_C_YTD_PAYMENT,
                                        TPCC_VAR_C_DATA});
}

void TPCCStateMachine::InitializeShardsForStockLevel(
    const uint32_t txnType, std::map<int32_t, Value>* input,
    std::map<uint32_t, std::set<int32_t>>* shardKeyMap) {
   std::map<int32_t, Value>& ws = (*input);
   uint32_t targetShard =
       PartitionFromKey(ws[TPCC_VAR_W_ID], &tb_info_warehouse_, shardNum_);
   (*shardKeyMap)[targetShard].insert(
       {TPCC_ORDER_LINE_ROW, TPCC_VAR_D_NEXT_O_ID, TPCC_VAR_S_QUANTITY});
}

void TPCCStateMachine::InitializeShardsForOrderStatus(
    const uint32_t txnType, std::map<int32_t, Value>* input,
    std::map<uint32_t, std::set<int32_t>>* shardKeyMap) {
   std::map<int32_t, Value>& ws = (*input);
   uint32_t targetShard =
       PartitionFromKey(ws[TPCC_VAR_W_ID], &tb_info_warehouse_, shardNum_);
   (*shardKeyMap)[targetShard].insert(
       {TPCC_ORDER_LINE_ROW, TPCC_VAR_C_BALANCE});
}

void TPCCStateMachine::InitializeShardsForDelivery(
    const uint32_t txnType, std::map<int32_t, Value>* input,
    std::map<uint32_t, std::set<int32_t>>* shardKeyMap) {
   std::map<int32_t, Value>& ws = (*input);
   uint32_t targetShard =
       PartitionFromKey(ws[TPCC_VAR_W_ID], &tb_info_warehouse_, shardNum_);
   (*shardKeyMap)[targetShard].insert(
       {TPCC_ORDER_ROW, TPCC_VAR_C_BALANCE, TPCC_VAR_C_DELIVERY_CNT});
}

void TPCCStateMachine::Execute(const uint32_t txnType,
                               const std::vector<int32_t>* localKeys,
                               std::map<int32_t, Value>* inputPtr,
                               std::map<int32_t, Value>* outputPtr,
                               const uint64_t txnId) {
   switch (txnType) {
      case TPCC_TXN_NEW_ORDER:
         ExecuteNewOrderTxn(txnType, localKeys, inputPtr, outputPtr, txnId);
         break;
      case TPCC_TXN_PAYMENT:
         ExecutePaymentTxn(txnType, localKeys, inputPtr, outputPtr, txnId);
         break;
      case TPCC_TXN_ORDER_STATUS:
         ExecuteOrderStatusTxn(txnType, localKeys, inputPtr, outputPtr, txnId);
         break;
      case TPCC_TXN_DELIVERY:
         ExecuteDeliveryTxn(txnType, localKeys, inputPtr, outputPtr, txnId);
         break;
      case TPCC_TXN_STOCK_LEVEL:
         ExecuteStockLevelTxn(txnType, localKeys, inputPtr, outputPtr, txnId);
         break;
      default:
         LOG(ERROR) << "Unrecognized txn type " << txnType;
         break;
   }
}

void TPCCStateMachine::ExecuteNewOrderTxn(const uint32_t txnType,
                                          const std::vector<int32_t>* localKeys,
                                          std::map<int32_t, Value>* inputPtr,
                                          std::map<int32_t, Value>* outputPtr,
                                          const uint64_t txnId) {
   std::map<int32_t, Value> itemInput;
   std::map<int32_t, Value> stockInput;
   std::map<int32_t, Value>& ws = (*inputPtr);
   std::map<int32_t, Value>& output = (*outputPtr);
   for (auto& kv : ws) {
      if (IS_S_W_ID(kv.first)) {
         stockInput[kv.first] = kv.second;
      }
   }
   /**
 * Piece 0
 * The row in the DISTRICT table with matching D_W_ID and D_ ID is
 selected
 * D_TAX, the district tax rate, is retrieved,
 * D_NEXT_O_ID, the next available order number for the district, is
 * retrieved and incremented by one.
 */
   bool isHomeShard =
       (shardId_ ==
        PartitionFromKey(ws[TPCC_VAR_W_ID], &tb_info_warehouse_, shardNum_));
   if (isHomeShard) {
      mdb::MultiBlob mb(2);
      mb[0] = ws[TPCC_VAR_D_ID].get_blob();
      mb[1] = ws[TPCC_VAR_W_ID].get_blob();
      mdb::Row* row_district = dbHandler_.query(tbl_dist_, mb).next();
      // R district
      dbHandler_.read_column(row_district, TPCC_COL_DISTRICT_D_TAX,
                             &output[TPCC_VAR_D_TAX]);
      dbHandler_.read_column(row_district, TPCC_COL_DISTRICT_D_NEXT_O_ID,
                             &output[TPCC_VAR_O_ID]);  // read d_next_o_id
      // read d_next_o_id, increment by 1
      Value buf(0);
      buf.set_i32((i32)(output[TPCC_VAR_O_ID].get_i32() + 1));
      dbHandler_.write_column(row_district, TPCC_COL_DISTRICT_D_NEXT_O_ID, buf);
   }
   // LOG(INFO) << "Piece-1 Done wsSize=" << ws.size();
   /**
    * Piece 1
    * The row in the WAREHOUSE table with matching W_ID is selected
    * and  W_TAX, the warehouse tax rate, is retrieved.
    */
   if (isHomeShard) {
      mdb::Row* row_warehouse =
          dbHandler_.query(tbl_warehouse_, ws[TPCC_VAR_W_ID].get_blob()).next();
      // R warehouse
      dbHandler_.read_column(row_warehouse, TPCC_COL_WAREHOUSE_W_TAX,
                             &output[TPCC_VAR_W_TAX]);  // read w_tax
   }
   // LOG(INFO) << "Piece-2 Done wsSize=" << ws.size();
   /**
    * Piece 2:
    * The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
    * selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
    * customer's last name, and C_CREDIT, the customer's credit status, are
    * retrieved.
    */
   if (isHomeShard) {
      mdb::MultiBlob mb(3);
      mb[0] = ws[TPCC_VAR_C_ID].get_blob();
      mb[1] = ws[TPCC_VAR_D_ID].get_blob();
      mb[2] = ws[TPCC_VAR_W_ID].get_blob();

      mdb::Row* row_customer = dbHandler_.query(tbl_customer_, mb).next();
      // R customer
      dbHandler_.read_column(row_customer, TPCC_COL_CUSTOMER_C_LAST,
                             &output[TPCC_VAR_C_LAST]);
      dbHandler_.read_column(row_customer, TPCC_COL_CUSTOMER_C_CREDIT,
                             &output[TPCC_VAR_C_CREDIT]);
      dbHandler_.read_column(row_customer, TPCC_COL_CUSTOMER_C_DISCOUNT,
                             &output[TPCC_VAR_C_DISCOUNT]);
   }
   // LOG(INFO) << "Piece-3 Done wsSize=" << ws.size();

   /**
    * Piece 3
    * A new row is inserted into both the NEW-ORDER table and the ORDER table
    to
    * reflect the creation of the new order. O_CARRIER_ID is set to a null
    * value. If the order includes only home order-lines, then O_ALL_LOCAL is
    * set to 1, otherwise O_ALL_LOCAL is set to 0.
    */
   /**
    * Here only insert into ORDER table
    * Next we will insert into NEW-ORDER table
    */
   if (isHomeShard) {
      // LOG(INFO) << "TPCC_NEW_ORDER, piece: " << TPCC_NEW_ORDER_3;
      mdb::MultiBlob mb(3);
      mb[0] = ws[TPCC_VAR_D_ID].get_blob();
      mb[1] = ws[TPCC_VAR_W_ID].get_blob();
      mb[2] = ws[TPCC_VAR_C_ID].get_blob();

      mdb::Row* r = dbHandler_.query(tbl_order_, mb).next();
      output[TPCC_VAR_O_ENTRY_D] = Value(std::to_string(time(NULL)));

      // W order
      std::vector<Value> row_data = {
          ws.at(TPCC_VAR_D_ID),       ws[TPCC_VAR_W_ID],
          output.at(TPCC_VAR_O_ID),   ws[TPCC_VAR_C_ID],
          output[TPCC_VAR_O_ENTRY_D],  // o_entry_d
          ws[TPCC_VAR_O_CARRIER_ID],  ws[TPCC_VAR_OL_CNT],
          ws[TPCC_VAR_O_ALL_LOCAL]};
      // LOG(INFO) << "done r->table=" << r->get_table()->Name();
      r = mdb::VersionedRow::create(tbl_order_->schema(), row_data);
      verify(r->schema_);
      dbHandler_.insert_row(tbl_order_, r);
      // dbHandler_.write_column(r, 3, ws[TPCC_VAR_W_ID]);  // ??

      // LOG(INFO) << "Piece-4-1 Done wsSize=" << ws.size();

      // Piece-3-continue: insert into NEW-ORDER table
      // W new_order
      std::vector<Value> row_data2 = {
          ws[TPCC_VAR_D_ID],      // o_d_id
          ws[TPCC_VAR_W_ID],      // o_w_id
          output[TPCC_VAR_O_ID],  // o_id
      };

      mdb::Row* r2 =
          mdb::VersionedRow::create(tbl_neworder_->schema(), row_data2);
      dbHandler_.insert_row(tbl_neworder_, r2);
   }
   // LOG(INFO) << "Piece-5-1 Done wsSize=" << ws.size();
   /**
    * Piece-4
    * The number of items, O_OL_CNT, is computed to match ol_cnt.
    * For each O_OL_CNT item on the order:
        - The row in the ITEM table with matching I_ID (equals OL_I_ID) is
    selected and I_PRICE, the price of the item, I_NAME, the name of the
    item, and I_DATA are retrieved.
    */

   /**
    * Piece-4-2
    * The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and
    * S_W_ID (equals OL_SUPPLY_W_ID) is selected. S_QUANTITY, the quantity in
    * stock, S_DIST_xx, where xx represents the district number, and S_DATA
    are
    * retrieved.
    */

   for (auto& kv : stockInput) {
      if (shardId_ != PartitionFromKey(kv.second, &tb_info_stock_, shardNum_)) {
         continue;
      }
      int I = S_W_ID_POS(kv.first);
      // LOG(INFO) << "TPCC_NEW_ORDER, piece: " << TPCC_NEW_ORDER_RS(I)
      //           << "-- I=" << I;
      mdb::MultiBlob mb(2);
      mb[0] = ws[TPCC_VAR_I_ID(I)].get_blob();
      mb[1] = ws[TPCC_VAR_S_W_ID(I)].get_blob();
      int32_t w_id = ws[TPCC_VAR_S_W_ID(I)].get_i32();
      int32_t i_id = ws[TPCC_VAR_I_ID(I)].get_i32();
      // LOG(INFO) << "new order read stock. item_id: " << i_id
      //           << " s_w_id: " << w_id;

      mdb::Row* r = dbHandler_.query(tbl_stock_, mb).next();
      verify(r->schema_);
      //  Ri stock
      //  FIXME compress all s_dist_xx into one column
      dbHandler_.read_column(
          r, TPCC_COL_STOCK_S_DIST_XX,
          &output[TPCC_VAR_OL_DIST_INFO(I)]);  // 0 ==> s_dist_xx
      dbHandler_.read_column(r, TPCC_COL_STOCK_S_DATA,
                             &output[TPCC_VAR_S_DATA(I)]);  // 1 ==> s_data
   }

   // LOG(INFO) << "Piece-5-2 Done wsSize=" << ws.size();
   /**
    * If the retrieved value for S_QUANTITY exceeds OL_QUANTITY by 10
    * or more, then S_QUANTITY is decreased by OL_QUANTITY; otherwise
    S_QUANTITY
    * is updated to (S_QUANTITY - OL_QUANTITY)+91. S_YTD is increased by
    * OL_QUANTITY and S_ORDER_CNT is incremented by 1. If the order-line is
    * remote, then S_REMOTE_CNT is incremented by 1.
    */

   for (auto& kv : stockInput) {
      if (shardId_ != PartitionFromKey(kv.second, &tb_info_stock_, shardNum_)) {
         continue;
      }
      int I = S_W_ID_POS(kv.first);
      mdb::Row* r = NULL;
      mdb::MultiBlob mb(2);
      mb[0] = ws[TPCC_VAR_I_ID(I)].get_blob();
      mb[1] = ws[TPCC_VAR_S_W_ID(I)].get_blob();
      r = dbHandler_.query(tbl_stock_, mb).next();
      // Ri stock
      Value buf(0);
      dbHandler_.read_column(r, TPCC_COL_STOCK_S_QUANTITY, &buf);
      int32_t new_ol_quantity =
          buf.get_i32() - ws[TPCC_VAR_OL_QUANTITY(I)].get_i32();

      dbHandler_.read_column(r, TPCC_COL_STOCK_S_YTD, &buf);
      Value new_s_ytd(buf.get_i32() + ws[TPCC_VAR_OL_QUANTITY(I)].get_i32());

      dbHandler_.read_column(r, TPCC_COL_STOCK_S_ORDER_CNT, &buf);
      Value new_s_order_cnt((i32)(buf.get_i32() + 1));

      dbHandler_.read_column(r, TPCC_COL_STOCK_S_REMOTE_CNT, &buf);
      Value new_s_remote_cnt(buf.get_i32() +
                             ws[TPCC_VAR_S_REMOTE_CNT(I)].get_i32());

      if (new_ol_quantity < 10) new_ol_quantity += 91;
      Value new_ol_quantity_value(new_ol_quantity);
      output[TPCC_VAR_AT_S_QUANTITY(I)] = new_ol_quantity_value;

      dbHandler_.write_columns(
          r,
          {TPCC_COL_STOCK_S_QUANTITY, TPCC_COL_STOCK_S_YTD,
           TPCC_COL_STOCK_S_ORDER_CNT, TPCC_COL_STOCK_S_REMOTE_CNT},
          {new_ol_quantity_value, new_s_ytd, new_s_order_cnt,
           new_s_remote_cnt});
   }

   // LOG(INFO) << "Piece-5-3 Done wsSize=" << ws.size();
   /**
    * A new row is inserted into the ORDER-LINE table to reflect the item on
    the
    * order. OL_DELIVERY_D is set to a null value, OL_NUMBER is set to a
    unique
    * value within all the ORDER-LINE rows that have the same OL_O_ID value,
    and
    * OL_DIST_INFO is set to the content of S_DIST_xx, where xx represents
    the
    * district number (OL_D_ID)
    */
   // item table are fully replicated
   if (isHomeShard) {
      for (auto& kv : stockInput) {
         int I = S_W_ID_POS(kv.first);
         mdb::Row* row_item =
             dbHandler_.query(tbl_item_, ws[TPCC_VAR_I_ID(I)].get_blob())
                 .next();
         // Ri item
         dbHandler_.read_column(row_item, TPCC_COL_ITEM_I_NAME,
                                &output[TPCC_VAR_I_NAME(I)]);
         dbHandler_.read_column(row_item, TPCC_COL_ITEM_I_PRICE,
                                &output[TPCC_VAR_I_PRICE(I)]);
         dbHandler_.read_column(row_item, TPCC_COL_ITEM_I_DATA,
                                &output[TPCC_VAR_I_DATA(I)]);
         double am = (double)ws[TPCC_VAR_OL_QUANTITY(I)].get_i32() *
                     output[TPCC_VAR_I_PRICE(I)]
                         .get_double(); /** Janus code misses this */
         // LOG(INFO) << "Piece-5-Mid Done wsSize=" << ws.size();
         // LOG(INFO) << "1=" << ws[TPCC_VAR_D_ID].get_kind();
         // LOG(INFO) << "2=" << ws[TPCC_VAR_W_ID].get_kind();
         // LOG(INFO) << "3=" << output[TPCC_VAR_O_ID].get_kind();
         // LOG(INFO) << "4=" << ws[TPCC_VAR_OL_NUMBER(I)].get_kind();
         // LOG(INFO) << "5=" << ws[TPCC_VAR_I_ID(I)].get_kind();
         // LOG(INFO) << "6=" << ws[TPCC_VAR_S_W_ID(I)].get_kind();
         // LOG(INFO) << "7=" << ws[TPCC_VAR_OL_DELIVER_D(I)].get_kind();
         // LOG(INFO) << "8=" << ws[TPCC_VAR_OL_QUANTITY(I)].get_kind();
         Value amount = Value(am);
         Value xxx = Value("");
         mdb::Row* r = mdb::VersionedRow::create(
             tbl_orderline_->schema(), vector<Value>({
                                           ws[TPCC_VAR_D_ID],
                                           ws[TPCC_VAR_W_ID],
                                           output[TPCC_VAR_O_ID],
                                           ws[TPCC_VAR_OL_NUMBER(I)],
                                           ws[TPCC_VAR_I_ID(I)],
                                           ws[TPCC_VAR_S_W_ID(I)],
                                           ws[TPCC_VAR_OL_DELIVER_D(I)],
                                           ws[TPCC_VAR_OL_QUANTITY(I)],
                                           amount,
                                           xxx,
                                       }));
         // LOG(INFO) << "Piece-5-2-Mid Done wsSize=" << ws.size();
         dbHandler_.insert_row(tbl_orderline_, r);
      }
      // LOG(INFO) << "Piece-5-4 Done wsSize=" << ws.size();
   }

   // Only fetch those key-values that is not submitted by clients, do not
   // include redundant info
}

void TPCCStateMachine::ExecutePaymentTxn(const uint32_t txnType,
                                         const std::vector<int32_t>* localKeys,
                                         std::map<int32_t, Value>* inputPtr,
                                         std::map<int32_t, Value>* outputPtr,
                                         const uint64_t txnId) {
   std::map<int32_t, Value>& ws = (*inputPtr);
   std::map<int32_t, Value>& output = (*outputPtr);
   bool isHomeShard =
       (shardId_ ==
        PartitionFromKey(ws[TPCC_VAR_W_ID], &tb_info_warehouse_, shardNum_));

   output[ABORT_FLAG] = 0;
   if (ws.count(TPCC_VAR_C_LAST_VERSION) > 0) {
      bool isCustomerShard =
          (shardId_ == PartitionFromKey(ws[TPCC_VAR_C_W_ID],
                                        &tb_info_warehouse_, shardNum_));
      if (isCustomerShard) {
         std::shared_lock lck(cid_mtx_);
         if (ws[TPCC_VAR_C_LAST_VERSION].get_i32() !=
             version_track_[TPCC_VAR_C_LAST_VERSION]) {
            output[ABORT_FLAG] = 1;
            return;
         }
      }
   }

   /**
    * The row in the WAREHOUSE table with matching W_ID is selected. W_NAME,
    * W_STREET_1, W_STREET_2, W_CITY, W_STATE, and W_ZIP are retrieved
    */
   if (isHomeShard) {
      mdb::Row* row_warehouse =
          dbHandler_.query(tbl_warehouse_, ws[TPCC_VAR_W_ID].get_blob()).next();
      // R warehouse
      dbHandler_.read_column(row_warehouse, TPCC_COL_WAREHOUSE_W_NAME,
                             &output[TPCC_VAR_W_NAME]);
      dbHandler_.read_column(row_warehouse, TPCC_COL_WAREHOUSE_W_STREET_1,
                             &output[TPCC_VAR_W_STREET_1]);
      dbHandler_.read_column(row_warehouse, TPCC_COL_WAREHOUSE_W_STREET_2,
                             &output[TPCC_VAR_W_STREET_2]);
      dbHandler_.read_column(row_warehouse, TPCC_COL_WAREHOUSE_W_CITY,
                             &output[TPCC_VAR_W_CITY]);
      dbHandler_.read_column(row_warehouse, TPCC_COL_WAREHOUSE_W_STATE,
                             &output[TPCC_VAR_W_STATE]);
      dbHandler_.read_column(row_warehouse, TPCC_COL_WAREHOUSE_W_ZIP,
                             &output[TPCC_VAR_W_ZIP]);
   }
   // Log_info("Piece-1 wssize=%u", ws.size());
   /***
    * The row in the DISTRICT table with matching D_W_ID and D_ID is
    selected.
    * D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are
    retrieved
    *
    */
   if (isHomeShard) {
      Value buf;
      mdb::MultiBlob mb(2);
      mb[0] = ws[TPCC_VAR_D_ID].get_blob();
      mb[1] = ws[TPCC_VAR_W_ID].get_blob();
      mdb::Row* row_district = dbHandler_.query(tbl_dist_, mb).next();
      ws[TPCC_VAR_D_NAME] = Value("");

      // R district
      dbHandler_.read_column(row_district, TPCC_COL_DISTRICT_D_NAME,
                             &output[TPCC_VAR_D_NAME]);
      dbHandler_.read_column(row_district, TPCC_COL_DISTRICT_D_STREET_1,
                             &output[TPCC_VAR_D_STREET_1]);
      dbHandler_.read_column(row_district, TPCC_COL_DISTRICT_D_STREET_2,
                             &output[TPCC_VAR_D_STREET_2]);
      dbHandler_.read_column(row_district, TPCC_COL_DISTRICT_D_CITY,
                             &output[TPCC_VAR_D_CITY]);
      dbHandler_.read_column(row_district, TPCC_COL_DISTRICT_D_STATE,
                             &output[TPCC_VAR_D_STATE]);
      dbHandler_.read_column(row_district, TPCC_COL_DISTRICT_D_ZIP,
                             &output[TPCC_VAR_D_ZIP]);
   }
   // Log_info("Piece-2 wssize=%u", ws.size());
   /***
    * D_YTD, the district's year-to-date balance, is increased by H_AMOUNT.
    *
    * [Original] W_YTD, the warehouse's year-to-date balance, is increased
    by H_ AMOUNT.
    */

   if (isHomeShard) {
      Value buf_temp(0.0);
      mdb::Row* row_temp = NULL;
      mdb::MultiBlob mb_temp(2);
      mb_temp[0] = ws[TPCC_VAR_D_ID].get_blob();
      mb_temp[1] = ws[TPCC_VAR_W_ID].get_blob();
      row_temp = dbHandler_.query(tbl_dist_, mb_temp).next();
      verify(row_temp->schema_ != nullptr);
      dbHandler_.read_column(row_temp, TPCC_COL_DISTRICT_D_YTD, &buf_temp);
      // W district
      Value buf(0.0);
      buf.set_double(buf_temp.get_double() +
                     ws[TPCC_VAR_H_AMOUNT].get_double());
      dbHandler_.write_column(row_temp, TPCC_COL_DISTRICT_D_YTD, buf_temp);
   }

   /**
    *   // piece 5, R customer secondary index, c_last -> c_id
    *    CustomerIndex is fully replicated
    */

   if (ws.find(TPCC_VAR_C_ID) == ws.end()) {
      // Log_info("Option-2");
      mdb::MultiBlob mbl(3), mbh(3);
      mbl[0] = ws[TPCC_VAR_C_D_ID].get_blob();
      mbh[0] = ws[TPCC_VAR_C_D_ID].get_blob();
      mbl[1] = ws[TPCC_VAR_C_W_ID].get_blob();
      mbh[1] = ws[TPCC_VAR_C_W_ID].get_blob();
      Value c_id_low(std::numeric_limits<i32>::min());
      Value c_id_high(std::numeric_limits<i32>::max());
      mbl[2] = c_id_low.get_blob();
      mbh[2] = c_id_high.get_blob();
      // LOG(INFO) << "TPCC_VAR_C_LAST no exist?="
      //           << (ws.find(TPCC_VAR_C_LAST) == ws.end());
      c_last_id_t key_low(ws[TPCC_VAR_C_LAST].get_str(), mbl, &(C_LAST_SCHEMA));
      c_last_id_t key_high(ws[TPCC_VAR_C_LAST].get_str(), mbh,
                           &(C_LAST_SCHEMA));
      std::multimap<c_last_id_t, rrr::i32>::iterator it, it_low, it_high,
          it_mid;
      bool inc = false, mid_set = false;

      it_low = C_LAST2ID.lower_bound(key_low);
      it_high = C_LAST2ID.upper_bound(key_high);
      int n_c = 0;
      for (it = it_low; it != it_high; it++) {
         n_c++;
         if (mid_set)
            if (inc) {
               it_mid++;
               inc = false;
            } else
               inc = true;
         else {
            mid_set = true;
            it_mid = it;
         }
      }
      verify(mid_set);
      ws[TPCC_VAR_C_ID] = Value(it_mid->second);
      // LOG(INFO) << "c_id=" << ws[TPCC_VAR_C_ID].get_i32();
   }

   // Log_info("Piece-5-1 wssize=%u", ws.size());
   /***
    * A new row is inserted into the HISTORY table with H_C_ID = C_ID,
    H_C_D_ID
    * = C_D_ID, H_C_W_ID = C_W_ID, H_D_ID = D_ID, and H_W_ID = W_ID.
    */

   assert(ws.find(TPCC_VAR_H_KEY) != ws.end());
   if (isHomeShard) {
      // insert history
      std::vector<Value> row_data(9);
      row_data[0] = ws[TPCC_VAR_H_KEY];  // h_key
      // if (ws.find(TPCC_VAR_C_ID) == ws.end()) {
      //    // hard code
      //    ws[TPCC_VAR_C_ID].set_i32(1);
      // }
      row_data[1] = ws[TPCC_VAR_C_ID];                  // h_c_id   =>  c_id
      row_data[2] = ws[TPCC_VAR_C_D_ID];                // h_c_d_id => c_d_id
      row_data[3] = ws[TPCC_VAR_C_W_ID];                // h_c_w_id => c_w_id
      row_data[4] = ws[TPCC_VAR_D_ID];                  // h_d_id   =>  d_id
      row_data[5] = ws[TPCC_VAR_W_ID];                  // h_d_w_id => d_w_id
      row_data[6] = Value(std::to_string(time(NULL)));  // h_date
      output[TPCC_VAR_H_DATE] = row_data[6];
      row_data[7] = ws[TPCC_VAR_H_AMOUNT];  // h_amount => h_amount
      row_data[8] =
          Value(output[TPCC_VAR_W_NAME].get_str() + "    " +
                output[TPCC_VAR_D_NAME].get_str());  // d_data => w_name +
                                                     // 4spaces + d_name
      mdb::Row* row_history =
          mdb::VersionedRow::create(tbl_history_->schema(), row_data);
      dbHandler_.insert_row(tbl_history_, row_history);
   }
   // Log_info("Piece-5 wssize=%u", ws.size());

   // Log_info("Piece-3 wssize=%u", ws.size());
   bool isCustomerShard =
       (shardId_ ==
        PartitionFromKey(ws[TPCC_VAR_C_W_ID], &tb_info_warehouse_, shardNum_));
   if (!isCustomerShard) {
      return;
   }
   /**
    * If the value of C_CREDIT is equal to "BC"[changed], then C_DATA is also
    * retrieved from the selected customer and the following history
    * information: C_ID, C_D_ID, C_W_ID, D_ID, W_ID, and H_AMOUNT, are
    inserted
    * at the left of the C_DATA field by shifting the existing content of
    C_DATA
    * to the right by an equal number of bytes and by discarding the bytes
    that
    * are shifted out of the right side of the C_DATA field. The content of
    the
    * C_DATA field never exceeds 500 characters. The selected customer is
    * updated with the new C_DATA field. If C_DATA is implemented as two
    fields
    * (see Clause 1.4.9), they must be treated and operated on as one single
    * field.
    */

   {

      mdb::Row* r = NULL;
      mdb::MultiBlob mb(3);
      // cell_locator_t cl(TPCC_TB_CUSTOMER, 3);
      mb[0] = ws[TPCC_VAR_C_ID].get_blob();
      mb[1] = ws[TPCC_VAR_C_D_ID].get_blob();
      mb[2] = ws[TPCC_VAR_C_W_ID].get_blob();
      // R customer
      r = dbHandler_.query(tbl_customer_, mb).next();
      ALock::type_t lock_20_type = ALock::RLOCK;
      if (ws[TPCC_VAR_C_ID].get_i32() % 10 == 0) lock_20_type = ALock::WLOCK;

      vector<Value> buf({Value(""), Value(""), Value(""), Value(""), Value(""),
                         Value(""), Value(""), Value(""), Value(""), Value(""),
                         Value(""), Value(""), Value(""), Value(0.0),
                         Value(0.0), Value("")});
      int oi = 0;
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_FIRST, &buf[0]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_MIDDLE, &buf[1]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_LAST, &buf[2]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_STREET_1, &buf[3]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_STREET_2, &buf[4]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_CITY, &buf[5]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_STATE, &buf[6]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_ZIP, &buf[7]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_PHONE, &buf[8]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_SINCE, &buf[9]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_CREDIT, &buf[10]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_CREDIT_LIM, &buf[11]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_DISCOUNT, &buf[12]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_BALANCE, &buf[13]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_YTD_PAYMENT, &buf[14]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_DATA, &buf[15]);

      // if c_credit == "BC" (bad) 10%
      // here we use c_id to pick up 10% instead of c_credit
      if (ws[TPCC_VAR_C_ID].get_i32() % 10 == 0) {
         Value c_data(
             (to_string(ws[TPCC_VAR_C_ID]) + to_string(ws[TPCC_VAR_C_D_ID]) +
              to_string(ws[TPCC_VAR_C_W_ID]) + to_string(ws[TPCC_VAR_D_ID]) +
              to_string(ws[TPCC_VAR_W_ID]) + to_string(ws[TPCC_VAR_H_AMOUNT]) +
              buf[15].get_str())
                 .substr(0, 500));
         std::vector<mdb::column_id_t> col_ids = {
             TPCC_COL_CUSTOMER_C_BALANCE, TPCC_COL_CUSTOMER_C_YTD_PAYMENT,
             TPCC_COL_CUSTOMER_C_DATA};
         std::vector<Value> col_data(
             {Value(buf[13].get_double() - ws[TPCC_VAR_H_AMOUNT].get_double()),
              Value(buf[14].get_double() + ws[TPCC_VAR_H_AMOUNT].get_double()),
              c_data});
         dbHandler_.write_columns(r, col_ids, col_data);
      } else {
         std::vector<mdb::column_id_t> col_ids(
             {TPCC_COL_CUSTOMER_C_BALANCE, TPCC_COL_CUSTOMER_C_YTD_PAYMENT});
         std::vector<Value> col_data(
             {Value(buf[13].get_double() - ws[TPCC_VAR_H_AMOUNT].get_double()),
              Value(buf[14].get_double() +
                    ws[TPCC_VAR_H_AMOUNT].get_double())});
         dbHandler_.write_columns(r, col_ids, col_data);
      }
      output[TPCC_VAR_C_FIRST] = buf[0];
      output[TPCC_VAR_C_MIDDLE] = buf[1];
      output[TPCC_VAR_C_LAST] = buf[2];
      output[TPCC_VAR_C_STREET_1] = buf[3];
      output[TPCC_VAR_C_STREET_2] = buf[4];
      output[TPCC_VAR_C_CITY] = buf[5];
      output[TPCC_VAR_C_STATE] = buf[6];
      output[TPCC_VAR_C_ZIP] = buf[7];
      output[TPCC_VAR_C_PHONE] = buf[8];
      output[TPCC_VAR_C_SINCE] = buf[9];
      output[TPCC_VAR_C_CREDIT] = buf[10];
      output[TPCC_VAR_C_CREDIT_LIM] = buf[11];
      output[TPCC_VAR_C_DISCOUNT] = buf[12];
      output[TPCC_VAR_C_BALANCE] =
          Value(buf[13].get_double() - ws[TPCC_VAR_H_AMOUNT].get_double());
   }
   // Log_info("Piece-6 wssize=%u", ws.size());

   // output[TPCC_VAR_C_DATA] = ws[TPCC_VAR_C_DATA]; // can be computed locally
   // by terminal
}

void TPCCStateMachine::ExecuteOrderStatusTxn(
    const uint32_t txnType, const std::vector<int32_t>* localKeys,
    std::map<int32_t, Value>* inputPtr, std::map<int32_t, Value>* outputPtr,
    const uint64_t txnId) {
   std::map<int32_t, Value>& ws = (*inputPtr);
   std::map<int32_t, Value>& output = (*outputPtr);
   /**
    *  Case 1, the customer is selected based on customer number:
    * Case 2, the customer is selected based on customer last name:
    */

   output[ABORT_FLAG] = 0;
   if (ws.count(TPCC_VAR_C_LAST_VERSION) > 0) {
      // In this txn, the passed TPCC_VAR_W_ID is the customer's wid
      bool isCustomerShard =
          (shardId_ ==
           PartitionFromKey(ws[TPCC_VAR_W_ID], &tb_info_warehouse_, shardNum_));
      if (isCustomerShard) {
         std::shared_lock lck(cid_mtx_);
         if (ws[TPCC_VAR_C_LAST_VERSION].get_i32() !=
             version_track_[TPCC_VAR_C_LAST_VERSION]) {
            output[ABORT_FLAG] = 1;
            return;
         }
      }
   }

   if (ws.find(TPCC_VAR_C_LAST) != ws.end()) {
      // Case 2
      mdb::MultiBlob mbl(3), mbh(3);
      mbl[0] = ws[TPCC_VAR_D_ID].get_blob();
      mbh[0] = ws[TPCC_VAR_D_ID].get_blob();
      mbl[1] = ws[TPCC_VAR_W_ID].get_blob();
      mbh[1] = ws[TPCC_VAR_W_ID].get_blob();
      Value c_id_low(std::numeric_limits<i32>::min());
      Value c_id_high(std::numeric_limits<i32>::max());
      mbl[2] = c_id_low.get_blob();
      mbh[2] = c_id_high.get_blob();
      c_last_id_t key_low(ws[TPCC_VAR_C_LAST].get_str(), mbl, &(C_LAST_SCHEMA));
      c_last_id_t key_high(ws[TPCC_VAR_C_LAST].get_str(), mbh,
                           &(C_LAST_SCHEMA));
      std::multimap<c_last_id_t, rrr::i32>::iterator it, it_low, it_high,
          it_mid;
      bool inc = false, mid_set = false;
      it_low = C_LAST2ID.lower_bound(key_low);
      it_high = C_LAST2ID.upper_bound(key_high);
      int n_c = 0;
      for (it = it_low; it != it_high; it++) {
         n_c++;
         if (mid_set)
            if (inc) {
               it_mid++;
               inc = false;
            } else
               inc = true;
         else {
            mid_set = true;
            it_mid = it;
         }
      }
      verify(mid_set);
      i32 oi = 0;
      ws[TPCC_VAR_C_ID] = Value(it_mid->second);
   }
   // Log_info("Piece-1 wssize=%u", ws.size());
   {
      // R customer
      Value buf;
      mdb::MultiBlob mb(3);
      mb[0] = ws[TPCC_VAR_C_ID].get_blob();
      mb[1] = ws[TPCC_VAR_D_ID].get_blob();
      mb[2] = ws[TPCC_VAR_W_ID].get_blob();
      mdb::Row* r = dbHandler_.query(tbl_customer_, mb).next();
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_FIRST,
                             &output[TPCC_VAR_C_FIRST]);  // read c_first
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_MIDDLE,
                             &output[TPCC_VAR_C_MIDDLE]);  // read c_middle
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_LAST,
                             &output[TPCC_VAR_C_LAST]);  // read c_last
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_BALANCE,
                             &output[TPCC_VAR_C_BALANCE]);  // read c_balance
   }
   // Log_info("Piece-2 wssize=%u", ws.size());

   /**
    * The row in the ORDER table with matching O_W_ID (equals C_W_ID), O_D_ID
    * (equals C_D_ID), O_C_ID (equals C_ID), and with the largest existing O_ID,
    * is selected. This is the most recent order placed by that customer. O_ID,
    * O_ENTRY_D, and O_CARRIER_ID are retrieved.
    */
   {

      mdb::MultiBlob mb_0(3);
      mb_0[0] = ws[TPCC_VAR_D_ID].get_blob();
      mb_0[1] = ws[TPCC_VAR_W_ID].get_blob();
      mb_0[2] = ws[TPCC_VAR_C_ID].get_blob();
      mdb::Row* r_0 = dbHandler_.query(tbl_order_cid_secondary_, mb_0).next();

      mdb::MultiBlob mb(3);
      mb[0] = ws[TPCC_VAR_D_ID].get_blob();
      mb[1] = ws[TPCC_VAR_W_ID].get_blob();
      mb[2] = r_0->get_blob(3);  // FIXME add lock before reading

      mdb::Row* r = dbHandler_.query(tbl_order_, mb).next();
      dbHandler_.read_column(r, TPCC_COL_ORDER_O_ID,
                             &output[TPCC_VAR_O_ID]);  // ws[0] ==> o_id
      dbHandler_.read_column(
          r, TPCC_COL_ORDER_O_ENTRY_D,
          &output[TPCC_VAR_O_ENTRY_D]);  // ws[1] ==> o_entry_d
      dbHandler_.read_column(
          r, TPCC_COL_ORDER_O_CARRIER_ID,
          &output[TPCC_VAR_O_CARRIER_ID]);  // ws[2] ==> o_carrier_id
   }
   // Log_info("Piece-3 wssize=%u", ws.size());

   /**
    * All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID),
    OL_D_ID (equals O_D_ID), and OL_O_ID (equals O_ID) are selected and the
    corresponding sets of OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, and
    OL_DELIVERY_D are retrieved.
   */
   {
      mdb::MultiBlob mbl(4), mbh(4);
      // Log_debug("ol_d_id: %d, ol_w_id: %d, ol_o_id: %d",
      //           ws[TPCC_VAR_O_ID].get_i32(), ws[TPCC_VAR_D_ID].get_i32(),
      //           ws[TPCC_VAR_W_ID].get_i32());
      mbl[0] = ws[TPCC_VAR_D_ID].get_blob();
      mbh[0] = ws[TPCC_VAR_D_ID].get_blob();
      mbl[1] = ws[TPCC_VAR_W_ID].get_blob();
      mbh[1] = ws[TPCC_VAR_W_ID].get_blob();
      mbl[2] = output[TPCC_VAR_O_ID].get_blob();
      mbh[2] = output[TPCC_VAR_O_ID].get_blob();
      Value ol_number_low(std::numeric_limits<i32>::min()),
          ol_number_high(std::numeric_limits<i32>::max());
      mbl[3] = ol_number_low.get_blob();
      mbh[3] = ol_number_high.get_blob();

      mdb::ResultSet rs =
          dbHandler_.query_in(tbl_orderline_, mbl, mbh, mdb::ORD_DESC);
      mdb::Row* r = NULL;
      std::vector<mdb::Row*> row_list;
      row_list.reserve(15);
      while (rs.has_next()) {
         row_list.push_back(rs.next());
      }

      verify(row_list.size() != 0);

      std::vector<mdb::column_lock_t> column_locks;
      column_locks.reserve(5 * row_list.size());

      int i = 0;
      // Log_debug("row_list size: %u", row_list.size());
      while (i < row_list.size()) {
         r = row_list[i++];
         dbHandler_.read_column(r, TPCC_COL_ORDER_LINE_OL_I_ID,
                                &output[TPCC_VAR_OL_I_ID(i)]);
         dbHandler_.read_column(r, TPCC_COL_ORDER_LINE_OL_SUPPLY_W_ID,
                                &output[TPCC_VAR_OL_W_ID(i)]);
         dbHandler_.read_column(r, TPCC_COL_ORDER_LINE_OL_DELIVERY_D,
                                &output[TPCC_VAR_OL_DELIVER_D(i)]);
         dbHandler_.read_column(r, TPCC_COL_ORDER_LINE_OL_QUANTITY,
                                &output[TPCC_VAR_OL_QUANTITY(i)]);
         dbHandler_.read_column(r, TPCC_COL_ORDER_LINE_OL_AMOUNT,
                                &output[TPCC_VAR_OL_AMOUNTS(i)]);
      }
   }
}

void TPCCStateMachine::ExecuteDeliveryTxn(const uint32_t txnType,
                                          const std::vector<int32_t>* localKeys,
                                          std::map<int32_t, Value>* inputPtr,
                                          std::map<int32_t, Value>* outputPtr,
                                          const uint64_t txnId) {
   std::map<int32_t, Value>& ws = (*inputPtr);
   std::map<int32_t, Value>& output = (*outputPtr);
   /***
    * For a given warehouse number (W_ID), for each of the 10 districts (D_W_ID
    * , D_ID) within that warehouse, and for a given carrier number
    * (O_CARRIER_ID):
    *
    * The row in the NEW-ORDER table with matching NO_W_ID (equals W_ID) and
    * NO_D_ID (equals D_ID) and with the lowest NO_O_ID value is selected.
    *
    * This is the oldest undelivered order of that district. NO_O_ID, the order
    * number, is retrieved. If no matching row is found, then the delivery of an
    * order for this district is skipped.
    */
   {
      Value buf;
      mdb::Row* r = NULL;
      mdb::MultiBlob mbl(3), mbh(3);
      mbl[0] = ws[TPCC_VAR_D_ID].get_blob();
      mbh[0] = ws[TPCC_VAR_D_ID].get_blob();
      mbl[1] = ws[TPCC_VAR_W_ID].get_blob();
      mbh[1] = ws[TPCC_VAR_W_ID].get_blob();
      Value no_o_id_low(std::numeric_limits<i32>::min()),
          no_o_id_high(std::numeric_limits<i32>::max());
      mbl[2] = no_o_id_low.get_blob();
      mbh[2] = no_o_id_high.get_blob();

      mdb::ResultSet rs =
          dbHandler_.query_in(tbl_neworder_, mbl, mbh, mdb::ORD_ASC);
      Value o_id(0);
      if (rs.has_next()) {
         r = rs.next();
         dbHandler_.read_column(r, TPCC_COL_NEW_ORDER_NO_W_ID, &o_id);
         ws[TPCC_VAR_O_ID] = o_id;
      } else {
         ws[TPCC_VAR_O_ID] = Value((i32)-1);
      }
   }
   // Log_info("Piece-1 wssize=%u", ws.size());
   /***
    * The row in the ORDER table with matching O_W_ID (equals W_ ID), O_D_ID
    * (equals D_ID), and O_ID (equals NO_O_ID) is selected, O_C_ID, the customer
    * number, is retrieved, and O_CARRIER_ID is updated.
    */
   if (ws[TPCC_VAR_O_ID].get_i32() >= 0) {
      mdb::MultiBlob mb(3);
      // cell_locator_t cl(TPCC_TB_ORDER, 3);
      mb[0] = ws[TPCC_VAR_D_ID].get_blob();
      mb[1] = ws[TPCC_VAR_W_ID].get_blob();
      mb[2] = ws[TPCC_VAR_O_ID].get_blob();
      // Log::debug("Delivery: o_d_id: %d, o_w_id: %d, o_id: %d, hash: %u",
      // ws[2].get_i32(), ws[1].get_i32(), ws[0].get_i32(),
      // mdb::MultiBlob::hash()(cl.primary_key));
      mdb::Row* row_order = dbHandler_.query(tbl_order_, mb).next();
      dbHandler_.read_column(row_order, TPCC_COL_ORDER_O_C_ID,
                             &ws[TPCC_VAR_C_ID]);  // read o_c_id
      dbHandler_.write_column(row_order, TPCC_COL_ORDER_O_CARRIER_ID,
                              ws[TPCC_VAR_O_CARRIER_ID]);  // write o_carrier_id
   }
   // Log_info("Piece-2 wssize=%u", ws.size());

   /***
    * All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID),
    * OL_D_ID (equals O_D_ID), and OL_O_ID (equals O_ID) are selected. All
    * OL_DELIVERY_D, the delivery dates, are updated to the current system time
    * as returned by the operating system and the sum of all OL_AMOUNT is
    * retrieved.
    */
   {
      mdb::MultiBlob mbl = mdb::MultiBlob(4);
      mdb::MultiBlob mbh = mdb::MultiBlob(4);
      //    mdb::MultiBlob mbl(4), mbh(4);
      mbl[0] = ws[TPCC_VAR_D_ID].get_blob();
      mbh[0] = ws[TPCC_VAR_D_ID].get_blob();
      mbl[1] = ws[TPCC_VAR_W_ID].get_blob();
      mbh[1] = ws[TPCC_VAR_W_ID].get_blob();
      mbl[2] = ws[TPCC_VAR_O_ID].get_blob();
      mbh[2] = ws[TPCC_VAR_O_ID].get_blob();
      Value ol_number_low(std::numeric_limits<i32>::min()),
          ol_number_high(std::numeric_limits<i32>::max());
      mbl[3] = ol_number_low.get_blob();
      mbh[3] = ol_number_high.get_blob();

      mdb::ResultSet rs_ol =
          dbHandler_.query_in(tbl_orderline_, mbl, mbh, mdb::ORD_ASC);
      mdb::Row* row_ol = nullptr;
      std::vector<mdb::Row*> row_list;
      row_list.reserve(15);
      while (rs_ol.has_next()) {
         row_list.push_back(rs_ol.next());
      }

      std::vector<mdb::column_lock_t> column_locks;
      column_locks.reserve(2 * row_list.size());

      int i = 0;
      double ol_amount_buf = 0.0;

      while (i < row_list.size()) {
         row_ol = row_list[i++];
         Value buf(0.0);
         dbHandler_.read_column(row_ol, TPCC_COL_ORDER_LINE_OL_AMOUNT,
                                &buf);  // read ol_amount
         ol_amount_buf += buf.get_double();
         dbHandler_.write_column(row_ol, TPCC_COL_ORDER_LINE_OL_DELIVERY_D,
                                 Value(std::to_string(time(NULL))));
      }
      ws[TPCC_VAR_OL_AMOUNT] = Value(ol_amount_buf);
   }
   // Log_info("Piece-3 wssize=%u", ws.size());
   /**
    * The row in the CUSTOMER table with matching C_W_ID (equals W_ID), C_D_ID
    * (equals D_ID), and C_ID (equals O_C_ID) is selected and C_BALANCE is
    * increased by the sum of all order-line amounts (OL_AMOUNT) previously
    * retrieved. C_DELIVERY_CNT is incremented by 1.
    */
   {
      mdb::MultiBlob mb = mdb::MultiBlob(3);
      // cell_locator_t cl(TPCC_TB_CUSTOMER, 3);
      mb[0] = ws[TPCC_VAR_C_ID].get_blob();
      mb[1] = ws[TPCC_VAR_D_ID].get_blob();
      mb[2] = ws[TPCC_VAR_W_ID].get_blob();
      mdb::Row* row_customer = dbHandler_.query(tbl_customer_, mb).next();
      Value buf = Value(0.0);
      dbHandler_.read_column(row_customer, TPCC_COL_CUSTOMER_C_BALANCE, &buf);
      buf.set_double(buf.get_double() + ws[TPCC_VAR_OL_AMOUNT].get_double());
      dbHandler_.write_column(row_customer, TPCC_COL_CUSTOMER_C_BALANCE, buf);
      dbHandler_.read_column(row_customer, TPCC_COL_CUSTOMER_C_DELIVERY_CNT,
                             &buf);
      buf.set_i32(buf.get_i32() + (i32)1);
      dbHandler_.write_column(row_customer, TPCC_COL_CUSTOMER_C_DELIVERY_CNT,
                              buf);
   }
   // Log_info("Piece-4 wssize=%u", ws.size());

   output[TPCC_VAR_O_CARRIER_ID] = ws[TPCC_VAR_O_CARRIER_ID];
}

void TPCCStateMachine::ExecuteStockLevelTxn(
    const uint32_t txnType, const std::vector<int32_t>* localKeys,
    std::map<int32_t, Value>* inputPtr, std::map<int32_t, Value>* outputPtr,
    const uint64_t txnId) {
   std::map<int32_t, Value>& ws = (*inputPtr);
   std::map<int32_t, Value>& output = (*outputPtr);
   /**
    * The row in the DISTRICT table with matching D_W_ID and D_ID is selected
    * and D_NEXT_O_ID is retrieved
    */
   {
      mdb::MultiBlob mb(2);
      // cell_locator_t cl(TPCC_TB_DISTRICT, 2);
      mb[0] = ws[TPCC_VAR_D_ID].get_blob();
      mb[1] = ws[TPCC_VAR_W_ID].get_blob();
      mdb::Row* r = dbHandler_.query(tbl_dist_, mb).next();
      dbHandler_.read_column(r, TPCC_COL_DISTRICT_D_NEXT_O_ID,
                             &ws[TPCC_VAR_D_NEXT_O_ID]);
   }
   // Log_info("Piece-1 wssize=%u", ws.size());
   /***
    * All rows in the ORDER-LINE table with matching OL_W_ID (equals W_ID),
    OL_D_ID (equals D_ID), and OL_O_ID (lower than D_NEXT_O_ID and greater than
    or equal to D_NEXT_O_ID minus 20) are selected. They are the items for 20
    recent orders of the district.
   */
   uint32_t recentOrderNum = 0;
   {
      mdb::MultiBlob mbl(4), mbh(4);
      mbl[0] = ws[TPCC_VAR_D_ID].get_blob();
      mbh[0] = ws[TPCC_VAR_D_ID].get_blob();
      mbl[1] = ws[TPCC_VAR_W_ID].get_blob();
      mbh[1] = ws[TPCC_VAR_W_ID].get_blob();
      Value ol_o_id_low(ws[TPCC_VAR_D_NEXT_O_ID].get_i32() - (i32)21);
      mbl[2] = ol_o_id_low.get_blob();
      mbh[2] = ws[TPCC_VAR_D_NEXT_O_ID].get_blob();
      Value ol_number_low(std::numeric_limits<i32>::max()),
          ol_number_high(std::numeric_limits<i32>::min());
      mbl[3] = ol_number_low.get_blob();
      mbh[3] = ol_number_high.get_blob();

      mdb::ResultSet rs =
          dbHandler_.query_in(tbl_orderline_, mbl, mbh, mdb::ORD_ASC);
      // Log_debug(
      //     "stock_level: piece 1: d_next_o_id: %d, ol_w_id: %d, "
      //     "ol_d_id: %d",
      //     ws[TPCC_VAR_D_NEXT_O_ID].get_i32(),
      //     ws[TPCC_VAR_W_ID].get_i32(), ws[TPCC_VAR_D_ID].get_i32());

      std::vector<mdb::Row*> row_list;
      row_list.reserve(20);

      int i = 0;
      while (i++ < 20 && rs.has_next()) {
         row_list.push_back(rs.next());
      }

      std::vector<mdb::column_lock_t> column_locks;
      column_locks.reserve(row_list.size());

      for (int i = 0; i < row_list.size(); i++) {
         dbHandler_.read_column(row_list[i], TPCC_COL_ORDER_LINE_OL_I_ID,
                                &ws[TPCC_VAR_OL_I_ID(i)]);
      }
      ws[TPCC_VAR_OL_AMOUNT] = Value((int32_t)row_list.size());
      recentOrderNum = row_list.size();
   }
   // Log_info("Piece-2 wssize=%u", ws.size());
   /**
    * All rows in the STOCK table with matching S_I_ID (equals OL_I_ID) and
    * S_W_ID (equals W_ID) from the list of distinct item numbers and with
    * S_QUANTITY lower than threshold are counted (giving low_stock).
    */
   uint32_t lowStock = 0;
   for (int I = 0; I < recentOrderNum; I++) {
      Value buf(0);
      mdb::MultiBlob mb(2);
      mb[0] = ws[TPCC_VAR_OL_I_ID(I)].get_blob();
      mb[1] = ws[TPCC_VAR_W_ID].get_blob();
      mdb::Row* r = dbHandler_.query(tbl_stock_, mb).next();
      dbHandler_.read_column(r, TPCC_COL_STOCK_S_QUANTITY, &buf);

      if (buf.get_i32() < ws[TPCC_VAR_THRESHOLD].get_i32()) lowStock++;
   }
   // Log_info("Piece-3 wssize=%u", ws.size());

   output[TPCC_VAR_UNKOWN].set_i32(lowStock);
}

TPCCStateMachine::~TPCCStateMachine() {}

void TPCCStateMachine::SpecExecute(const uint32_t txnType,
                                   const std::vector<int32_t>* localKeys,
                                   std::map<int32_t, Value>* inputPtr,
                                   std::map<int32_t, Value>* outputPtr,
                                   const uint64_t txnId) {
   switch (txnType) {
      case TPCC_TXN_NEW_ORDER:
         SpecExecuteNewOrderTxn(txnType, localKeys, inputPtr, outputPtr, txnId);
         break;
      case TPCC_TXN_PAYMENT:
         SpecExecutePaymentTxn(txnType, localKeys, inputPtr, outputPtr, txnId);
         break;
      case TPCC_TXN_ORDER_STATUS:
         SpecExecuteOrderStatusTxn(txnType, localKeys, inputPtr, outputPtr,
                                   txnId);
         break;
      case TPCC_TXN_DELIVERY:
         SpecExecuteDeliveryTxn(txnType, localKeys, inputPtr, outputPtr, txnId);
         break;
      case TPCC_TXN_STOCK_LEVEL:
         SpecExecuteStockLevelTxn(txnType, localKeys, inputPtr, outputPtr,
                                  txnId);
         break;
      default:
         LOG(ERROR) << "Unrecognized txn type " << txnType;
         break;
   }
}

void TPCCStateMachine::SpecExecuteNewOrderTxn(
    const uint32_t txnType, const std::vector<int32_t>* localKeys,
    std::map<int32_t, Value>* inputPtr, std::map<int32_t, Value>* outputPtr,
    const uint64_t txnId) {

   std::map<int32_t, Value> itemInput;
   std::map<int32_t, Value> stockInput;
   std::map<int32_t, Value>& ws = (*inputPtr);
   std::map<int32_t, Value>& output = (*outputPtr);
   for (auto& kv : ws) {
      if (IS_S_W_ID(kv.first)) {
         stockInput[kv.first] = kv.second;
      }
   }
   /**
    * Piece 0
    * The row in the DISTRICT table with matching D_W_ID and D_ ID is selected
    * D_TAX, the district tax rate, is retrieved,
    * D_NEXT_O_ID, the next available order number for the district, is
    * retrieved and incremented by one.
    */
   bool isHomeShard =
       (shardId_ ==
        PartitionFromKey(ws[TPCC_VAR_W_ID], &tb_info_warehouse_, shardNum_));
   if (isHomeShard) {
      mdb::MultiBlob mb(2);
      mb[0] = ws[TPCC_VAR_D_ID].get_blob();
      mb[1] = ws[TPCC_VAR_W_ID].get_blob();
      mdb::Row* row_district = dbHandler_.query(tbl_dist_, mb).next();
      // R district
      dbHandler_.read_column(row_district, TPCC_COL_DISTRICT_D_TAX,
                             &output[TPCC_VAR_D_TAX]);

      dbHandler_.read_column(row_district, TPCC_COL_DISTRICT_D_NEXT_O_ID,
                             &output[TPCC_VAR_O_ID]);  // read d_next_o_id
      /** To Commit
      // read d_next_o_id, increment by 1
      Value buf(0);
      buf.set_i32((i32)(output[TPCC_VAR_O_ID].get_i32() + 1));
      dbHandler_.write_column(row_district, TPCC_COL_DISTRICT_D_NEXT_O_ID, buf);
      **/
   }
   // LOG(INFO) << "Piece-1 Done wsSize=" << ws.size();
   /**
    * Piece 1
    * The row in the WAREHOUSE table with matching W_ID is selected
    * and  W_TAX, the warehouse tax rate, is retrieved.
    */
   if (isHomeShard) {
      mdb::Row* row_warehouse =
          dbHandler_.query(tbl_warehouse_, ws[TPCC_VAR_W_ID].get_blob()).next();
      // R warehouse
      dbHandler_.read_column(row_warehouse, TPCC_COL_WAREHOUSE_W_TAX,
                             &output[TPCC_VAR_W_TAX]);  // read w_tax
   }
   // LOG(INFO) << "Piece-2 Done wsSize=" << ws.size();
   /**
    * Piece 2:
    * The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
    * selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
    * customer's last name, and C_CREDIT, the customer's credit status, are
    * retrieved.
    */
   if (isHomeShard) {
      mdb::MultiBlob mb(3);
      mb[0] = ws[TPCC_VAR_C_ID].get_blob();
      mb[1] = ws[TPCC_VAR_D_ID].get_blob();
      mb[2] = ws[TPCC_VAR_W_ID].get_blob();

      mdb::Row* row_customer = dbHandler_.query(tbl_customer_, mb).next();
      // R customer
      dbHandler_.read_column(row_customer, TPCC_COL_CUSTOMER_C_LAST,
                             &output[TPCC_VAR_C_LAST]);
      dbHandler_.read_column(row_customer, TPCC_COL_CUSTOMER_C_CREDIT,
                             &output[TPCC_VAR_C_CREDIT]);
      dbHandler_.read_column(row_customer, TPCC_COL_CUSTOMER_C_DISCOUNT,
                             &output[TPCC_VAR_C_DISCOUNT]);
   }
   // LOG(INFO) << "Piece-3 Done wsSize=" << ws.size();

   /**
    * Piece 3
    * A new row is inserted into both the NEW-ORDER table and the ORDER table
    to
    * reflect the creation of the new order. O_CARRIER_ID is set to a null
    * value. If the order includes only home order-lines, then O_ALL_LOCAL is
    * set to 1, otherwise O_ALL_LOCAL is set to 0.
    */
   /**
    * Here only insert into ORDER table
    * Next we will insert into NEW-ORDER table
    */
   if (isHomeShard) {
      // LOG(INFO) << "TPCC_NEW_ORDER, piece: " << TPCC_NEW_ORDER_3;

      output[TPCC_VAR_O_ENTRY_D] = Value(std::to_string(time(NULL)));

      /*** To Commit */
      /****
      mdb::MultiBlob mb(3);
      mb[0] = ws[TPCC_VAR_D_ID].get_blob();
      mb[1] = ws[TPCC_VAR_W_ID].get_blob();
      mb[2] = ws[TPCC_VAR_C_ID].get_blob();
       mdb::Row* r = dbHandler_.query(tbl_order_, mb).next();
      // W order
      std::vector<Value> row_data = {
          ws.at(TPCC_VAR_D_ID),       ws[TPCC_VAR_W_ID],
          output.at(TPCC_VAR_O_ID),   ws[TPCC_VAR_C_ID],
          output[TPCC_VAR_O_ENTRY_D],  // o_entry_d
          ws[TPCC_VAR_O_CARRIER_ID],  ws[TPCC_VAR_OL_CNT],
          ws[TPCC_VAR_O_ALL_LOCAL]};
      // LOG(INFO) << "done r->table=" << r->get_table()->Name();
      r = mdb::VersionedRow::create(tbl_order_->schema(), row_data);
      verify(r->schema_);
      dbHandler_.insert_row(tbl_order_, r);

      // LOG(INFO) << "Piece-4-1 Done wsSize=" << ws.size();

      // Piece-3-continue: insert into NEW-ORDER table
      // W new_order
      std::vector<Value> row_data2 = {
          ws[TPCC_VAR_D_ID],      // o_d_id
          ws[TPCC_VAR_W_ID],      // o_w_id
          output[TPCC_VAR_O_ID],  // o_id
      };

      mdb::Row* r2 =
          mdb::VersionedRow::create(tbl_neworder_->schema(), row_data2);
      dbHandler_.insert_row(tbl_neworder_, r2);
      **/
   }
   // LOG(INFO) << "Piece-5-1 Done wsSize=" << ws.size();
   /**
    * Piece-4
    * The number of items, O_OL_CNT, is computed to match ol_cnt.
    * For each O_OL_CNT item on the order:
        - The row in the ITEM table with matching I_ID (equals OL_I_ID) is
    selected and I_PRICE, the price of the item, I_NAME, the name of the
    item, and I_DATA are retrieved.
    */

   /**
    * Piece-4-2
    * The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and
    * S_W_ID (equals OL_SUPPLY_W_ID) is selected. S_QUANTITY, the quantity in
    * stock, S_DIST_xx, where xx represents the district number, and S_DATA
    are
    * retrieved.
    */

   for (auto& kv : stockInput) {
      if (shardId_ != PartitionFromKey(kv.second, &tb_info_stock_, shardNum_)) {
         continue;
      }
      int I = S_W_ID_POS(kv.first);
      // LOG(INFO) << "TPCC_NEW_ORDER, piece: " << TPCC_NEW_ORDER_RS(I)
      //           << "-- I=" << I;
      mdb::MultiBlob mb(2);
      mb[0] = ws[TPCC_VAR_I_ID(I)].get_blob();
      mb[1] = ws[TPCC_VAR_S_W_ID(I)].get_blob();
      int32_t w_id = ws[TPCC_VAR_S_W_ID(I)].get_i32();
      int32_t i_id = ws[TPCC_VAR_I_ID(I)].get_i32();
      // LOG(INFO) << "new order read stock. item_id: " << i_id
      //           << " s_w_id: " << w_id;

      mdb::Row* r = dbHandler_.query(tbl_stock_, mb).next();
      verify(r->schema_);
      //  Ri stock
      //  FIXME compress all s_dist_xx into one column
      dbHandler_.read_column(
          r, TPCC_COL_STOCK_S_DIST_XX,
          &output[TPCC_VAR_OL_DIST_INFO(I)]);  // 0 ==> s_dist_xx
      dbHandler_.read_column(r, TPCC_COL_STOCK_S_DATA,
                             &output[TPCC_VAR_S_DATA(I)]);  // 1 ==> s_data
   }

   // LOG(INFO) << "Piece-5-2 Done wsSize=" << ws.size();
   /**
    * If the retrieved value for S_QUANTITY exceeds OL_QUANTITY by 10
    * or more, then S_QUANTITY is decreased by OL_QUANTITY; otherwise
    S_QUANTITY
    * is updated to (S_QUANTITY - OL_QUANTITY)+91. S_YTD is increased by
    * OL_QUANTITY and S_ORDER_CNT is incremented by 1. If the order-line is
    * remote, then S_REMOTE_CNT is incremented by 1.
    */

   for (auto& kv : stockInput) {
      if (shardId_ != PartitionFromKey(kv.second, &tb_info_stock_, shardNum_)) {
         continue;
      }
      int I = S_W_ID_POS(kv.first);
      mdb::Row* r = NULL;
      mdb::MultiBlob mb(2);
      mb[0] = ws[TPCC_VAR_I_ID(I)].get_blob();
      mb[1] = ws[TPCC_VAR_S_W_ID(I)].get_blob();
      r = dbHandler_.query(tbl_stock_, mb).next();
      // Ri stock
      Value buf(0);
      dbHandler_.read_column(r, TPCC_COL_STOCK_S_QUANTITY, &buf);
      int32_t new_ol_quantity =
          buf.get_i32() - ws[TPCC_VAR_OL_QUANTITY(I)].get_i32();

      dbHandler_.read_column(r, TPCC_COL_STOCK_S_YTD, &buf);
      Value new_s_ytd(buf.get_i32() + ws[TPCC_VAR_OL_QUANTITY(I)].get_i32());

      dbHandler_.read_column(r, TPCC_COL_STOCK_S_ORDER_CNT, &buf);
      Value new_s_order_cnt((i32)(buf.get_i32() + 1));

      dbHandler_.read_column(r, TPCC_COL_STOCK_S_REMOTE_CNT, &buf);
      Value new_s_remote_cnt(buf.get_i32() +
                             ws[TPCC_VAR_S_REMOTE_CNT(I)].get_i32());

      if (new_ol_quantity < 10) new_ol_quantity += 91;
      Value new_ol_quantity_value(new_ol_quantity);
      output[TPCC_VAR_AT_S_QUANTITY(I)] = new_ol_quantity_value;

      // intermediate result
      output[TPCC_VAR_STOCK_QUANTITY(I)] = buf;

      /** To Commit */
      /****
      dbHandler_.write_columns(
          r,
          {TPCC_COL_STOCK_S_QUANTITY, TPCC_COL_STOCK_S_YTD,
           TPCC_COL_STOCK_S_ORDER_CNT, TPCC_COL_STOCK_S_REMOTE_CNT},
          {new_ol_quantity_value, new_s_ytd, new_s_order_cnt,
           new_s_remote_cnt});
      **/
   }

   // LOG(INFO) << "Piece-5-3 Done wsSize=" << ws.size();
   /**
    * A new row is inserted into the ORDER-LINE table to reflect the item on
    the
    * order. OL_DELIVERY_D is set to a null value, OL_NUMBER is set to a
    unique
    * value within all the ORDER-LINE rows that have the same OL_O_ID value,
    and
    * OL_DIST_INFO is set to the content of S_DIST_xx, where xx represents
    the
    * district number (OL_D_ID)
    */
   // item table are fully replicated
   if (isHomeShard) {
      for (auto& kv : stockInput) {
         int I = S_W_ID_POS(kv.first);
         mdb::Row* row_item =
             dbHandler_.query(tbl_item_, ws[TPCC_VAR_I_ID(I)].get_blob())
                 .next();
         // Ri item
         dbHandler_.read_column(row_item, TPCC_COL_ITEM_I_NAME,
                                &output[TPCC_VAR_I_NAME(I)]);
         dbHandler_.read_column(row_item, TPCC_COL_ITEM_I_PRICE,
                                &output[TPCC_VAR_I_PRICE(I)]);
         dbHandler_.read_column(row_item, TPCC_COL_ITEM_I_DATA,
                                &output[TPCC_VAR_I_DATA(I)]);

         /*** To Commit */
         /****
         double am = (double)ws[TPCC_VAR_OL_QUANTITY(I)].get_i32() *
                     output[TPCC_VAR_I_PRICE(I)].get_double();
         Value amount = Value(am);
         Value xxx = Value("");
         mdb::Row* r = mdb::VersionedRow::create(
             tbl_orderline_->schema(), vector<Value>({
                                           ws[TPCC_VAR_D_ID],
                                           ws[TPCC_VAR_W_ID],
                                           output[TPCC_VAR_O_ID],
                                           ws[TPCC_VAR_OL_NUMBER(I)],
                                           ws[TPCC_VAR_I_ID(I)],
                                           ws[TPCC_VAR_S_W_ID(I)],
                                           ws[TPCC_VAR_OL_DELIVER_D(I)],
                                           ws[TPCC_VAR_OL_QUANTITY(I)],
                                           amount,
                                           xxx,
                                       }));
         // LOG(INFO) << "Piece-5-2-Mid Done wsSize=" << ws.size();
         dbHandler_.insert_row(tbl_orderline_, r);
         **/
      }
      // LOG(INFO) << "Piece-5-4 Done wsSize=" << ws.size();
   }

   // Only fetch those key-values that is not submitted by clients, do not
   // include redundant info
}

void TPCCStateMachine::SpecExecutePaymentTxn(
    const uint32_t txnType, const std::vector<int32_t>* localKeys,
    std::map<int32_t, Value>* inputPtr, std::map<int32_t, Value>* outputPtr,
    const uint64_t txnId) {
   std::map<int32_t, Value>& ws = (*inputPtr);
   std::map<int32_t, Value>& output = (*outputPtr);
   bool isHomeShard =
       (shardId_ ==
        PartitionFromKey(ws[TPCC_VAR_W_ID], &tb_info_warehouse_, shardNum_));

   output[ABORT_FLAG] = 0;
   if (ws.count(TPCC_VAR_C_LAST_VERSION) > 0) {
      bool isCustomerShard =
          (shardId_ == PartitionFromKey(ws[TPCC_VAR_C_W_ID],
                                        &tb_info_warehouse_, shardNum_));
      if (isCustomerShard) {
         std::shared_lock lck(cid_mtx_);
         if (ws[TPCC_VAR_C_LAST_VERSION].get_i32() !=
             version_track_[TPCC_VAR_C_LAST_VERSION]) {
            output[ABORT_FLAG] = 1;
            return;
         }
      }
   }

   /**
    * The row in the WAREHOUSE table with matching W_ID is selected. W_NAME,
    * W_STREET_1, W_STREET_2, W_CITY, W_STATE, and W_ZIP are retrieved
    */
   if (isHomeShard) {
      mdb::Row* row_warehouse =
          dbHandler_.query(tbl_warehouse_, ws[TPCC_VAR_W_ID].get_blob()).next();
      // R warehouse
      dbHandler_.read_column(row_warehouse, TPCC_COL_WAREHOUSE_W_NAME,
                             &output[TPCC_VAR_W_NAME]);
      dbHandler_.read_column(row_warehouse, TPCC_COL_WAREHOUSE_W_STREET_1,
                             &output[TPCC_VAR_W_STREET_1]);
      dbHandler_.read_column(row_warehouse, TPCC_COL_WAREHOUSE_W_STREET_2,
                             &output[TPCC_VAR_W_STREET_2]);
      dbHandler_.read_column(row_warehouse, TPCC_COL_WAREHOUSE_W_CITY,
                             &output[TPCC_VAR_W_CITY]);
      dbHandler_.read_column(row_warehouse, TPCC_COL_WAREHOUSE_W_STATE,
                             &output[TPCC_VAR_W_STATE]);
      dbHandler_.read_column(row_warehouse, TPCC_COL_WAREHOUSE_W_ZIP,
                             &output[TPCC_VAR_W_ZIP]);
   }
   /***
    * The row in the DISTRICT table with matching D_W_ID and D_ID is
    selected.
    * D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are
    retrieved
    */
   if (isHomeShard) {
      Value buf;
      mdb::MultiBlob mb(2);
      mb[0] = ws[TPCC_VAR_D_ID].get_blob();
      mb[1] = ws[TPCC_VAR_W_ID].get_blob();
      mdb::Row* row_district = dbHandler_.query(tbl_dist_, mb).next();
      ws[TPCC_VAR_D_NAME] = Value("");

      // R district
      dbHandler_.read_column(row_district, TPCC_COL_DISTRICT_D_NAME,
                             &output[TPCC_VAR_D_NAME]);
      dbHandler_.read_column(row_district, TPCC_COL_DISTRICT_D_STREET_1,
                             &output[TPCC_VAR_D_STREET_1]);
      dbHandler_.read_column(row_district, TPCC_COL_DISTRICT_D_STREET_2,
                             &output[TPCC_VAR_D_STREET_2]);
      dbHandler_.read_column(row_district, TPCC_COL_DISTRICT_D_CITY,
                             &output[TPCC_VAR_D_CITY]);
      dbHandler_.read_column(row_district, TPCC_COL_DISTRICT_D_STATE,
                             &output[TPCC_VAR_D_STATE]);
      dbHandler_.read_column(row_district, TPCC_COL_DISTRICT_D_ZIP,
                             &output[TPCC_VAR_D_ZIP]);
   }
   /***
    * D_YTD, the district's year-to-date balance, is increased by H_AMOUNT.
    *
    * [Original] W_YTD, the warehouse's year-to-date balance, is increased
    by H_ AMOUNT.
    */

   /*** To Commit */
   /****
   if (isHomeShard) {
      Value buf_temp(0.0);
      mdb::Row* row_temp = NULL;
      mdb::MultiBlob mb_temp(2);
      mb_temp[0] = ws[TPCC_VAR_D_ID].get_blob();
      mb_temp[1] = ws[TPCC_VAR_W_ID].get_blob();
      row_temp = dbHandler_.query(tbl_dist_, mb_temp).next();
      verify(row_temp->schema_ != nullptr);
      dbHandler_.read_column(row_temp, TPCC_COL_DISTRICT_D_YTD, &buf_temp);
      // W district
      Value buf(0.0);
      buf.set_double(buf_temp.get_double() +
                     ws[TPCC_VAR_H_AMOUNT].get_double());
      dbHandler_.write_column(row_temp, TPCC_COL_DISTRICT_D_YTD, buf_temp);
   }
   ***/

   /**
    *   // piece 5, R customer secondary index, c_last -> c_id
    *    CustomerIndex is fully replicated
    */

   if (ws.find(TPCC_VAR_C_ID) == ws.end()) {
      // Log_info("Option-2");
      mdb::MultiBlob mbl(3), mbh(3);
      mbl[0] = ws[TPCC_VAR_C_D_ID].get_blob();
      mbh[0] = ws[TPCC_VAR_C_D_ID].get_blob();
      mbl[1] = ws[TPCC_VAR_C_W_ID].get_blob();
      mbh[1] = ws[TPCC_VAR_C_W_ID].get_blob();
      Value c_id_low(std::numeric_limits<i32>::min());
      Value c_id_high(std::numeric_limits<i32>::max());
      mbl[2] = c_id_low.get_blob();
      mbh[2] = c_id_high.get_blob();
      // LOG(INFO) << "TPCC_VAR_C_LAST no exist?="
      //           << (ws.find(TPCC_VAR_C_LAST) == ws.end());
      c_last_id_t key_low(ws[TPCC_VAR_C_LAST].get_str(), mbl, &(C_LAST_SCHEMA));
      c_last_id_t key_high(ws[TPCC_VAR_C_LAST].get_str(), mbh,
                           &(C_LAST_SCHEMA));
      std::multimap<c_last_id_t, rrr::i32>::iterator it, it_low, it_high,
          it_mid;
      bool inc = false, mid_set = false;

      it_low = C_LAST2ID.lower_bound(key_low);
      it_high = C_LAST2ID.upper_bound(key_high);
      int n_c = 0;
      for (it = it_low; it != it_high; it++) {
         n_c++;
         if (mid_set)
            if (inc) {
               it_mid++;
               inc = false;
            } else
               inc = true;
         else {
            mid_set = true;
            it_mid = it;
         }
      }
      verify(mid_set);
      ws[TPCC_VAR_C_ID] = Value(it_mid->second);
      // LOG(INFO) << "c_id=" << ws[TPCC_VAR_C_ID].get_i32();
   }

   // Log_info("Piece-5-1 wssize=%u", ws.size());
   /*** To Commit */
   /***
    * A new row is inserted into the HISTORY table with H_C_ID = C_ID,
    H_C_D_ID
    * = C_D_ID, H_C_W_ID = C_W_ID, H_D_ID = D_ID, and H_W_ID = W_ID.
    */
   /***
   assert(ws.find(TPCC_VAR_H_KEY) != ws.end());
   if (isHomeShard) {
      // insert history
      std::vector<Value> row_data(9);
      row_data[0] = ws[TPCC_VAR_H_KEY];  // h_key
      // if (ws.find(TPCC_VAR_C_ID) == ws.end()) {
      //    // hard code
      //    ws[TPCC_VAR_C_ID].set_i32(1);
      // }
      row_data[1] = ws[TPCC_VAR_C_ID];                  // h_c_id   =>  c_id
      row_data[2] = ws[TPCC_VAR_C_D_ID];                // h_c_d_id => c_d_id
      row_data[3] = ws[TPCC_VAR_C_W_ID];                // h_c_w_id => c_w_id
      row_data[4] = ws[TPCC_VAR_D_ID];                  // h_d_id   =>  d_id
      row_data[5] = ws[TPCC_VAR_W_ID];                  // h_d_w_id => d_w_id
      row_data[6] = Value(std::to_string(time(NULL)));  // h_date
      output[TPCC_VAR_H_DATE] = row_data[6];
      row_data[7] = ws[TPCC_VAR_H_AMOUNT];  // h_amount => h_amount
      row_data[8] =
          Value(output[TPCC_VAR_W_NAME].get_str() + "    " +
                output[TPCC_VAR_D_NAME].get_str());  // d_data => w_name +
                                                     // 4spaces + d_name
      mdb::Row* row_history =
          mdb::VersionedRow::create(tbl_history_->schema(), row_data);
      dbHandler_.insert_row(tbl_history_, row_history);
   }
   // Log_info("Piece-5 wssize=%u", ws.size());
   **/

   // Log_info("Piece-3 wssize=%u", ws.size());
   bool isCustomerShard =
       (shardId_ ==
        PartitionFromKey(ws[TPCC_VAR_C_W_ID], &tb_info_warehouse_, shardNum_));
   if (!isCustomerShard) {
      return;
   }
   /**
    * If the value of C_CREDIT is equal to "BC"[changed], then C_DATA is also
    * retrieved from the selected customer and the following history
    * information: C_ID, C_D_ID, C_W_ID, D_ID, W_ID, and H_AMOUNT, are
    inserted
    * at the left of the C_DATA field by shifting the existing content of
    C_DATA
    * to the right by an equal number of bytes and by discarding the bytes
    that
    * are shifted out of the right side of the C_DATA field. The content of
    the
    * C_DATA field never exceeds 500 characters. The selected customer is
    * updated with the new C_DATA field. If C_DATA is implemented as two
    fields
    * (see Clause 1.4.9), they must be treated and operated on as one single
    * field.
    */

   {

      mdb::Row* r = NULL;
      mdb::MultiBlob mb(3);
      // cell_locator_t cl(TPCC_TB_CUSTOMER, 3);
      mb[0] = ws[TPCC_VAR_C_ID].get_blob();
      mb[1] = ws[TPCC_VAR_C_D_ID].get_blob();
      mb[2] = ws[TPCC_VAR_C_W_ID].get_blob();
      // R customer
      r = dbHandler_.query(tbl_customer_, mb).next();
      ALock::type_t lock_20_type = ALock::RLOCK;
      if (ws[TPCC_VAR_C_ID].get_i32() % 10 == 0) lock_20_type = ALock::WLOCK;

      vector<Value> buf({Value(""), Value(""), Value(""), Value(""), Value(""),
                         Value(""), Value(""), Value(""), Value(""), Value(""),
                         Value(""), Value(""), Value(""), Value(0.0),
                         Value(0.0), Value("")});
      int oi = 0;
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_FIRST, &buf[0]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_MIDDLE, &buf[1]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_LAST, &buf[2]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_STREET_1, &buf[3]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_STREET_2, &buf[4]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_CITY, &buf[5]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_STATE, &buf[6]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_ZIP, &buf[7]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_PHONE, &buf[8]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_SINCE, &buf[9]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_CREDIT, &buf[10]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_CREDIT_LIM, &buf[11]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_DISCOUNT, &buf[12]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_BALANCE, &buf[13]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_YTD_PAYMENT, &buf[14]);
      dbHandler_.read_column(r, TPCC_COL_CUSTOMER_C_DATA, &buf[15]);

      /** To Commit */
      /*****
      // if c_credit == "BC" (bad) 10%
      // here we use c_id to pick up 10% instead of c_credit
      if (ws[TPCC_VAR_C_ID].get_i32() % 10 == 0) {
         Value c_data(
             (to_string(ws[TPCC_VAR_C_ID]) + to_string(ws[TPCC_VAR_C_D_ID]) +
              to_string(ws[TPCC_VAR_C_W_ID]) + to_string(ws[TPCC_VAR_D_ID]) +
              to_string(ws[TPCC_VAR_W_ID]) + to_string(ws[TPCC_VAR_H_AMOUNT]) +
              buf[15].get_str())
                 .substr(0, 500));
         std::vector<mdb::column_id_t> col_ids = {
             TPCC_COL_CUSTOMER_C_BALANCE, TPCC_COL_CUSTOMER_C_YTD_PAYMENT,
             TPCC_COL_CUSTOMER_C_DATA};
         std::vector<Value> col_data(
             {Value(buf[13].get_double() - ws[TPCC_VAR_H_AMOUNT].get_double()),
              Value(buf[14].get_double() + ws[TPCC_VAR_H_AMOUNT].get_double()),
              c_data});
         dbHandler_.write_columns(r, col_ids, col_data);
      } else {
         std::vector<mdb::column_id_t> col_ids(
             {TPCC_COL_CUSTOMER_C_BALANCE, TPCC_COL_CUSTOMER_C_YTD_PAYMENT});
         std::vector<Value> col_data(
             {Value(buf[13].get_double() - ws[TPCC_VAR_H_AMOUNT].get_double()),
              Value(buf[14].get_double() +
                    ws[TPCC_VAR_H_AMOUNT].get_double())});
         dbHandler_.write_columns(r, col_ids, col_data);
      }
      **/
      /** Intermediate results */
      output[TPCC_COL_CUSTOMER_C_BALANCE] = buf[13];
      output[TPCC_COL_CUSTOMER_C_YTD_PAYMENT] = buf[14];
      output[TPCC_COL_CUSTOMER_C_DATA] = buf[15];
      ////////////////////////////////////////////

      output[TPCC_VAR_C_FIRST] = buf[0];
      output[TPCC_VAR_C_MIDDLE] = buf[1];
      output[TPCC_VAR_C_LAST] = buf[2];
      output[TPCC_VAR_C_STREET_1] = buf[3];
      output[TPCC_VAR_C_STREET_2] = buf[4];
      output[TPCC_VAR_C_CITY] = buf[5];
      output[TPCC_VAR_C_STATE] = buf[6];
      output[TPCC_VAR_C_ZIP] = buf[7];
      output[TPCC_VAR_C_PHONE] = buf[8];
      output[TPCC_VAR_C_SINCE] = buf[9];
      output[TPCC_VAR_C_CREDIT] = buf[10];
      output[TPCC_VAR_C_CREDIT_LIM] = buf[11];
      output[TPCC_VAR_C_DISCOUNT] = buf[12];
      output[TPCC_VAR_C_BALANCE] =
          Value(buf[13].get_double() - ws[TPCC_VAR_H_AMOUNT].get_double());
   }
   // Log_info("Piece-6 wssize=%u", ws.size());

   // output[TPCC_VAR_C_DATA] = ws[TPCC_VAR_C_DATA]; // can be computed locally
   // by terminal
}

void TPCCStateMachine::SpecExecuteOrderStatusTxn(
    const uint32_t txnType, const std::vector<int32_t>* localKeys,
    std::map<int32_t, Value>* inputPtr, std::map<int32_t, Value>* outputPtr,
    const uint64_t txnId) {
   // Read-only txn, spec is same as normal
   ExecuteOrderStatusTxn(txnType, localKeys, inputPtr, outputPtr, txnId);
}

void TPCCStateMachine::SpecExecuteDeliveryTxn(
    const uint32_t txnType, const std::vector<int32_t>* localKeys,
    std::map<int32_t, Value>* inputPtr, std::map<int32_t, Value>* outputPtr,
    const uint64_t txnId) {
   std::map<int32_t, Value>& ws = (*inputPtr);
   std::map<int32_t, Value>& output = (*outputPtr);
   /***
    * For a given warehouse number (W_ID), for each of the 10 districts (D_W_ID
    * , D_ID) within that warehouse, and for a given carrier number
    * (O_CARRIER_ID):
    *
    * The row in the NEW-ORDER table with matching NO_W_ID (equals W_ID) and
    * NO_D_ID (equals D_ID) and with the lowest NO_O_ID value is selected.
    *
    * This is the oldest undelivered order of that district. NO_O_ID, the order
    * number, is retrieved. If no matching row is found, then the delivery of an
    * order for this district is skipped.
    */
   {
      Value buf;
      mdb::Row* r = NULL;
      mdb::MultiBlob mbl(3), mbh(3);
      mbl[0] = ws[TPCC_VAR_D_ID].get_blob();
      mbh[0] = ws[TPCC_VAR_D_ID].get_blob();
      mbl[1] = ws[TPCC_VAR_W_ID].get_blob();
      mbh[1] = ws[TPCC_VAR_W_ID].get_blob();
      Value no_o_id_low(std::numeric_limits<i32>::min()),
          no_o_id_high(std::numeric_limits<i32>::max());
      mbl[2] = no_o_id_low.get_blob();
      mbh[2] = no_o_id_high.get_blob();

      mdb::ResultSet rs =
          dbHandler_.query_in(tbl_neworder_, mbl, mbh, mdb::ORD_ASC);
      Value o_id(0);
      if (rs.has_next()) {
         r = rs.next();
         dbHandler_.read_column(r, TPCC_COL_NEW_ORDER_NO_W_ID, &o_id);
         ws[TPCC_VAR_O_ID] = o_id;
      } else {
         ws[TPCC_VAR_O_ID] = Value((i32)-1);
      }
   }
   // Log_info("Piece-1 wssize=%u", ws.size());

   output[TPCC_VAR_O_CARRIER_ID] = ws[TPCC_VAR_O_CARRIER_ID];
}

void TPCCStateMachine::SpecExecuteStockLevelTxn(
    const uint32_t txnType, const std::vector<int32_t>* localKeys,
    std::map<int32_t, Value>* inputPtr, std::map<int32_t, Value>* outputPtr,
    const uint64_t txnId) {
   // Read-only txns same as normal execute
   ExecuteStockLevelTxn(txnType, localKeys, inputPtr, outputPtr, txnId);
}

void TPCCStateMachine::CommitExecute(const uint32_t txnType,
                                     const std::vector<int32_t>* localKeys,
                                     std::map<int32_t, Value>* inputPtr,
                                     std::map<int32_t, Value>* outputPtr,
                                     const uint64_t txnId) {
   switch (txnType) {
      case TPCC_TXN_NEW_ORDER:
         CommitNewOrderTxn(txnType, localKeys, inputPtr, outputPtr, txnId);
         break;
      case TPCC_TXN_PAYMENT:
         CommitPaymentTxn(txnType, localKeys, inputPtr, outputPtr, txnId);
         break;
      case TPCC_TXN_ORDER_STATUS:
         CommitOrderStatusTxn(txnType, localKeys, inputPtr, outputPtr, txnId);
         break;
      case TPCC_TXN_DELIVERY:
         CommitDeliveryTxn(txnType, localKeys, inputPtr, outputPtr, txnId);
         break;
      case TPCC_TXN_STOCK_LEVEL:
         CommitStockLevelTxn(txnType, localKeys, inputPtr, outputPtr, txnId);
         break;
      default:
         LOG(ERROR) << "Unrecognized txn type " << txnType;
         break;
   }
}

void TPCCStateMachine::CommitNewOrderTxn(const uint32_t txnType,
                                         const std::vector<int32_t>* localKeys,
                                         std::map<int32_t, Value>* inputPtr,
                                         std::map<int32_t, Value>* outputPtr,
                                         const uint64_t txnId) {

   LOG(INFO) << "Not Implemented So far";
   std::map<int32_t, Value> itemInput;
   std::map<int32_t, Value> stockInput;
   std::map<int32_t, Value>& ws = (*inputPtr);
   std::map<int32_t, Value>& output = (*outputPtr);
   for (auto& kv : ws) {
      if (IS_S_W_ID(kv.first)) {
         stockInput[kv.first] = kv.second;
      }
   }
   bool isHomeShard =
       (shardId_ ==
        PartitionFromKey(ws[TPCC_VAR_W_ID], &tb_info_warehouse_, shardNum_));

   if (!isHomeShard) {
      return;
   }
   // Commit Piece 0

   mdb::MultiBlob mb(2);
   mb[0] = ws[TPCC_VAR_D_ID].get_blob();
   mb[1] = ws[TPCC_VAR_W_ID].get_blob();
   mdb::Row* row_district = dbHandler_.query(tbl_dist_, mb).next();
   Value buf(0);
   buf.set_i32((i32)(output[TPCC_VAR_O_ID].get_i32() + 1));
   dbHandler_.write_column(row_district, TPCC_COL_DISTRICT_D_NEXT_O_ID, buf);

   // Commit Piece 3
   mb[2] = ws[TPCC_VAR_C_ID].get_blob();
   mdb::Row* r = dbHandler_.query(tbl_order_, mb).next();
   // W order
   std::vector<Value> row_data = {
       ws.at(TPCC_VAR_D_ID),       ws[TPCC_VAR_W_ID],
       output.at(TPCC_VAR_O_ID),   ws[TPCC_VAR_C_ID],
       output[TPCC_VAR_O_ENTRY_D],  // o_entry_d
       ws[TPCC_VAR_O_CARRIER_ID],  ws[TPCC_VAR_OL_CNT],
       ws[TPCC_VAR_O_ALL_LOCAL]};
   // LOG(INFO) << "done r->table=" << r->get_table()->Name();
   r = mdb::VersionedRow::create(tbl_order_->schema(), row_data);
   verify(r->schema_);
   dbHandler_.insert_row(tbl_order_, r);

   // LOG(INFO) << "Piece-4-1 Done wsSize=" << ws.size();

   // Piece-3-continue: insert into NEW-ORDER table
   // W new_order
   std::vector<Value> row_data2 = {
       ws[TPCC_VAR_D_ID],      // o_d_id
       ws[TPCC_VAR_W_ID],      // o_w_id
       output[TPCC_VAR_O_ID],  // o_id
   };

   mdb::Row* r2 = mdb::VersionedRow::create(tbl_neworder_->schema(), row_data2);
   dbHandler_.insert_row(tbl_neworder_, r2);

   // Commit Piece 5
   for (auto& kv : stockInput) {
      if (shardId_ != PartitionFromKey(kv.second, &tb_info_stock_, shardNum_)) {
         continue;
      }
      int I = S_W_ID_POS(kv.first);
      mdb::Value buf = output[TPCC_VAR_AT_S_QUANTITY(I)];
      mdb::Value new_ol_quantity_value = output[TPCC_VAR_AT_S_QUANTITY(I)];
      mdb::Value new_s_ytd(buf.get_i32() +
                           ws[TPCC_VAR_OL_QUANTITY(I)].get_i32());
      mdb::Value new_s_order_cnt((i32)(buf.get_i32() + 1));
      mdb::Value new_s_remote_cnt(buf.get_i32() +
                                  ws[TPCC_VAR_S_REMOTE_CNT(I)].get_i32());
      dbHandler_.write_columns(
          r,
          {TPCC_COL_STOCK_S_QUANTITY, TPCC_COL_STOCK_S_YTD,
           TPCC_COL_STOCK_S_ORDER_CNT, TPCC_COL_STOCK_S_REMOTE_CNT},
          {new_ol_quantity_value, new_s_ytd, new_s_order_cnt,
           new_s_remote_cnt});
   }

   for (auto& kv : stockInput) {
      int I = S_W_ID_POS(kv.first);
      double am = (double)ws[TPCC_VAR_OL_QUANTITY(I)].get_i32() *
                  output[TPCC_VAR_I_PRICE(I)]
                      .get_double(); /** Janus code misses this */
      Value amount = Value(am);
      Value xxx = Value("");
      mdb::Row* r = mdb::VersionedRow::create(tbl_orderline_->schema(),
                                              vector<Value>({
                                                  ws[TPCC_VAR_D_ID],
                                                  ws[TPCC_VAR_W_ID],
                                                  output[TPCC_VAR_O_ID],
                                                  ws[TPCC_VAR_OL_NUMBER(I)],
                                                  ws[TPCC_VAR_I_ID(I)],
                                                  ws[TPCC_VAR_S_W_ID(I)],
                                                  ws[TPCC_VAR_OL_DELIVER_D(I)],
                                                  ws[TPCC_VAR_OL_QUANTITY(I)],
                                                  amount,
                                                  xxx,
                                              }));
      // LOG(INFO) << "Piece-5-2-Mid Done wsSize=" << ws.size();
      dbHandler_.insert_row(tbl_orderline_, r);
   }
}

void TPCCStateMachine::CommitPaymentTxn(const uint32_t txnType,
                                        const std::vector<int32_t>* localKeys,
                                        std::map<int32_t, Value>* inputPtr,
                                        std::map<int32_t, Value>* outputPtr,
                                        const uint64_t txnId) {
   std::map<int32_t, Value>& ws = (*inputPtr);
   std::map<int32_t, Value>& output = (*outputPtr);
   bool isHomeShard =
       (shardId_ ==
        PartitionFromKey(ws[TPCC_VAR_W_ID], &tb_info_warehouse_, shardNum_));
   if (!isHomeShard) {
      return;
   }

   {
      Value buf_temp(0.0);
      mdb::Row* row_temp = NULL;
      mdb::MultiBlob mb_temp(2);
      mb_temp[0] = ws[TPCC_VAR_D_ID].get_blob();
      mb_temp[1] = ws[TPCC_VAR_W_ID].get_blob();
      row_temp = dbHandler_.query(tbl_dist_, mb_temp).next();
      verify(row_temp->schema_ != nullptr);
      dbHandler_.read_column(row_temp, TPCC_COL_DISTRICT_D_YTD, &buf_temp);
      // W district
      Value buf(0.0);
      buf.set_double(buf_temp.get_double() +
                     ws[TPCC_VAR_H_AMOUNT].get_double());
      dbHandler_.write_column(row_temp, TPCC_COL_DISTRICT_D_YTD, buf_temp);
   }

   {
      /***
    * A new row is inserted into the HISTORY table with H_C_ID = C_ID,
    H_C_D_ID
    * = C_D_ID, H_C_W_ID = C_W_ID, H_D_ID = D_ID, and H_W_ID = W_ID.
    */
      assert(ws.find(TPCC_VAR_H_KEY) != ws.end());

      // insert history
      std::vector<Value> row_data(9);
      row_data[0] = ws[TPCC_VAR_H_KEY];  // h_key
      // if (ws.find(TPCC_VAR_C_ID) == ws.end()) {
      //    // hard code
      //    ws[TPCC_VAR_C_ID].set_i32(1);
      // }
      row_data[1] = ws[TPCC_VAR_C_ID];                  // h_c_id   =>  c_id
      row_data[2] = ws[TPCC_VAR_C_D_ID];                // h_c_d_id => c_d_id
      row_data[3] = ws[TPCC_VAR_C_W_ID];                // h_c_w_id => c_w_id
      row_data[4] = ws[TPCC_VAR_D_ID];                  // h_d_id   =>  d_id
      row_data[5] = ws[TPCC_VAR_W_ID];                  // h_d_w_id => d_w_id
      row_data[6] = Value(std::to_string(time(NULL)));  // h_date
      output[TPCC_VAR_H_DATE] = row_data[6];
      row_data[7] = ws[TPCC_VAR_H_AMOUNT];  // h_amount => h_amount
      row_data[8] =
          Value(output[TPCC_VAR_W_NAME].get_str() + "    " +
                output[TPCC_VAR_D_NAME].get_str());  // d_data => w_name +
                                                     // 4spaces + d_name
      mdb::Row* row_history =
          mdb::VersionedRow::create(tbl_history_->schema(), row_data);
      dbHandler_.insert_row(tbl_history_, row_history);
      // Log_info("Piece-5 wssize=%u", ws.size());
   }

   {
      mdb::Row* r = NULL;
      mdb::MultiBlob mb(3);
      mb[0] = ws[TPCC_VAR_C_ID].get_blob();
      mb[1] = ws[TPCC_VAR_C_D_ID].get_blob();
      mb[2] = ws[TPCC_VAR_C_W_ID].get_blob();
      // R customer
      r = dbHandler_.query(tbl_customer_, mb).next();
      // if c_credit == "BC" (bad) 10%
      // here we use c_id to pick up 10% instead of c_credit
      if (ws[TPCC_VAR_C_ID].get_i32() % 10 == 0) {
         Value c_data(
             (to_string(ws[TPCC_VAR_C_ID]) + to_string(ws[TPCC_VAR_C_D_ID]) +
              to_string(ws[TPCC_VAR_C_W_ID]) + to_string(ws[TPCC_VAR_D_ID]) +
              to_string(ws[TPCC_VAR_W_ID]) + to_string(ws[TPCC_VAR_H_AMOUNT]) +
              output[TPCC_COL_CUSTOMER_C_DATA].get_str())
                 .substr(0, 500));
         std::vector<mdb::column_id_t> col_ids = {
             TPCC_COL_CUSTOMER_C_BALANCE, TPCC_COL_CUSTOMER_C_YTD_PAYMENT,
             TPCC_COL_CUSTOMER_C_DATA};
         std::vector<Value> col_data(
             {Value(output[TPCC_COL_CUSTOMER_C_BALANCE].get_double() -
                    ws[TPCC_VAR_H_AMOUNT].get_double()),
              Value(output[TPCC_COL_CUSTOMER_C_YTD_PAYMENT].get_double() +
                    ws[TPCC_VAR_H_AMOUNT].get_double()),
              c_data});
         dbHandler_.write_columns(r, col_ids, col_data);
      } else {
         std::vector<mdb::column_id_t> col_ids(
             {TPCC_COL_CUSTOMER_C_BALANCE, TPCC_COL_CUSTOMER_C_YTD_PAYMENT});

         std::vector<Value> col_data(
             {Value(output[TPCC_COL_CUSTOMER_C_BALANCE].get_double() -
                    ws[TPCC_VAR_H_AMOUNT].get_double()),
              Value(output[TPCC_COL_CUSTOMER_C_YTD_PAYMENT].get_double() +
                    ws[TPCC_VAR_H_AMOUNT].get_double())});
         dbHandler_.write_columns(r, col_ids, col_data);
      }
   }
}

void TPCCStateMachine::CommitDeliveryTxn(const uint32_t txnType,
                                         const std::vector<int32_t>* localKeys,
                                         std::map<int32_t, Value>* inputPtr,
                                         std::map<int32_t, Value>* outputPtr,
                                         const uint64_t txnId) {

   std::map<int32_t, Value>& ws = (*inputPtr);
   std::map<int32_t, Value>& output = (*outputPtr);
   /***
    * The row in the ORDER table with matching O_W_ID (equals W_ ID), O_D_ID
    * (equals D_ID), and O_ID (equals NO_O_ID) is selected, O_C_ID, the customer
    * number, is retrieved, and O_CARRIER_ID is updated.
    */
   if (ws[TPCC_VAR_O_ID].get_i32() >= 0) {
      mdb::MultiBlob mb(3);
      // cell_locator_t cl(TPCC_TB_ORDER, 3);
      mb[0] = ws[TPCC_VAR_D_ID].get_blob();
      mb[1] = ws[TPCC_VAR_W_ID].get_blob();
      mb[2] = ws[TPCC_VAR_O_ID].get_blob();
      // Log::debug("Delivery: o_d_id: %d, o_w_id: %d, o_id: %d, hash: %u",
      // ws[2].get_i32(), ws[1].get_i32(), ws[0].get_i32(),
      // mdb::MultiBlob::hash()(cl.primary_key));
      mdb::Row* row_order = dbHandler_.query(tbl_order_, mb).next();
      dbHandler_.read_column(row_order, TPCC_COL_ORDER_O_C_ID,
                             &ws[TPCC_VAR_C_ID]);  // read o_c_id

      dbHandler_.write_column(row_order, TPCC_COL_ORDER_O_CARRIER_ID,
                              ws[TPCC_VAR_O_CARRIER_ID]);  // write o_carrier_id
   }
   // Log_info("Piece-2 wssize=%u", ws.size());

   /***
    * All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID),
    * OL_D_ID (equals O_D_ID), and OL_O_ID (equals O_ID) are selected. All
    * OL_DELIVERY_D, the delivery dates, are updated to the current system time
    * as returned by the operating system and the sum of all OL_AMOUNT is
    * retrieved.
    */
   {
      mdb::MultiBlob mbl = mdb::MultiBlob(4);
      mdb::MultiBlob mbh = mdb::MultiBlob(4);
      //    mdb::MultiBlob mbl(4), mbh(4);
      mbl[0] = ws[TPCC_VAR_D_ID].get_blob();
      mbh[0] = ws[TPCC_VAR_D_ID].get_blob();
      mbl[1] = ws[TPCC_VAR_W_ID].get_blob();
      mbh[1] = ws[TPCC_VAR_W_ID].get_blob();
      mbl[2] = ws[TPCC_VAR_O_ID].get_blob();
      mbh[2] = ws[TPCC_VAR_O_ID].get_blob();
      Value ol_number_low(std::numeric_limits<i32>::min()),
          ol_number_high(std::numeric_limits<i32>::max());
      mbl[3] = ol_number_low.get_blob();
      mbh[3] = ol_number_high.get_blob();

      mdb::ResultSet rs_ol =
          dbHandler_.query_in(tbl_orderline_, mbl, mbh, mdb::ORD_ASC);
      mdb::Row* row_ol = nullptr;
      std::vector<mdb::Row*> row_list;
      row_list.reserve(15);
      while (rs_ol.has_next()) {
         row_list.push_back(rs_ol.next());
      }

      std::vector<mdb::column_lock_t> column_locks;
      column_locks.reserve(2 * row_list.size());

      int i = 0;
      double ol_amount_buf = 0.0;

      while (i < row_list.size()) {
         row_ol = row_list[i++];
         Value buf(0.0);
         dbHandler_.read_column(row_ol, TPCC_COL_ORDER_LINE_OL_AMOUNT,
                                &buf);  // read ol_amount
         ol_amount_buf += buf.get_double();
         dbHandler_.write_column(row_ol, TPCC_COL_ORDER_LINE_OL_DELIVERY_D,
                                 Value(std::to_string(time(NULL))));
      }
      ws[TPCC_VAR_OL_AMOUNT] = Value(ol_amount_buf);
   }
   // Log_info("Piece-3 wssize=%u", ws.size());
   /**
    * The row in the CUSTOMER table with matching C_W_ID (equals W_ID), C_D_ID
    * (equals D_ID), and C_ID (equals O_C_ID) is selected and C_BALANCE is
    * increased by the sum of all order-line amounts (OL_AMOUNT) previously
    * retrieved. C_DELIVERY_CNT is incremented by 1.
    */
   {
      mdb::MultiBlob mb = mdb::MultiBlob(3);
      // cell_locator_t cl(TPCC_TB_CUSTOMER, 3);
      mb[0] = ws[TPCC_VAR_C_ID].get_blob();
      mb[1] = ws[TPCC_VAR_D_ID].get_blob();
      mb[2] = ws[TPCC_VAR_W_ID].get_blob();
      mdb::Row* row_customer = dbHandler_.query(tbl_customer_, mb).next();
      Value buf = Value(0.0);
      dbHandler_.read_column(row_customer, TPCC_COL_CUSTOMER_C_BALANCE, &buf);
      buf.set_double(buf.get_double() + ws[TPCC_VAR_OL_AMOUNT].get_double());
      dbHandler_.write_column(row_customer, TPCC_COL_CUSTOMER_C_BALANCE, buf);
      dbHandler_.read_column(row_customer, TPCC_COL_CUSTOMER_C_DELIVERY_CNT,
                             &buf);
      buf.set_i32(buf.get_i32() + (i32)1);
      dbHandler_.write_column(row_customer, TPCC_COL_CUSTOMER_C_DELIVERY_CNT,
                              buf);
   }
   // Log_info("Piece-4 wssize=%u", ws.size());
}

void TPCCStateMachine::CommitStockLevelTxn(
    const uint32_t txnType, const std::vector<int32_t>* localKeys,
    std::map<int32_t, Value>* input, std::map<int32_t, Value>* output,
    const uint64_t txnId) {
   // Nothing to commit (read-only txn)
}

void TPCCStateMachine::CommitOrderStatusTxn(
    const uint32_t txnType, const std::vector<int32_t>* localKeys,
    std::map<int32_t, Value>* input, std::map<int32_t, Value>* output,
    const uint64_t txnId) {
   // Nothing to commit (read-only txn)
}

void TPCCStateMachine::RollbackExecute(const uint32_t txnType,
                                       const std::vector<int32_t>* localKeys,
                                       std::map<int32_t, Value>* inputPtr,
                                       std::map<int32_t, Value>* outputPtr,
                                       const uint64_t txnId) {
   // clear output (The previous read all erased, and write has not been
   // executed)
   outputPtr->clear();
}

void TPCCStateMachine::PreRead(const uint32_t txnType,
                               const std::map<int32_t, Value>* inputPtr,
                               std::map<int32_t, Value>* outputPtr) {
   const std::map<int32_t, Value>& ws = (*inputPtr);
   std::map<int32_t, Value>& output = (*outputPtr);

   if (txnType == TPCC_TXN_PAYMENT || txnType == TPCC_TXN_ORDER_STATUS) {
      if (ws.count(TPCC_VAR_C_LAST) > 0) {
         // Completely read operations, no other txns will write cid, so
         // it should be lock-free. but for generability, let's use the lock
         const auto& iter = ws.find(TPCC_VAR_W_ID);
         bool isHomeShard =
             (shardId_ ==
              PartitionFromKey(iter->second, &tb_info_warehouse_, shardNum_));
         if (isHomeShard) {
            std::shared_lock lck(cid_mtx_);
            mdb::MultiBlob mbl(3), mbh(3);
            const auto& iter0 = ws.find(TPCC_VAR_C_D_ID);
            const auto& iter1 = ws.find(TPCC_VAR_C_W_ID);
            const auto& iter2 = ws.find(TPCC_VAR_C_LAST);
            mbl[0] = iter0->second.get_blob();
            mbh[0] = iter0->second.get_blob();
            mbl[1] = iter1->second.get_blob();
            mbh[1] = iter1->second.get_blob();
            Value c_id_low(std::numeric_limits<i32>::min());
            Value c_id_high(std::numeric_limits<i32>::max());
            mbl[2] = c_id_low.get_blob();
            mbh[2] = c_id_high.get_blob();
            // LOG(INFO) << "TPCC_VAR_C_LAST no exist?="
            //           << (ws.find(TPCC_VAR_C_LAST) == ws.end());
            c_last_id_t key_low(iter2->second.get_str(), mbl, &(C_LAST_SCHEMA));
            c_last_id_t key_high(iter2->second.get_str(), mbh,
                                 &(C_LAST_SCHEMA));
            std::multimap<c_last_id_t, rrr::i32>::iterator it, it_low, it_high,
                it_mid;
            bool inc = false, mid_set = false;

            it_low = C_LAST2ID.lower_bound(key_low);
            it_high = C_LAST2ID.upper_bound(key_high);
            int n_c = 0;
            for (it = it_low; it != it_high; it++) {
               n_c++;
               if (mid_set)
                  if (inc) {
                     it_mid++;
                     inc = false;
                  } else
                     inc = true;
               else {
                  mid_set = true;
                  it_mid = it;
               }
            }
            verify(mid_set);
            output[TPCC_VAR_C_ID] = Value(it_mid->second);
            output[TPCC_VAR_C_LAST_VERSION] =
                version_track_[TPCC_VAR_C_LAST_VERSION];
         }
      }
   }
}