#include "TPCCSharding.h"

TPCCSharding::TPCCSharding(const uint32_t shardId, const uint32_t replicaId,
                           const uint32_t shardNum, const uint32_t replicaNum,
                           const YAML::Node& config, const bool populateTable)
    : shardId_(shardId),
      replicaId_(replicaId),
      shardNum_(shardNum),
      replicaNum_(replicaNum),
      config_(config),
      populateTable_(populateTable) {
   mgr_ = new mdb::TxnMgrUnsafe();
   LoadBenchYML();
   LoadSchemaYML();
   LoadShardingYML();
   // Optional
   if (populateTable) {
      PopTable();
      for (auto& kv : g_c_last2id) {
         const c_last_id_t& key = kv.first;
         for (int32_t otherWId = 0;
              otherWId < tb_infos_[TPCC_TB_WAREHOUSE].num_records * shardNum;
              otherWId++) {
            Value w;
            w.set_i32(otherWId);
            if (PartitionFromKey(w, &(tb_infos_[TPCC_TB_WAREHOUSE]),
                                 shardNum_) == shardId_) {
               continue;
            }
            CustomerIndex index;
            index.cLast_ = key.c_last;
            index.dId_.set_i32(*(int32_t*)(void*)(key.c_index_smk.mb_[0].data));
            index.wId_.set_i32(otherWId);
            index.cId_.set_i32(*(int32_t*)(void*)(key.c_index_smk.mb_[1].data));
            customerIdxes_.push_back(index);
         }
      }
      for (uint32_t i = 0; i < customerIdxes_.size(); i++) {
         size_t mb_size = g_c_last_schema.key_columns_id().size();
         mdb::MultiBlob mb_buf(mb_size);
         mdb::Schema::iterator col_info_it = g_c_last_schema.begin();
         mb_buf[0] = customerIdxes_[i].dId_.get_blob();
         mb_buf[1] = customerIdxes_[i].wId_.get_blob();
         mb_buf[2] = customerIdxes_[i].cId_.get_blob();
         g_c_last2id.insert(std::make_pair(
             c_last_id_t(customerIdxes_[i].cLast_, mb_buf, &g_c_last_schema),
             customerIdxes_[i].cId_.get_i32()));
      }
      LOG(INFO) << "g_c_last2id size=" << g_c_last2id.size();
   }
}

TPCCSharding::~TPCCSharding() {}

int TPCCSharding::get_number_rows(std::map<std::string, uint64_t>& table_map) {
   for (auto it = tb_infos_.begin(); it != tb_infos_.end(); it++)
      table_map[it->first] = (uint64_t)(it->second.num_records);
   return 0;
}

int TPCCSharding::GetTablePartitions(const std::string& tb_name,
                                     vector<uint32_t>& par_ids) {
   std::map<std::string, tb_info_t>::iterator it = tb_infos_.find(tb_name);
   if (it == tb_infos_.end()) return -1;
   if (it->second.par_ids.size() == 0) return -2;

   tb_info_t& tbl_info = it->second;
   par_ids.clear();
   par_ids.insert(par_ids.end(), tbl_info.par_ids.begin(),
                  tbl_info.par_ids.end());
   return 0;
}

void TPCCSharding::LoadBenchYML() {
   const auto& config = config_["bench"];
   scale_factor_ = config["scale"].as<uint32_t>();
   auto weights = config["weight"];
   for (auto it = weights.begin(); it != weights.end(); it++) {
      auto txn_name = it->first.as<string>();
      auto weight = it->second.as<double>();
      txn_weights_[txn_name] = weight;
   }

   txn_weight_.push_back(txn_weights_["new_order"]);
   txn_weight_.push_back(txn_weights_["payment"]);
   txn_weight_.push_back(txn_weights_["order_status"]);
   txn_weight_.push_back(txn_weights_["delivery"]);
   txn_weight_.push_back(txn_weights_["stock_level"]);

   auto populations = config["population"];
   auto& tb_infos = tb_infos_;
   for (auto it = populations.begin(); it != populations.end(); it++) {
      auto tbl_name = it->first.as<string>();
      auto info_it = tb_infos.find(tbl_name);
      if (info_it == tb_infos.end()) {
         tb_infos[tbl_name] = tb_info_t();
         info_it = tb_infos.find(tbl_name);
      }
      auto& tbl_info = info_it->second;
      int pop = it->second.as<int>();
      tbl_info.num_records = scale_factor_ * pop;
      verify(tbl_info.num_records > 0);
   }
}

void TPCCSharding::LoadSchemaYML() {
   for (uint32_t i = 0; i < config_["schema"].size(); i++) {
      std::string tbl_name = config_["schema"][i]["name"].as<std::string>();
      LOG(INFO) << "TableName=" << tbl_name;
      auto info_it = tb_infos_.find(tbl_name);
      if (info_it == tb_infos_.end()) {
         tb_infos_[tbl_name] = tb_info_t();
         info_it = tb_infos_.find(tbl_name);
      }
      auto& tbl_info = info_it->second;
      auto columns = config_["schema"][i]["column"];

      for (auto iitt = columns.begin(); iitt != columns.end(); iitt++) {
         auto column = *iitt;
         LoadSchemaTableColumnYML(tbl_info, column);
      }
      tbl_info.tb_name = tbl_name;
      tb_infos_[tbl_name] = tbl_info;
      LOG(INFO) << "Fin tableName=" << tbl_name;
   }
   LOG(INFO) << "Fin Table Init";
   BuildTableInfoPtr();
   LOG(INFO) << "Fin BuildTableInfoPtr";
}

void TPCCSharding::LoadSchemaTableColumnYML(tb_info_t& tb_info,
                                            YAML::Node column) {
   std::string c_type = column["type"].as<string>();
   verify(c_type.size() > 0);
   Value::kind c_v_type = StringToValueKind(c_type);
   // LOG(INFO) << "ctype=" << c_type << "--c_v_type=" << (int)(c_v_type);
   std::string c_name = column["name"].as<string>();
   verify(c_name.size() > 0);
   bool c_primary = column["primary"].as<bool>(false);
   std::string c_foreign = column["foreign"].as<string>("");
   column_t* foreign_key_column = NULL;
   tb_info_t* foreign_key_tb = NULL;
   std::string ftbl_name;
   std::string fcol_name;
   bool is_foreign = (c_foreign.size() > 0);
   if (is_foreign) {
      size_t pos = c_foreign.find('.');
      verify(pos != std::string::npos);

      ftbl_name = c_foreign.substr(0, pos);
      fcol_name = c_foreign.substr(pos + 1);
      verify(c_foreign.size() > pos + 1);
   }
   tb_info.columns.push_back(
       column_t(c_v_type, c_name, c_primary, is_foreign, ftbl_name, fcol_name));
}

void TPCCSharding::BuildTableInfoPtr() {
   verify(tb_infos_.size() > 0);
   for (auto tbl_it = tb_infos_.begin(); tbl_it != tb_infos_.end(); tbl_it++) {
      auto& tbl = tbl_it->second;
      auto& columns = tbl.columns;
      verify(columns.size() > 0);
      for (auto col_it = columns.begin(); col_it != columns.end(); col_it++) {
         auto& col = *col_it;
         if (col.is_foreign) {
            verify(!col.foreign_col_name.empty());
            auto ftb_it = tb_infos_.find(col.foreign_tbl_name);
            verify(ftb_it != tb_infos_.end());
            auto& ftbl = ftb_it->second;
            col.foreign_tb = &ftbl;
            auto fcol_it = ftbl.columns.begin();
            for (; fcol_it != ftbl.columns.end(); fcol_it++) {
               if (fcol_it->name == col.foreign_col_name) {
                  verify(fcol_it->type == col.type);
                  if (fcol_it->values == NULL) {
                     fcol_it->values = new std::vector<Value>();
                  }
                  col.foreign = &(*fcol_it);
                  break;
               }
            }
            verify(fcol_it != ftbl.columns.end());
         }
      }
   }
}

void TPCCSharding::LoadShardingYML() {
   auto& tb_infos = tb_infos_;
   for (uint32_t i = 0; i < config_["schema"].size(); i++) {
      auto tbl_name = config_["schema"][i]["name"].as<std::string>();
      auto info_it = tb_infos.find(tbl_name);
      verify(info_it != tb_infos.end());
      auto& tbl_info = info_it->second;
      string method = config_["sharding"][tbl_name].as<std::string>();

      for (uint32_t shardId = 0; shardId < shardNum_; shardId++) {
         tbl_info.par_ids.push_back(shardId);
         tbl_info.symbol = mdb::TBL_SORTED;
      }
      verify(tbl_info.par_ids.size() > 0);
   }
   LOG(INFO) << "Fin LoadShardingYML";
}

void TPCCSharding::PopTable() {
   for (auto& kv : tb_infos_) {
      auto table_name = kv.first;
      mdb::Schema* schema = new mdb::Schema();
      mdb::symbol_t symbol;
      InitSchema(table_name, schema, &symbol);
      mdb::Table* tb;
      switch (symbol) {
         case mdb::TBL_SORTED:
            tb = new mdb::SortedTable(table_name, schema);
            break;
         case mdb::TBL_UNSORTED:
            tb = new mdb::UnsortedTable(table_name, schema);
            break;
         case mdb::TBL_SNAPSHOT:
            tb = new mdb::SnapshotTable(table_name, schema);
            break;
         default:
            verify(0);
      }
      mgr_->reg_table(table_name, tb);
      if (table_name == TPCC_TB_ORDER) {
         mdb::Schema* schema = new mdb::Schema();
         const mdb::Schema* o_schema = tb->schema();
         mdb::Schema::iterator it = o_schema->begin();
         for (; it != o_schema->end(); it++)
            if (it->indexed)
               if (it->name != "o_id") {
                  schema->add_column(it->name.c_str(), it->type, true);
               }

         schema->add_column("o_c_id", Value::I32, true);
         schema->add_column("o_id", Value::I32, false);
         mgr_->reg_table(TPCC_TB_ORDER_C_ID_SECONDARY,
                         new mdb::SortedTable(table_name, schema));
      }
   }
   PopulateTables(shardId_);
}

int TPCCSharding::InitSchema(const std::string& tb_name, mdb::Schema* schema,
                             mdb::symbol_t* symbol) {
   auto& tb_infos = tb_infos_;
   std::map<std::string, tb_info_t>::iterator it;
   it = tb_infos.find(tb_name);
   if (it == tb_infos.end()) return -1;
   auto& tb_info = it->second;
   auto column_it = tb_info.columns.begin();

   for (; column_it != tb_info.columns.end(); column_it++) {
      schema->add_column(column_it->name.c_str(), column_it->type,
                         column_it->is_primary);
      // Log_info(
      //     "name=%s After add-col=%s fixed_part_size=%u var_size_cols=%u "
      //     "colType=%u",
      //     tb_name.c_str(), column_it->name.c_str(),
      //     schema->fixed_part_size(), schema->var_size_cols(),
      //     (int)(column_it->type));
   }

   *symbol = tb_info.symbol;
   // Log_info("name=%s fixed_part_size=%u", tb_name.c_str(),
   //          schema->fixed_part_size());
   return schema->columns_count();
}

void TPCCSharding::PopulateTables(const uint32_t shardId) {
   auto n_left = tb_infos_.size();
   verify(n_left > 0);
   do {
      bool populated = false;
      for (auto tb_it = tb_infos_.begin(); tb_it != tb_infos_.end(); tb_it++) {
         tb_info_t* tb_info = &(tb_it->second);
         verify(tb_it->first == tb_info->tb_name);

         // TODO is this unnecessary?
         auto it = tb_info->populated.find(shardId);
         if (it == tb_info->populated.end()) {
            tb_info->populated[shardId] = false;
         }
         bool readyToPopulate = Ready2Populate(tb_info);
         if (!tb_info->populated[shardId] && readyToPopulate) {
            PopulateTable(tb_info, shardId);
            tb_info->populated[shardId] = true;
            // finish populate one table
            n_left--;
            populated = true;
         }
      }
      verify(populated);
   } while (n_left > 0);

   // release_foreign_values
   std::map<std::string, tb_info_t>::iterator tb_it = tb_infos_.begin();
   std::vector<column_t>::iterator c_it;
   for (; tb_it != tb_infos_.end(); tb_it++)
      for (c_it = tb_it->second.columns.begin();
           c_it != tb_it->second.columns.end(); c_it++)
         if (c_it->values != NULL) {
            delete c_it->values;
            c_it->values = NULL;
         }
}

bool TPCCSharding::Ready2Populate(tb_info_t* tb_info) {
   auto& columns = tb_info->columns;
   for (auto c_it = columns.begin(); c_it != columns.end(); c_it++) {
      auto fcol = c_it->foreign;
      // std::cout << "Table=" << tb_info->tb_name << "\t colName=" <<
      // c_it->name
      //           << "\n";
      // std::cout << "fcol != nullptr? " << (fcol != nullptr) << std::endl;
      // if (fcol != nullptr) {
      //    std::cout << "fcol->values != nullptr? " << (fcol->values !=
      //    nullptr)
      //              << std::endl;
      //    std::cout << "fcol->values->size() == 0? "
      //              << (fcol->values->size() == 0) << std::endl;
      // }
      if ((fcol != nullptr) && (fcol->values != nullptr) &&
          (fcol->values->size() == 0)) {
         // have foreign table
         // foreign table has some mysterious values
         // those values have not been put in
         return false;
      }
   }
   return true;
}

void TPCCSharding::PopulateTable(tb_info_t* tb_info_ptr, uint32_t shardId) {
   LOG(INFO) << "Populating " << tb_info_ptr->tb_name;
   mdb::Table* const table_ptr = mgr_->get_table(tb_info_ptr->tb_name);
   const mdb::Schema* schema = table_ptr->schema();
   verify(schema->columns_count() == tb_info_ptr->columns.size());

   Log_info("Schemal Size=%u", schema->fixed_part_size());
   uint32_t col_index = 0;
   if (tb_info_ptr->tb_name == TPCC_TB_WAREHOUSE) {  // warehouse table
      Value key_value, max_key;
      mdb::Schema::iterator col_it = schema->begin();
      for (col_index = 0; col_index < tb_info_ptr->columns.size();
           col_index++) {
         verify(col_it != schema->end());
         verify(tb_info_ptr->columns[col_index].name == col_it->name);
         verify(tb_info_ptr->columns[col_index].type == col_it->type);
         if (tb_info_ptr->columns[col_index].is_primary) {
            verify(col_it->indexed);
            key_value = value_get_zero(tb_info_ptr->columns[col_index].type);
            max_key = value_get_n(tb_info_ptr->columns[col_index].type,
                                  tb_info_ptr->num_records * shardNum_);
            LOG(INFO) << "num_records=" << tb_info_ptr->num_records
                      << "--shardNum=" << shardNum_;
         }
         col_it++;
      }
      verify(col_it == schema->end());
      std::vector<Value> row_data;
      LOG(INFO) << "key_value=" << key_value.get_i32()
                << ", max_key=" << max_key.get_i32();
      for (; key_value < max_key; ++key_value) {
         row_data.clear();
         for (col_index = 0; col_index < tb_info_ptr->columns.size();
              col_index++) {
            if (tb_info_ptr->columns[col_index].is_primary) {
               if (shardId !=
                   PartitionFromKey(key_value, tb_info_ptr, shardNum_)) {
                  // item table=>fully replicated
                  break;
               }

               if (tb_info_ptr->columns[col_index].values != NULL)
                  tb_info_ptr->columns[col_index].values->push_back(key_value);
               row_data.push_back(key_value);
            } else if (tb_info_ptr->columns[col_index].foreign != NULL) {
               // TODO (ycui) use RandomGenerator
               Log_fatal("Table %s shouldn't have a foreign key!",
                         TPCC_TB_WAREHOUSE);
               verify(0);
            } else {
               // TODO (ycui) use RandomGenerator
               Value v_buf = random_value(tb_info_ptr->columns[col_index].type);
               row_data.push_back(v_buf);
            }
         }
         if (col_index == tb_info_ptr->columns.size()) {
            auto row = RCCRow::create(schema, row_data);
            table_ptr->insert(row);
         }
      }
      LOG(INFO) << "WareHouse size=" << table_ptr->size();
   } else {  // non warehouse tables
      unsigned long long int num_foreign_row = 1;
      unsigned long long int num_self_primary = 0;
      unsigned int self_primary_col = 0;
      bool self_primary_col_find = false;
      std::map<unsigned int, std::pair<unsigned int, unsigned int> >
          prim_foreign_index;
      mdb::Schema::iterator col_it = schema->begin();

      mdb::SortedTable* tbl_sec_ptr = NULL;
      if (tb_info_ptr->tb_name == TPCC_TB_ORDER)
         tbl_sec_ptr =
             (mdb::SortedTable*)mgr_->get_table(TPCC_TB_ORDER_C_ID_SECONDARY);
      for (col_index = 0; col_index < tb_info_ptr->columns.size();
           col_index++) {
         verify(col_it != schema->end());
         verify(tb_info_ptr->columns[col_index].name == col_it->name);
         verify(tb_info_ptr->columns[col_index].type == col_it->type);
         if (tb_info_ptr->columns[col_index].is_primary) {
            verify(col_it->indexed);
            if (tb_info_ptr->tb_name == TPCC_TB_CUSTOMER) {  // XXX

               if (col_it->name != "c_id")
                  g_c_last_schema.add_column(col_it->name.c_str(), col_it->type,
                                             true);
            }  // XXX
            if (tb_info_ptr->columns[col_index].foreign_tb != NULL) {
               unsigned int tmp_int;
               if (tb_info_ptr->columns[col_index].foreign->name ==
                   "i_id") {  // refers to item.i_id, use all available i_id
                              // instead of local i_id
                  tmp_int =
                      tb_info_ptr->columns[col_index].foreign_tb->num_records;
                  if (tb_info_ptr->columns[col_index].values != NULL) {
                     tb_info_ptr->columns[col_index].values->resize(tmp_int);
                     for (i32 i = 0; i < tmp_int; i++)
                        (*tb_info_ptr->columns[col_index].values)[i] = Value(i);
                  }
               } else {
                  column_t* foreign_column =
                      tb_info_ptr->columns[col_index].foreign;
                  tmp_int = foreign_column->values->size();
                  if (tb_info_ptr->columns[col_index].values != NULL) {
                     tb_info_ptr->columns[col_index].values->assign(
                         foreign_column->values->begin(),
                         foreign_column->values->end());
                  }
               }
               verify(tmp_int > 0);
               prim_foreign_index[col_index] =
                   std::pair<unsigned int, unsigned int>(0, tmp_int);
               num_foreign_row *= tmp_int;
            } else {
               // only one primary key can refer to no other table.
               verify(!self_primary_col_find);
               self_primary_col = col_index;
               self_primary_col_find = true;
            }
         }
         col_it++;
      }

      if (tb_info_ptr->tb_name == TPCC_TB_CUSTOMER) {  // XXX
         g_c_last_schema.add_column("c_id", mdb::Value::I32, true);
      }  // XXX

      // TODO (ycui) add a vector in tb_info_t to record used values for key.
      verify(tb_info_ptr->num_records % num_foreign_row == 0 ||
             tb_info_ptr->num_records < num_foreign_row);
      // Log_debug("foreign row: %llu, this row: %llu", num_foreign_row,
      // tb_info_ptr->num_records);
      num_self_primary = tb_info_ptr->num_records / num_foreign_row;
      Value key_value =
          value_get_zero(tb_info_ptr->columns[self_primary_col].type);
      Value max_key = value_get_n(tb_info_ptr->columns[self_primary_col].type,
                                  num_self_primary);

      // if (tb_info_ptr->tb_name == TPCC_TB_CUSTOMER) {
      //    LOG(INFO) << "maxKey=" << max_key.get_i32()
      //              << "--num_self_primary=" << num_self_primary
      //              << "--tb_info_ptr->num_records=" <<
      //              tb_info_ptr->num_records
      //              << "--num_foreign_row=" << num_foreign_row;
      // }
      std::vector<Value> row_data;
      // Log_info("Begin primary key: %s, Max primary key: %s",
      //          to_string(key_value).c_str(), to_string(max_key).c_str());

      for (; key_value < max_key || num_self_primary == 0; ++key_value) {

         bool record_key = true;
         init_index(prim_foreign_index);
         int counter = 0;
         while (true) {
            row_data.clear();
            for (col_index = 0; col_index < tb_info_ptr->columns.size();
                 col_index++) {

               if (tb_info_ptr->columns[col_index].is_primary) {
                  if (prim_foreign_index.size() == 0) {
                     if (tb_info_ptr->tb_name !=
                             TPCC_TB_ITEM /**FUlly replicated Item table*/
                         && shardId != PartitionFromKey(key_value, tb_info_ptr,
                                                        shardNum_))
                        break;
                     row_data.push_back(key_value);
                     if (tb_info_ptr->columns[col_index].values != NULL) {
                        tb_info_ptr->columns[col_index].values->push_back(
                            key_value);
                     }
                     // Log_debug("%s (primary): %s",
                     // tb_info_ptr->columns[col_index].name.c_str(),
                     // to_string(key_value).c_str());
                  }
                  // primary key and foreign key
                  else if (tb_info_ptr->columns[col_index].foreign != NULL) {
                     Value v_buf;
                     if (tb_info_ptr->columns[col_index].foreign->name ==
                         "i_id")
                        v_buf = Value((i32)prim_foreign_index[col_index].first);
                     else
                        v_buf =
                            (*tb_info_ptr->columns[col_index].foreign->values)
                                [prim_foreign_index[col_index].first];
                     // Log_debug("%s (primary, foreign): %s",
                     // tb_info_ptr->columns[col_index].name.c_str(),
                     // to_string(v_buf).c_str());
                     row_data.push_back(v_buf);
                  } else {  // primary key
                     row_data.push_back(key_value);
                     if (tb_info_ptr->columns[col_index].values != NULL &&
                         record_key) {
                        tb_info_ptr->columns[col_index].values->push_back(
                            key_value);
                        record_key = false;
                     }
                     // Log_debug("%s (primary): %s",
                     // tb_info_ptr->columns[col_index].name.c_str(),
                     // to_string(key_value).c_str());
                  }
               } else if (tb_info_ptr->columns[col_index].foreign != NULL) {
                  bool use_key_value = false;
                  int n;
                  size_t fc_size =
                      tb_info_ptr->columns[col_index].foreign->name.size();
                  std::string last4 =
                      tb_info_ptr->columns[col_index].foreign->name.substr(
                          fc_size - 4, 4);
                  if (last4 == "i_id") {
                     n = tb_infos_[std::string(TPCC_TB_ITEM)].num_records;
                  } else if (last4 == "w_id") {
                     n = tb_infos_[std::string(TPCC_TB_WAREHOUSE)].num_records *
                         shardNum_;
                  } else if (last4 == "c_id") {
                     if (tb_info_ptr->columns[col_index].name == "o_c_id")
                        use_key_value = true;
                     else
                        n = tb_infos_[std::string(TPCC_TB_CUSTOMER)]
                                .num_records /
                            tb_infos_[std::string(TPCC_TB_DISTRICT)]
                                .num_records;
                  } else if (last4 == "d_id") {
                     n = tb_infos_[std::string(TPCC_TB_DISTRICT)].num_records /
                         tb_infos_[std::string(TPCC_TB_WAREHOUSE)].num_records;
                  } else {
                     n = tb_info_ptr->columns[col_index]
                             .foreign_tb->num_records;
                  }
                  Value v_buf;
                  if (use_key_value)
                     v_buf = key_value;
                  else
                     v_buf = value_get_n(tb_info_ptr->columns[col_index].type,
                                         RandomGenerator::rand(0, n - 1));
                  row_data.push_back(v_buf);
                  // Log_debug("%s (foreign): %s",
                  // tb_info_ptr->columns[col_index].name.c_str(),
                  // to_string(v_buf).c_str());
               } else {
                  Value v_buf =
                      random_value(tb_info_ptr->columns[col_index].type);
                  if (tb_info_ptr->columns[col_index].name == "d_next_o_id")
                     v_buf =
                         Value((i32)(tb_infos_[std::string(TPCC_TB_ORDER)]
                                         .num_records /
                                     tb_infos_[std::string(TPCC_TB_DISTRICT)]
                                         .num_records));  // XXX
                  if (tb_info_ptr->columns[col_index].name == "c_last")
                     v_buf = Value(RandomGenerator::int2str_n(
                         key_value.get_i32() % 1000, 3));

                  row_data.push_back(v_buf);

                  // Log_debug("%s: %s",
                  // tb_info_ptr->columns[col_index].name.c_str(),
                  // to_string(v_buf).c_str());
               }
               // if (tb_info_ptr->tb_name == TPCC_TB_CUSTOMER &&
               //     tb_info_ptr->columns[col_index].name == "c_d_id") {
               //    Log_info("cid=%d c_d_id=%d", row_data[0].get_i32(),
               //             row_data[1].get_i32());
               // }
            }

            if (col_index == tb_info_ptr->columns.size()) {
               mdb::Row* r = RCCRow::create(schema, row_data);
               table_ptr->insert(r);
               //
               if (tbl_sec_ptr) {
                  rrr::i32 cur_o_id_buf = r->get_column("o_id").get_i32();
                  const mdb::Schema* sch_buf = tbl_sec_ptr->schema();
                  mdb::MultiBlob mb_buf(sch_buf->key_columns_id().size());
                  mdb::Schema::iterator col_info_it = sch_buf->begin();
                  size_t mb_i = 0;
                  for (; col_info_it != sch_buf->end(); col_info_it++)
                     if (col_info_it->indexed)
                        mb_buf[mb_i++] = r->get_blob(col_info_it->name);
                  mdb::SortedTable::Cursor rs = tbl_sec_ptr->query(mb_buf);
                  if (rs.has_next()) {
                     mdb::Row* r_buf = rs.next();
                     rrr::i32 o_id_buf = r_buf->get_column("o_id").get_i32();
                     if (o_id_buf < cur_o_id_buf)
                        r_buf->update("o_id", cur_o_id_buf);
                  } else {
                     std::vector<Value> sec_row_data_buf;
                     for (col_info_it = sch_buf->begin();
                          col_info_it != sch_buf->end(); col_info_it++)
                        sec_row_data_buf.push_back(
                            r->get_column(col_info_it->name));
                     mdb::Row* r_buf =
                         RCCRow::create(sch_buf, sec_row_data_buf);
                     tbl_sec_ptr->insert(r_buf);
                  }
               }

               // XXX c_last secondary index
               if (tb_info_ptr->tb_name == TPCC_TB_CUSTOMER) {
                  // customer's lastName2Id is fully replicated (here we
                  // simulate that)
                  std::string c_last_buf = r->get_column("c_last").get_str();
                  rrr::i32 c_id_buf = r->get_column("c_id").get_i32();
                  size_t mb_size = g_c_last_schema.key_columns_id().size(),
                         mb_i = 0;
                  mdb::MultiBlob mb_buf(mb_size);
                  mdb::Schema::iterator col_info_it = g_c_last_schema.begin();
                  for (; col_info_it != g_c_last_schema.end(); col_info_it++) {
                     mb_buf[mb_i++] = r->get_blob(col_info_it->name);
                  }
                  g_c_last2id.insert(std::make_pair(
                      c_last_id_t(c_last_buf, mb_buf, &g_c_last_schema),
                      c_id_buf));

                  // LOG(INFO) << "size=" << g_c_last2id.size()
                  //           << "--c_last_buf=" << c_last_buf
                  //           << "--counter=" << counter;
               }  // XXX

               // Log_debug("Row inserted");
               counter++;
               if (num_self_primary == 0 &&
                   counter >= tb_info_ptr->num_records) {
                  num_self_primary = 1;
                  break;
               }
            }
            if (num_self_primary == 0) {
               if (0 != index_increase(prim_foreign_index)) verify(0);
            } else if (0 != index_increase(prim_foreign_index))
               break;
         }
      }
   }
   LOG(INFO) << "Populated " << tb_info_ptr->tb_name
             << "--size=" << table_ptr->size();

   // exit(0);
}

TxnMgr* TPCCSharding::GetTxnMgr() { return mgr_; }