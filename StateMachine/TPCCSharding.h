#pragma once
#include <glog/logging.h>
#include <yaml-cpp/yaml.h>
#include "TPCCCommon.h"
#include "TableCommon.h"
#include "memdb/txn_unsafe.h"

struct CustomerIndex {
   std::string cLast_;
   mdb::Value dId_;
   mdb::Value wId_;
   mdb::Value cId_;
};

class TPCCSharding {
  private:
   YAML::Node config_;
   uint32_t shardId_;
   uint32_t shardNum_;
   uint32_t replicaId_;
   uint32_t replicaNum_;
   bool populateTable_;

  public:
   std::multimap<c_last_id_t, rrr::i32> g_c_last2id;  // XXX hardcode
   std::vector<CustomerIndex> customerIdxes_;
   mdb::Schema g_c_last_schema;  // XXX

   uint32_t scale_factor_;
   std::map<string, double> txn_weights_;
   std::vector<double> txn_weight_;
   mdb::TxnMgr* mgr_;

   std::map<std::string, tb_info_t> tb_infos_;
   // std::map<MultiValue, MultiValue> dist2sid_;
   // std::map<MultiValue, MultiValue> stock2sid_;

   // number of all combination of foreign columns
   uint64_t num_foreign_row = 1;
   // the column index for foreign w_id and d_id.
   vector<uint32_t> bound_foreign_index = {};
   // the index of column that is primary but not foreign
   uint32_t self_primary_col = 0;
   // col index -> (0, number of records in foreign table or size of value
   // vector)
   map<uint32_t, std::pair<uint32_t, uint32_t> > prim_foreign_index = {};
   uint64_t num_self_primary = 0;
   // the number of row that have been inserted.
   int n_row_inserted_ = 0;
   bool record_key = true;  // ?
  public:
   TPCCSharding(const uint32_t shardId, const uint32_t replicaId,
                const uint32_t shardNum, const uint32_t replicaNum,
                const YAML::Node& config, const bool populateTable);
   void LoadBenchYML();
   void LoadSchemaYML();
   void LoadSchemaTableColumnYML(tb_info_t& tb_info, YAML::Node column);
   void LoadShardingYML();
   void BuildTableInfoPtr();
   void PopTable();
   void PopulateTables(const uint32_t shardId);
   void PopulateTable(tb_info_t* tb_info, const uint32_t shardId);
   bool Ready2Populate(tb_info_t* tb_info);
   int InitSchema(const std::string& tb_name, mdb::Schema* schema,
                  mdb::symbol_t* symbol);
   int get_number_rows(std::map<std::string, uint64_t>& table_map);
   int GetTablePartitions(const std::string& tb_name,
                          std::vector<unsigned int>& par_ids);
   ~TPCCSharding();
   TxnMgr* GetTxnMgr();
};
