#pragma once
// clang-format off
#include <string>
#include <vector>
#include "rrr/misc/rand.hpp"

#include "Constants.h"
#include "memdb/multi_value.h"
#include "memdb/rcc_row.h"
// #include "memdb/table_base.h"
// #include "memdb/txn.h"

//clang-format on
using namespace std;
using namespace mdb;
using rrr::RandomGenerator;

struct tb_info_t;
struct column_t;

enum method_t {
   MODULUS,
   INT_MODULUS,
};

typedef struct c_last_id_t {
   std::string c_last;
   mdb::SortedMultiKey c_index_smk;

   c_last_id_t(const std::string &_c_last, const mdb::MultiBlob &mb,
               const mdb::Schema *schema)
       : c_last(_c_last), c_index_smk(mb, schema) {}

   bool operator<(const c_last_id_t &rhs) const {
      int ret = strcmp(c_last.c_str(), rhs.c_last.c_str());

      if (ret < 0)
         return true;
      else if (ret == 0) {
         if (c_index_smk < rhs.c_index_smk) return true;
      }
      return false;
   }
} c_last_id_t;

typedef struct column_t {
   column_t(Value::kind _type, std::string _name, bool _is_primary,
            bool _is_foreign, string ftbl_name, string fcol_name)
       : type(_type),
         name(_name),
         is_primary(_is_primary),
         values(nullptr),
         is_foreign(_is_foreign),
         foreign_tbl_name(ftbl_name),
         foreign_col_name(fcol_name),
         foreign_tb(nullptr),
         foreign(nullptr) {}

   column_t(const column_t &c)
       : type(c.type),
         name(c.name),
         is_primary(c.is_primary),
         is_foreign(c.is_foreign),
         foreign_tbl_name(c.foreign_tbl_name),
         foreign_col_name(c.foreign_col_name),
         foreign(nullptr),
         foreign_tb(nullptr),
         values(nullptr) {}

   Value::kind type;
   std::string name;
   bool is_primary;
   bool is_foreign;
   string foreign_tbl_name;
   string foreign_col_name;
   column_t *foreign;
   tb_info_t *foreign_tb;
   std::vector<Value> *values;  // ? what is this about?
} column_t;

struct tb_info_t {
   tb_info_t() : sharding_method(MODULUS), num_records(0) {}

   tb_info_t(std::string method, uint32_t ns = 0,
             std::vector<uint32_t> *pars = nullptr, uint64_t _num_records = 0,
             mdb::symbol_t _symbol = mdb::TBL_UNSORTED)
       : num_records(_num_records), symbol(_symbol) {
      if (pars) {
         par_ids = vector<uint32_t>(*pars);
         verify(0);
      }
      if (method == "modulus")
         sharding_method = MODULUS;
      else if (method == "int_modulus")
         sharding_method = INT_MODULUS;
      else
         sharding_method = MODULUS;
   }

   method_t sharding_method;
   vector<uint32_t> par_ids = {};
   uint64_t num_records = 0;
   map<parid_t, bool> populated = {};  // partition_id -> populated

   vector<column_t> columns = {};
   mdb::symbol_t symbol;
   std::string tb_name = "";
};

mdb::Value::kind StringToValueKind(const std::string &c_type);

int init_index(std::map<uint32_t, std::pair<uint32_t, uint32_t> > &index);

int index_increase(std::map<uint32_t, std::pair<uint32_t, uint32_t> > &index);

int index_increase(std::map<uint32_t, std::pair<uint32_t, uint32_t> > &index,
                   const std::vector<uint32_t> &bound_index);

int index_reverse_increase(
    std::map<uint32_t, std::pair<uint32_t, uint32_t> > &index);

int index_reverse_increase(
    std::map<uint32_t, std::pair<uint32_t, uint32_t> > &index,
    const std::vector<uint32_t> &bound_index);

Value value_get_zero(Value::kind k);

Value operator++(Value &lhs, int);

Value &operator++(Value &rhs);

Value random_value(Value::kind k);

Value value_get_n(Value::kind k, int v);

uint32_t PartitionFromKey(const MultiValue &key, const tb_info_t *tb_info,
                          const uint32_t shardNum);

uint32_t my_modulus(const MultiValue &key, uint32_t num_partitions);
uint32_t int_modulus(const MultiValue &key, uint32_t num_partitions);