#include "TableCommon.h"

mdb::Value::kind StringToValueKind(const std::string &c_type) {
   mdb::Value::kind c_v_type;
   if (c_type == "i32" || c_type == "integer") {
      c_v_type = Value::I32;
   } else if (c_type == "i64") {
      c_v_type = Value::I64;
   } else if (c_type == "double") {
      c_v_type = Value::DOUBLE;
   } else if (c_type == "str" || c_type == "string") {
      c_v_type = Value::STR;
   } else {
      c_v_type = Value::UNKNOWN;
      verify(0);
   }
   return c_v_type;
}

int init_index(std::map<uint32_t, std::pair<uint32_t, uint32_t> > &index) {
   if (index.size() == 0) return -1;

   std::map<uint32_t, std::pair<uint32_t, uint32_t> >::iterator it =
       index.begin();

   for (; it != index.end(); it++) {
      verify(it->second.second > 0);
      it->second.first = 0;
   }
   return 0;
}

int index_increase(std::map<uint32_t, std::pair<uint32_t, uint32_t> > &index) {
   if (index.size() == 0) return -1;

   std::map<uint32_t, std::pair<uint32_t, uint32_t> >::iterator it =
       index.begin();
   it->second.first++;

   while (it->second.first >= it->second.second) {
      it->second.first -= it->second.second;
      verify(it->second.first == 0);

      it++;

      if (it == index.end()) return -1;

      it->second.first++;
   }

   return 0;
}

int index_increase(std::map<uint32_t, std::pair<uint32_t, uint32_t> > &index,
                   const std::vector<uint32_t> &bound_index) {
   if (bound_index.size() <= 1) return index_increase(index);

   std::vector<uint32_t>::const_iterator it = bound_index.begin();
   it++;

   for (; it != bound_index.end(); it++) index.erase(*it);
   int ret = index_increase(index);
   it = bound_index.begin();
   it++;

   for (; it != bound_index.end(); it++) index[*it] = index[bound_index[0]];
   return ret;
}

int index_reverse_increase(
    std::map<uint32_t, std::pair<uint32_t, uint32_t> > &index) {
   if (index.size() == 0) return -1;

   std::map<uint32_t, std::pair<uint32_t, uint32_t> >::reverse_iterator it =
       index.rbegin();
   it->second.first++;

   while (it->second.first >= it->second.second) {
      it->second.first -= it->second.second;
      verify(it->second.first == 0);

      it++;

      if (it == index.rend()) return -1;

      it->second.first++;
   }

   return 0;
}

int index_reverse_increase(
    std::map<uint32_t, std::pair<uint32_t, uint32_t> > &index,
    const std::vector<uint32_t> &bound_index) {
   if (bound_index.size() <= 1) return index_reverse_increase(index);

   std::vector<uint32_t>::const_iterator it = bound_index.begin();
   it++;

   for (; it != bound_index.end(); it++) index.erase(*it);
   int ret = index_reverse_increase(index);
   it = bound_index.begin();
   it++;

   for (; it != bound_index.end(); it++) index[*it] = index[bound_index[0]];
   return ret;
}

Value value_get_zero(Value::kind k) {
   switch (k) {
      case Value::I32:
         return Value((i32)0);

      case Value::I64:
         return Value((i64)0);

      case Value::DOUBLE:
         return Value((double)0.0);

      case Value::STR:
         // TODO (ycui) str zero
         verify(0);
         return Value(std::string(""));

      case Value::UNKNOWN:
         verify(0);
         return Value(std::string(""));

      default:
         verify(0);
         return Value(std::string(""));
   }
}

Value operator++(Value &lhs, int) {
   Value ret = lhs;

   switch (lhs.get_kind()) {
      case Value::I32:
         lhs.set_i32(lhs.get_i32() + 1);
         break;

      case Value::I64:
         lhs.set_i64(lhs.get_i64() + 1);
         break;

      case Value::DOUBLE:
         lhs.set_double(lhs.get_double() + 1.0);
         break;

      case Value::STR:

         // TODO (ycui) str increment
         verify(0);
         break;

      case Value::UNKNOWN:
         verify(0);
         break;

      default:
         verify(0);
         break;
   }
   return ret;
}

Value &operator++(Value &rhs) {
   rhs++;
   return rhs;
}

Value random_value(Value::kind k) {
   switch (k) {
      case Value::I32:
         return Value((i32)RandomGenerator::rand());

      case Value::I64:
         return Value((i64)RandomGenerator::rand());

      case Value::DOUBLE:
         return Value(RandomGenerator::rand_double());

      case Value::STR:
         return Value(
             RandomGenerator::int2str_n(RandomGenerator::rand(0, 999), 3));

      case Value::UNKNOWN:
      default:
         verify(0);
         return Value();
   }
}

Value value_get_n(Value::kind k, int v) {
   switch (k) {
      case Value::I32:
         return Value((i32)v);

      case Value::I64:
         return Value((i64)v);

      case Value::DOUBLE:
         return Value((double)v);

      case Value::STR:
         return Value(std::to_string(v));

      case Value::UNKNOWN:
         verify(0);
         return Value(std::string(""));

      default:
         verify(0);
         return Value(std::string(""));
   }
}

uint32_t PartitionFromKey(const MultiValue &key, const tb_info_t *tb_info,
                          const uint32_t shardNum) {
   const MultiValue &key_buf = key;
   const int num_partitions = shardNum;
   uint32_t ret;

   //  Log_info("GJK num_partitions=%u sharding_method=%d", num_partitions,
   //           tb_info->sharding_method);

   switch (tb_info->sharding_method) {
      case MODULUS:
         ret = my_modulus(key_buf, num_partitions);
         break;

      case INT_MODULUS:
         ret = int_modulus(key_buf, num_partitions);
         break;

      default:
         ret = my_modulus(key_buf, num_partitions);
         break;
   }
   return ret;
}

uint32_t my_modulus(const MultiValue &key, uint32_t num_partitions) {
   uint32_t index = 0;
   long long int buf;
   int i = 0;

   while (i < key.size()) {
      switch (key[i].get_kind()) {
         case Value::I32:
            buf = key[i].get_i32() % num_partitions;
            index += buf > 0 ? (uint32_t)buf : (uint32_t)(-buf);
            break;

         case Value::I64:
            buf = key[i].get_i64() % num_partitions;
            index += buf > 0 ? (uint32_t)buf : (uint32_t)(-buf);
            break;

         case Value::DOUBLE:
            buf = ((long long int)floor(key[i].get_double())) % num_partitions;
            index += buf > 0 ? (uint32_t)buf : (uint32_t)(-buf);
            break;

         case Value::STR: {
            uint32_t sum = 0;
            const std::string &str_buf = key[i].get_str();
            size_t i = 0;

            for (; i < str_buf.size(); i++) sum += (uint32_t)str_buf[i];
            index += sum % num_partitions;
         }

         case Value::UNKNOWN:
            Log_error("NOT IMPLEMENTED");
            verify(0);
            break;

         default:
            Log_error("UNKWOWN VALUE TYPE");
            verify(0);
            break;
      }
      i++;
      index %= num_partitions;
   }
   return index % num_partitions;
}

uint32_t int_modulus(const MultiValue &key, uint32_t num_partitions) {
   uint32_t index = 0;
   long long int buf;
   int i = 0;

   while (i < key.size()) {
      switch (key[i].get_kind()) {
         case Value::I32:
            buf = key[i].get_i32() % num_partitions;
            index += buf > 0 ? (uint32_t)buf : (uint32_t)(-buf);
            break;

         case Value::I64:
            buf = key[i].get_i64() % num_partitions;
            index += buf > 0 ? (uint32_t)buf : (uint32_t)(-buf);
            break;

         case Value::DOUBLE:
            buf = ((long long int)floor(key[i].get_double())) % num_partitions;
            index += buf > 0 ? (uint32_t)buf : (uint32_t)(-buf);
            break;

         case Value::STR: {
            uint32_t sum = 0;
            uint32_t mod = 1;
            const std::string &str_buf = key[i].get_str();
            std::string::const_reverse_iterator rit = str_buf.rbegin();

            for (; rit != str_buf.rend(); rit++) {
               sum += mod * (uint32_t)(*rit);
               sum %= num_partitions;
               mod *= 127;
               mod %= num_partitions;
            }
            index += sum % num_partitions;
         }

         case Value::UNKNOWN:
            Log_error("NOT IMPLEMENTED");
            verify(0);
            break;

         default:
            Log_error("ERROR");
            verify(0);
            break;
      }

      i++;
      index %= num_partitions;
   }
   return index % num_partitions;
}