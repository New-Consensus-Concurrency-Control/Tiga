#pragma once
#include "table_base.h"

using mdb::Value;
struct entry_t {
   void* last_{nullptr};  // last transaction(write) that touches this
   // item. (arriving order)

   const entry_t& operator=(const entry_t& rhs) {
      last_ = rhs.last_;
      return *this;
   }

   entry_t() {}

   entry_t(const entry_t& o) { last_ = o.last_; }
};

class RCCRow : public mdb::CoarseLockedRow {
  private:
  protected:
   // protected dtor as required by RefCounted
   ~RCCRow();

  public:
   entry_t* dep_entry_;

   void copy_into(RCCRow* row) const;

   void init_dep(int n_columns);

   virtual mdb::Row* copy() const {
      RCCRow* row = new RCCRow();
      copy_into(row);
      return row;
   }

   virtual entry_t* get_dep_entry(int col_id);

   template <class Container>
   static RCCRow* create(const mdb::Schema* schema, const Container& values) {
      verify(values.size() == schema->columns_count());
      std::vector<const Value*> values_ptr(values.size(), nullptr);
      size_t fill_counter = 0;
      for (auto it = values.begin(); it != values.end(); ++it) {
         fill_values_ptr(schema, values_ptr, *it, fill_counter);
         fill_counter++;
      }
      RCCRow* raw_row = new RCCRow();
      raw_row->init_dep(schema->columns_count());
      return (RCCRow*)mdb::Row::create(raw_row, schema, values_ptr);
   }
};
