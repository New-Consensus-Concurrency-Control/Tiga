#pragma once

#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include "locking.h"
#include "snapshot.h"
#include "utils.h"
#include "value.h"

namespace mdb {
class Table;
class Row;
class Schema;
using namespace std;

struct column_info {
   column_info()
       : id(-1), indexed(false), type(Value::UNKNOWN), fixed_size_offst(-1) {}

   column_id_t id;
   std::string name;
   bool indexed;  // primary index or secondary index
   Value::kind type;

   union {
      // if fixed size (i32, i64, double)
      int fixed_size_offst;

      // if not fixed size (str)
      // need to lookup a index table on row
      int var_size_idx;
   };
};

class Row : public RefCounted {
   // fixed size part
   char* fixed_part_;
   int n_columns_ = 0;
   enum {
      DENSE,
      SPARSE,
   };

   int kind_;

   union {
      // for DENSE rows
      struct {
         // var size part
         char* dense_var_part_;

         // index table for var size part (marks the stop of a var segment)
         int* dense_var_idx_;
      };

      // for SPARSE rows
      std::string* sparse_var_;
   };

   Table* tbl_;

   // protected:
  public:
   void update_fixed(const column_info* col, void* ptr, int len);

   bool rdonly_;
   const Schema* schema_;

   // hidden ctor, factory model
   Row();

   // RefCounted should have protected dtor
   virtual ~Row();

   void copy_into(Row* row) const;

   // generic row creation
   static Row* create(Row* raw_row, const Schema* schema,
                      const std::vector<const Value*>& values);

   // helper function for row creation
   static void fill_values_ptr(const Schema* schema,
                               std::vector<const Value*>& values_ptr,
                               const Value& value, size_t fill_counter);

   // helper function for row creation
   static void fill_values_ptr(const Schema* schema,
                               std::vector<const Value*>& values_ptr,
                               const std::pair<const std::string, Value>& pair,
                               size_t fill_counter);

  public:
   virtual symbol_t rtti() const;

   const Schema* schema() const;
   bool readonly() const;
   void make_readonly();
   void make_sparse();
   void set_table(Table* tbl);
   const Table* get_table() const;

   Value get_column(int column_id) const;
   Value get_column(const std::string& col_name) const;
   virtual MultiBlob get_key() const;

   blob get_blob(int column_id) const;
   blob get_blob(const std::string& col_name) const;

   void update(int column_id, i32 v);

   void update(int column_id, i64 v);
   void update(int column_id, double v);
   void update(int column_id, const std::string& str);
   void update(int column_id, const Value& v);

   void update(const std::string& col_name, i32 v);
   void update(const std::string& col_name, i64 v);
   void update(const std::string& col_name, double v);
   void update(const std::string& col_name, const std::string& v);
   void update(const std::string& col_name, const Value& v);

   // compare based on keys
   // must have same schema!
   int compare(const Row& another) const;

   bool operator==(const Row& o) const;
   bool operator!=(const Row& o) const;
   bool operator<(const Row& o) const;
   bool operator>(const Row& o) const;
   bool operator<=(const Row& o) const;
   bool operator>=(const Row& o) const;

   virtual Row* copy() const;

   template <class Container>
   static Row* create(const Schema* schema, const Container& values);

   void to_string(std::string& str);
};

class CoarseLockedRow : public Row {
   RWLock lock_;

  protected:
   CoarseLockedRow();
   // protected dtor as required by RefCounted
   ~CoarseLockedRow();

   void copy_into(CoarseLockedRow* row) const;

  public:
   virtual symbol_t rtti() const;

   bool rlock_row_by(lock_owner_t o);
   bool wlock_row_by(lock_owner_t o);
   bool unlock_row_by(lock_owner_t o);

   virtual Row* copy() const;

   template <class Container>
   static CoarseLockedRow* create(const Schema* schema,
                                  const Container& values);
};

class FineLockedRow : public Row {
   typedef enum { WAIT_DIE, WOUND_WAIT, TIMEOUT } type_2pl_t;
   static type_2pl_t type_2pl_;
   rrr::ALock* lock_;
   // rrr::ALock *lock_;
   void init_lock(int n_columns);

  protected:
   // protected dtor as required by RefCounted
   ~FineLockedRow();

   // FIXME
   void copy_into(FineLockedRow* row) const;

  public:
   static void set_wait_die();

   static void set_wound_wait();

   virtual symbol_t rtti() const;

   rrr::ALock* get_alock(column_id_t column_id);

   uint64_t reg_wlock(column_id_t column_id,
                      std::function<void(uint64_t)> succ_callback,
                      std::function<void(void)> fail_callback);

   uint64_t reg_rlock(column_id_t column_id,
                      std::function<void(uint64_t)> succ_callback,
                      std::function<void(void)> fail_callback);

   void abort_lock_req(column_id_t column_id, uint64_t lock_req_id);

   void unlock_column_by(column_id_t column_id, uint64_t lock_req_id);

   virtual Row* copy() const;

   template <class Container>
   static FineLockedRow* create(const Schema* schema, const Container& values);
};

// inherit from CoarseLockedRow since we need locking on commit phase, when
// doing 2 phase commit
class VersionedRow : public CoarseLockedRow {
   //  version_t *ver_ = nullptr;
   std::vector<version_t> ver_ = {};
   void init_ver(int n_columns);

  protected:
   // protected dtor as required by RefCounted
   ~VersionedRow();

   void copy_into(VersionedRow* row) const;

  public:
   virtual symbol_t rtti() const;

   version_t get_column_ver(column_id_t column_id) const;

   void incr_column_ver(column_id_t column_id);

   virtual Row* copy() const;

   Value get_column(int column_id) const;

   // Don't know why,if we define static template functions in .cc file, then
   // the other files cannot find it
   template <class Container>
   static VersionedRow* create(const Schema* schema, const Container& values) {
      verify(values.size() == schema->columns_count());
      std::vector<const Value*> values_ptr(values.size(), nullptr);
      size_t fill_counter = 0;
      for (auto it = values.begin(); it != values.end(); ++it) {
         fill_values_ptr(schema, values_ptr, *it, fill_counter);
         fill_counter++;
      }
      VersionedRow* raw_row = new VersionedRow();
      raw_row->init_ver(schema->columns_count());
      return (VersionedRow*)Row::create(raw_row, schema, values_ptr);
   }
};

class Schema {
   friend class Row;

  public:
   Schema();
   virtual ~Schema();

   int add_column(const char* name, Value::kind type, bool key = false);
   int add_key_column(const char* name, Value::kind type);

   column_id_t get_column_id(const std::string& name) const;
   const std::vector<column_id_t>& key_columns_id() const;

   const column_info* get_column_info(const std::string& name) const;
   const column_info* get_column_info(column_id_t column_id) const;

   typedef std::vector<column_info>::const_iterator iterator;
   iterator begin() const;
   iterator end() const;
   size_t columns_count() const;
   virtual void freeze();
   size_t fixed_part_size() const { return fixed_part_size_; }  // for debug
   size_t var_size_cols() const { return var_size_cols_; }      // for debug
  protected:
   int add_hidden_column(const char* name, Value::kind type);

   std::unordered_map<std::string, column_id_t> col_name_to_id_;
   std::vector<column_info> col_info_;
   std::vector<column_id_t> key_cols_id_;  // key: primary index only

   // number of variable size cols (lookup table on row data)
   int var_size_cols_;
   int fixed_part_size_;

   // number of hidden fixed and var size columns, they are behind visible
   // columns
   int hidden_fixed_;
   int hidden_var_;
   bool frozen_;

  private:
   int do_add_column(const char* name, Value::kind type, bool key);
};

class IndexedSchema : public Schema {
   int idx_col_;

   std::vector<std::vector<column_id_t>> all_idx_;
   std::map<std::string, int> idx_name_;

   void index_sanity_check(const std::vector<column_id_t>& idx);

  public:
   IndexedSchema();

   int index_column_id() const;

   int add_index(const char* name, const std::vector<column_id_t>& idx);
   int add_index_by_column_names(const char* name,
                                 const std::vector<std::string>& named_idx);

   int get_index_id(const std::string& name);
   const std::vector<column_id_t>& get_index(const std::string& name);
   const std::vector<column_id_t>& get_index(int idx_id);

   virtual void freeze();

   std::vector<std::vector<column_id_t>>::const_iterator index_begin() const;
   std::vector<std::vector<column_id_t>>::const_iterator index_end() const;
};

// Tables are NoCopy, because they might maintain a pointer to schema, which
// should not be shared
class Table : public NoCopy {
   // protected:
  public:  // for gjk debug
   std::string name_;
   const Schema* schema_;

  public:
   Table(std::string name, const Schema* schema);
   virtual ~Table();

   const Schema* schema() const;
   const std::string Name() const;

   virtual void insert(Row* row) = 0;
   virtual void remove(Row* row, bool do_free = true) = 0;
   virtual uint64_t size();
   virtual void notify_before_update(Row* row, int updated_column_id);
   virtual void notify_after_update(Row* row, int updated_column_id);

   virtual symbol_t rtti() const = 0;
};

class SortedMultiKey {
  public:  // for debug gjk
   MultiBlob mb_;
   const Schema* schema_;

  public:
   SortedMultiKey(const MultiBlob& mb, const Schema* schema);
   // -1: this < o, 0: this == o, 1: this > o
   // UNKNOWN == UNKNOWN
   // both side should have same kind
   int compare(const SortedMultiKey& o) const;

   bool operator==(const SortedMultiKey& o) const;
   bool operator!=(const SortedMultiKey& o) const;
   bool operator<(const SortedMultiKey& o) const;
   bool operator>(const SortedMultiKey& o) const;
   bool operator<=(const SortedMultiKey& o) const;
   bool operator>=(const SortedMultiKey& o) const;

   const MultiBlob& get_multi_blob() const;
};

class SortedTable : public Table {
  protected:
   typedef std::multimap<SortedMultiKey, Row*>::const_iterator iterator;
   typedef std::multimap<SortedMultiKey, Row*>::const_reverse_iterator
       reverse_iterator;

   virtual iterator remove(iterator it, bool do_free = true);

   // indexed by key values
   std::multimap<SortedMultiKey, Row*> rows_;

  public:
   class Cursor : public Enumerator<const Row*> {
      iterator begin_, end_, next_;
      reverse_iterator r_begin_, r_end_, r_next_;
      int count_;
      bool reverse_;

     public:
      Cursor(const iterator& begin, const iterator& end)
          : count_(-1), reverse_(false) {
         begin_ = begin;
         end_ = end;
         next_ = begin;
      }
      Cursor(const reverse_iterator& begin, const reverse_iterator& end)
          : count_(-1), reverse_(true) {
         r_begin_ = begin;
         r_end_ = end;
         r_next_ = begin;
      }

      void reset() {
         if (reverse_) {
            r_next_ = r_begin_;
         } else {
            next_ = begin_;
         }
      }

      const iterator& begin() const { return begin_; }
      const iterator& end() const { return end_; }
      const reverse_iterator& rbegin() const { return r_begin_; }
      const reverse_iterator& rend() const { return r_end_; }
      bool has_next() {
         if (reverse_) {
            return r_next_ != r_end_;
         } else {
            return next_ != end_;
         }
      }
      operator bool() { return has_next(); }
      Row* next() {
         Row* row = nullptr;
         if (reverse_) {
            verify(r_next_ != r_end_);
            row = r_next_->second;
            ++r_next_;
         } else {
            verify(next_ != end_);
            row = next_->second;
            ++next_;
         }
         return row;
      }
      int count() {
         if (count_ < 0) {
            count_ = 0;
            if (reverse_) {
               for (auto it = r_begin_; it != r_end_; ++it) {
                  count_++;
               }
            } else {
               for (auto it = begin_; it != end_; ++it) {
                  count_++;
               }
            }
         }
         return count_;
      }
   };

   virtual uint64_t size();

   SortedTable(std::string name, const Schema* schema);

   ~SortedTable();

   virtual symbol_t rtti() const;

   void insert(Row* row);

   Cursor query(const Value& kv);
   Cursor query(const MultiBlob& mb);
   Cursor query(const SortedMultiKey& smk);
   Cursor query_lt(const Value& kv, symbol_t order = symbol_t::ORD_ASC);
   Cursor query_lt(const MultiBlob& mb, symbol_t order = symbol_t::ORD_ASC);
   Cursor query_lt(const SortedMultiKey& smk,
                   symbol_t order = symbol_t::ORD_ASC);

   Cursor query_gt(const Value& kv, symbol_t order = symbol_t::ORD_ASC);
   Cursor query_gt(const MultiBlob& mb, symbol_t order = symbol_t::ORD_ASC);
   Cursor query_gt(const SortedMultiKey& smk,
                   symbol_t order = symbol_t::ORD_ASC);

   // (low, high) not inclusive
   Cursor query_in(const Value& low, const Value& high,
                   symbol_t order = symbol_t::ORD_ASC);
   Cursor query_in(const MultiBlob& low, const MultiBlob& high,
                   symbol_t order = symbol_t::ORD_ASC);
   Cursor query_in(const SortedMultiKey& low, const SortedMultiKey& high,
                   symbol_t order = symbol_t::ORD_ASC);

   Cursor all(symbol_t order = symbol_t::ORD_ASC) const;

   void clear();

   void remove(const Value& kv);
   void remove(const MultiBlob& mb);
   void remove(const SortedMultiKey& smk);
   void remove(Row* row, bool do_free = true);
   void remove(Cursor cur);
};

class UnsortedTable : public Table {
   typedef std::unordered_multimap<MultiBlob, Row*,
                                   MultiBlob::hash>::const_iterator iterator;

  public:
   class Cursor : public Enumerator<const Row*> {
      iterator begin_, end_, next_;
      int count_;

     public:
      Cursor(const iterator& begin, const iterator& end)
          : begin_(begin), end_(end), next_(begin), count_(-1) {}

      void reset() { next_ = begin_; }

      bool has_next() { return next_ != end_; }
      operator bool() { return has_next(); }
      Row* next() {
         verify(next_ != end_);
         Row* row = next_->second;
         ++next_;
         return row;
      }
      int count() {
         if (count_ < 0) {
            count_ = 0;
            for (auto it = begin_; it != end_; ++it) {
               count_++;
            }
         }
         return count_;
      }
   };

   UnsortedTable(std::string name, const Schema* schema);
   ~UnsortedTable();

   virtual symbol_t rtti() const;

   void insert(Row* row);

   Cursor query(const Value& kv);
   Cursor query(const MultiBlob& key);
   Cursor all() const;

   void clear();

   void remove(const Value& kv);
   void remove(const MultiBlob& key);
   void remove(Row* row, bool do_free = true);

  private:
   iterator remove(iterator it, bool do_free = true);

   // indexed by key values
   std::unordered_multimap<MultiBlob, Row*, MultiBlob::hash> rows_;
};

class RefCountedRow {
   Row* row_;

  public:
   RefCountedRow(Row* row);
   RefCountedRow(const RefCountedRow& r);
   ~RefCountedRow();
   const RefCountedRow& operator=(const RefCountedRow& r);
   Row* get() const;
};

class SnapshotTable : public Table {
   // indexed by key values
   typedef snapshot_sortedmap<SortedMultiKey, RefCountedRow> table_type;
   table_type rows_;

  public:
   class Cursor : public Enumerator<const Row*> {
      table_type::range_type* range_;
      table_type::reverse_range_type* reverse_range_;

     public:
      Cursor(const table_type::range_type& range) : reverse_range_(nullptr) {
         range_ = new table_type::range_type(range);
      }
      Cursor(const table_type::reverse_range_type& range) : range_(nullptr) {
         reverse_range_ = new table_type::reverse_range_type(range);
      }
      ~Cursor() {
         if (range_ != nullptr) {
            delete range_;
         }
         if (reverse_range_ != nullptr) {
            delete reverse_range_;
         }
      }
      virtual bool has_next() {
         if (range_ != nullptr) {
            return range_->has_next();
         } else {
            return reverse_range_->has_next();
         }
      }
      virtual const Row* next() {
         verify(has_next());
         if (range_ != nullptr) {
            return range_->next().second.get();
         } else {
            return reverse_range_->next().second.get();
         }
      }
      int count() {
         if (range_ != nullptr) {
            return range_->count();
         } else {
            return reverse_range_->count();
         }
      }
      bool is_reverse() const { return reverse_range_ != nullptr; }
      const table_type::range_type& get_range() const { return *range_; }
      const table_type::reverse_range_type& get_reverse_range() const {
         return *reverse_range_;
      }
   };

   SnapshotTable(std::string name, const Schema* sch);

   virtual symbol_t rtti() const;

   SnapshotTable* snapshot() const;

   void insert(Row* row);
   Cursor query(const Value& kv);
   Cursor query(const MultiBlob& mb);
   Cursor query(const SortedMultiKey& smk);

   Cursor query_lt(const Value& kv, symbol_t order = symbol_t::ORD_ASC);
   Cursor query_lt(const MultiBlob& mb, symbol_t order = symbol_t::ORD_ASC);
   Cursor query_lt(const SortedMultiKey& smk,
                   symbol_t order = symbol_t::ORD_ASC);

   Cursor query_gt(const Value& kv, symbol_t order = symbol_t::ORD_ASC);
   Cursor query_gt(const MultiBlob& mb, symbol_t order = symbol_t::ORD_ASC);
   Cursor query_gt(const SortedMultiKey& smk,
                   symbol_t order = symbol_t::ORD_ASC);

   // (low, high) not inclusive
   Cursor query_in(const Value& low, const Value& high,
                   symbol_t order = symbol_t::ORD_ASC);
   Cursor query_in(const MultiBlob& low, const MultiBlob& high,
                   symbol_t order = symbol_t::ORD_ASC);
   Cursor query_in(const SortedMultiKey& low, const SortedMultiKey& high,
                   symbol_t order = symbol_t::ORD_ASC);

   Cursor all(symbol_t order = symbol_t::ORD_ASC) const;

   void clear();
   void remove(const Value& kv);
   void remove(const MultiBlob& mb);
   void remove(const SortedMultiKey& smk);

   void remove(Row* row, bool do_free = true);

   void remove(const Cursor& cur);
};

// forward declaration
class IndexedTable;

typedef std::vector<Row*> master_index;

class Index {
   const IndexedTable* idx_tbl_;
   int idx_id_;

   const Schema* get_schema() const;
   SortedTable* get_index_table() const;

  public:
   class Cursor : public Enumerator<const Row*> {
      SortedTable::Cursor base_cur_;

     public:
      Cursor(const SortedTable::Cursor& base) : base_cur_(base) {}
      void reset() { base_cur_.reset(); }
      bool has_next() { return base_cur_.has_next(); }
      operator bool() { return has_next(); }
      int count() { return base_cur_.count(); }
      const Row* next() {
         Row* index_row = base_cur_.next();
         column_id_t last_column_id = index_row->schema()->columns_count() - 1;
         verify(last_column_id >= 0);
         Value pointer_value = index_row->get_column(last_column_id);
         master_index* master_idx = (master_index*)pointer_value.get_i64();
         verify(master_idx != nullptr);
         const Row* base_row = master_idx->back();
         verify(base_row != nullptr);
         return base_row;
      }
   };

   Index(const IndexedTable* idx_tbl, int idx_id);

   const IndexedTable* get_table();
   int id();

   Cursor query(const Value& kv);
   Cursor query(const MultiBlob& mb);
   Cursor query(const SortedMultiKey& smk);

   Cursor query_lt(const Value& kv, symbol_t order = symbol_t::ORD_ASC);
   Cursor query_lt(const MultiBlob& mb, symbol_t order = symbol_t::ORD_ASC);
   Cursor query_lt(const SortedMultiKey& smk,
                   symbol_t order = symbol_t::ORD_ASC);

   Cursor query_gt(const Value& kv, symbol_t order = symbol_t::ORD_ASC);
   Cursor query_gt(const MultiBlob& mb, symbol_t order = symbol_t::ORD_ASC);
   Cursor query_gt(const SortedMultiKey& smk,
                   symbol_t order = symbol_t::ORD_ASC);

   // (low, high) not inclusive
   Cursor query_in(const Value& low, const Value& high,
                   symbol_t order = symbol_t::ORD_ASC);
   Cursor query_in(const MultiBlob& low, const MultiBlob& high,
                   symbol_t order = symbol_t::ORD_ASC);
   Cursor query_in(const SortedMultiKey& low, const SortedMultiKey& high,
                   symbol_t order = symbol_t::ORD_ASC);

   Cursor all(symbol_t order = symbol_t::ORD_ASC) const;
};

class IndexedTable : public SortedTable {
   friend class Index;

   // all the secondary indices and their schemas
   std::vector<SortedTable*> indices_;
   std::vector<Schema*> index_schemas_;

   int index_column_id() const;

   void destroy_secondary_indices(master_index* master_idx);

   virtual iterator remove(iterator it, bool do_free = true);

   Row* make_index_row(Row* base, int idx_id, master_index* master_idx);

  public:
   IndexedTable(std::string name, const IndexedSchema* schema);
   ~IndexedTable();

   void insert(Row* row);

   void remove(Index::Cursor idx_cursor);

   // enable searching SortedTable for overloaded `remove` functions
   using SortedTable::remove;

   virtual void notify_before_update(Row* row, int updated_column_id);
   virtual void notify_after_update(Row* row, int updated_column_id);

   Index get_index(int idx_id) const;
   Index get_index(const std::string& idx_name) const;
};

}  // namespace mdb