#include "table_base.h"

namespace mdb {
Row::Row()
    : fixed_part_(nullptr),
      kind_(DENSE),
      dense_var_part_(nullptr),
      dense_var_idx_(nullptr),
      tbl_(nullptr),
      rdonly_(false),
      schema_(nullptr) {}

Row::~Row() {
   delete[] fixed_part_;
   if (schema_->var_size_cols_ > 0) {
      if (kind_ == DENSE) {
         delete[] dense_var_part_;
         delete[] dense_var_idx_;
      } else {
         verify(kind_ == SPARSE);
         delete[] sparse_var_;
      }
   }
}

Row* Row::copy() const {
   Row* row = new Row();
   copy_into(row);
   return row;
}

// helper function for row creation
void Row::fill_values_ptr(const Schema* schema,
                          std::vector<const Value*>& values_ptr,
                          const Value& value, size_t fill_counter) {
   values_ptr[fill_counter] = &value;
}

symbol_t Row::rtti() const { return symbol_t::ROW_BASIC; }

const Schema* Row::schema() const { return schema_; }
bool Row::readonly() const { return rdonly_; }
void Row::make_readonly() { rdonly_ = true; }

void Row::make_sparse() {
   if (kind_ == SPARSE) {
      // already sparse data
      return;
   }

   kind_ = SPARSE;

   if (schema_->var_size_cols_ == 0) {
      // special case, no memory copying required
      return;
   }

   // save those 2 values to prevent overwriting (union type!)
   char* var_data = dense_var_part_;
   int* var_idx = dense_var_idx_;

   assert(schema_->var_size_cols_ > 0);
   sparse_var_ = new std::string[schema_->var_size_cols_];
   sparse_var_[0] = std::string(var_data, var_idx[0]);
   for (int i = 1; i < schema_->var_size_cols_; i++) {
      int var_start = var_idx[i - 1];
      int var_len = var_idx[i] - var_idx[i - 1];
      sparse_var_[i] = std::string(&var_data[var_start], var_len);
   }

   delete[] var_data;
   delete[] var_idx;
}

void Row::set_table(Table* tbl) {
   if (tbl != nullptr) {
      verify(tbl_ == nullptr);
   }
   tbl_ = tbl;
}
const Table* Row::get_table() const { return tbl_; }

Value Row::get_column(int column_id) const {
   Value v;
   verify(schema_);
   const column_info* info = schema_->get_column_info(column_id);
   blob b = this->get_blob(column_id);
   verify(info != nullptr);
   switch (info->type) {
      case Value::I32:
         v = Value(*((i32*)b.data));
         break;
      case Value::I64:
         v = Value(*((i64*)b.data));
         break;
      case Value::DOUBLE:
         v = Value(*((double*)b.data));
         break;
      case Value::STR:
         v = Value(std::string(b.data, b.len));
         break;
      default:
         Log::fatal("unexpected value type %d", info->type);
         verify(0);
         break;
   }
   return v;
}

Value Row::get_column(const std::string& col_name) const {
   return get_column(schema_->get_column_id(col_name));
}

MultiBlob Row::get_key() const {
   const std::vector<int>& key_cols = schema_->key_columns_id();
   MultiBlob mb(key_cols.size());
   for (int i = 0; i < mb.count(); i++) {
      mb[i] = this->get_blob(key_cols[i]);
   }
   return mb;
}

blob Row::get_blob(int column_id) const {
   blob b;
   const column_info* info = schema_->get_column_info(column_id);
   verify(info != nullptr);
   switch (info->type) {
      case Value::I32:
         b.data = &fixed_part_[info->fixed_size_offst];
         b.len = sizeof(i32);
         break;
      case Value::I64:
         b.data = &fixed_part_[info->fixed_size_offst];
         b.len = sizeof(i64);
         break;
      case Value::DOUBLE:
         b.data = &fixed_part_[info->fixed_size_offst];
         b.len = sizeof(double);
         break;
      case Value::STR:
         if (kind_ == DENSE) {
            int var_start = 0;
            int var_len = 0;
            if (info->var_size_idx == 0) {
               var_start = 0;
               var_len = dense_var_idx_[0];
            } else {
               var_start = dense_var_idx_[info->var_size_idx - 1];
               var_len = dense_var_idx_[info->var_size_idx] -
                         dense_var_idx_[info->var_size_idx - 1];
            }
            b.data = &dense_var_part_[var_start];
            b.len = var_len;
         } else {
            verify(kind_ == SPARSE);
            b.data = &(sparse_var_[info->var_size_idx][0]);
            b.len = sparse_var_[info->var_size_idx].size();
         }
         break;
      default:
         Log::fatal("unexpected value type %d", info->type);
         verify(0);
         break;
   }
   return b;
}

blob Row::get_blob(const std::string& col_name) const {
   return get_blob(schema_->get_column_id(col_name));
}

void Row::update(int column_id, i32 v) {
   const column_info* info = schema_->get_column_info(column_id);
   verify(info->type == Value::I32);
   update_fixed(info, &v, sizeof(v));
}

void Row::update(int column_id, i64 v) {
   const column_info* info = schema_->get_column_info(column_id);
   verify(info->type == Value::I64);
   update_fixed(info, &v, sizeof(v));
}
void Row::update(int column_id, double v) {
   const column_info* info = schema_->get_column_info(column_id);
   verify(info->type == Value::DOUBLE);
   update_fixed(info, &v, sizeof(v));
}

void Row::update(const std::string& col_name, i32 v) {
   this->update(schema_->get_column_id(col_name), v);
}
void Row::update(const std::string& col_name, i64 v) {
   this->update(schema_->get_column_id(col_name), v);
}
void Row::update(const std::string& col_name, double v) {
   this->update(schema_->get_column_id(col_name), v);
}
void Row::update(const std::string& col_name, const std::string& v) {
   this->update(schema_->get_column_id(col_name), v);
}
void Row::update(const std::string& col_name, const Value& v) {
   this->update(schema_->get_column_id(col_name), v);
}

void Row::copy_into(Row* row) const {
   row->fixed_part_ = new char[this->schema_->fixed_part_size_];
   memcpy(row->fixed_part_, this->fixed_part_, this->schema_->fixed_part_size_);

   row->kind_ = DENSE;  // always make a dense copy

   int var_part_size = 0;
   int var_count = 0;
   for (auto& it : *this->schema_) {
      if (it.type != Value::STR) {
         continue;
      }
      var_part_size += this->get_blob(it.id).len;
      var_count++;
   }
   row->dense_var_part_ = new char[var_part_size];
   row->dense_var_idx_ = new int[var_count];

   int var_idx = 0;
   int var_pos = 0;
   for (auto& it : *this->schema_) {
      if (it.type != Value::STR) {
         continue;
      }
      blob b = this->get_blob(it.id);
      memcpy(&row->dense_var_part_[var_pos], b.data, b.len);
      var_pos += b.len;
      row->dense_var_idx_[var_idx] = var_pos;
      var_idx++;
   }

   row->tbl_ = nullptr;  // do not mark it as inside some table

   row->rdonly_ = false;  // always make it writable
   row->schema_ = this->schema_;
}

void Row::update_fixed(const column_info* col, void* ptr, int len) {
   verify(!rdonly_);
   // check if really updating (new data!), and if necessary to remove/insert
   // into table
   bool re_insert = false;
   if (memcmp(&fixed_part_[col->fixed_size_offst], ptr, len) == 0) {
      // not really updating
      return;
   }

   if (col->indexed) {
      re_insert = true;
   }

   // save tbl_, because tbl_->remove() will set it to nullptr
   Table* tbl = tbl_;
   if (re_insert && tbl != nullptr) {
      tbl->notify_before_update(this, col->id);
      tbl->remove(this, false);
   }

   memcpy(&fixed_part_[col->fixed_size_offst], ptr, len);

   if (re_insert && tbl != nullptr) {
      tbl->insert(this);
      tbl->notify_after_update(this, col->id);
   }
}

void Row::update(int column_id, const std::string& v) {
   verify(!rdonly_);
   const column_info* col = schema_->get_column_info(column_id);
   verify(col->type == Value::STR);

   // check if really updating (new data!), and if necessary to remove/insert
   // into table
   bool re_insert = false;

   blob b;
   if (kind_ == SPARSE) {
      if (this->sparse_var_[col->var_size_idx] == v) {
         return;
      }
   } else {
      verify(kind_ == DENSE);
      b = this->get_blob(column_id);
      if (size_t(b.len) == v.size() && memcmp(b.data, &v[0], v.size()) == 0) {
         return;
      }
   }

   if (col->indexed) {
      re_insert = true;
   }

   // save tbl_, because tbl_->remove() will set it to nullptr
   Table* tbl = tbl_;
   if (re_insert && tbl != nullptr) {
      tbl->notify_before_update(this, column_id);
      tbl->remove(this, false);
   }

   if (kind_ == DENSE && size_t(b.len) == v.size()) {
      // tiny optimization: in-place update if string size is not changed
      memcpy(const_cast<char*>(b.data), &v[0], b.len);
   } else {
      this->make_sparse();
      this->sparse_var_[col->var_size_idx] = v;
   }

   if (re_insert && tbl != nullptr) {
      tbl->insert(this);
      tbl->notify_after_update(this, column_id);
   }
}

void Row::update(int column_id, const Value& v) {
   switch (v.get_kind()) {
      case Value::I32:
         this->update(column_id, v.get_i32());
         break;
      case Value::I64:
         this->update(column_id, v.get_i64());
         break;
      case Value::DOUBLE:
         this->update(column_id, v.get_double());
         break;
      case Value::STR:
         this->update(column_id, v.get_str());
         break;
      default:
         Log::fatal("unexpected value type %d", v.get_kind());
         verify(0);
         break;
   }
}

int Row::compare(const Row& o) const {
   if (&o == this) {
      return 0;
   }
   verify(schema_ == o.schema_);

   // compare based on keys
   SortedMultiKey mine(this->get_key(), schema_);
   SortedMultiKey other(o.get_key(), o.schema_);
   return mine.compare(other);
}

bool Row::operator==(const Row& o) const { return compare(o) == 0; }
bool Row::operator!=(const Row& o) const { return compare(o) != 0; }
bool Row::operator<(const Row& o) const { return compare(o) == -1; }
bool Row::operator>(const Row& o) const { return compare(o) == 1; }
bool Row::operator<=(const Row& o) const { return compare(o) != 1; }
bool Row::operator>=(const Row& o) const { return compare(o) != -1; }

void Row::to_string(std::string& str) {
   size_t s = str.size();
   int len = s;
   len += (sizeof(schema_->fixed_part_size_) + schema_->fixed_part_size_ +
           sizeof(kind_));
   if (kind_ == DENSE && schema_->var_size_cols_ > 0) {
      len += schema_->var_size_cols_;
      len += dense_var_idx_[schema_->var_size_cols_ - 1];
      str.resize(len);
      int i = s;
      memcpy((void*)(str.data() + i), (void*)(&schema_->fixed_part_size_),
             sizeof(schema_->fixed_part_size_));
      i += sizeof(schema_->fixed_part_size_);
      memcpy((void*)(str.data() + i), (void*)(fixed_part_),
             schema_->fixed_part_size_);
      i += schema_->fixed_part_size_;
      memcpy((void*)(str.data() + i), (void*)(&kind_), sizeof(kind_));
      i += sizeof(kind_);
      memcpy((void*)(str.data() + i), (void*)dense_var_idx_,
             schema_->var_size_cols_);
      i += schema_->var_size_cols_;
      memcpy((void*)(str.data() + i), (void*)dense_var_part_,
             dense_var_idx_[schema_->var_size_cols_ - 1]);
      i += dense_var_idx_[schema_->var_size_cols_ - 1];
      verify(i == len);
   } else {
      str.resize(len);
      int i = s;
      memcpy((void*)(str.data() + i), (void*)(&schema_->fixed_part_size_),
             sizeof(schema_->fixed_part_size_));
      i += sizeof(schema_->fixed_part_size_);
      memcpy((void*)(str.data() + i), (void*)(fixed_part_),
             schema_->fixed_part_size_);
      i += schema_->fixed_part_size_;
      memcpy((void*)(str.data() + i), (void*)(&kind_), sizeof(kind_));
      i += sizeof(kind_);
      verify(i == len);
   }
}

Row* Row::create(Row* raw_row, const Schema* schema,
                 const std::vector<const Value*>& values) {
   Row* row = raw_row;
   row->schema_ = schema;
   row->fixed_part_ = new char[schema->fixed_part_size_];
   memset(row->fixed_part_, 0, schema->fixed_part_size_);
   if (schema->var_size_cols_ > 0) {
      row->dense_var_idx_ = new int[schema->var_size_cols_];
   }

   // 1st pass, write fixed part, and calculate var part size
   int var_part_size = 0;
   int fixed_pos = 0;
   for (auto& it : values) {
      switch (it->get_kind()) {
         case Value::I32:
            it->write_binary(&row->fixed_part_[fixed_pos]);
            fixed_pos += sizeof(i32);
            break;
         case Value::I64:
            it->write_binary(&row->fixed_part_[fixed_pos]);
            fixed_pos += sizeof(i64);
            break;
         case Value::DOUBLE:
            it->write_binary(&row->fixed_part_[fixed_pos]);
            fixed_pos += sizeof(double);
            break;
         case Value::STR:
            var_part_size += it->get_str().size();
            break;
         default:
            Log::fatal("unexpected value type %d", it->get_kind());
            verify(0);
            break;
      }
   }
   for (size_t i = values.size(); i < schema->col_info_.size(); i++) {
      // fake advancing fixed_pos on hidden columns
      switch (schema->col_info_[i].type) {
         case Value::I32:
            fixed_pos += sizeof(i32);
            break;
         case Value::I64:
            fixed_pos += sizeof(i64);
            break;
         case Value::DOUBLE:
            fixed_pos += sizeof(double);
            break;
         case Value::STR:
            break;
         default:
            Log::fatal("unexpected value type %d", schema->col_info_[i].type);
            verify(0);
            break;
      }
   }
   // Log::info("fixed_pos=%u fixed_part_size_=%u", fixed_pos,
   //           schema->fixed_part_size_);
   verify(fixed_pos == schema->fixed_part_size_);

   if (schema->var_size_cols_ > 0) {
      // 2nd pass, write var part
      int var_counter = 0;
      int var_pos = 0;
      row->dense_var_part_ = new char[var_part_size];
      for (auto& it : values) {
         if (it->get_kind() == Value::STR) {
            it->write_binary(&row->dense_var_part_[var_pos]);
            var_pos += it->get_str().size();
            row->dense_var_idx_[var_counter] = var_pos;
            var_counter++;
         }
      }
      verify(var_part_size == var_pos);
      for (size_t i = values.size(); i < schema->col_info_.size(); i++) {
         if (schema->col_info_[i].type == Value::STR) {
            // create an empty var column
            row->dense_var_idx_[var_counter] =
                row->dense_var_idx_[var_counter - 1];
            var_counter++;
         }
      }
      verify(var_counter == schema->var_size_cols_);
   }
   return row;
}

template <class Container>
Row* Row::create(const Schema* schema, const Container& values) {
   verify(values.size() == schema->columns_count());
   std::vector<const Value*> values_ptr(values.size(), nullptr);
   size_t fill_counter = 0;
   for (auto it = values.begin(); it != values.end(); ++it) {
      fill_values_ptr(schema, values_ptr, *it, fill_counter);
      fill_counter++;
   }
   return Row::create(new Row(), schema, values_ptr);
}

/******
 *
 * RefCountedRow
 *
 *
 */

RefCountedRow::RefCountedRow(Row* row) : row_(row) {}
RefCountedRow::RefCountedRow(const RefCountedRow& r)
    : row_((Row*)r.row_->ref_copy()) {}
RefCountedRow::~RefCountedRow() { row_->release(); }
const RefCountedRow& RefCountedRow::operator=(const RefCountedRow& r) {
   if (this != &r) {
      row_->release();
      row_ = (Row*)r.row_->ref_copy();
   }
   return *this;
}
Row* RefCountedRow::get() const { return row_; }

/**
 * CoarseLockedRow
 */

CoarseLockedRow::CoarseLockedRow() : Row(), lock_() {}
// protected dtor as required by RefCounted
CoarseLockedRow::~CoarseLockedRow() {}

void CoarseLockedRow::copy_into(CoarseLockedRow* row) const {
   this->Row::copy_into((Row*)row);
   row->lock_ = lock_;
}

symbol_t CoarseLockedRow::rtti() const { return symbol_t::ROW_COARSE; }

bool CoarseLockedRow::rlock_row_by(lock_owner_t o) { return lock_.rlock_by(o); }
bool CoarseLockedRow::wlock_row_by(lock_owner_t o) { return lock_.wlock_by(o); }
bool CoarseLockedRow::unlock_row_by(lock_owner_t o) {
   return lock_.unlock_by(o);
}

Row* CoarseLockedRow::copy() const {
   CoarseLockedRow* row = new CoarseLockedRow();
   copy_into(row);
   return row;
}

template <class Container>
CoarseLockedRow* CoarseLockedRow::create(const Schema* schema,
                                         const Container& values) {
   verify(values.size() == schema->columns_count());
   std::vector<const Value*> values_ptr(values.size(), nullptr);
   size_t fill_counter = 0;
   for (auto it = values.begin(); it != values.end(); ++it) {
      fill_values_ptr(schema, values_ptr, *it, fill_counter);
      fill_counter++;
   }
   return (CoarseLockedRow*)Row::create(new CoarseLockedRow(), schema,
                                        values_ptr);
}

/**
 * FineLockedRow
 */
void FineLockedRow::init_lock(int n_columns) {
   // lock_ = new rrr::ALock *[n_columns];
   switch (type_2pl_) {
      case WAIT_DIE: {
         lock_ = new rrr::WaitDieALock[n_columns];
         // rrr::WaitDieALock *locks = new rrr::WaitDieALock[n_columns];
         // for (int i = 0; i < n_columns; i++)
         //     lock_[i] = (locks + i);
         break;
      }
      case WOUND_WAIT: {
         lock_ = new rrr::WoundDieALock[n_columns];
         // rrr::WoundDieALock *locks = new rrr::WoundDieALock[n_columns];
         // for (int i = 0; i < n_columns; i++)
         //     lock_[i] = (locks + i);
         break;
      }
      case TIMEOUT: {
         verify(0);
         lock_ = new rrr::TimeoutALock[n_columns];
         // rrr::TimeoutALock *locks = new rrr::TimeoutALock[n_columns];
         // for (int i = 0; i < n_columns; i++)
         //     lock_[i] = (locks + i);
         break;
      }
      default:
         verify(0);
   }
}

// protected dtor as required by RefCounted
FineLockedRow::~FineLockedRow() {
   switch (type_2pl_) {
      case WAIT_DIE:
         delete[] ((rrr::WaitDieALock*)lock_);
         // delete[] ((rrr::WaitDieALock *)lock_[0]);
         break;
      case WOUND_WAIT:
         delete[] ((rrr::WoundDieALock*)lock_);
         // delete[] ((rrr::WoundDieALock *)lock_[0]);
         break;
      case TIMEOUT:
         delete[] ((rrr::TimeoutALock*)lock_);
         // delete[] ((rrr::TimeoutALock *)lock_[0]);
         break;
      default:
         verify(0);
   }
   // delete [] lock_;
}

// FIXME
void FineLockedRow::copy_into(FineLockedRow* row) const {
   verify(0);
   this->Row::copy_into((Row*)row);
   int n_columns = schema_->columns_count();
   row->init_lock(n_columns);
   for (int i = 0; i < n_columns; i++) {
      // row->lock_[i] = lock_[i];
   }
}

void FineLockedRow::set_wait_die() { type_2pl_ = WAIT_DIE; }

void FineLockedRow::set_wound_wait() { type_2pl_ = WOUND_WAIT; }

symbol_t FineLockedRow::rtti() const { return symbol_t::ROW_FINE; }

rrr::ALock* FineLockedRow::get_alock(column_id_t column_id) {
   // return lock_[column_id];
   switch (type_2pl_) {
      case WAIT_DIE:
         return ((rrr::WaitDieALock*)lock_) + column_id;
      case WOUND_WAIT:
         return ((rrr::WoundDieALock*)lock_) + column_id;
      case TIMEOUT:
         return ((rrr::TimeoutALock*)lock_) + column_id;
      default:
         verify(0);
   }
}

Row* FineLockedRow::copy() const {
   FineLockedRow* row = new FineLockedRow();
   copy_into(row);
   return row;
}

template <class Container>
static FineLockedRow* create(const Schema* schema, const Container& values) {
   verify(values.size() == schema->columns_count());
   std::vector<const Value*> values_ptr(values.size(), nullptr);
   size_t fill_counter = 0;
   for (auto it = values.begin(); it != values.end(); ++it) {
      fill_values_ptr(schema, values_ptr, *it, fill_counter);
      fill_counter++;
   }
   FineLockedRow* raw_row = new FineLockedRow();
   raw_row->init_lock(schema->columns_count());
   return (FineLockedRow*)Row::create(raw_row, schema, values_ptr);
}

// **** deprecated **** //
FineLockedRow::type_2pl_t FineLockedRow::type_2pl_ = FineLockedRow::TIMEOUT;
uint64_t FineLockedRow::reg_rlock(column_id_t column_id,
                                  std::function<void(uint64_t)> succ_callback,
                                  std::function<void(void)> fail_callback) {
   verify(0);
   // return lock_[column_id]->lock(succ_callback, fail_callback,
   // rrr::ALock::RLOCK);
}

uint64_t FineLockedRow::reg_wlock(column_id_t column_id,
                                  std::function<void(uint64_t)> succ_callback,
                                  std::function<void(void)> fail_callback) {
   verify(0);
   // return lock_[column_id]->lock(succ_callback, fail_callback,
   // rrr::ALock::WLOCK);
}

void FineLockedRow::abort_lock_req(column_id_t column_id,
                                   uint64_t lock_req_id) {
   verify(0);
   // lock_[column_id]->abort(lock_req_id);
}

void FineLockedRow::unlock_column_by(column_id_t column_id,
                                     uint64_t lock_req_id) {
   verify(0);
   // lock_[column_id]->abort(lock_req_id);
}

/**
 * VersionedRow
 */

void VersionedRow::init_ver(int n_columns) {
   //    ver_ = new version_t[n_columns];
   //    memset(ver_, 0, sizeof(version_t) * n_columns);
   ver_.resize(n_columns, 0);
}

// protected dtor as required by RefCounted
VersionedRow::~VersionedRow() {
   //    delete[] ver_;
}

void VersionedRow::copy_into(VersionedRow* row) const {
   this->CoarseLockedRow::copy_into((CoarseLockedRow*)row);
   int n_columns = schema_->columns_count();
   row->init_ver(n_columns);
   //    memcpy(row->ver_, this->ver_, n_columns * sizeof(version_t));
   row->ver_ = this->ver_;
   verify(row->ver_.size() > 0);
}

symbol_t VersionedRow::rtti() const { return symbol_t::ROW_VERSIONED; }

version_t VersionedRow::get_column_ver(column_id_t column_id) const {
   verify(ver_.size() > 0);
   verify(column_id < ver_.size());
   return ver_[column_id];
}

void VersionedRow::incr_column_ver(column_id_t column_id) { ver_[column_id]++; }

Row* VersionedRow::copy() const {
   VersionedRow* row = new VersionedRow();
   copy_into(row);
   return row;
}

Value VersionedRow::get_column(int column_id) const {
   Value v = Row::get_column(column_id);
   v.ver_ = get_column_ver(column_id);
   return v;
}

// template <class Container>
// VersionedRow* VersionedRow::create(const Schema* schema,
//                                    const Container& values) {
//    verify(values.size() == schema->columns_count());
//    std::vector<const Value*> values_ptr(values.size(), nullptr);
//    size_t fill_counter = 0;
//    for (auto it = values.begin(); it != values.end(); ++it) {
//       fill_values_ptr(schema, values_ptr, *it, fill_counter);
//       fill_counter++;
//    }
//    VersionedRow* raw_row = new VersionedRow();
//    raw_row->init_ver(schema->columns_count());
//    return (VersionedRow*)Row::create(raw_row, schema, values_ptr);
// }

Schema::Schema()
    : var_size_cols_(0),
      fixed_part_size_(0),
      hidden_fixed_(0),
      hidden_var_(0),
      frozen_(false) {}

Schema::~Schema() {}

int Schema::add_hidden_column(const char* name, Value::kind type) {
   verify(!frozen_);
   const bool key = false;  // key: primary index only
   int ret = do_add_column(name, type, key);
   if (type == Value::STR) {
      hidden_var_++;
   } else {
      hidden_fixed_++;
   }
   return ret;
}

int Schema::add_column(const char* name, Value::kind type, bool key) {
   verify(!frozen_ && hidden_fixed_ == 0 && hidden_var_ == 0);
   return do_add_column(name, type, key);  // key: primary index only
}
int Schema::add_key_column(const char* name, Value::kind type) {
   // key: primary index only
   return add_column(name, type, true);
}

column_id_t Schema::get_column_id(const std::string& name) const {
   auto it = col_name_to_id_.find(name);
   if (it != std::end(col_name_to_id_)) {
      assert(col_info_[it->second].id == it->second);
      return it->second;
   }
   return -1;
}
const std::vector<column_id_t>& Schema::key_columns_id() const {
   // key: primary index only
   return key_cols_id_;
}

const column_info* Schema::get_column_info(const std::string& name) const {
   column_id_t col_id = get_column_id(name);
   if (col_id < 0) {
      return nullptr;
   } else {
      return &col_info_[col_id];
   }
}
const column_info* Schema::get_column_info(column_id_t column_id) const {
   return &col_info_[column_id];
}

Schema::iterator Schema::begin() const { return std::begin(col_info_); }
Schema::iterator Schema::end() const {
   return std::end(col_info_) - hidden_fixed_ - hidden_var_;
}
size_t Schema::columns_count() const {
   return col_info_.size() - hidden_fixed_ - hidden_var_;
}
void Schema::freeze() { frozen_ = true; }

int Schema::do_add_column(const char* name, Value::kind type, bool key) {
   column_id_t this_column_id = col_name_to_id_.size();
   if (col_name_to_id_.find(name) != col_name_to_id_.end()) {
      return -1;
   }

   column_info col_info;
   col_info.name = name;
   col_info.id = this_column_id;
   col_info.indexed = key;
   col_info.type = type;

   if (col_info.indexed) {
      key_cols_id_.push_back(col_info.id);
   }

   if (col_info.type == Value::STR) {
      // var size
      col_info.var_size_idx = var_size_cols_;
      var_size_cols_++;
   } else {
      // fixed size
      col_info.fixed_size_offst = fixed_part_size_;
      switch (type) {
         case Value::I32:
            fixed_part_size_ += sizeof(i32);
            break;
         case Value::I64:
            fixed_part_size_ += sizeof(i64);
            break;
         case Value::DOUBLE:
            fixed_part_size_ += sizeof(double);
            break;
         default:
            Log::fatal("value type %d not recognized", (int)type);
            verify(0);
            break;
      }
   }

   insert_into_map(col_name_to_id_, string(name), col_info.id);
   col_info_.push_back(col_info);
   return col_info.id;
}

IndexedSchema::IndexedSchema() : idx_col_(-1) {}

int IndexedSchema::index_column_id() const { return idx_col_; }

void IndexedSchema::index_sanity_check(const std::vector<column_id_t>& idx) {
   set<column_id_t> s(idx.begin(), idx.end());
   verify(s.size() == idx.size());
   for (auto& column_id : idx) {
      verify(column_id >= 0 && column_id < (column_id_t)columns_count());
   }
}

int IndexedSchema::add_index(const char* name,
                             const std::vector<column_id_t>& idx) {
   index_sanity_check(idx);
   int this_idx_id = all_idx_.size();
   if (idx_name_.find(name) != idx_name_.end()) {
      return -1;
   }
   for (auto& col_id : idx) {
      // set up the indexed mark
      col_info_[col_id].indexed = true;
   }
   idx_name_[name] = this_idx_id;
   all_idx_.push_back(idx);
   return this_idx_id;
}

int IndexedSchema::add_index_by_column_names(
    const char* name, const std::vector<std::string>& named_idx) {
   std::vector<column_id_t> idx;
   for (auto& col_name : named_idx) {
      column_id_t col_id = this->get_column_id(col_name);
      verify(col_id >= 0);
      idx.push_back(col_id);
   }
   return this->add_index(name, idx);
}

int IndexedSchema::get_index_id(const std::string& name) {
   auto it = idx_name_.find(name);
   verify(it != idx_name_.end());
   return it->second;
}
const std::vector<column_id_t>& IndexedSchema::get_index(
    const std::string& name) {
   return get_index(get_index_id(name));
}
const std::vector<column_id_t>& IndexedSchema::get_index(int idx_id) {
   return all_idx_[idx_id];
}

void IndexedSchema::freeze() {
   if (!frozen_) {
      idx_col_ = this->add_hidden_column(".index", Value::I64);
      verify(idx_col_ >= 0);
   }
   Schema::freeze();
}

std::vector<std::vector<column_id_t>>::const_iterator
IndexedSchema::index_begin() const {
   return all_idx_.begin();
}

std::vector<std::vector<column_id_t>>::const_iterator IndexedSchema::index_end()
    const {
   return all_idx_.end();
}

Table::Table(std::string name, const Schema* schema)
    : name_(name), schema_(schema) {
   // prevent further changes
   const_cast<Schema*>(schema_)->freeze();
}
Table::~Table() {}

const Schema* Table::schema() const { return schema_; }
const std::string Table::Name() const { return name_; }

uint64_t Table::size() {
   verify(0);
   return 0;
}

void Table::notify_before_update(Row* row, int updated_column_id) {
   // used to notify IndexedTable to update secondary index
}

void Table::notify_after_update(Row* row, int updated_column_id) {
   // used to notify IndexedTable to update secondary index
}

SortedMultiKey::SortedMultiKey(const MultiBlob& mb, const Schema* schema)
    : mb_(mb), schema_(schema) {
   verify(mb_.count() == (int)schema->key_columns_id().size());
}

bool SortedMultiKey::operator==(const SortedMultiKey& o) const {
   return compare(o) == 0;
}
bool SortedMultiKey::operator!=(const SortedMultiKey& o) const {
   return compare(o) != 0;
}
bool SortedMultiKey::operator<(const SortedMultiKey& o) const {
   return compare(o) == -1;
}
bool SortedMultiKey::operator>(const SortedMultiKey& o) const {
   return compare(o) == 1;
}
bool SortedMultiKey::operator<=(const SortedMultiKey& o) const {
   return compare(o) != 1;
}
bool SortedMultiKey::operator>=(const SortedMultiKey& o) const {
   return compare(o) != -1;
}

// -1: this < o, 0: this == o, 1: this > o
// UNKNOWN == UNKNOWN
// both side should have same kind
const MultiBlob& SortedMultiKey::get_multi_blob() const { return mb_; }

int SortedMultiKey::compare(const SortedMultiKey& o) const {
   verify(schema_ == o.schema_);
   const std::vector<int>& key_cols = schema_->key_columns_id();
   for (size_t i = 0; i < key_cols.size(); i++) {
      const column_info* info = schema_->get_column_info(key_cols[i]);
      verify(info->indexed);
      switch (info->type) {
         case Value::I32: {
            i32 mine = *(i32*)mb_[i].data;
            i32 other = *(i32*)o.mb_[i].data;
            assert(mb_[i].len == (int)sizeof(i32));
            assert(o.mb_[i].len == (int)sizeof(i32));
            if (mine < other) {
               return -1;
            } else if (mine > other) {
               return 1;
            }
         } break;
         case Value::I64: {
            i64 mine = *(i64*)mb_[i].data;
            i64 other = *(i64*)o.mb_[i].data;
            assert(mb_[i].len == (int)sizeof(i64));
            assert(o.mb_[i].len == (int)sizeof(i64));
            if (mine < other) {
               return -1;
            } else if (mine > other) {
               return 1;
            }
         } break;
         case Value::DOUBLE: {
            double mine = *(double*)mb_[i].data;
            double other = *(double*)o.mb_[i].data;
            assert(mb_[i].len == (int)sizeof(double));
            assert(o.mb_[i].len == (int)sizeof(double));
            if (mine < other) {
               return -1;
            } else if (mine > other) {
               return 1;
            }
         } break;
         case Value::STR: {
            int min_size = std::min(mb_[i].len, o.mb_[i].len);
            int cmp = memcmp(mb_[i].data, o.mb_[i].data, min_size);
            if (cmp < 0) {
               return -1;
            } else if (cmp > 0) {
               return 1;
            }
            // now check who's longer
            if (mb_[i].len < o.mb_[i].len) {
               return -1;
            } else if (mb_[i].len > o.mb_[i].len) {
               return 1;
            }
         } break;
         default:
            Log::fatal("unexpected column type %d", info->type);
            verify(0);
      }
   }
   return 0;
}

uint64_t SortedTable::size() { return rows_.size(); }

SortedTable::SortedTable(std::string name, const Schema* schema)
    : Table(name, schema) {}

symbol_t SortedTable::rtti() const { return TBL_SORTED; }

void SortedTable::insert(Row* row) {
   SortedMultiKey key = SortedMultiKey(row->get_key(), schema_);
   verify(row->schema() == schema_);
   row->set_table(this);
   insert_into_map(rows_, key, row);
}

SortedTable::Cursor SortedTable::query(const Value& kv) {
   return query(kv.get_blob());
}
SortedTable::Cursor SortedTable::query(const MultiBlob& mb) {
   return query(SortedMultiKey(mb, schema_));
}
SortedTable::Cursor SortedTable::query(const SortedMultiKey& smk) {
   //    auto first = rows_.begin();
   //    auto last = rows_.rbegin();
   // Log::info("rows Size=%u ", rows_.size());
   // if (smk.mb_.count() == 3) {
   //    for (auto& kv : rows_) {
   //       Log::info("first=%d second=%d", smk.mb_ == kv.first.mb_,
   //                 smk.schema_ == kv.first.schema_);
   //       i32 k1 = *(i32*)(void*)(kv.first.mb_[0].data);
   //       i32 k2 = *(i32*)(void*)(smk.mb_[0].data);
   //       Log::info("d_id==%u  %u", k1, k2);
   //       i32 m1 = *(i32*)(void*)(kv.first.mb_[1].data);
   //       i32 m2 = *(i32*)(void*)(smk.mb_[1].data);
   //       Log::info("w_id==%u  %u", m1, m2);
   //    }
   // }

   auto range = rows_.equal_range(smk);
   // for (auto i = range.first; i != range.second; ++i) {
   //    if (i->first.mb_.count() == 3) {
   //       i32 m0 = *(i32*)(void*)(i->first.mb_[0].data);
   //       i32 m1 = *(i32*)(void*)(i->first.mb_[1].data);
   //       i32 m2 = *(i32*)(void*)(i->first.mb_[2].data);
   //       Log_info("%u %u %u", m0, m1, m2);
   //    }
   // }
   uint32_t sz = std::distance(range.first, range.second);
   // Log_info("sz=%u", sz);
   return Cursor(range.first, range.second);
}

SortedTable::Cursor SortedTable::query_lt(const Value& kv, symbol_t order) {
   return query_lt(kv.get_blob(), order);
}
SortedTable::Cursor SortedTable::query_lt(const MultiBlob& mb, symbol_t order) {
   return query_lt(SortedMultiKey(mb, schema_), order);
}
SortedTable::Cursor SortedTable::query_lt(const SortedMultiKey& smk,
                                          symbol_t order) {
   verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC ||
          order == symbol_t::ORD_ANY);
   auto bound = rows_.lower_bound(smk);
   if (order == symbol_t::ORD_DESC) {
      return Cursor(reverse_iterator(bound), rows_.rend());
   } else {
      return Cursor(rows_.begin(), bound);
   }
}

SortedTable::Cursor SortedTable::query_gt(const Value& kv, symbol_t order) {
   return query_gt(kv.get_blob(), order);
}
SortedTable::Cursor SortedTable::query_gt(const MultiBlob& mb, symbol_t order) {
   return query_gt(SortedMultiKey(mb, schema_), order);
}
SortedTable::Cursor SortedTable::query_gt(const SortedMultiKey& smk,
                                          symbol_t order) {
   verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC ||
          order == symbol_t::ORD_ANY);
   auto bound = rows_.upper_bound(smk);
   if (order == symbol_t::ORD_DESC) {
      return Cursor(rows_.rbegin(), reverse_iterator(bound));
   } else {
      return Cursor(bound, rows_.end());
   }
}

// (low, high) not inclusive
SortedTable::Cursor SortedTable::query_in(const Value& low, const Value& high,
                                          symbol_t order) {
   return query_in(low.get_blob(), high.get_blob(), order);
}
SortedTable::Cursor SortedTable::query_in(const MultiBlob& low,
                                          const MultiBlob& high,
                                          symbol_t order) {
   return query_in(SortedMultiKey(low, schema_), SortedMultiKey(high, schema_),
                   order);
}
SortedTable::Cursor SortedTable::query_in(const SortedMultiKey& low,
                                          const SortedMultiKey& high,
                                          symbol_t order) {
   verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC ||
          order == symbol_t::ORD_ANY);
   verify(low < high);
   auto low_bound = rows_.upper_bound(low);
   auto high_bound = rows_.lower_bound(high);
   if (order == symbol_t::ORD_DESC) {
      return Cursor(reverse_iterator(high_bound), reverse_iterator(low_bound));
   } else {
      return Cursor(low_bound, high_bound);
   }
}

SortedTable::Cursor SortedTable::all(symbol_t order) const {
   verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC ||
          order == symbol_t::ORD_ANY);
   if (order == symbol_t::ORD_DESC) {
      return Cursor(rows_.rbegin(), rows_.rend());
   } else {
      return Cursor(std::begin(rows_), std::end(rows_));
   }
}

SortedTable::~SortedTable() {
   for (auto& it : rows_) {
      it.second->release();
   }
}

void SortedTable::clear() {
   for (auto& it : rows_) {
      it.second->release();
   }
   rows_.clear();
}

void SortedTable::remove(const SortedMultiKey& smk) {
   auto query_range = rows_.equal_range(smk);
   iterator it = query_range.first;
   while (it != query_range.second) {
      it = remove(it);
   }
}

void SortedTable::remove(Row* row, bool do_free /* =? */) {
   SortedMultiKey smk = SortedMultiKey(row->get_key(), schema_);
   auto query_range = rows_.equal_range(smk);
   iterator it = query_range.first;
   while (it != query_range.second) {
      if (it->second == row) {
         it->second->set_table(nullptr);
         it = remove(it, do_free);
         break;
      } else {
         ++it;
      }
   }
}

void SortedTable::remove(Cursor cur) {
   iterator it = cur.begin();
   while (it != cur.end()) {
      it = this->remove(it);
   }
}

SortedTable::iterator SortedTable::remove(iterator it, bool do_free /* =? */) {
   if (it != rows_.end()) {
      if (do_free) {
         it->second->release();
      }
      return rows_.erase(it);
   } else {
      return rows_.end();
   }
}
void SortedTable::remove(const Value& kv) { remove(kv.get_blob()); }
void SortedTable::remove(const MultiBlob& mb) {
   remove(SortedMultiKey(mb, schema_));
}
/***
 *
 * UnSortedTable
 *
 *
 */

UnsortedTable::UnsortedTable(std::string name, const Schema* schema)
    : Table(name, schema) {}

symbol_t UnsortedTable::rtti() const { return TBL_UNSORTED; }

void UnsortedTable::insert(Row* row) {
   MultiBlob key = row->get_key();
   verify(row->schema() == schema_);
   row->set_table(this);
   insert_into_map(rows_, key, row);
}

UnsortedTable::Cursor UnsortedTable::query(const Value& kv) {
   return query(kv.get_blob());
}
UnsortedTable::Cursor UnsortedTable::query(const MultiBlob& key) {
   auto range = rows_.equal_range(key);
   return Cursor(range.first, range.second);
}
UnsortedTable::Cursor UnsortedTable::all() const {
   return Cursor(std::begin(rows_), std::end(rows_));
}

UnsortedTable::~UnsortedTable() {
   for (auto& it : rows_) {
      it.second->release();
   }
}

void UnsortedTable::clear() {
   for (auto& it : rows_) {
      it.second->release();
   }
   rows_.clear();
}

void UnsortedTable::remove(const MultiBlob& key) {
   auto query_range = rows_.equal_range(key);
   iterator it = query_range.first;
   while (it != query_range.second) {
      it = remove(it);
   }
}

void UnsortedTable::remove(Row* row, bool do_free /* =? */) {
   auto query_range = rows_.equal_range(row->get_key());
   iterator it = query_range.first;
   while (it != query_range.second) {
      if (it->second == row) {
         it->second->set_table(nullptr);
         it = remove(it, do_free);
         break;
      } else {
         ++it;
      }
   }
}

UnsortedTable::iterator UnsortedTable::remove(iterator it,
                                              bool do_free /* =? */) {
   if (it != rows_.end()) {
      if (do_free) {
         it->second->release();
      }
      return rows_.erase(it);
   } else {
      return rows_.end();
   }
}

/***
 *
 *
 * Snapshot Table
 *
 *
 */

SnapshotTable::SnapshotTable(std::string name, const Schema* sch)
    : Table(name, sch) {}

symbol_t SnapshotTable::rtti() const { return TBL_SNAPSHOT; }

SnapshotTable* SnapshotTable::snapshot() const {
   SnapshotTable* copy = new SnapshotTable(name_, schema_);
   copy->rows_ = rows_.snapshot();
   return copy;
}

void SnapshotTable::insert(Row* row) {
   SortedMultiKey key = SortedMultiKey(row->get_key(), schema_);
   verify(row->schema() == schema_);
   row->set_table(this);

   // make the row readonly, to gaurante snapshot is not changed
   row->make_readonly();

   insert_into_map(rows_, key, RefCountedRow(row));
}
SnapshotTable::Cursor SnapshotTable::query(const Value& kv) {
   return query(kv.get_blob());
}
SnapshotTable::Cursor SnapshotTable::query(const MultiBlob& mb) {
   return query(SortedMultiKey(mb, schema_));
}
SnapshotTable::Cursor SnapshotTable::query(const SortedMultiKey& smk) {
   return Cursor(rows_.query(smk));
}

SnapshotTable::Cursor SnapshotTable::query_lt(const Value& kv, symbol_t order) {
   return query_lt(kv.get_blob(), order);
}
SnapshotTable::Cursor SnapshotTable::query_lt(const MultiBlob& mb,
                                              symbol_t order) {
   return query_lt(SortedMultiKey(mb, schema_), order);
}
SnapshotTable::Cursor SnapshotTable::query_lt(const SortedMultiKey& smk,
                                              symbol_t order) {
   verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC ||
          order == symbol_t::ORD_ANY);
   if (order == symbol_t::ORD_DESC) {
      return Cursor(rows_.reverse_query_lt(smk));
   } else {
      return Cursor(rows_.query_lt(smk));
   }
}

SnapshotTable::Cursor SnapshotTable::query_gt(const Value& kv, symbol_t order) {
   return query_gt(kv.get_blob(), order);
}

SnapshotTable::Cursor SnapshotTable::query_gt(const MultiBlob& mb,
                                              symbol_t order) {
   return query_gt(SortedMultiKey(mb, schema_), order);
}

SnapshotTable::Cursor SnapshotTable::query_gt(const SortedMultiKey& smk,
                                              symbol_t order) {
   verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC ||
          order == symbol_t::ORD_ANY);
   if (order == symbol_t::ORD_DESC) {
      return Cursor(rows_.reverse_query_gt(smk));
   } else {
      return Cursor(rows_.query_gt(smk));
   }
}

// (low, high) not inclusive
SnapshotTable::Cursor SnapshotTable::query_in(const Value& low,
                                              const Value& high,
                                              symbol_t order) {
   return query_in(low.get_blob(), high.get_blob(), order);
}

SnapshotTable::Cursor SnapshotTable::query_in(const MultiBlob& low,
                                              const MultiBlob& high,
                                              symbol_t order) {
   return query_in(SortedMultiKey(low, schema_), SortedMultiKey(high, schema_),
                   order);
}

SnapshotTable::Cursor SnapshotTable::query_in(const SortedMultiKey& low,
                                              const SortedMultiKey& high,
                                              symbol_t order) {
   verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC ||
          order == symbol_t::ORD_ANY);
   verify(low < high);
   if (order == symbol_t::ORD_DESC) {
      return Cursor(rows_.reverse_query_in(low, high));
   } else {
      return Cursor(rows_.query_in(low, high));
   }
}

SnapshotTable::Cursor SnapshotTable::all(symbol_t order) const {
   verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC ||
          order == symbol_t::ORD_ANY);
   if (order == symbol_t::ORD_DESC) {
      return Cursor(rows_.reverse_all());
   } else {
      return Cursor(rows_.all());
   }
}

void SnapshotTable::clear() { rows_ = table_type(); }

void SnapshotTable::remove(const Value& kv) { remove(kv.get_blob()); }
void SnapshotTable::remove(const MultiBlob& mb) {
   remove(SortedMultiKey(mb, schema_));
}
void SnapshotTable::remove(const SortedMultiKey& smk) { rows_.erase(smk); }

void SnapshotTable::remove(Row* row, bool do_free) {
   verify(row->readonly());  // if the row is in this table, it should've
                             // been made readonly
   verify(do_free);  // SnapshotTable only allow do_free == true, because
                     // there won't be any updates
   SortedMultiKey key = SortedMultiKey(row->get_key(), schema_);
   verify(row->schema() == schema_);
   rows_.erase(key, row);
}

void SnapshotTable::remove(const Cursor& cur) {
   if (cur.is_reverse()) {
      rows_.erase(cur.get_reverse_range());
   } else {
      rows_.erase(cur.get_range());
   }
}

/***
 *
 *
 * Index
 *
 *
 */

const Schema* Index::get_schema() const {
   return idx_tbl_->index_schemas_[idx_id_];
}

SortedTable* Index::get_index_table() const {
   return idx_tbl_->indices_[idx_id_];
}

Index::Index(const IndexedTable* idx_tbl, int idx_id)
    : idx_tbl_(idx_tbl), idx_id_(idx_id) {
   verify(idx_id >= 0);
}

const IndexedTable* Index::get_table() { return idx_tbl_; }
int Index::id() { return idx_id_; }

Index::Cursor Index::query(const Value& kv) { return query(kv.get_blob()); }

Index::Cursor Index::query(const MultiBlob& mb) {
   return query(SortedMultiKey(mb, get_schema()));
}

Index::Cursor Index::query_lt(const Value& kv, symbol_t order) {
   return query_lt(kv.get_blob(), order);
}
Index::Cursor Index::query_lt(const MultiBlob& mb, symbol_t order) {
   return query_lt(SortedMultiKey(mb, get_schema()), order);
}
Index::Cursor Index::query_gt(const Value& kv, symbol_t order) {
   return query_gt(kv.get_blob(), order);
}
Index::Cursor Index::query_gt(const MultiBlob& mb, symbol_t order) {
   return query_gt(SortedMultiKey(mb, get_schema()), order);
}

// (low, high) not inclusive
Index::Cursor Index::query_in(const Value& low, const Value& high,
                              symbol_t order) {
   return query_in(low.get_blob(), high.get_blob(), order);
}

Index::Cursor Index::query_in(const MultiBlob& low, const MultiBlob& high,
                              symbol_t order) {
   return query_in(SortedMultiKey(low, get_schema()),
                   SortedMultiKey(high, get_schema()), order);
}

Index::Cursor Index::query(const SortedMultiKey& smk) {
   return Index::Cursor(get_index_table()->query(smk));
}

Index::Cursor Index::query_lt(const SortedMultiKey& smk,
                              symbol_t order /* =? */) {
   return Index::Cursor(get_index_table()->query_lt(smk, order));
}

Index::Cursor Index::query_gt(const SortedMultiKey& smk,
                              symbol_t order /* =? */) {
   return Index::Cursor(get_index_table()->query_gt(smk, order));
}

Index::Cursor Index::query_in(const SortedMultiKey& low,
                              const SortedMultiKey& high,
                              symbol_t order /* =? */) {
   return Index::Cursor(get_index_table()->query_in(low, high, order));
}

Index::Cursor Index::all(symbol_t order /* =? */) const {
   return Index::Cursor(get_index_table()->all());
}

/***
 *
 * IndexTable
 *
 *
 */

int IndexedTable::index_column_id() const {
   return ((IndexedSchema*)schema_)->index_column_id();
}

void IndexedTable::destroy_secondary_indices(master_index* master_idx) {
   // we stop at id = master_idx->size() - 1, since master_idx.back() is the
   // original Row*
   for (size_t id = 0; id < master_idx->size() - 1; id++) {
      Row* row = master_idx->at(id);
      Table* tbl = const_cast<Table*>(row->get_table());
      if (tbl == nullptr) {
         row->release();
      } else {
         tbl->remove(row, true);
      }
   }
   delete master_idx;
}

Row* IndexedTable::make_index_row(Row* base, int idx_id,
                                  master_index* master_idx) {
   vector<Value> idx_keys;

   // pick columns from base row into the index
   for (column_id_t col_id : ((IndexedSchema*)schema_)->get_index(idx_id)) {
      Value picked_value = base->get_column(col_id);
      idx_keys.push_back(picked_value);
   }

   // append pointer to master index on Rows in index table
   idx_keys.push_back(Value((i64)master_idx));

   // create index row and insert them into index table
   Schema* idx_schema = index_schemas_[idx_id];
   Row* idx_row = Row::create(idx_schema, idx_keys);

   // register the index row in master_idx
   verify((*master_idx)[idx_id] == nullptr);
   (*master_idx)[idx_id] = idx_row;

   return idx_row;
}

IndexedTable::IndexedTable(std::string name, const IndexedSchema* schema)
    : SortedTable(name, schema) {
   for (auto idx = schema->index_begin(); idx != schema->index_end(); ++idx) {
      Schema* idx_schema = new Schema;
      for (auto& col_id : *idx) {
         auto col_info = schema->get_column_info(col_id);
         idx_schema->add_key_column(col_info->name.c_str(), col_info->type);
      }
      verify(idx_schema->add_column(".hidden", Value::I64) >= 0);
      SortedTable* idx_tbl = new SortedTable(name, idx_schema);
      index_schemas_.push_back(idx_schema);
      indices_.push_back(idx_tbl);
   }
}

IndexedTable::~IndexedTable() {
   for (auto& it : rows_) {
      // get rid of the index
      Value ptr_value = it.second->get_column(index_column_id());
      master_index* idx = (master_index*)ptr_value.get_i64();
      delete idx;
   }
   for (auto& idx_table : indices_) {
      delete idx_table;
   }
   for (auto& idx_schema : index_schemas_) {
      delete idx_schema;
   }
   // NOTE: ~SortedTable() will be called, releasing Rows in table
}

void IndexedTable::insert(Row* row) {
   Value ptr_value = row->get_column(index_column_id());
   if (ptr_value.get_i64() == 0) {
      master_index* master_idx = new master_index(indices_.size() + 1);

      // the last element in master index points back to the base Row
      master_idx->back() = row;

      for (size_t idx_id = 0; idx_id < master_idx->size() - 1; idx_id++) {
         // pointer slots in master_idx will also be updated
         Row* idx_row = make_index_row(row, idx_id, master_idx);

         SortedTable* idx_tbl = indices_[idx_id];
         idx_tbl->insert(idx_row);
      }
      row->update(index_column_id(), (i64)master_idx);
   }
   this->SortedTable::insert(row);
}

void IndexedTable::remove(Index::Cursor idx_cursor) {
   vector<Row*> rows;
   while (idx_cursor) {
      Row* row = const_cast<Row*>(idx_cursor.next());
      rows.push_back(row);
   }
   for (auto& row : rows) {
      remove(row);
   }
}

IndexedTable::iterator IndexedTable::remove(iterator it,
                                            bool do_free /* =? */) {
   if (it != rows_.end()) {
      if (do_free) {
         Row* row = it->second;
         Value ptr_value = row->get_column(index_column_id());
         master_index* idx = (master_index*)ptr_value.get_i64();
         destroy_secondary_indices(idx);
         row->release();
      }
      return rows_.erase(it);
   } else {
      return rows_.end();
   }
}

void IndexedTable::notify_before_update(Row* row, int updated_column_id) {
   verify(row->get_table() == this);

   Value ptr_value = row->get_column(index_column_id());
   master_index* master_idx = (master_index*)ptr_value.get_i64();
   verify(master_idx != nullptr);

   // remove the affected secondary indices
   for (size_t idx_id = 0; idx_id < indices_.size(); idx_id++) {
      bool affected = false;
      for (column_id_t col_id : ((IndexedSchema*)schema_)->get_index(idx_id)) {
         if (updated_column_id == col_id) {
            affected = true;
            break;
         }
      }
      if (!affected) {
         continue;
      }
      Row* affected_index_row = master_idx->at(idx_id);
      verify(affected_index_row != nullptr);

      // erase affected index row
      SortedTable* idx_tbl = indices_[idx_id];
      idx_tbl->remove(affected_index_row, true);

      // also remove the pointer
      (*master_idx)[idx_id] = nullptr;
   }
}

void IndexedTable::notify_after_update(Row* row, int updated_column_id) {
   verify(row->get_table() == this);

   Value ptr_value = row->get_column(index_column_id());
   master_index* master_idx = (master_index*)ptr_value.get_i64();
   verify(master_idx != nullptr);

   // re-insert the affected secondary indices
   for (size_t idx_id = 0; idx_id < indices_.size(); idx_id++) {
      bool affected = false;
      for (column_id_t col_id : ((IndexedSchema*)schema_)->get_index(idx_id)) {
         if (updated_column_id == col_id) {
            affected = true;
            break;
         }
      }
      if (!affected) {
         continue;
      }

      // NOTE: master index pointers will be updated by make_index_row, so we
      // don't need to explicitly write it
      Row* reconstructed_index_row = make_index_row(row, idx_id, master_idx);
      verify(master_idx->at(idx_id) != nullptr);

      // re-insert reconstructed index row
      SortedTable* idx_tbl = indices_[idx_id];
      idx_tbl->insert(reconstructed_index_row);
   }
}

Index IndexedTable::get_index(int idx_id) const { return Index(this, idx_id); }
Index IndexedTable::get_index(const std::string& idx_name) const {
   return Index(this, ((IndexedSchema*)schema_)->get_index_id(idx_name));
}

}  // namespace mdb
