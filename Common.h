
#pragma once
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <openssl/sha.h>
#include <semaphore.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <yaml-cpp/yaml.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <functional>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include "concurrentqueue.h"
#include "memdb/value.h"
#include "rrr/misc/rand.hpp"
#include "rrr/rrr.hpp"

// clang-format off
using namespace mdb;
using namespace rrr;
// clang-format on
// Define messages and must write the Marshaling/UnMarshaling method by
// ourselves

#define OWD_SAMPLE_WINDOW_LENGTH (1000)
#define MAX_CLIENT_NUM_PER_VM (12800)
#define MAX_CLIENT_NUM (128000)
// it refers to the max clients per VM/coordinator
#define MAX_SHARD_NUM (6)
#define MAX_REPLICA_NUM (3)
#define MAX_COORDINATOR_NUM (16)
#define TASK_CHANNEL_NUM (8)

#define DEADLINE_SHIFT(a, cid) (((uint64_t)a << 13) + cid)
#define RANK_DEADLINE_INCREMENT(r) (r + (1ul << 13))
#define RANK_TO_DEADLINE(a) (a >> 13)
#define MARK_DEADLINE_MODIFICATION(a, shardId) ((a) | (shardId << 9))
#define FULL_BITMAP(shardNum) ((1u << (shardNum)) - 1)
#define CONCAT_UINT16(a, b) ((((uint32_t)a) << 16u) | ((uint16_t)b))
#define CONCAT_UINT32(a, b) ((((uint64_t)a) << 32u) | ((uint32_t)b))
#define HIGH_32BIT(a) ((uint32_t)(a >> 32))
#define LOW_32BIT(a) ((uint32_t)a)

template <typename T>
using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

#define HASH_PARTITION_ID(key) (key & 0x1f)
#define HASH_PARTITION_NUM (32)

enum STAGE { Dispatching = 1, Commiting, Aborting };

enum CoordSignal { CSTATUS_RUN = 1, CSTATUS_STOP, CSTATUS_SUSPEND };

enum SERVER_STATUS {
   STATUS_NORMAL = 1,
   STATUS_CLEAN_BEFORE_VIEWCHANGE,
   STATUS_WAIT_FOR_CONFIG_MANAGER,
   STATUS_VIEWCHANGE,
   STATUS_VIEWCHANGE_COLLECT_VC,
   STATUS_VIEWCHANGE_REBUILD_LOG,
   STATUS_VIEWCHANGE_LOG_READY,
   STATUS_VIEWCHANGE_LOG_CONFIRMING,
   STATUS_VIEWCHANGE_LOG_CONFIRMED,
   STATUS_VIEWCHANGE_WAIT_START_VIEW,
   STATUS_VIEWCHANGE_STATE_TRANSFER,
   STATUS_TERMINATE,
   STATUS_BEFORE_FAIL,
   STATUS_FAILING
};

enum CM_ACTION_STATUS {
   CM_MONITORING = 100,
   CM_PREPARING_NEW_GVIEW,
   CM_NEW_GVIEW_READY,
   CM_NO_ACTION,
   CM_KILLING
};

enum TXN_TYPE {
   MICRO_TXN = 1,
   TPCC_TXN_MIN,  // boundary
   TPCC_TXN_NEW_ORDER,
   TPCC_TXN_PAYMENT,
   TPCC_TXN_ORDER_STATUS,
   TPCC_TXN_DELIVERY,
   TPCC_TXN_STOCK_LEVEL,
   TPCC_TXN_MAX,  // boundary
};

struct ClientCommand {
   uint32_t clientId_;
   uint32_t reqId_;
   uint32_t txnType_;
   std::map<int32_t, Value> ws_;
   ClientCommand() : clientId_(0), reqId_(0), txnType_(~0) {}
   ClientCommand(const ClientCommand& cmd)
       : clientId_(cmd.clientId_),
         reqId_(cmd.reqId_),
         txnType_(cmd.txnType_),
         ws_(cmd.ws_) {}
   uint64_t TxnKey() const { return CONCAT_UINT32(clientId_, reqId_); }
};

struct ClientReply {
   uint32_t reqId_;
   uint32_t clientId_;
   std::map<int32_t, Value> result_;
   ClientReply() : reqId_(0), clientId_(0) {}
};

struct ClientRequest {
   ClientCommand cmd_;
   std::set<uint32_t> targetShards_;
   std::function<void(const ClientReply& rep)> callback_;
   ClientRequest() : callback_(NULL) {}
};

struct TigaHash {
   uint32_t h1_;
   uint32_t h2_;
   uint32_t h3_;
   uint32_t h4_;
   uint32_t h5_;
   TigaHash() { h1_ = h2_ = h3_ = h4_ = h5_ = 0u; }
   TigaHash(const uint64_t ddl, const uint64_t txnKey) {
      this->CalculateHash(ddl, txnKey);
   }
   void CalculateHash(const uint64_t ddl, const uint64_t txnKey) {
      uint64_t content[2];
      content[0] = ddl;
      content[1] = txnKey;
      SHA1((unsigned char*)content, sizeof(uint64_t) * 2,
           (unsigned char*)(this));
   }
   void XOR(const TigaHash& hsh) {
      h1_ ^= hsh.h1_;
      h2_ ^= hsh.h2_;
      h3_ ^= hsh.h3_;
      h4_ ^= hsh.h4_;
      h5_ ^= hsh.h5_;
   }
   bool Equal(const TigaHash& hsh) const {
      return (h1_ == hsh.h1_ && h2_ == hsh.h2_ && h3_ == hsh.h3_ &&
              h4_ == hsh.h4_ && h5_ == hsh.h5_);
   }
   bool Empty() const {
      return (h1_ == 0 && h2_ == 0 && h3_ == 0 && h4_ == 0 && h5_ == 0);
   }
   void Reset() { h1_ = h2_ = h3_ = h4_ = h5_ = 0; }
   std::string ToString() const {
      return std::to_string(h1_) + "-" + std::to_string(h2_) + "-" +
             std::to_string(h3_) + "-" + std::to_string(h4_) + "-" +
             std::to_string(h5_);
   }
};

using NezhaHash = TigaHash;

// Helper Structs and Functions

struct SyncedHashItem {
   uint32_t key_;
   uint64_t deadlineRank_;
   NezhaHash accumulativeHash_;
   mutable std::shared_mutex mtx_;
   SyncedHashItem(const uint32_t key = 0) : key_(key), deadlineRank_(0) {}
   void Reset() {
      deadlineRank_ = 0;
      accumulativeHash_.Reset();
   }
};

struct SyncedHashMark {
   uint64_t deadlineRank_ = 0;
   NezhaHash accumulativeHash_;
};

template <typename T>
struct EntryMap {
   mutable std::shared_mutex mtx_;
   std::unordered_map<uint64_t, T*> entryMap_;
};

// helpers
template <typename T>
inline T* InsertEntry(EntryMap<T>* mp, uint64_t txnKey, T* entry) {
   EntryMap<T>& entryMap = mp[HASH_PARTITION_ID(txnKey)];
   {
      std::shared_lock lock(entryMap.mtx_);
      auto iter = entryMap.entryMap_.find(txnKey);
      if (iter != entryMap.entryMap_.end()) {
         return iter->second;
      }
   }
   {
      std::unique_lock lock(entryMap.mtx_);
      entryMap.entryMap_[txnKey] = entry;
      return entry;
   }
}

template <typename T>
inline T* GetEntry(EntryMap<T>* mp, uint64_t txnKey) {
   EntryMap<T>& entryMap = mp[HASH_PARTITION_ID(txnKey)];
   std::shared_lock lock(entryMap.mtx_);
   auto iter = entryMap.entryMap_.find(txnKey);
   if (iter == entryMap.entryMap_.end()) {
      return NULL;
   } else {
      return iter->second;
   }
}

template <typename T>
inline void RemoveEntry(EntryMap<T>* mp, uint64_t txnKey) {
   EntryMap<T>& entryMap = mp[HASH_PARTITION_ID(txnKey)];
   std::unique_lock lock(entryMap.mtx_);
   entryMap.entryMap_.erase(txnKey);
}

template <typename T>
inline T* GetOrCreateEntryByKey(EntryMap<T>& mp, uint64_t key,
                                std::shared_mutex& mtx) {
   {
      std::shared_lock lock(mtx);
      const auto& iter = mp.entryMap_.find(key);
      if (iter != mp.entryMap_.end()) {
         return iter->second;
      }
   }
   // If this entry has not been created, we create it
   std::unique_lock lock(mtx);
   T* itm = new T(key);
   mp.entryMap_[key] = itm;
   return itm;
}

template <typename T>
struct EntryQu {
   mutable std::shared_mutex mtx_;
   uint64_t key_;
   std::queue<T*> qu_;
   EntryQu(const uint64_t key = 0) : key_(key) {}
};

inline uint64_t GetMicrosecondTimestamp(const int64_t offset = 0) {
   auto tse = std::chrono::system_clock::now().time_since_epoch();
   int64_t t =
       std::chrono::duration_cast<std::chrono::microseconds>(tse).count();
   return t + offset;
}

inline Marshal& operator<<(Marshal& m, const std::map<int32_t, Value>& res) {
   uint32_t resultSize = res.size();
   m << resultSize;
   for (const auto& kv : res) {
      m << kv.first << kv.second;
   }
   return m;
}

inline Marshal& operator>>(Marshal& m, std::map<int32_t, Value>& res) {
   uint32_t resultSize = 0;
   m >> resultSize;
   for (uint32_t i = 0; i < resultSize; i++) {
      int32_t k;
      Value v;
      m >> k >> v;
      res[k] = v;
   }
   return m;
}

inline Marshal& operator<<(Marshal& m, const ClientCommand& cmd) {
   m << cmd.clientId_ << cmd.reqId_ << cmd.txnType_ << cmd.ws_;
   return m;
}

inline Marshal& operator>>(Marshal& m, ClientCommand& cmd) {
   m >> cmd.clientId_ >> cmd.reqId_ >> cmd.txnType_ >> cmd.ws_;
   return m;
}

inline Marshal& operator<<(Marshal& m, const TigaHash& msg) {
   m << msg.h1_ << msg.h2_ << msg.h3_ << msg.h4_ << msg.h5_;
   return m;
}

inline Marshal& operator>>(Marshal& m, TigaHash& msg) {
   m >> msg.h1_ >> msg.h2_ >> msg.h3_ >> msg.h4_ >> msg.h5_;
   return m;
}

inline TigaHash AddKeyToHash(const TigaHash& hsh, const uint32_t key) {
   unsigned char ch[sizeof(TigaHash) + sizeof(uint32_t)];
   memcpy(ch, &hsh, sizeof(TigaHash));
   memcpy(ch + sizeof(TigaHash), &key, sizeof(uint32_t));
   TigaHash ret;
   SHA1((unsigned char*)ch, sizeof(TigaHash) + sizeof(uint32_t),
        (unsigned char*)(void*)(&ret));
   return ret;
}

inline void ThreadSleepFor(const uint32_t sleepMicroSecond) {
   std::chrono::microseconds duration(sleepMicroSecond);
   std::this_thread::yield();
   std::this_thread::sleep_for(duration);
}