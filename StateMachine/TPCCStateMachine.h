
#pragma once
#include <iostream>
#include "StateMachine.h"
#include "TPCCSharding.h"

// inherited from Janus

using rrr::RandomGenerator;

#define C_LAST_SCHEMA (sharding_.g_c_last_schema)
#define C_LAST2ID (sharding_.g_c_last2id)

class TPCCStateMachine : public StateMachine {
  public:
   TPCCSharding sharding_;
   // Used for managing the memdb tables
   mdb::TxnUnsafe dbHandler_;
   // Initialize once instead of lookup every time
   mdb::Table* tbl_dist_;
   mdb::Table* tbl_warehouse_;
   mdb::Table* tbl_customer_;
   mdb::Table* tbl_order_;
   mdb::Table* tbl_neworder_;
   mdb::Table* tbl_item_;
   mdb::Table* tbl_stock_;
   mdb::Table* tbl_orderline_;
   mdb::Table* tbl_history_;
   mdb::Table* tbl_order_cid_secondary_;

   tb_info_t tb_info_warehouse_;
   tb_info_t tb_info_item_;
   tb_info_t tb_info_stock_;

   std::map<int32_t, int32_t> version_track_;
   mutable std::shared_mutex cid_mtx_;

  public:
   TPCCStateMachine(const uint32_t shardId, const uint32_t replicaId,
                    const uint32_t shardNum, const uint32_t replicaNum,
                    const YAML::Node& config);
   std::string RTTI() override;
   void InitializeRelatedShards(
       const uint32_t txnType, std::map<int32_t, Value>* input,
       std::map<uint32_t, std::set<int32_t>>* shardKeyMap) override;
   void InitializeShardsForNewOrder(
       const uint32_t txnType, std::map<int32_t, Value>* input,
       std::map<uint32_t, std::set<int32_t>>* shardKeyMap);
   void InitializeShardsForPayment(
       const uint32_t txnType, std::map<int32_t, Value>* input,
       std::map<uint32_t, std::set<int32_t>>* shardKeyMap);
   void InitializeShardsForStockLevel(
       const uint32_t txnType, std::map<int32_t, Value>* input,
       std::map<uint32_t, std::set<int32_t>>* shardKeyMap);
   void InitializeShardsForOrderStatus(
       const uint32_t txnType, std::map<int32_t, Value>* input,
       std::map<uint32_t, std::set<int32_t>>* shardKeyMap);
   void InitializeShardsForDelivery(
       const uint32_t txnType, std::map<int32_t, Value>* input,
       std::map<uint32_t, std::set<int32_t>>* shardKeyMap);

   void Execute(const uint32_t txnType, const std::vector<int32_t>* localKeys,
                std::map<int32_t, Value>* input,
                std::map<int32_t, Value>* output,
                const uint64_t txnId = 0) override;
   void ExecuteNewOrderTxn(const uint32_t txnType,
                           const std::vector<int32_t>* localKeys,
                           std::map<int32_t, Value>* input,
                           std::map<int32_t, Value>* output,
                           const uint64_t txnId = 0);
   void ExecutePaymentTxn(const uint32_t txnType,
                          const std::vector<int32_t>* localKeys,
                          std::map<int32_t, Value>* input,
                          std::map<int32_t, Value>* output,
                          const uint64_t txnId = 0);
   void ExecuteStockLevelTxn(const uint32_t txnType,
                             const std::vector<int32_t>* localKeys,
                             std::map<int32_t, Value>* input,
                             std::map<int32_t, Value>* output,
                             const uint64_t txnId = 0);
   void ExecuteOrderStatusTxn(const uint32_t txnType,
                              const std::vector<int32_t>* localKeys,
                              std::map<int32_t, Value>* input,
                              std::map<int32_t, Value>* output,
                              const uint64_t txnId = 0);
   void ExecuteDeliveryTxn(const uint32_t txnType,
                           const std::vector<int32_t>* localKeys,
                           std::map<int32_t, Value>* input,
                           std::map<int32_t, Value>* output,
                           const uint64_t txnId = 0);

   void SpecExecute(const uint32_t txnType,
                    const std::vector<int32_t>* localKeys,
                    std::map<int32_t, Value>* input,
                    std::map<int32_t, Value>* output,
                    const uint64_t txnId = 0) override;

   void SpecExecuteNewOrderTxn(const uint32_t txnType,
                               const std::vector<int32_t>* localKeys,
                               std::map<int32_t, Value>* input,
                               std::map<int32_t, Value>* output,
                               const uint64_t txnId = 0);
   void SpecExecutePaymentTxn(const uint32_t txnType,
                              const std::vector<int32_t>* localKeys,
                              std::map<int32_t, Value>* input,
                              std::map<int32_t, Value>* output,
                              const uint64_t txnId = 0);
   void SpecExecuteStockLevelTxn(const uint32_t txnType,
                                 const std::vector<int32_t>* localKeys,
                                 std::map<int32_t, Value>* input,
                                 std::map<int32_t, Value>* output,
                                 const uint64_t txnId = 0);
   void SpecExecuteOrderStatusTxn(const uint32_t txnType,
                                  const std::vector<int32_t>* localKeys,
                                  std::map<int32_t, Value>* input,
                                  std::map<int32_t, Value>* output,
                                  const uint64_t txnId = 0);
   void SpecExecuteDeliveryTxn(const uint32_t txnType,
                               const std::vector<int32_t>* localKeys,
                               std::map<int32_t, Value>* input,
                               std::map<int32_t, Value>* output,
                               const uint64_t txnId = 0);

   void CommitExecute(const uint32_t txnType,
                      const std::vector<int32_t>* localKeys,
                      std::map<int32_t, Value>* input,
                      std::map<int32_t, Value>* output,
                      const uint64_t txnId = 0) override;

   void CommitNewOrderTxn(const uint32_t txnType,
                          const std::vector<int32_t>* localKeys,
                          std::map<int32_t, Value>* input,
                          std::map<int32_t, Value>* output,
                          const uint64_t txnId = 0);
   void CommitPaymentTxn(const uint32_t txnType,
                         const std::vector<int32_t>* localKeys,
                         std::map<int32_t, Value>* input,
                         std::map<int32_t, Value>* output,
                         const uint64_t txnId = 0);
   void CommitStockLevelTxn(const uint32_t txnType,
                            const std::vector<int32_t>* localKeys,
                            std::map<int32_t, Value>* input,
                            std::map<int32_t, Value>* output,
                            const uint64_t txnId = 0);
   void CommitOrderStatusTxn(const uint32_t txnType,
                             const std::vector<int32_t>* localKeys,
                             std::map<int32_t, Value>* input,
                             std::map<int32_t, Value>* output,
                             const uint64_t txnId = 0);
   void CommitDeliveryTxn(const uint32_t txnType,
                          const std::vector<int32_t>* localKeys,
                          std::map<int32_t, Value>* input,
                          std::map<int32_t, Value>* output,
                          const uint64_t txnId = 0);

   void RollbackExecute(const uint32_t txnType,
                        const std::vector<int32_t>* localKeys,
                        std::map<int32_t, Value>* input,
                        std::map<int32_t, Value>* output,
                        const uint64_t txnId = 0) override;
   uint32_t TotalNumberofKeys() override;

   ~TPCCStateMachine();
   void PreRead(const uint32_t txnType, const std::map<int32_t, Value>* input,
                std::map<int32_t, Value>* output) override;
};