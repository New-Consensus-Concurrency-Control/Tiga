#pragma once

#include "__dep__.h"
#include "communicator.h"
#include "config.h"
#include "procedure.h"

namespace janus {

class ClientControlServiceImpl;
class Workload;
class CoordinatorBase;
class Frame;
class Coordinator;
class TxnRegistry;
class TxReply;

struct LatencyItem {
   uint64_t reqId_;
   uint64_t sendTime_;
   uint64_t commitTime_;
   int ntry_;
   uint64_t preDisPatchCompleteTime_;
   uint32_t txnType_;
};

class ClientWorker {
  public:
   std::mutex latencyVecMtx_;
   std::vector<LatencyItem> latencyVec_;
   uint64_t last_report_time_ = 0;
   PollMgr *poll_mgr_{nullptr};
   Frame *frame_{nullptr};
   Communicator *commo_{nullptr};
   cliid_t cli_id_;
   int32_t benchmark;
   int32_t mode;
   bool batch_start;
   uint32_t id;
   uint32_t duration;
   ClientControlServiceImpl *ccsi{nullptr};
   int32_t n_concurrent_;
   rrr::Mutex finish_mutex{};
   rrr::CondVar finish_cond{};
   bool forward_requests_to_leader_ = false;

   // coordinators_{mutex, cond} synchronization currently only used for open
   // clients
   std::mutex request_gen_mutex{};
   std::mutex coordinator_mutex{};
   vector<Coordinator *> free_coordinators_{};
   vector<Coordinator *> created_coordinators_{};
   //  rrr::ThreadPool* dispatch_pool_ = new rrr::ThreadPool();

   std::atomic<uint32_t> num_txn, success, num_try, ssid_consistent, decided,
       validation_passed, cascading_aborts, rotxn_aborts, early_aborts;
   //                offset_valid,
   //                cascading_aborts, single_shard, single_shard_write_only;
   int all_done_{0};
   int64_t n_tx_issued_{0};
   SharedIntEvent n_ceased_client_{};
   SharedIntEvent sp_n_tx_done_{};  // TODO refactor: remove sp_
   Workload *tx_generator_{nullptr};
   Timer *timer_{nullptr};
   shared_ptr<TxnRegistry> txn_reg_{nullptr};
   Config *config_{nullptr};
   Config::SiteInfo &my_site_;
   vector<string> servers_;
   std::atomic<uint32_t> globalIssued_, globalCommitted_;
   rrr::ThreadPool *dispatch_pool_ = new rrr::ThreadPool();
   std::atomic<uint32_t> outstandingRequests_;

  public:
   ClientWorker(uint32_t id, Config::SiteInfo &site_info, Config *config,
                ClientControlServiceImpl *ccsi, PollMgr *mgr);
   ClientWorker() = delete;
   ~ClientWorker();
   void Log();
   // This is called from a different thread.
   void Work();
   void Work2();
   Coordinator *FindOrCreateCoordinator();
   void DispatchRequest2(Coordinator *coo);
   void DispatchRequest(Coordinator *coo);
   void AcceptForwardedRequest(TxRequest &request, TxReply *txn_reply,
                               rrr::DeferredReply *defer);

  protected:
   std::shared_ptr<OneTimeJob> CreateOneTimeJob();
   Coordinator *CreateCoordinator(uint16_t offset_id);
   void RequestDone(Coordinator *coo, TxReply &txn_reply);
   void ForwardRequestDone(Coordinator *coo, TxReply *output,
                           rrr::DeferredReply *defer, TxReply &txn_reply);
};
}  // namespace janus
