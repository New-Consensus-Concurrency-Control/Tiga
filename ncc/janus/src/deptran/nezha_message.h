#include <memory>

#pragma once

#include "__dep__.h"
#include "command.h"
#include "command_marshaler.h"
#include "rcc/graph.h"
#include "txn_reg.h"

namespace PaxosRPC {
struct ClientRecord {
   uint64_t cmdId_;
   uint64_t ssidLow_;
   uint64_t ssidHigh_;
   uint64_t ssidNew_;
};

struct ClientRecordRep {
   uint64_t cmdId_;
};
Marshal& operator<<(Marshal& m, const ClientRecord& msg);

Marshal& operator>>(Marshal& m, ClientRecord& msg);

Marshal& operator<<(Marshal& m, const ClientRecordRep& msg);

Marshal& operator>>(Marshal& m, ClientRecordRep& msg);

}  // namespace PaxosRPC