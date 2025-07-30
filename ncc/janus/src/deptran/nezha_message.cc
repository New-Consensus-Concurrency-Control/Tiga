#include "nezha_message.h"

namespace PaxosRPC {
Marshal& operator<<(Marshal& m, const ClientRecord& msg) {
   m << msg.cmdId_ << msg.ssidLow_ << msg.ssidHigh_ << msg.ssidNew_;
   return m;
}

Marshal& operator>>(Marshal& m, ClientRecord& msg) {
   m >> msg.cmdId_ >> msg.ssidLow_ >> msg.ssidHigh_ >> msg.ssidNew_;
   return m;
}

Marshal& operator<<(Marshal& m, const ClientRecordRep& msg) {
   m << msg.cmdId_;
   return m;
}

Marshal& operator>>(Marshal& m, ClientRecordRep& msg) {
   m >> msg.cmdId_;
   return m;
}

}  // namespace PaxosRPC