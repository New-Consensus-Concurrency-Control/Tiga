#include "PaxosMessage.h"

/**
 * Message Serialization and Deserialization
 */

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

Marshal& operator<<(Marshal& m, const PaxosAppend& msg) {
   m << msg.record_ << msg.viewId_ << msg.logId_ << msg.shardId_
     << msg.replicaId_;
   return m;
}

Marshal& operator>>(Marshal& m, PaxosAppend& msg) {
   m >> msg.record_ >> msg.viewId_ >> msg.logId_ >> msg.shardId_ >>
       msg.replicaId_;
   return m;
}

Marshal& operator<<(Marshal& m, const PaxosAppendRep& msg) {
   m << msg.viewId_ << msg.logId_ << msg.shardId_ << msg.replicaId_;
   return m;
}

Marshal& operator>>(Marshal& m, PaxosAppendRep& msg) {
   m >> msg.viewId_ >> msg.logId_ >> msg.shardId_ >> msg.replicaId_;
   return m;
}

Marshal& operator<<(Marshal& m, const PaxosCommitReq& msg) {
   m << msg.viewId_ << msg.logId_;
   return m;
}

Marshal& operator>>(Marshal& m, PaxosCommitReq& msg) {
   m >> msg.viewId_ >> msg.logId_;
   return m;
}