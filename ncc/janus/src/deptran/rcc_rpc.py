import os
from simplerpc.marshal import Marshal
from simplerpc.future import Future

ValueTimesPair = Marshal.reg_type('ValueTimesPair', [('value', 'rrr::i64'), ('times', 'rrr::i64')])

TxnInfoRes = Marshal.reg_type('TxnInfoRes', [('start_txn', 'rrr::i32'), ('total_txn', 'rrr::i32'), ('total_try', 'rrr::i32'), ('commit_txn', 'rrr::i32'), ('num_exhausted', 'rrr::i32'), ('this_latency', 'std::vector<double>'), ('last_latency', 'std::vector<double>'), ('attempt_latency', 'std::vector<double>'), ('interval_latency', 'std::vector<double>'), ('all_interval_latency', 'std::vector<double>'), ('num_try', 'std::vector<rrr::i32>')])

ServerResponse = Marshal.reg_type('ServerResponse', [('statistics', 'std::map<std::string, ValueTimesPair>'), ('cpu_util', 'double'), ('r_cnt_sum', 'rrr::i64'), ('r_cnt_num', 'rrr::i64'), ('r_sz_sum', 'rrr::i64'), ('r_sz_num', 'rrr::i64')])

ClientResponse = Marshal.reg_type('ClientResponse', [('txn_info', 'std::map<rrr::i32, TxnInfoRes>'), ('run_sec', 'rrr::i64'), ('run_nsec', 'rrr::i64'), ('period_sec', 'rrr::i64'), ('period_nsec', 'rrr::i64'), ('is_finish', 'rrr::i32'), ('n_asking', 'rrr::i64')])

TxDispatchRequest = Marshal.reg_type('TxDispatchRequest', [('id', 'rrr::i32'), ('tx_type', 'rrr::i32'), ('input', 'std::vector<Value>')])

TxnDispatchResponse = Marshal.reg_type('TxnDispatchResponse', [])

class MultiPaxosService(object):
    FORWARD = 0x33586f09
    PREPARE = 0x2fe7386b
    ACCEPT = 0x451f1afe
    DECIDE = 0x678fa9d9

    __input_type_info__ = {
        'Forward': ['MarshallDeputy'],
        'Prepare': ['uint64_t','ballot_t'],
        'Accept': ['uint64_t','ballot_t','MarshallDeputy'],
        'Decide': ['uint64_t','ballot_t','MarshallDeputy'],
    }

    __output_type_info__ = {
        'Forward': [],
        'Prepare': ['ballot_t'],
        'Accept': ['ballot_t'],
        'Decide': [],
    }

    def __bind_helper__(self, func):
        def f(*args):
            return getattr(self, func.__name__)(*args)
        return f

    def __reg_to__(self, server):
        server.__reg_func__(MultiPaxosService.FORWARD, self.__bind_helper__(self.Forward), ['MarshallDeputy'], [])
        server.__reg_func__(MultiPaxosService.PREPARE, self.__bind_helper__(self.Prepare), ['uint64_t','ballot_t'], ['ballot_t'])
        server.__reg_func__(MultiPaxosService.ACCEPT, self.__bind_helper__(self.Accept), ['uint64_t','ballot_t','MarshallDeputy'], ['ballot_t'])
        server.__reg_func__(MultiPaxosService.DECIDE, self.__bind_helper__(self.Decide), ['uint64_t','ballot_t','MarshallDeputy'], [])

    def Forward(__self__, cmd):
        raise NotImplementedError('subclass MultiPaxosService and implement your own Forward function')

    def Prepare(__self__, slot, ballot):
        raise NotImplementedError('subclass MultiPaxosService and implement your own Prepare function')

    def Accept(__self__, slot, ballot, cmd):
        raise NotImplementedError('subclass MultiPaxosService and implement your own Accept function')

    def Decide(__self__, slot, ballot, cmd):
        raise NotImplementedError('subclass MultiPaxosService and implement your own Decide function')

class MultiPaxosProxy(object):
    def __init__(self, clnt):
        self.__clnt__ = clnt

    def async_Forward(__self__, cmd):
        return __self__.__clnt__.async_call(MultiPaxosService.FORWARD, [cmd], MultiPaxosService.__input_type_info__['Forward'], MultiPaxosService.__output_type_info__['Forward'])

    def async_Prepare(__self__, slot, ballot):
        return __self__.__clnt__.async_call(MultiPaxosService.PREPARE, [slot, ballot], MultiPaxosService.__input_type_info__['Prepare'], MultiPaxosService.__output_type_info__['Prepare'])

    def async_Accept(__self__, slot, ballot, cmd):
        return __self__.__clnt__.async_call(MultiPaxosService.ACCEPT, [slot, ballot, cmd], MultiPaxosService.__input_type_info__['Accept'], MultiPaxosService.__output_type_info__['Accept'])

    def async_Decide(__self__, slot, ballot, cmd):
        return __self__.__clnt__.async_call(MultiPaxosService.DECIDE, [slot, ballot, cmd], MultiPaxosService.__input_type_info__['Decide'], MultiPaxosService.__output_type_info__['Decide'])

    def sync_Forward(__self__, cmd):
        __result__ = __self__.__clnt__.sync_call(MultiPaxosService.FORWARD, [cmd], MultiPaxosService.__input_type_info__['Forward'], MultiPaxosService.__output_type_info__['Forward'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_Prepare(__self__, slot, ballot):
        __result__ = __self__.__clnt__.sync_call(MultiPaxosService.PREPARE, [slot, ballot], MultiPaxosService.__input_type_info__['Prepare'], MultiPaxosService.__output_type_info__['Prepare'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_Accept(__self__, slot, ballot, cmd):
        __result__ = __self__.__clnt__.sync_call(MultiPaxosService.ACCEPT, [slot, ballot, cmd], MultiPaxosService.__input_type_info__['Accept'], MultiPaxosService.__output_type_info__['Accept'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_Decide(__self__, slot, ballot, cmd):
        __result__ = __self__.__clnt__.sync_call(MultiPaxosService.DECIDE, [slot, ballot, cmd], MultiPaxosService.__input_type_info__['Decide'], MultiPaxosService.__output_type_info__['Decide'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

class ClassicService(object):
    MSGSTRING = 0x2ff0de6c
    MSGMARSHALL = 0x48ff3951
    DISPATCH = 0x31a46cbb
    PREPARE = 0x3ff9af5c
    COMMIT = 0x23f48b32
    ABORT = 0x2a061746
    UPGRADEEPOCH = 0x1761a376
    TRUNCATEEPOCH = 0x1e16ec03
    RPC_NULL = 0x3d75326c
    TAPIRACCEPT = 0x6cd6646a
    TAPIRFASTACCEPT = 0x373b0c11
    TAPIRDECIDE = 0x280428a6
    RCCDISPATCH = 0x54c8fa62
    RCCFINISH = 0x5588bbe2
    RCCINQUIRE = 0x33d9b435
    RCCDISPATCHRO = 0x1fd2dd13
    RCCINQUIREVALIDATION = 0x10e4cb5d
    RCCNOTIFYGLOBALVALIDATION = 0x4b1d8966
    JANUSDISPATCH = 0x5db6a23d
    RCCCOMMIT = 0x361584bf
    JANUSCOMMIT = 0x3db5db7d
    JANUSCOMMITWOGRAPH = 0x61854dd7
    JANUSINQUIRE = 0x35015411
    RCCPREACCEPT = 0x58456ae0
    JANUSPREACCEPT = 0x4655941d
    JANUSPREACCEPTWOGRAPH = 0x52204b15
    RCCACCEPT = 0x27c966c7
    JANUSACCEPT = 0x40c2dab8
    PREACCEPTFEBRUUS = 0x3cdd9493
    ACCEPTFEBRUUS = 0x2627ed9b
    COMMITFEBRUUS = 0x240c2617
    ACCDISPATCH = 0x18dbb5db
    ACCROTXNDISPATCH = 0x3ad2902a
    ACCVALIDATE = 0x65707775
    ACCFINALIZE = 0x19742c66
    ACCSTATUSQUERY = 0x419b3ed0
    ACCRESOLVESTATUSCOORD = 0x1d41f268
    ACCGETRECORD = 0x5fc5ea62

    __input_type_info__ = {
        'MsgString': ['std::string'],
        'MsgMarshall': ['MarshallDeputy'],
        'Dispatch': ['uint32_t','rrr::i64','MarshallDeputy'],
        'Prepare': ['rrr::i64','std::vector<rrr::i32>'],
        'Commit': ['rrr::i64'],
        'Abort': ['rrr::i64'],
        'UpgradeEpoch': ['uint32_t'],
        'TruncateEpoch': ['uint32_t'],
        'rpc_null': [],
        'TapirAccept': ['uint64_t','int64_t','int32_t'],
        'TapirFastAccept': ['uint64_t','std::vector<SimpleCommand>'],
        'TapirDecide': ['uint64_t','rrr::i32'],
        'RccDispatch': ['std::vector<SimpleCommand>'],
        'RccFinish': ['cmdid_t','MarshallDeputy'],
        'RccInquire': ['txnid_t','int32_t'],
        'RccDispatchRo': ['SimpleCommand'],
        'RccInquireValidation': ['txid_t','int32_t'],
        'RccNotifyGlobalValidation': ['txid_t','int32_t','int32_t'],
        'JanusDispatch': ['std::vector<SimpleCommand>'],
        'RccCommit': ['cmdid_t','rank_t','int32_t','parent_set_t'],
        'JanusCommit': ['cmdid_t','rank_t','int32_t','MarshallDeputy'],
        'JanusCommitWoGraph': ['cmdid_t','rank_t','int32_t'],
        'JanusInquire': ['epoch_t','txnid_t'],
        'RccPreAccept': ['cmdid_t','rank_t','std::vector<SimpleCommand>'],
        'JanusPreAccept': ['cmdid_t','rank_t','std::vector<SimpleCommand>','MarshallDeputy'],
        'JanusPreAcceptWoGraph': ['cmdid_t','rank_t','std::vector<SimpleCommand>'],
        'RccAccept': ['cmdid_t','rrr::i32','ballot_t','parent_set_t'],
        'JanusAccept': ['cmdid_t','rrr::i32','ballot_t','MarshallDeputy'],
        'PreAcceptFebruus': ['txid_t'],
        'AcceptFebruus': ['txid_t','ballot_t','uint64_t'],
        'CommitFebruus': ['txid_t','uint64_t'],
        'AccDispatch': ['uint32_t','rrr::i64','MarshallDeputy','uint64_t','uint8_t'],
        'AccRotxnDispatch': ['uint32_t','rrr::i64','MarshallDeputy','uint64_t','uint64_t'],
        'AccValidate': ['rrr::i64','uint64_t'],
        'AccFinalize': ['rrr::i64','int8_t'],
        'AccStatusQuery': ['rrr::i64'],
        'AccResolveStatusCoord': ['cmdid_t'],
        'AccGetRecord': ['cmdid_t'],
    }

    __output_type_info__ = {
        'MsgString': ['std::string'],
        'MsgMarshall': ['MarshallDeputy'],
        'Dispatch': ['rrr::i32','TxnOutput'],
        'Prepare': ['rrr::i32'],
        'Commit': ['rrr::i32'],
        'Abort': ['rrr::i32'],
        'UpgradeEpoch': ['int32_t'],
        'TruncateEpoch': [],
        'rpc_null': [],
        'TapirAccept': [],
        'TapirFastAccept': ['rrr::i32'],
        'TapirDecide': [],
        'RccDispatch': ['rrr::i32','TxnOutput','MarshallDeputy'],
        'RccFinish': ['std::map<uint32_t, std::map<int32_t, Value>>'],
        'RccInquire': ['std::map<uint64_t, parent_set_t>'],
        'RccDispatchRo': ['std::map<rrr::i32, Value>'],
        'RccInquireValidation': ['int32_t'],
        'RccNotifyGlobalValidation': [],
        'JanusDispatch': ['rrr::i32','TxnOutput','MarshallDeputy'],
        'RccCommit': ['int32_t','TxnOutput'],
        'JanusCommit': ['int32_t','TxnOutput'],
        'JanusCommitWoGraph': ['int32_t','TxnOutput'],
        'JanusInquire': ['MarshallDeputy'],
        'RccPreAccept': ['rrr::i32','parent_set_t'],
        'JanusPreAccept': ['rrr::i32','MarshallDeputy'],
        'JanusPreAcceptWoGraph': ['rrr::i32','MarshallDeputy'],
        'RccAccept': ['rrr::i32'],
        'JanusAccept': ['rrr::i32'],
        'PreAcceptFebruus': ['rrr::i32','uint64_t'],
        'AcceptFebruus': ['rrr::i32'],
        'CommitFebruus': ['rrr::i32'],
        'AccDispatch': ['rrr::i32','uint64_t','uint64_t','uint64_t','TxnOutput','uint64_t','uint8_t','std::pair<parid_t, uint64_t>'],
        'AccRotxnDispatch': ['rrr::i32','uint64_t','uint64_t','uint64_t','TxnOutput','uint64_t','uint8_t','std::pair<parid_t, uint64_t>'],
        'AccValidate': ['int8_t'],
        'AccFinalize': [],
        'AccStatusQuery': ['int8_t'],
        'AccResolveStatusCoord': ['uint8_t'],
        'AccGetRecord': ['uint8_t','uint64_t','uint64_t'],
    }

    def __bind_helper__(self, func):
        def f(*args):
            return getattr(self, func.__name__)(*args)
        return f

    def __reg_to__(self, server):
        server.__reg_func__(ClassicService.MSGSTRING, self.__bind_helper__(self.MsgString), ['std::string'], ['std::string'])
        server.__reg_func__(ClassicService.MSGMARSHALL, self.__bind_helper__(self.MsgMarshall), ['MarshallDeputy'], ['MarshallDeputy'])
        server.__reg_func__(ClassicService.DISPATCH, self.__bind_helper__(self.Dispatch), ['uint32_t','rrr::i64','MarshallDeputy'], ['rrr::i32','TxnOutput'])
        server.__reg_func__(ClassicService.PREPARE, self.__bind_helper__(self.Prepare), ['rrr::i64','std::vector<rrr::i32>'], ['rrr::i32'])
        server.__reg_func__(ClassicService.COMMIT, self.__bind_helper__(self.Commit), ['rrr::i64'], ['rrr::i32'])
        server.__reg_func__(ClassicService.ABORT, self.__bind_helper__(self.Abort), ['rrr::i64'], ['rrr::i32'])
        server.__reg_func__(ClassicService.UPGRADEEPOCH, self.__bind_helper__(self.UpgradeEpoch), ['uint32_t'], ['int32_t'])
        server.__reg_func__(ClassicService.TRUNCATEEPOCH, self.__bind_helper__(self.TruncateEpoch), ['uint32_t'], [])
        server.__reg_func__(ClassicService.RPC_NULL, self.__bind_helper__(self.rpc_null), [], [])
        server.__reg_func__(ClassicService.TAPIRACCEPT, self.__bind_helper__(self.TapirAccept), ['uint64_t','int64_t','int32_t'], [])
        server.__reg_func__(ClassicService.TAPIRFASTACCEPT, self.__bind_helper__(self.TapirFastAccept), ['uint64_t','std::vector<SimpleCommand>'], ['rrr::i32'])
        server.__reg_func__(ClassicService.TAPIRDECIDE, self.__bind_helper__(self.TapirDecide), ['uint64_t','rrr::i32'], [])
        server.__reg_func__(ClassicService.RCCDISPATCH, self.__bind_helper__(self.RccDispatch), ['std::vector<SimpleCommand>'], ['rrr::i32','TxnOutput','MarshallDeputy'])
        server.__reg_func__(ClassicService.RCCFINISH, self.__bind_helper__(self.RccFinish), ['cmdid_t','MarshallDeputy'], ['std::map<uint32_t, std::map<int32_t, Value>>'])
        server.__reg_func__(ClassicService.RCCINQUIRE, self.__bind_helper__(self.RccInquire), ['txnid_t','int32_t'], ['std::map<uint64_t, parent_set_t>'])
        server.__reg_func__(ClassicService.RCCDISPATCHRO, self.__bind_helper__(self.RccDispatchRo), ['SimpleCommand'], ['std::map<rrr::i32, Value>'])
        server.__reg_func__(ClassicService.RCCINQUIREVALIDATION, self.__bind_helper__(self.RccInquireValidation), ['txid_t','int32_t'], ['int32_t'])
        server.__reg_func__(ClassicService.RCCNOTIFYGLOBALVALIDATION, self.__bind_helper__(self.RccNotifyGlobalValidation), ['txid_t','int32_t','int32_t'], [])
        server.__reg_func__(ClassicService.JANUSDISPATCH, self.__bind_helper__(self.JanusDispatch), ['std::vector<SimpleCommand>'], ['rrr::i32','TxnOutput','MarshallDeputy'])
        server.__reg_func__(ClassicService.RCCCOMMIT, self.__bind_helper__(self.RccCommit), ['cmdid_t','rank_t','int32_t','parent_set_t'], ['int32_t','TxnOutput'])
        server.__reg_func__(ClassicService.JANUSCOMMIT, self.__bind_helper__(self.JanusCommit), ['cmdid_t','rank_t','int32_t','MarshallDeputy'], ['int32_t','TxnOutput'])
        server.__reg_func__(ClassicService.JANUSCOMMITWOGRAPH, self.__bind_helper__(self.JanusCommitWoGraph), ['cmdid_t','rank_t','int32_t'], ['int32_t','TxnOutput'])
        server.__reg_func__(ClassicService.JANUSINQUIRE, self.__bind_helper__(self.JanusInquire), ['epoch_t','txnid_t'], ['MarshallDeputy'])
        server.__reg_func__(ClassicService.RCCPREACCEPT, self.__bind_helper__(self.RccPreAccept), ['cmdid_t','rank_t','std::vector<SimpleCommand>'], ['rrr::i32','parent_set_t'])
        server.__reg_func__(ClassicService.JANUSPREACCEPT, self.__bind_helper__(self.JanusPreAccept), ['cmdid_t','rank_t','std::vector<SimpleCommand>','MarshallDeputy'], ['rrr::i32','MarshallDeputy'])
        server.__reg_func__(ClassicService.JANUSPREACCEPTWOGRAPH, self.__bind_helper__(self.JanusPreAcceptWoGraph), ['cmdid_t','rank_t','std::vector<SimpleCommand>'], ['rrr::i32','MarshallDeputy'])
        server.__reg_func__(ClassicService.RCCACCEPT, self.__bind_helper__(self.RccAccept), ['cmdid_t','rrr::i32','ballot_t','parent_set_t'], ['rrr::i32'])
        server.__reg_func__(ClassicService.JANUSACCEPT, self.__bind_helper__(self.JanusAccept), ['cmdid_t','rrr::i32','ballot_t','MarshallDeputy'], ['rrr::i32'])
        server.__reg_func__(ClassicService.PREACCEPTFEBRUUS, self.__bind_helper__(self.PreAcceptFebruus), ['txid_t'], ['rrr::i32','uint64_t'])
        server.__reg_func__(ClassicService.ACCEPTFEBRUUS, self.__bind_helper__(self.AcceptFebruus), ['txid_t','ballot_t','uint64_t'], ['rrr::i32'])
        server.__reg_func__(ClassicService.COMMITFEBRUUS, self.__bind_helper__(self.CommitFebruus), ['txid_t','uint64_t'], ['rrr::i32'])
        server.__reg_func__(ClassicService.ACCDISPATCH, self.__bind_helper__(self.AccDispatch), ['uint32_t','rrr::i64','MarshallDeputy','uint64_t','uint8_t'], ['rrr::i32','uint64_t','uint64_t','uint64_t','TxnOutput','uint64_t','uint8_t','std::pair<parid_t, uint64_t>'])
        server.__reg_func__(ClassicService.ACCROTXNDISPATCH, self.__bind_helper__(self.AccRotxnDispatch), ['uint32_t','rrr::i64','MarshallDeputy','uint64_t','uint64_t'], ['rrr::i32','uint64_t','uint64_t','uint64_t','TxnOutput','uint64_t','uint8_t','std::pair<parid_t, uint64_t>'])
        server.__reg_func__(ClassicService.ACCVALIDATE, self.__bind_helper__(self.AccValidate), ['rrr::i64','uint64_t'], ['int8_t'])
        server.__reg_func__(ClassicService.ACCFINALIZE, self.__bind_helper__(self.AccFinalize), ['rrr::i64','int8_t'], [])
        server.__reg_func__(ClassicService.ACCSTATUSQUERY, self.__bind_helper__(self.AccStatusQuery), ['rrr::i64'], ['int8_t'])
        server.__reg_func__(ClassicService.ACCRESOLVESTATUSCOORD, self.__bind_helper__(self.AccResolveStatusCoord), ['cmdid_t'], ['uint8_t'])
        server.__reg_func__(ClassicService.ACCGETRECORD, self.__bind_helper__(self.AccGetRecord), ['cmdid_t'], ['uint8_t','uint64_t','uint64_t'])

    def MsgString(__self__, arg):
        raise NotImplementedError('subclass ClassicService and implement your own MsgString function')

    def MsgMarshall(__self__, arg):
        raise NotImplementedError('subclass ClassicService and implement your own MsgMarshall function')

    def Dispatch(__self__, coo_id, tid, cmd):
        raise NotImplementedError('subclass ClassicService and implement your own Dispatch function')

    def Prepare(__self__, tid, sids):
        raise NotImplementedError('subclass ClassicService and implement your own Prepare function')

    def Commit(__self__, tid):
        raise NotImplementedError('subclass ClassicService and implement your own Commit function')

    def Abort(__self__, tid):
        raise NotImplementedError('subclass ClassicService and implement your own Abort function')

    def UpgradeEpoch(__self__, curr_epoch):
        raise NotImplementedError('subclass ClassicService and implement your own UpgradeEpoch function')

    def TruncateEpoch(__self__, old_epoch):
        raise NotImplementedError('subclass ClassicService and implement your own TruncateEpoch function')

    def rpc_null(__self__):
        raise NotImplementedError('subclass ClassicService and implement your own rpc_null function')

    def TapirAccept(__self__, cmd_id, ballot, decision):
        raise NotImplementedError('subclass ClassicService and implement your own TapirAccept function')

    def TapirFastAccept(__self__, cmd_id, txn_cmds):
        raise NotImplementedError('subclass ClassicService and implement your own TapirFastAccept function')

    def TapirDecide(__self__, cmd_id, commit):
        raise NotImplementedError('subclass ClassicService and implement your own TapirDecide function')

    def RccDispatch(__self__, cmd):
        raise NotImplementedError('subclass ClassicService and implement your own RccDispatch function')

    def RccFinish(__self__, id, md_graph):
        raise NotImplementedError('subclass ClassicService and implement your own RccFinish function')

    def RccInquire(__self__, txn_id, rank):
        raise NotImplementedError('subclass ClassicService and implement your own RccInquire function')

    def RccDispatchRo(__self__, cmd):
        raise NotImplementedError('subclass ClassicService and implement your own RccDispatchRo function')

    def RccInquireValidation(__self__, tx_id, rank):
        raise NotImplementedError('subclass ClassicService and implement your own RccInquireValidation function')

    def RccNotifyGlobalValidation(__self__, tx_id, rank, res):
        raise NotImplementedError('subclass ClassicService and implement your own RccNotifyGlobalValidation function')

    def JanusDispatch(__self__, cmd):
        raise NotImplementedError('subclass ClassicService and implement your own JanusDispatch function')

    def RccCommit(__self__, id, rank, need_validation, parents):
        raise NotImplementedError('subclass ClassicService and implement your own RccCommit function')

    def JanusCommit(__self__, id, rank, need_validation, graph):
        raise NotImplementedError('subclass ClassicService and implement your own JanusCommit function')

    def JanusCommitWoGraph(__self__, id, rank, need_validation):
        raise NotImplementedError('subclass ClassicService and implement your own JanusCommitWoGraph function')

    def JanusInquire(__self__, epoch, txn_id):
        raise NotImplementedError('subclass ClassicService and implement your own JanusInquire function')

    def RccPreAccept(__self__, txn_id, rank, cmd):
        raise NotImplementedError('subclass ClassicService and implement your own RccPreAccept function')

    def JanusPreAccept(__self__, txn_id, rank, cmd, graph):
        raise NotImplementedError('subclass ClassicService and implement your own JanusPreAccept function')

    def JanusPreAcceptWoGraph(__self__, txn_id, rank, cmd):
        raise NotImplementedError('subclass ClassicService and implement your own JanusPreAcceptWoGraph function')

    def RccAccept(__self__, txn_id, rank, ballot, p):
        raise NotImplementedError('subclass ClassicService and implement your own RccAccept function')

    def JanusAccept(__self__, txn_id, rank, ballot, graph):
        raise NotImplementedError('subclass ClassicService and implement your own JanusAccept function')

    def PreAcceptFebruus(__self__, tx_id):
        raise NotImplementedError('subclass ClassicService and implement your own PreAcceptFebruus function')

    def AcceptFebruus(__self__, tx_id, ballot, timestamp):
        raise NotImplementedError('subclass ClassicService and implement your own AcceptFebruus function')

    def CommitFebruus(__self__, tx_id, timestamp):
        raise NotImplementedError('subclass ClassicService and implement your own CommitFebruus function')

    def AccDispatch(__self__, coo_id, tid, cmd, ssid_spec, is_single_shard_write_only):
        raise NotImplementedError('subclass ClassicService and implement your own AccDispatch function')

    def AccRotxnDispatch(__self__, coo_id, tid, cmd, ssid_spec, safe_ts):
        raise NotImplementedError('subclass ClassicService and implement your own AccRotxnDispatch function')

    def AccValidate(__self__, tid, ssid_new):
        raise NotImplementedError('subclass ClassicService and implement your own AccValidate function')

    def AccFinalize(__self__, tid, decision):
        raise NotImplementedError('subclass ClassicService and implement your own AccFinalize function')

    def AccStatusQuery(__self__, tid):
        raise NotImplementedError('subclass ClassicService and implement your own AccStatusQuery function')

    def AccResolveStatusCoord(__self__, cmd_id):
        raise NotImplementedError('subclass ClassicService and implement your own AccResolveStatusCoord function')

    def AccGetRecord(__self__, cmd_id):
        raise NotImplementedError('subclass ClassicService and implement your own AccGetRecord function')

class ClassicProxy(object):
    def __init__(self, clnt):
        self.__clnt__ = clnt

    def async_MsgString(__self__, arg):
        return __self__.__clnt__.async_call(ClassicService.MSGSTRING, [arg], ClassicService.__input_type_info__['MsgString'], ClassicService.__output_type_info__['MsgString'])

    def async_MsgMarshall(__self__, arg):
        return __self__.__clnt__.async_call(ClassicService.MSGMARSHALL, [arg], ClassicService.__input_type_info__['MsgMarshall'], ClassicService.__output_type_info__['MsgMarshall'])

    def async_Dispatch(__self__, coo_id, tid, cmd):
        return __self__.__clnt__.async_call(ClassicService.DISPATCH, [coo_id, tid, cmd], ClassicService.__input_type_info__['Dispatch'], ClassicService.__output_type_info__['Dispatch'])

    def async_Prepare(__self__, tid, sids):
        return __self__.__clnt__.async_call(ClassicService.PREPARE, [tid, sids], ClassicService.__input_type_info__['Prepare'], ClassicService.__output_type_info__['Prepare'])

    def async_Commit(__self__, tid):
        return __self__.__clnt__.async_call(ClassicService.COMMIT, [tid], ClassicService.__input_type_info__['Commit'], ClassicService.__output_type_info__['Commit'])

    def async_Abort(__self__, tid):
        return __self__.__clnt__.async_call(ClassicService.ABORT, [tid], ClassicService.__input_type_info__['Abort'], ClassicService.__output_type_info__['Abort'])

    def async_UpgradeEpoch(__self__, curr_epoch):
        return __self__.__clnt__.async_call(ClassicService.UPGRADEEPOCH, [curr_epoch], ClassicService.__input_type_info__['UpgradeEpoch'], ClassicService.__output_type_info__['UpgradeEpoch'])

    def async_TruncateEpoch(__self__, old_epoch):
        return __self__.__clnt__.async_call(ClassicService.TRUNCATEEPOCH, [old_epoch], ClassicService.__input_type_info__['TruncateEpoch'], ClassicService.__output_type_info__['TruncateEpoch'])

    def async_rpc_null(__self__):
        return __self__.__clnt__.async_call(ClassicService.RPC_NULL, [], ClassicService.__input_type_info__['rpc_null'], ClassicService.__output_type_info__['rpc_null'])

    def async_TapirAccept(__self__, cmd_id, ballot, decision):
        return __self__.__clnt__.async_call(ClassicService.TAPIRACCEPT, [cmd_id, ballot, decision], ClassicService.__input_type_info__['TapirAccept'], ClassicService.__output_type_info__['TapirAccept'])

    def async_TapirFastAccept(__self__, cmd_id, txn_cmds):
        return __self__.__clnt__.async_call(ClassicService.TAPIRFASTACCEPT, [cmd_id, txn_cmds], ClassicService.__input_type_info__['TapirFastAccept'], ClassicService.__output_type_info__['TapirFastAccept'])

    def async_TapirDecide(__self__, cmd_id, commit):
        return __self__.__clnt__.async_call(ClassicService.TAPIRDECIDE, [cmd_id, commit], ClassicService.__input_type_info__['TapirDecide'], ClassicService.__output_type_info__['TapirDecide'])

    def async_RccDispatch(__self__, cmd):
        return __self__.__clnt__.async_call(ClassicService.RCCDISPATCH, [cmd], ClassicService.__input_type_info__['RccDispatch'], ClassicService.__output_type_info__['RccDispatch'])

    def async_RccFinish(__self__, id, md_graph):
        return __self__.__clnt__.async_call(ClassicService.RCCFINISH, [id, md_graph], ClassicService.__input_type_info__['RccFinish'], ClassicService.__output_type_info__['RccFinish'])

    def async_RccInquire(__self__, txn_id, rank):
        return __self__.__clnt__.async_call(ClassicService.RCCINQUIRE, [txn_id, rank], ClassicService.__input_type_info__['RccInquire'], ClassicService.__output_type_info__['RccInquire'])

    def async_RccDispatchRo(__self__, cmd):
        return __self__.__clnt__.async_call(ClassicService.RCCDISPATCHRO, [cmd], ClassicService.__input_type_info__['RccDispatchRo'], ClassicService.__output_type_info__['RccDispatchRo'])

    def async_RccInquireValidation(__self__, tx_id, rank):
        return __self__.__clnt__.async_call(ClassicService.RCCINQUIREVALIDATION, [tx_id, rank], ClassicService.__input_type_info__['RccInquireValidation'], ClassicService.__output_type_info__['RccInquireValidation'])

    def async_RccNotifyGlobalValidation(__self__, tx_id, rank, res):
        return __self__.__clnt__.async_call(ClassicService.RCCNOTIFYGLOBALVALIDATION, [tx_id, rank, res], ClassicService.__input_type_info__['RccNotifyGlobalValidation'], ClassicService.__output_type_info__['RccNotifyGlobalValidation'])

    def async_JanusDispatch(__self__, cmd):
        return __self__.__clnt__.async_call(ClassicService.JANUSDISPATCH, [cmd], ClassicService.__input_type_info__['JanusDispatch'], ClassicService.__output_type_info__['JanusDispatch'])

    def async_RccCommit(__self__, id, rank, need_validation, parents):
        return __self__.__clnt__.async_call(ClassicService.RCCCOMMIT, [id, rank, need_validation, parents], ClassicService.__input_type_info__['RccCommit'], ClassicService.__output_type_info__['RccCommit'])

    def async_JanusCommit(__self__, id, rank, need_validation, graph):
        return __self__.__clnt__.async_call(ClassicService.JANUSCOMMIT, [id, rank, need_validation, graph], ClassicService.__input_type_info__['JanusCommit'], ClassicService.__output_type_info__['JanusCommit'])

    def async_JanusCommitWoGraph(__self__, id, rank, need_validation):
        return __self__.__clnt__.async_call(ClassicService.JANUSCOMMITWOGRAPH, [id, rank, need_validation], ClassicService.__input_type_info__['JanusCommitWoGraph'], ClassicService.__output_type_info__['JanusCommitWoGraph'])

    def async_JanusInquire(__self__, epoch, txn_id):
        return __self__.__clnt__.async_call(ClassicService.JANUSINQUIRE, [epoch, txn_id], ClassicService.__input_type_info__['JanusInquire'], ClassicService.__output_type_info__['JanusInquire'])

    def async_RccPreAccept(__self__, txn_id, rank, cmd):
        return __self__.__clnt__.async_call(ClassicService.RCCPREACCEPT, [txn_id, rank, cmd], ClassicService.__input_type_info__['RccPreAccept'], ClassicService.__output_type_info__['RccPreAccept'])

    def async_JanusPreAccept(__self__, txn_id, rank, cmd, graph):
        return __self__.__clnt__.async_call(ClassicService.JANUSPREACCEPT, [txn_id, rank, cmd, graph], ClassicService.__input_type_info__['JanusPreAccept'], ClassicService.__output_type_info__['JanusPreAccept'])

    def async_JanusPreAcceptWoGraph(__self__, txn_id, rank, cmd):
        return __self__.__clnt__.async_call(ClassicService.JANUSPREACCEPTWOGRAPH, [txn_id, rank, cmd], ClassicService.__input_type_info__['JanusPreAcceptWoGraph'], ClassicService.__output_type_info__['JanusPreAcceptWoGraph'])

    def async_RccAccept(__self__, txn_id, rank, ballot, p):
        return __self__.__clnt__.async_call(ClassicService.RCCACCEPT, [txn_id, rank, ballot, p], ClassicService.__input_type_info__['RccAccept'], ClassicService.__output_type_info__['RccAccept'])

    def async_JanusAccept(__self__, txn_id, rank, ballot, graph):
        return __self__.__clnt__.async_call(ClassicService.JANUSACCEPT, [txn_id, rank, ballot, graph], ClassicService.__input_type_info__['JanusAccept'], ClassicService.__output_type_info__['JanusAccept'])

    def async_PreAcceptFebruus(__self__, tx_id):
        return __self__.__clnt__.async_call(ClassicService.PREACCEPTFEBRUUS, [tx_id], ClassicService.__input_type_info__['PreAcceptFebruus'], ClassicService.__output_type_info__['PreAcceptFebruus'])

    def async_AcceptFebruus(__self__, tx_id, ballot, timestamp):
        return __self__.__clnt__.async_call(ClassicService.ACCEPTFEBRUUS, [tx_id, ballot, timestamp], ClassicService.__input_type_info__['AcceptFebruus'], ClassicService.__output_type_info__['AcceptFebruus'])

    def async_CommitFebruus(__self__, tx_id, timestamp):
        return __self__.__clnt__.async_call(ClassicService.COMMITFEBRUUS, [tx_id, timestamp], ClassicService.__input_type_info__['CommitFebruus'], ClassicService.__output_type_info__['CommitFebruus'])

    def async_AccDispatch(__self__, coo_id, tid, cmd, ssid_spec, is_single_shard_write_only):
        return __self__.__clnt__.async_call(ClassicService.ACCDISPATCH, [coo_id, tid, cmd, ssid_spec, is_single_shard_write_only], ClassicService.__input_type_info__['AccDispatch'], ClassicService.__output_type_info__['AccDispatch'])

    def async_AccRotxnDispatch(__self__, coo_id, tid, cmd, ssid_spec, safe_ts):
        return __self__.__clnt__.async_call(ClassicService.ACCROTXNDISPATCH, [coo_id, tid, cmd, ssid_spec, safe_ts], ClassicService.__input_type_info__['AccRotxnDispatch'], ClassicService.__output_type_info__['AccRotxnDispatch'])

    def async_AccValidate(__self__, tid, ssid_new):
        return __self__.__clnt__.async_call(ClassicService.ACCVALIDATE, [tid, ssid_new], ClassicService.__input_type_info__['AccValidate'], ClassicService.__output_type_info__['AccValidate'])

    def async_AccFinalize(__self__, tid, decision):
        return __self__.__clnt__.async_call(ClassicService.ACCFINALIZE, [tid, decision], ClassicService.__input_type_info__['AccFinalize'], ClassicService.__output_type_info__['AccFinalize'])

    def async_AccStatusQuery(__self__, tid):
        return __self__.__clnt__.async_call(ClassicService.ACCSTATUSQUERY, [tid], ClassicService.__input_type_info__['AccStatusQuery'], ClassicService.__output_type_info__['AccStatusQuery'])

    def async_AccResolveStatusCoord(__self__, cmd_id):
        return __self__.__clnt__.async_call(ClassicService.ACCRESOLVESTATUSCOORD, [cmd_id], ClassicService.__input_type_info__['AccResolveStatusCoord'], ClassicService.__output_type_info__['AccResolveStatusCoord'])

    def async_AccGetRecord(__self__, cmd_id):
        return __self__.__clnt__.async_call(ClassicService.ACCGETRECORD, [cmd_id], ClassicService.__input_type_info__['AccGetRecord'], ClassicService.__output_type_info__['AccGetRecord'])

    def sync_MsgString(__self__, arg):
        __result__ = __self__.__clnt__.sync_call(ClassicService.MSGSTRING, [arg], ClassicService.__input_type_info__['MsgString'], ClassicService.__output_type_info__['MsgString'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_MsgMarshall(__self__, arg):
        __result__ = __self__.__clnt__.sync_call(ClassicService.MSGMARSHALL, [arg], ClassicService.__input_type_info__['MsgMarshall'], ClassicService.__output_type_info__['MsgMarshall'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_Dispatch(__self__, coo_id, tid, cmd):
        __result__ = __self__.__clnt__.sync_call(ClassicService.DISPATCH, [coo_id, tid, cmd], ClassicService.__input_type_info__['Dispatch'], ClassicService.__output_type_info__['Dispatch'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_Prepare(__self__, tid, sids):
        __result__ = __self__.__clnt__.sync_call(ClassicService.PREPARE, [tid, sids], ClassicService.__input_type_info__['Prepare'], ClassicService.__output_type_info__['Prepare'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_Commit(__self__, tid):
        __result__ = __self__.__clnt__.sync_call(ClassicService.COMMIT, [tid], ClassicService.__input_type_info__['Commit'], ClassicService.__output_type_info__['Commit'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_Abort(__self__, tid):
        __result__ = __self__.__clnt__.sync_call(ClassicService.ABORT, [tid], ClassicService.__input_type_info__['Abort'], ClassicService.__output_type_info__['Abort'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_UpgradeEpoch(__self__, curr_epoch):
        __result__ = __self__.__clnt__.sync_call(ClassicService.UPGRADEEPOCH, [curr_epoch], ClassicService.__input_type_info__['UpgradeEpoch'], ClassicService.__output_type_info__['UpgradeEpoch'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_TruncateEpoch(__self__, old_epoch):
        __result__ = __self__.__clnt__.sync_call(ClassicService.TRUNCATEEPOCH, [old_epoch], ClassicService.__input_type_info__['TruncateEpoch'], ClassicService.__output_type_info__['TruncateEpoch'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_rpc_null(__self__):
        __result__ = __self__.__clnt__.sync_call(ClassicService.RPC_NULL, [], ClassicService.__input_type_info__['rpc_null'], ClassicService.__output_type_info__['rpc_null'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_TapirAccept(__self__, cmd_id, ballot, decision):
        __result__ = __self__.__clnt__.sync_call(ClassicService.TAPIRACCEPT, [cmd_id, ballot, decision], ClassicService.__input_type_info__['TapirAccept'], ClassicService.__output_type_info__['TapirAccept'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_TapirFastAccept(__self__, cmd_id, txn_cmds):
        __result__ = __self__.__clnt__.sync_call(ClassicService.TAPIRFASTACCEPT, [cmd_id, txn_cmds], ClassicService.__input_type_info__['TapirFastAccept'], ClassicService.__output_type_info__['TapirFastAccept'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_TapirDecide(__self__, cmd_id, commit):
        __result__ = __self__.__clnt__.sync_call(ClassicService.TAPIRDECIDE, [cmd_id, commit], ClassicService.__input_type_info__['TapirDecide'], ClassicService.__output_type_info__['TapirDecide'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_RccDispatch(__self__, cmd):
        __result__ = __self__.__clnt__.sync_call(ClassicService.RCCDISPATCH, [cmd], ClassicService.__input_type_info__['RccDispatch'], ClassicService.__output_type_info__['RccDispatch'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_RccFinish(__self__, id, md_graph):
        __result__ = __self__.__clnt__.sync_call(ClassicService.RCCFINISH, [id, md_graph], ClassicService.__input_type_info__['RccFinish'], ClassicService.__output_type_info__['RccFinish'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_RccInquire(__self__, txn_id, rank):
        __result__ = __self__.__clnt__.sync_call(ClassicService.RCCINQUIRE, [txn_id, rank], ClassicService.__input_type_info__['RccInquire'], ClassicService.__output_type_info__['RccInquire'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_RccDispatchRo(__self__, cmd):
        __result__ = __self__.__clnt__.sync_call(ClassicService.RCCDISPATCHRO, [cmd], ClassicService.__input_type_info__['RccDispatchRo'], ClassicService.__output_type_info__['RccDispatchRo'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_RccInquireValidation(__self__, tx_id, rank):
        __result__ = __self__.__clnt__.sync_call(ClassicService.RCCINQUIREVALIDATION, [tx_id, rank], ClassicService.__input_type_info__['RccInquireValidation'], ClassicService.__output_type_info__['RccInquireValidation'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_RccNotifyGlobalValidation(__self__, tx_id, rank, res):
        __result__ = __self__.__clnt__.sync_call(ClassicService.RCCNOTIFYGLOBALVALIDATION, [tx_id, rank, res], ClassicService.__input_type_info__['RccNotifyGlobalValidation'], ClassicService.__output_type_info__['RccNotifyGlobalValidation'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_JanusDispatch(__self__, cmd):
        __result__ = __self__.__clnt__.sync_call(ClassicService.JANUSDISPATCH, [cmd], ClassicService.__input_type_info__['JanusDispatch'], ClassicService.__output_type_info__['JanusDispatch'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_RccCommit(__self__, id, rank, need_validation, parents):
        __result__ = __self__.__clnt__.sync_call(ClassicService.RCCCOMMIT, [id, rank, need_validation, parents], ClassicService.__input_type_info__['RccCommit'], ClassicService.__output_type_info__['RccCommit'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_JanusCommit(__self__, id, rank, need_validation, graph):
        __result__ = __self__.__clnt__.sync_call(ClassicService.JANUSCOMMIT, [id, rank, need_validation, graph], ClassicService.__input_type_info__['JanusCommit'], ClassicService.__output_type_info__['JanusCommit'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_JanusCommitWoGraph(__self__, id, rank, need_validation):
        __result__ = __self__.__clnt__.sync_call(ClassicService.JANUSCOMMITWOGRAPH, [id, rank, need_validation], ClassicService.__input_type_info__['JanusCommitWoGraph'], ClassicService.__output_type_info__['JanusCommitWoGraph'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_JanusInquire(__self__, epoch, txn_id):
        __result__ = __self__.__clnt__.sync_call(ClassicService.JANUSINQUIRE, [epoch, txn_id], ClassicService.__input_type_info__['JanusInquire'], ClassicService.__output_type_info__['JanusInquire'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_RccPreAccept(__self__, txn_id, rank, cmd):
        __result__ = __self__.__clnt__.sync_call(ClassicService.RCCPREACCEPT, [txn_id, rank, cmd], ClassicService.__input_type_info__['RccPreAccept'], ClassicService.__output_type_info__['RccPreAccept'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_JanusPreAccept(__self__, txn_id, rank, cmd, graph):
        __result__ = __self__.__clnt__.sync_call(ClassicService.JANUSPREACCEPT, [txn_id, rank, cmd, graph], ClassicService.__input_type_info__['JanusPreAccept'], ClassicService.__output_type_info__['JanusPreAccept'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_JanusPreAcceptWoGraph(__self__, txn_id, rank, cmd):
        __result__ = __self__.__clnt__.sync_call(ClassicService.JANUSPREACCEPTWOGRAPH, [txn_id, rank, cmd], ClassicService.__input_type_info__['JanusPreAcceptWoGraph'], ClassicService.__output_type_info__['JanusPreAcceptWoGraph'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_RccAccept(__self__, txn_id, rank, ballot, p):
        __result__ = __self__.__clnt__.sync_call(ClassicService.RCCACCEPT, [txn_id, rank, ballot, p], ClassicService.__input_type_info__['RccAccept'], ClassicService.__output_type_info__['RccAccept'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_JanusAccept(__self__, txn_id, rank, ballot, graph):
        __result__ = __self__.__clnt__.sync_call(ClassicService.JANUSACCEPT, [txn_id, rank, ballot, graph], ClassicService.__input_type_info__['JanusAccept'], ClassicService.__output_type_info__['JanusAccept'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_PreAcceptFebruus(__self__, tx_id):
        __result__ = __self__.__clnt__.sync_call(ClassicService.PREACCEPTFEBRUUS, [tx_id], ClassicService.__input_type_info__['PreAcceptFebruus'], ClassicService.__output_type_info__['PreAcceptFebruus'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_AcceptFebruus(__self__, tx_id, ballot, timestamp):
        __result__ = __self__.__clnt__.sync_call(ClassicService.ACCEPTFEBRUUS, [tx_id, ballot, timestamp], ClassicService.__input_type_info__['AcceptFebruus'], ClassicService.__output_type_info__['AcceptFebruus'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_CommitFebruus(__self__, tx_id, timestamp):
        __result__ = __self__.__clnt__.sync_call(ClassicService.COMMITFEBRUUS, [tx_id, timestamp], ClassicService.__input_type_info__['CommitFebruus'], ClassicService.__output_type_info__['CommitFebruus'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_AccDispatch(__self__, coo_id, tid, cmd, ssid_spec, is_single_shard_write_only):
        __result__ = __self__.__clnt__.sync_call(ClassicService.ACCDISPATCH, [coo_id, tid, cmd, ssid_spec, is_single_shard_write_only], ClassicService.__input_type_info__['AccDispatch'], ClassicService.__output_type_info__['AccDispatch'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_AccRotxnDispatch(__self__, coo_id, tid, cmd, ssid_spec, safe_ts):
        __result__ = __self__.__clnt__.sync_call(ClassicService.ACCROTXNDISPATCH, [coo_id, tid, cmd, ssid_spec, safe_ts], ClassicService.__input_type_info__['AccRotxnDispatch'], ClassicService.__output_type_info__['AccRotxnDispatch'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_AccValidate(__self__, tid, ssid_new):
        __result__ = __self__.__clnt__.sync_call(ClassicService.ACCVALIDATE, [tid, ssid_new], ClassicService.__input_type_info__['AccValidate'], ClassicService.__output_type_info__['AccValidate'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_AccFinalize(__self__, tid, decision):
        __result__ = __self__.__clnt__.sync_call(ClassicService.ACCFINALIZE, [tid, decision], ClassicService.__input_type_info__['AccFinalize'], ClassicService.__output_type_info__['AccFinalize'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_AccStatusQuery(__self__, tid):
        __result__ = __self__.__clnt__.sync_call(ClassicService.ACCSTATUSQUERY, [tid], ClassicService.__input_type_info__['AccStatusQuery'], ClassicService.__output_type_info__['AccStatusQuery'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_AccResolveStatusCoord(__self__, cmd_id):
        __result__ = __self__.__clnt__.sync_call(ClassicService.ACCRESOLVESTATUSCOORD, [cmd_id], ClassicService.__input_type_info__['AccResolveStatusCoord'], ClassicService.__output_type_info__['AccResolveStatusCoord'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_AccGetRecord(__self__, cmd_id):
        __result__ = __self__.__clnt__.sync_call(ClassicService.ACCGETRECORD, [cmd_id], ClassicService.__input_type_info__['AccGetRecord'], ClassicService.__output_type_info__['AccGetRecord'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

class ServerControlService(object):
    SERVER_SHUTDOWN = 0x33b51172
    SERVER_READY = 0x6d77ec06
    SERVER_HEART_BEAT_WITH_DATA = 0x3a7607dc
    SERVER_HEART_BEAT = 0x53517a65

    __input_type_info__ = {
        'server_shutdown': [],
        'server_ready': [],
        'server_heart_beat_with_data': [],
        'server_heart_beat': [],
    }

    __output_type_info__ = {
        'server_shutdown': [],
        'server_ready': ['rrr::i32'],
        'server_heart_beat_with_data': ['ServerResponse'],
        'server_heart_beat': [],
    }

    def __bind_helper__(self, func):
        def f(*args):
            return getattr(self, func.__name__)(*args)
        return f

    def __reg_to__(self, server):
        server.__reg_func__(ServerControlService.SERVER_SHUTDOWN, self.__bind_helper__(self.server_shutdown), [], [])
        server.__reg_func__(ServerControlService.SERVER_READY, self.__bind_helper__(self.server_ready), [], ['rrr::i32'])
        server.__reg_func__(ServerControlService.SERVER_HEART_BEAT_WITH_DATA, self.__bind_helper__(self.server_heart_beat_with_data), [], ['ServerResponse'])
        server.__reg_func__(ServerControlService.SERVER_HEART_BEAT, self.__bind_helper__(self.server_heart_beat), [], [])

    def server_shutdown(__self__):
        raise NotImplementedError('subclass ServerControlService and implement your own server_shutdown function')

    def server_ready(__self__):
        raise NotImplementedError('subclass ServerControlService and implement your own server_ready function')

    def server_heart_beat_with_data(__self__):
        raise NotImplementedError('subclass ServerControlService and implement your own server_heart_beat_with_data function')

    def server_heart_beat(__self__):
        raise NotImplementedError('subclass ServerControlService and implement your own server_heart_beat function')

class ServerControlProxy(object):
    def __init__(self, clnt):
        self.__clnt__ = clnt

    def async_server_shutdown(__self__):
        return __self__.__clnt__.async_call(ServerControlService.SERVER_SHUTDOWN, [], ServerControlService.__input_type_info__['server_shutdown'], ServerControlService.__output_type_info__['server_shutdown'])

    def async_server_ready(__self__):
        return __self__.__clnt__.async_call(ServerControlService.SERVER_READY, [], ServerControlService.__input_type_info__['server_ready'], ServerControlService.__output_type_info__['server_ready'])

    def async_server_heart_beat_with_data(__self__):
        return __self__.__clnt__.async_call(ServerControlService.SERVER_HEART_BEAT_WITH_DATA, [], ServerControlService.__input_type_info__['server_heart_beat_with_data'], ServerControlService.__output_type_info__['server_heart_beat_with_data'])

    def async_server_heart_beat(__self__):
        return __self__.__clnt__.async_call(ServerControlService.SERVER_HEART_BEAT, [], ServerControlService.__input_type_info__['server_heart_beat'], ServerControlService.__output_type_info__['server_heart_beat'])

    def sync_server_shutdown(__self__):
        __result__ = __self__.__clnt__.sync_call(ServerControlService.SERVER_SHUTDOWN, [], ServerControlService.__input_type_info__['server_shutdown'], ServerControlService.__output_type_info__['server_shutdown'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_server_ready(__self__):
        __result__ = __self__.__clnt__.sync_call(ServerControlService.SERVER_READY, [], ServerControlService.__input_type_info__['server_ready'], ServerControlService.__output_type_info__['server_ready'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_server_heart_beat_with_data(__self__):
        __result__ = __self__.__clnt__.sync_call(ServerControlService.SERVER_HEART_BEAT_WITH_DATA, [], ServerControlService.__input_type_info__['server_heart_beat_with_data'], ServerControlService.__output_type_info__['server_heart_beat_with_data'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_server_heart_beat(__self__):
        __result__ = __self__.__clnt__.sync_call(ServerControlService.SERVER_HEART_BEAT, [], ServerControlService.__input_type_info__['server_heart_beat'], ServerControlService.__output_type_info__['server_heart_beat'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

class ClientControlService(object):
    CLIENT_GET_TXN_NAMES = 0x4e903c07
    CLIENT_SHUTDOWN = 0x6f65941c
    CLIENT_FORCE_STOP = 0x6ff03ab9
    CLIENT_RESPONSE = 0x49b1b514
    CLIENT_READY = 0x3a15dd62
    CLIENT_READY_BLOCK = 0x54e94bd8
    CLIENT_START = 0x5c8841d8
    DISPATCHTXN = 0x5788dff4

    __input_type_info__ = {
        'client_get_txn_names': [],
        'client_shutdown': [],
        'client_force_stop': [],
        'client_response': [],
        'client_ready': [],
        'client_ready_block': [],
        'client_start': [],
        'DispatchTxn': ['TxDispatchRequest'],
    }

    __output_type_info__ = {
        'client_get_txn_names': ['std::map<rrr::i32, std::string>'],
        'client_shutdown': [],
        'client_force_stop': [],
        'client_response': ['ClientResponse'],
        'client_ready': ['rrr::i32'],
        'client_ready_block': ['rrr::i32'],
        'client_start': [],
        'DispatchTxn': ['TxReply'],
    }

    def __bind_helper__(self, func):
        def f(*args):
            return getattr(self, func.__name__)(*args)
        return f

    def __reg_to__(self, server):
        server.__reg_func__(ClientControlService.CLIENT_GET_TXN_NAMES, self.__bind_helper__(self.client_get_txn_names), [], ['std::map<rrr::i32, std::string>'])
        server.__reg_func__(ClientControlService.CLIENT_SHUTDOWN, self.__bind_helper__(self.client_shutdown), [], [])
        server.__reg_func__(ClientControlService.CLIENT_FORCE_STOP, self.__bind_helper__(self.client_force_stop), [], [])
        server.__reg_func__(ClientControlService.CLIENT_RESPONSE, self.__bind_helper__(self.client_response), [], ['ClientResponse'])
        server.__reg_func__(ClientControlService.CLIENT_READY, self.__bind_helper__(self.client_ready), [], ['rrr::i32'])
        server.__reg_func__(ClientControlService.CLIENT_READY_BLOCK, self.__bind_helper__(self.client_ready_block), [], ['rrr::i32'])
        server.__reg_func__(ClientControlService.CLIENT_START, self.__bind_helper__(self.client_start), [], [])
        server.__reg_func__(ClientControlService.DISPATCHTXN, self.__bind_helper__(self.DispatchTxn), ['TxDispatchRequest'], ['TxReply'])

    def client_get_txn_names(__self__):
        raise NotImplementedError('subclass ClientControlService and implement your own client_get_txn_names function')

    def client_shutdown(__self__):
        raise NotImplementedError('subclass ClientControlService and implement your own client_shutdown function')

    def client_force_stop(__self__):
        raise NotImplementedError('subclass ClientControlService and implement your own client_force_stop function')

    def client_response(__self__):
        raise NotImplementedError('subclass ClientControlService and implement your own client_response function')

    def client_ready(__self__):
        raise NotImplementedError('subclass ClientControlService and implement your own client_ready function')

    def client_ready_block(__self__):
        raise NotImplementedError('subclass ClientControlService and implement your own client_ready_block function')

    def client_start(__self__):
        raise NotImplementedError('subclass ClientControlService and implement your own client_start function')

    def DispatchTxn(__self__, req):
        raise NotImplementedError('subclass ClientControlService and implement your own DispatchTxn function')

class ClientControlProxy(object):
    def __init__(self, clnt):
        self.__clnt__ = clnt

    def async_client_get_txn_names(__self__):
        return __self__.__clnt__.async_call(ClientControlService.CLIENT_GET_TXN_NAMES, [], ClientControlService.__input_type_info__['client_get_txn_names'], ClientControlService.__output_type_info__['client_get_txn_names'])

    def async_client_shutdown(__self__):
        return __self__.__clnt__.async_call(ClientControlService.CLIENT_SHUTDOWN, [], ClientControlService.__input_type_info__['client_shutdown'], ClientControlService.__output_type_info__['client_shutdown'])

    def async_client_force_stop(__self__):
        return __self__.__clnt__.async_call(ClientControlService.CLIENT_FORCE_STOP, [], ClientControlService.__input_type_info__['client_force_stop'], ClientControlService.__output_type_info__['client_force_stop'])

    def async_client_response(__self__):
        return __self__.__clnt__.async_call(ClientControlService.CLIENT_RESPONSE, [], ClientControlService.__input_type_info__['client_response'], ClientControlService.__output_type_info__['client_response'])

    def async_client_ready(__self__):
        return __self__.__clnt__.async_call(ClientControlService.CLIENT_READY, [], ClientControlService.__input_type_info__['client_ready'], ClientControlService.__output_type_info__['client_ready'])

    def async_client_ready_block(__self__):
        return __self__.__clnt__.async_call(ClientControlService.CLIENT_READY_BLOCK, [], ClientControlService.__input_type_info__['client_ready_block'], ClientControlService.__output_type_info__['client_ready_block'])

    def async_client_start(__self__):
        return __self__.__clnt__.async_call(ClientControlService.CLIENT_START, [], ClientControlService.__input_type_info__['client_start'], ClientControlService.__output_type_info__['client_start'])

    def async_DispatchTxn(__self__, req):
        return __self__.__clnt__.async_call(ClientControlService.DISPATCHTXN, [req], ClientControlService.__input_type_info__['DispatchTxn'], ClientControlService.__output_type_info__['DispatchTxn'])

    def sync_client_get_txn_names(__self__):
        __result__ = __self__.__clnt__.sync_call(ClientControlService.CLIENT_GET_TXN_NAMES, [], ClientControlService.__input_type_info__['client_get_txn_names'], ClientControlService.__output_type_info__['client_get_txn_names'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_client_shutdown(__self__):
        __result__ = __self__.__clnt__.sync_call(ClientControlService.CLIENT_SHUTDOWN, [], ClientControlService.__input_type_info__['client_shutdown'], ClientControlService.__output_type_info__['client_shutdown'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_client_force_stop(__self__):
        __result__ = __self__.__clnt__.sync_call(ClientControlService.CLIENT_FORCE_STOP, [], ClientControlService.__input_type_info__['client_force_stop'], ClientControlService.__output_type_info__['client_force_stop'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_client_response(__self__):
        __result__ = __self__.__clnt__.sync_call(ClientControlService.CLIENT_RESPONSE, [], ClientControlService.__input_type_info__['client_response'], ClientControlService.__output_type_info__['client_response'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_client_ready(__self__):
        __result__ = __self__.__clnt__.sync_call(ClientControlService.CLIENT_READY, [], ClientControlService.__input_type_info__['client_ready'], ClientControlService.__output_type_info__['client_ready'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_client_ready_block(__self__):
        __result__ = __self__.__clnt__.sync_call(ClientControlService.CLIENT_READY_BLOCK, [], ClientControlService.__input_type_info__['client_ready_block'], ClientControlService.__output_type_info__['client_ready_block'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_client_start(__self__):
        __result__ = __self__.__clnt__.sync_call(ClientControlService.CLIENT_START, [], ClientControlService.__input_type_info__['client_start'], ClientControlService.__output_type_info__['client_start'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

    def sync_DispatchTxn(__self__, req):
        __result__ = __self__.__clnt__.sync_call(ClientControlService.DISPATCHTXN, [req], ClientControlService.__input_type_info__['DispatchTxn'], ClientControlService.__output_type_info__['DispatchTxn'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

