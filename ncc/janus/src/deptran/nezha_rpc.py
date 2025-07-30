import os
from simplerpc.marshal import Marshal
from simplerpc.future import Future

class PaxosService(object):
    RECORDREQUEST = 0x60a6d5df

    __input_type_info__ = {
        'RecordRequest': ['ClientRecord'],
    }

    __output_type_info__ = {
        'RecordRequest': ['ClientRecordRep'],
    }

    def __bind_helper__(self, func):
        def f(*args):
            return getattr(self, func.__name__)(*args)
        return f

    def __reg_to__(self, server):
        server.__reg_func__(PaxosService.RECORDREQUEST, self.__bind_helper__(self.RecordRequest), ['ClientRecord'], ['ClientRecordRep'])

    def RecordRequest(__self__, req):
        raise NotImplementedError('subclass PaxosService and implement your own RecordRequest function')

class PaxosProxy(object):
    def __init__(self, clnt):
        self.__clnt__ = clnt

    def async_RecordRequest(__self__, req):
        return __self__.__clnt__.async_call(PaxosService.RECORDREQUEST, [req], PaxosService.__input_type_info__['RecordRequest'], PaxosService.__output_type_info__['RecordRequest'])

    def sync_RecordRequest(__self__, req):
        __result__ = __self__.__clnt__.sync_call(PaxosService.RECORDREQUEST, [req], PaxosService.__input_type_info__['RecordRequest'], PaxosService.__output_type_info__['RecordRequest'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

