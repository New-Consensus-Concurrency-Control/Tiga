
cc_library(
    name = "TigaMessage",
    srcs = ["TigaMessage.cc"],
    hdrs = ["TigaMessage.h"],
    deps= [
        "//:CommonHeader"
    ],
    visibility = ["//visibility:public"],
)



cc_library(
    name = "TigaService",
    srcs = ["TigaService.h"],
    hdrs = ["TigaService.h"],
    visibility = ["//visibility:public"],
)




cc_library(
    name = "TigaServiceImpl",
    srcs = ["TigaServiceImpl.cc"],
    hdrs = ["TigaServiceImpl.h"],
    deps= [
        ":TigaReplica"
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "TigaLocalServiceImpl",
    srcs = ["TigaLocalServiceImpl.cc"],
    hdrs = ["TigaLocalServiceImpl.h"],
    deps= [
        ":TigaReplica"
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "TigaGlobalServiceImpl",
    srcs = ["TigaGlobalServiceImpl.cc"],
    hdrs = ["TigaGlobalServiceImpl.h"],
    deps= [
        ":TigaReplica"
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "TigaViewChangeServiceImpl",
    srcs = ["TigaViewChangeServiceImpl.cc"],
    hdrs = ["TigaViewChangeServiceImpl.h"],
    deps= [
        ":TigaReplica"
    ],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "TigaReplica",
    srcs = ["TigaReplica.cc", "TigaReplicaFR.cc"],
    hdrs = ["TigaReplica.h"],
    deps= [
        ":TigaMessage",
        ":TigaService",
        "//StateMachine:MicroStateMachine",
        "//StateMachine:YCSBStateMachine",
        "//StateMachine:TPCCStateMachine"
    ],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "TigaCommunicator",
    srcs = ["TigaCommunicator.cc"],
    hdrs = ["TigaCommunicator.h"],
    deps= [
        ":TigaServiceImpl"
    ],
    visibility = ["//visibility:public"],
)