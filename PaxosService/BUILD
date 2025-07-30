
cc_library(
    name = "PaxosMessage",
    srcs = ["PaxosMessage.cc"],
    hdrs = [
        "PaxosMessage.h"
    ],
    deps= [
        "//:CommonHeader"

    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "PaxosService",
    srcs = ["PaxosService.h"],
    hdrs = ["PaxosService.h"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "PaxosReplica",
    srcs = ["PaxosReplica.cc"],
    hdrs = ["PaxosReplica.h"],
    deps= [
        ":PaxosMessage",
        ":PaxosService",
    ],
    visibility = ["//visibility:public"],
)



cc_library(
    name = "PaxosServiceImpl",
    srcs = ["PaxosServiceImpl.cc"],
    hdrs = ["PaxosServiceImpl.h"],
    deps= [
        ":PaxosReplica"
    ],
    visibility = ["//visibility:public"],
)

