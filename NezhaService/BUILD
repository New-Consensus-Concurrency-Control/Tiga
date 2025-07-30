
cc_library(
    name = "NezhaMessage",
    srcs = ["NezhaMessage.cc"],
    hdrs = [
        "NezhaMessage.h"
    ],
    deps= [
        "//:CommonHeader"

    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "NezhaService",
    srcs = ["NezhaService.h"],
    hdrs = ["NezhaService.h"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "NezhaReplica",
    srcs = ["NezhaReplica.cc"],
    hdrs = ["NezhaReplica.h"],
    deps= [
        ":NezhaMessage",
        ":NezhaService",
    ],
    visibility = ["//visibility:public"],
)



cc_library(
    name = "NezhaServiceImpl",
    srcs = ["NezhaServiceImpl.cc"],
    hdrs = ["NezhaServiceImpl.h"],
    deps= [
        ":NezhaReplica"
    ],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "NezhaBridgeService",
    srcs = ["nezha_rpc.h"],
    hdrs = ["nezha_rpc.h"],
    visibility = ["//visibility:public"],
)



cc_library(
    name = "NezhaBridgeServiceImpl",
    srcs = ["NezhaBridgeServiceImpl.cc"],
    hdrs = ["NezhaBridgeServiceImpl.h"],
    deps= [
        ":NezhaMessage",
        ":NezhaBridgeService",
    ],
    visibility = ["//visibility:public"],
)

