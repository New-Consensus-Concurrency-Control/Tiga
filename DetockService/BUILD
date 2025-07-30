
cc_library(
    name = "DetockMessage",
    srcs = ["DetockMessage.cc"],
    hdrs = [
        "DetockMessage.h"
    ],
    deps= [
        "//:CommonHeader"
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "SCCFinder",
    srcs = ["SCCFinder.cc"],
    hdrs = [
        "SCCFinder.h"
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "DetockService",
    srcs = ["DetockService.h"],
    hdrs = ["DetockService.h"],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "DetockLogManager",
    srcs = ["DetockLogManager.cc"],
    hdrs = ["DetockLogManager.h"],
    deps= [
        ":DetockMessage",
        ":DetockService",
        "//StateMachine:MicroStateMachine",
        "//StateMachine:TPCCStateMachine"
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "DetockLogManagerServiceImpl",
    srcs = ["DetockLogManagerServiceImpl.cc"],
    hdrs = ["DetockLogManagerServiceImpl.h"],
    deps= [
        ":DetockLogManager",
    ],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "DetockSequencerServiceImpl",
    srcs = ["DetockSequencerServiceImpl.cc"],
    hdrs = ["DetockSequencerServiceImpl.h"],
    deps= [
        ":DetockMessage",
        ":DetockService"
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "DetockExecutor",
    srcs = ["DetockExecutor.cc"],
    hdrs = ["DetockExecutor.h"],
    deps= [
        ":DetockLogManager",
        ":SCCFinder"
    ],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "DetockServiceImpl",
    srcs = ["DetockServiceImpl.cc"],
    hdrs = ["DetockServiceImpl.h"],
    deps= [
        ":DetockExecutor",
    ],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "DetockCommunicator",
    srcs = ["DetockCommunicator.cc"],
    hdrs = ["DetockCommunicator.h"],
    deps= [
        ":DetockServiceImpl"
    ],
    visibility = ["//visibility:public"],
)

