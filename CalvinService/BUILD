
cc_library(
    name = "CalvinMessage",
    srcs = ["CalvinMessage.cc"],
    hdrs = [
        "CalvinMessage.h"
    ],
    deps= [
        "//:CommonHeader"

    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "CalvinService",
    srcs = ["CalvinService.h"],
    hdrs = ["CalvinService.h"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "CalvinSequencer",
    srcs = ["CalvinSequencer.cc"],
    hdrs = ["CalvinSequencer.h"],
    deps= [
        ":CalvinMessage",
        ":CalvinService",
        "//StateMachine:MicroStateMachine",
        "//StateMachine:TPCCStateMachine"
    ],
    visibility = ["//visibility:public"],
)



cc_library(
    name = "CalvinScheduler",
    srcs = ["CalvinScheduler.cc"],
    hdrs = ["CalvinScheduler.h"],
    deps= [
        ":CalvinMessage",
        ":CalvinService",
        "//StateMachine:MicroStateMachine",
        "//StateMachine:TPCCStateMachine"
    ],
    visibility = ["//visibility:public"],
)



cc_library(
    name = "CalvinServiceImpl",
    srcs = ["CalvinServiceImpl.cc"],
    hdrs = ["CalvinServiceImpl.h"],
    deps= [
        ":CalvinSequencer",
        ":CalvinScheduler"
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "CalvinSequencerServiceImpl",
    srcs = ["CalvinSequencerServiceImpl.cc"],
    hdrs = ["CalvinSequencerServiceImpl.h"],
    deps= [
        ":CalvinSequencer"
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "CalvinSchedulerServiceImpl",
    srcs = ["CalvinSchedulerServiceImpl.cc"],
    hdrs = ["CalvinSchedulerServiceImpl.h"],
    deps= [
        ":CalvinScheduler"
    ],
    visibility = ["//visibility:public"],
)



cc_library(
    name = "CalvinCommunicator",
    srcs = ["CalvinCommunicator.cc"],
    hdrs = ["CalvinCommunicator.h"],
    deps= [
        ":CalvinServiceImpl"
    ],
    visibility = ["//visibility:public"],
)