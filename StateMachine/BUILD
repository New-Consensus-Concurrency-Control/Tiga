

cc_library(
    name = "StateMachine",
    srcs = ["StateMachine.cc"],
    hdrs = ["StateMachine.h"],
    deps= [
        "//:CommonHeader",
    ],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "MicroStateMachine",
    srcs = ["MicroStateMachine.cc"],
    hdrs = ["MicroStateMachine.h"],
    deps= [
        ":StateMachine",
    ],
    visibility = ["//visibility:public"],
)



cc_library(
    name = "TPCCCommon",
    srcs = ["TPCCCommon.h"],
    hdrs = ["TPCCCommon.h"],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "Constants",
    srcs = ["Constants.h"],
    hdrs = ["Constants.h"],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "TableCommon",
    srcs = ["TableCommon.cc"],
    hdrs = ["TableCommon.h"],
    deps = [
        ":Constants",
        "//memdb:multi_value_lib",
        "//memdb:rcc_row_lib",
    ],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "TPCCSharding",
    srcs = ["TPCCSharding.cc"],
    hdrs = ["TPCCSharding.h"],
    deps= [
        ":TPCCCommon",
        ":TableCommon",
        "//memdb:txn_unsafe_lib"
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "TPCCStateMachine",
    srcs = ["TPCCStateMachine.cc"],
    hdrs = ["TPCCStateMachine.h"],
    deps= [
        ":TPCCSharding",
        ":StateMachine"
    ],
    visibility = ["//visibility:public"],
)
