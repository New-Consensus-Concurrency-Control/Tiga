
cc_library(
    name = "ZipfDist",
    srcs = ["ZipfDist.h"],
    hdrs = ["ZipfDist.h"],
    visibility = ["//visibility:public"],
)



cc_library(
    name = "TxnGenerator",
    srcs = ["TxnGenerator.h"],
    hdrs = ["TxnGenerator.h"],
    deps= [
        "//:CommonHeader",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "MicroTxnGenerator",
    srcs = ["MicroTxnGenerator.cc"],
    hdrs = ["MicroTxnGenerator.h"],
    deps= [
        ":TxnGenerator",
        ":ZipfDist"
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "TPCCTxnGenerator",
    srcs = ["TPCCTxnGenerator.cc"],
    hdrs = ["TPCCTxnGenerator.h"],
    deps= [
        "//StateMachine:TPCCSharding",
        ":TxnGenerator"
    ],
    visibility = ["//visibility:public"],
)

