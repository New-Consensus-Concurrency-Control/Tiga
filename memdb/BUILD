cc_library(
    name = "murmurhash_lib",
    srcs = ["MurmurHash3.cc"],
    hdrs = ["MurmurHash3.h"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "xxhash_lib",
    srcs = ["xxhash.cc"],
    hdrs = ["xxhash.h"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "utils_lib",
    srcs = ["utils.cc"],
    hdrs = ["utils.h"],
    deps = [
        ":murmurhash_lib",
        ":xxhash_lib",
        "//rrr:rrrLib"
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "value_lib",
    srcs = ["value.cc"],
    hdrs =["value.h", "blob.h"],
    deps = [
        ":utils_lib",
    ],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "multi_value_lib",
    srcs = ["multi_value.cc"],
    hdrs =["multi_value.h"],
    deps = [
        ":value_lib",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "lock_lib",
    srcs = ["locking.h"],
    hdrs = ["locking.h"],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "snapshot_lib",
    srcs = ["snapshot.h"],
    hdrs = ["snapshot.h"],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "table_base_lib",
    srcs = ["table_base.cc"],
    hdrs = ["table_base.h"],
    deps = [
        ":value_lib",
        ":lock_lib",
        ":snapshot_lib"
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "rcc_row_lib",
    srcs = ["rcc_row.cc"],
    hdrs =["rcc_row.h"],
    deps = [
        ":table_base_lib",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "txn_lib",
    srcs = ["txn.cc"],
    hdrs = ["txn.h"],
    deps = [
        ":table_base_lib",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "txn_unsafe_lib",
    srcs = ["txn_unsafe.cc"],
    hdrs = ["txn_unsafe.h"],
    deps= [
        ":txn_lib"
    ],
    visibility = ["//visibility:public"],
)