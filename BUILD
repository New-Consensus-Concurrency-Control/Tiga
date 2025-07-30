
cc_library(
    name = "concurrentqueue",
    hdrs = ["concurrentqueue.h"],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "CommonHeader",
    hdrs = ["Common.h"],
    deps = [
        "//memdb:value_lib",
        ":concurrentqueue"
    ],
    visibility = ["//visibility:public"],
)



cc_binary(
     name = "TPCCTest",
     srcs = ["TPCCTest.cc"],
     deps = [
        "//StateMachine:TPCCStateMachine",
        "//TxnGenerator:TPCCTxnGenerator",
        "//TigaService:TigaMessage",
        "//rrr:rrrLib"
     ],
     copts = [
         "-I/usr/local/include"
     ],
     linkopts = [ "-L/usr/local/lib",  "-pthread",
                "-lgflags", "-lglog", "-lcrypto","-lyaml-cpp"
                ],
)



cc_binary(
     name = "ClockAdj",
     srcs = ["ClockAdj.cc"],
     copts = [
         "-I/usr/local/include"
     ],
     linkopts = [ "-L/usr/local/lib", "-lrt"
                ],
)

# cc_binary(
#      name = "ClockAdj2",
#      srcs = ["ClockAdj.cc"],
#      copts = [
#          "-I/usr/local/include",
#          "-D_MY_IDENTIFIER_",
#          '-D_MY_MESSAGE_="\\"hi\\""'
#      ],
#      linkopts = [ "-L/usr/local/lib", "-lrt"
#                 ],
# )