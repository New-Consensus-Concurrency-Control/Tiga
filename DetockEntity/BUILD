cc_binary(
     name = "DetockServer",
     srcs = ["DetockServer.cc"],
     deps = [
        "//DetockService:DetockServiceImpl",
        "//DetockService:DetockSequencerServiceImpl",
        "//DetockService:DetockLogManagerServiceImpl",
        "//rrr:rrrLib"
     ],
     copts = [
         "-I/usr/local/include"
     ],
     linkopts = [ "-L/usr/local/lib",  "-pthread",
                "-lgflags", "-lglog", "-lcrypto","-lyaml-cpp"
                ],
)



cc_library(
    name = "DetockCoordinator",
    srcs = ["DetockCoordinator.cc"],
    hdrs = ["DetockCoordinator.h"],
    deps= [
        "//DetockService:DetockCommunicator",
        "//TxnGenerator:MicroTxnGenerator",
        "//TxnGenerator:TPCCTxnGenerator",
        "//rrr:rrrLib"
    ],
    visibility = ["//visibility:public"],
)

cc_binary(
     name = "DetockClient",
     srcs = ["DetockClient.cc"],
     deps = [
        ":DetockCoordinator"
     ],
     copts = [
         "-I/usr/local/include"
     ],
     linkopts = [ "-L/usr/local/lib",  "-pthread",
                "-lgflags", "-lglog", "-lcrypto","-lyaml-cpp"
                ],
)




# cc_binary(
#      name = "TPCCTest",
#      srcs = ["TPCCTest.cc"],
#      deps = [
#         "//StateMachine:TPCCStateMachine",
#         "//TxnGenerator:TPCCTxnGenerator"
#      ],
#      copts = [
#          "-I/usr/local/include"
#      ],
#      linkopts = [ "-L/usr/local/lib",  "-pthread","-lrpc","-lbase", "-lcoroutine",
#                 "-lgflags", "-lglog", "-lcrypto","-lyaml-cpp"
#                 ],
# )



# cc_binary(
#      name = "ClockAdj",
#      srcs = ["ClockAdj.cc"],
#      copts = [
#          "-I/usr/local/include"
#      ],
#      linkopts = [ "-L/usr/local/lib", "-lrt"
#                 ],
# )

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