cc_binary(
     name = "CalvinServer",
     srcs = ["CalvinServer.cc"],
     deps = [
        "//CalvinService:CalvinServiceImpl",
        "//CalvinService:CalvinSequencerServiceImpl",
        "//CalvinService:CalvinSchedulerServiceImpl",
        "//rrr:rrrLib"
     ],
     copts = [
         "-I/usr/local/include"
     ],
     linkopts = [ "-L/usr/local/lib",  "-pthread",
                "-lgflags", "-lglog", "-lcrypto","-lyaml-cpp",
                ],
)



cc_library(
    name = "CalvinCoordinator",
    srcs = ["CalvinCoordinator.cc"],
    hdrs = ["CalvinCoordinator.h"],
    deps= [
        "//TxnGenerator:MicroTxnGenerator",
        "//TxnGenerator:TPCCTxnGenerator",
        "//CalvinService:CalvinCommunicator"
    ],
    visibility = ["//visibility:public"],
)

cc_binary(
     name = "CalvinClient",
     srcs = ["CalvinClient.cc"],
     deps = [
        ":CalvinCoordinator",
        "//rrr:rrrLib"
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