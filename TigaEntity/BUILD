cc_binary(
     name = "TigaServer",
     srcs = ["TigaServer.cc"],
     deps = [
        "//TigaService:TigaServiceImpl",
        "//TigaService:TigaLocalServiceImpl",
        "//TigaService:TigaGlobalServiceImpl",
        "//TigaService:TigaViewChangeServiceImpl",
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
    name = "TigaCoordinator",
    srcs = ["TigaCoordinator.cc"],
    hdrs = ["TigaCoordinator.h"],
    deps= [
        "//TigaService:TigaCommunicator",
        "//TxnGenerator:MicroTxnGenerator",
        "//TxnGenerator:TPCCTxnGenerator",
    ],
    visibility = ["//visibility:public"],
)

cc_binary(
     name = "TigaClient",
     srcs = ["TigaClient.cc"],
     deps = [
        ":TigaCoordinator",
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