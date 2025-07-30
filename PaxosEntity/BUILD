cc_binary(
     name = "PaxosServer",
     srcs = ["PaxosServer.cc"],
     deps = [
        "//PaxosService:PaxosServiceImpl",
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
     name = "PaxosClient",
     srcs = ["PaxosClient.cc"],
     deps = [
        "//PaxosService:PaxosServiceImpl",
         "//rrr:rrrLib"
     ],
     copts = [
         "-I/usr/local/include"
     ],
     linkopts = [ "-L/usr/local/lib",  "-pthread",
                "-lgflags", "-lglog", "-lcrypto","-lyaml-cpp"
                ],
)

