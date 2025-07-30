cc_binary(
     name = "NezhaServer",
     srcs = ["NezhaServer.cc"],
     deps = [
        "//NezhaService:NezhaServiceImpl",
        "//NezhaService:NezhaBridgeServiceImpl",
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
     name = "NezhaClient",
     srcs = ["NezhaClient.cc"],
     deps = [
         "//NezhaService:NezhaBridgeServiceImpl",
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
     name = "NezhaBridge",
     srcs = ["NezhaBridge.cc"],
     deps = [
         "//NezhaService:NezhaBridgeServiceImpl",
         "//NezhaService:NezhaService",
         "//rrr:rrrLib"
     ],
     copts = [
         "-I/usr/local/include"
     ],
     linkopts = [ "-L/usr/local/lib",  "-pthread",
                "-lgflags", "-lglog", "-lcrypto","-lyaml-cpp"
                ],
)
