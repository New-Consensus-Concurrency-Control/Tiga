cc_library(
    name = "rrrLib",
    srcs = glob([
        "base/*.cpp",
        "misc/*.cpp",
        "rpc/*.cpp",
        "coroutine/*.cc"
    ]),
    hdrs = glob([
        "base/*.hpp",
        "misc/*.hpp",
        "rpc/*.hpp",
        "coroutine/*.h",
        "rrr.h",
        "rrr.hpp"
    ]),
    includes = ["./","/usr/include", "/usr/include/x86_64-linux-gnu"], 
    linkopts = [ 
      "-L/usr/local/lib", 
      "-L/usr/lib/x86_64-linux-gnu",
      "-pthread",
       "-lboost_system", "-lboost_filesystem", 
       "-lboost_coroutine", "-lboost_context"
    ],
    visibility = ["//visibility:public"],
)
