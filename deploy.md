## General Settings

- Operating System: Ubuntu 16 (GCP Image: Ubuntu 16.04 LTS Pro Server)
- gcc version:  7.5.0
- g++ version:  7.5.0

## Install Bazel 5.2.0
```
# Install bazel 5.2.0
# Please follow the instructions at https://bazel.build/install/ubuntu#install-on-ubuntu, 
# or simply run the following commands

sudo apt install -y apt-transport-https curl gnupg
curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor >bazel-archive-keyring.gpg
sudo mv bazel-archive-keyring.gpg /usr/share/keyrings
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/bazel-archive-keyring.gpg] https://storage.googleapis.com/bazel-apt stable jdk1.8" | sudo tee /etc/apt/sources.list.d/bazel.list
sudo apt update
sudo apt install -y bazel-5.2.0
sudo mv /usr/bin/bazel-5.2.0 /usr/bin/bazel
bazel --version
```
## Install Boost Libaray

We inherit the codebase from [Janus](https://www.usenix.org/system/files/conference/osdi16/osdi16-mu.pdf) and [NCC](https://www.usenix.org/system/files/osdi23-lu.pdf) for some baselines. Based on that, we made some modifcation so that we can collect performance data in the same format. These codebase have also been included in Tiga's repo, but they are compiled separately using the WAF compilation tool. 

To support Janus and NCC binary, we need to install boost version **1.74.0**. Please make sure you installed the correct version. (Different version of Boost has different APIs, and may cause failure to compile Janus and NCC's baselines). On Ubuntu 16, the default version should be 1.74.0, so we just need to run 
```
sudo apt-get install -y libboost-all-dev 
``` 


Meanwhile, we also need to ensure the libraries can be searchable by `dpkg`. If it cannot be searched, we need to add the `.pc` file. 

```
# Location /usr/lib/x86_64-linux-gnu/pkgconfig/boost.pc
prefix=/usr
exec_prefix=${prefix}
includedir=${prefix}/include
libdir=${exec_prefix}/lib/x86_64-linux-gnu

Name: boost-dev
Description: Boost Lib for C++
Version: 1.74.0
Requires:
Libs: -L${libdir} -lboost_system -lboost_filesystem -lboost_coroutine -lboost_context
Cflags: -I${includedir}
```


## Install Other Dependencies
```
sudo apt-get update
sudo apt-get install -y \
    git \
    pkg-config \
    build-essential \
    clang \
    libapr1-dev libaprutil1-dev \
    libyaml-cpp-dev \
    python-dev \
    python-pip \
    libgoogle-perftools-dev
```


## Clone Repository
```
git clone  https://github.com/Steamgjk/Tiga.git
```

## Install Conda

Due to legacy issues, Janus and NCC needs to be compiled in different python environment, so we suggest [installing conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html) to create isolated and clean python environment. 

After installing conda, we create two python environment py310 (Python 3.10) and py27 (Python 2.7). py27 is used to compile Janus and py310 is used to compile NCC. 

## Compile Janus
```
## Assume py27 enviornment is activated
cd Tiga/janus
## Install Python libs in this environment
sudo pip install -r requirements.txt
./waf configure build -t
```
After waf compilation, the binary `deptran_server` is located in `Tiga/janus/build` folder.


## Compile NCC
```
## Assume py310 enviornment is activated
cd Tiga/ncc/janus/
## Install Python libs in this environment
sudo pip3 install -r requirements.tx
python waf configure build 
```
After waf compilation, the binary `deptran_server` is located in `Tiga/ncc/janus/build` folder.


## Compile Tiga

Tiga is compiled using bazel.
```
cd Tiga
bazel build //...
```
After bazel compilation, the binares are located in `Tiga/bazel-bin` folder.

## Run Test Case

We use python scripts in `Tiga/scripts` to run the test cases automatically. The python enviornment is also py310.

```
## Assume py310 enviornment is activated
## Install dependencies
pip install pandas
pip install numpy
pip install ruamel.yaml
pip install ipython
pip install termcolor
```

Then we can run the test cases automatically, following the instructions at [README.md](./README.md).