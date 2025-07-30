#!/bin/bash

## Generate the shell command 

CMD="python ../rrr/rpcgen"

RPC_PATH=$(pwd)
echo $RPC_PATH
for file in "$RPC_PATH"/*; do
    if [[ "$file" == *.rpc ]]; then
        echo $file
        $CMD $file 
    fi
done