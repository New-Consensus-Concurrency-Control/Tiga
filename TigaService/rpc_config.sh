#!/bin/bash

## Generate the shell command 

CMD="python ../rrr/rpcgen"
RPC_PATH=$(pwd)/TigaService

for file in "$RPC_PATH"/*; do
    if [[ "$file" == *.rpc ]]; then
        $CMD $file 
    fi
done

RPC_PATH=$(pwd)

for file in "$RPC_PATH"/*; do
    if [[ "$file" == *.rpc ]]; then
        $CMD $file 
    fi
done