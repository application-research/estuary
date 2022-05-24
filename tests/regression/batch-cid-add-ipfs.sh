#!/bin/bash

###################################################################
#Script Name	  : batch-cid-add-ipfs.sh                                                                                             
#Description	  : This is a script to add a files using CID to the estuary content server.
#Author           : ARG
#Email            : 
###################################################################


. ../data/config/run.config

## Some sample CIDs.
array=(
    "QmTkvGHnzSfqU3vNJ4DJtsFEMLsRUwHabZivGqLbgrrumy"
    "Qmad6w4R5657hGgZcjqnWEpJGGLUSEh7X2jSVkFb1YPjQb"
    "QmScyXmQQcvYAXdDUZjKwZibTqQqDfD8b6Hom7XFkK3Jze"
    "QmYXE5TJwnM4DRqTXJi2r7N966168z7wv3ssYZBcUoU7YM"
    "QmX2dP9zysg2PRMuMRRXhvmGtxuyc3o4hsSnKwu68aaCpA"
    "QmR538saYZ1o3B7pM94MxXn5yjTHDKYUCumS2fxq53Nihe"
    "QmTA4Mf2qJ3qFcPSqkGKRSBUtepHKE5zjAiuCmZ4VwXwT2"
    "QmNsjeRHKKw4qVHJ4MjVSghvHJaan9Cmx6w6jeBrDLiDqg"
    "QmX83WSwfTbsJwobQiAUfnJLYLdNN7QWD8iZokmxgAsKyd"
    "QmYnpSBkF5n7d54YBg1B5g4F6E9Hr9kLo66VBF4jJWfbEm"
    "QmQW9j6syR6gNBAFiReY9CY5sfnREVmm8dQPZYGsGC1mcT"
)

for i in "${array[@]}"
do
    echo $i
    data="$(echo {} | jq --raw-output \
    --arg root "$i" \
    '. + {
            "root": $root,
        }'
    )"
    set -x
    curl --progress-bar -X POST -H "Authorization: Bearer $ESTUARY_TOKEN" -H "Content-Type: application/json" -d "$data" $EST_API_HOST/content/add-ipfs

    sleep 2
done


