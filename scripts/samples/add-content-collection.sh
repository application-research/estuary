#!/bin/bash

###################################################################
#Script Name	  : add-content-collection.sh                                                                                             
#Description	  : This is a script to list all collections.
#Author           : ARG
#Email            : 
###################################################################

. run.config

fname=$(basename $EST_SAMPLE_FILE)
contents="[1,1]"
collection="This is a sample collection"
cids="[1,1]"

# Let's add a collection
data="$(echo {} | jq --raw-output \
  --argjson contents "$contents" \
  --arg collection "$collection" \
  --argjson cids "$cids" \
  '. + { 
         "contents": $contents,
         "coluuid": $collection,
         "cids": $cids
       }'
)"

echo $data

set -x
curl --progress-bar -X POST -H "Authorization: Bearer $ESTUARY_TOKEN" -H "Content-Type: application/json" -d "$data" $EST_HOST/collections/add-content
