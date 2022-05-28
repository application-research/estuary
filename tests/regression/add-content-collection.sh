#!/bin/bash

###################################################################
#Script Name	  : add-content-collection.sh                                                                                             
#Description	  : This is a script to list all collections.
#Author           : ARG
#Email            : 
###################################################################

. ../data/config/run.config

echo '#####################################'
echo `basename "$0"`
echo '#####################################'
echo ''


fname=$(basename $EST_SAMPLE_FILE)
contents='[0,1,2,3,4]'
collection="185d7ffc-85d8-4b73-86bb-1ca6419bc10d"
cids='["QmTkvGHnzSfqU3vNJ4DJtsFEMLsRUwHabZivGqLbgrrumy","Qmad6w4R5657hGgZcjqnWEpJGGLUSEh7X2jSVkFb1YPjQb"]'

# Let's add a collection
data="$(echo {} | jq --raw-output \
  --argjson contents $contents \
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
curl --trace - --trace-time --progress-bar POST -H "Authorization: Bearer $ESTUARY_TOKEN" -H "Content-Type: application/json" -d "$data" $EST_API_HOST/collections/add-content