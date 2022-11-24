#!/bin/bash

###################################################################
#Script Name	: create-bucket.sh
#Description	: This is a script file that runs a curl command to add a file to the estuary content server.
#Author         : ARG
#Email          :
###################################################################

. ../data/config/run.config

echo '#####################################'
echo `basename "$0"`
echo '#####################################'
echo ''

. config/run.config

fname=$(basename $EST_SAMPLE_FILE)
name="Sample Bucket"
description="This is a sample bucket"

# Let's add a bucket
data="$(echo {} | jq --raw-output \
  --arg name "$name" \
  --arg description "$description" \
  '. + { "name": $name,
         "description": $description
       }'
)"

echo $data

set -x
curl --trace - --trace-time --progress-bar -X POST -H "Authorization: Bearer $ESTUARY_TOKEN" -H "Content-Type: application/json" -d "$data" $EST_API_HOST/buckets/create
