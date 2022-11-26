#!/bin/bash

###################################################################
#Script Name	: add-content-with-expired-api-key.sh
#Author         : ARG
#Email          :
###################################################################

. ../data/config/run.config

echo '#####################################'
echo `basename "$0"`
echo '#####################################'
echo ''

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
curl --trace - --trace-time --progress-bar -X POST -H "Authorization: Bearer $ESTUARY_EXPIRED_TOKEN" -H "Content-Type: application/json" -d "$data" $EST_API_HOST/content/add
