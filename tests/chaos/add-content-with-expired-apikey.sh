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

fname=$(basename $EST_SAMPLE_FILE)
name="Sample Collection"
description="This is a sample collection"

# Let's add a collection
data="$(echo {} | jq --raw-output \
  --arg name "$name" \
  --arg description "$description" \
  '. + { "name": $name,
         "description": $description
       }'
)"

echo $data

set -x
curl --progress-bar -X POST -H "Authorization: Bearer $ESTUARY_EXPIRED_TOKEN" -H "Content-Type: application/json" -d "$data" $EST_API_HOST/content/add
