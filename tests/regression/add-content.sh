#!/bin/bash

###################################################################
#Script Name	: add-content.sh                                                                                             
#Description	: This is a script file that runs a curl command to add a file to the estuary content server.                                                                                       
#Author       : ARG
#Email          : 
###################################################################

. ../data/config/run.config

echo '#####################################'
echo `basename "$0"`
echo '#####################################'
echo ''

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

echo "Data: " "$data"
EST_API_HOST=https://upload.estuary.tech
set -x
curl --trace - --trace-time --progress-bar -X POST $EST_UPLOAD_HOST/content/add -H "Authorization: Bearer $ESTUARY_TOKEN" -H "Accept: application/json" -H "Content-Type: multipart/form-data" -F "data=@$EST_SAMPLE_LARGE_FILE"