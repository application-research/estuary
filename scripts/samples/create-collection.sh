#!/bin/bash

###################################################################
#Script Name	  : create-collection.sh                                                                                             
#Description	  : This is a script to create a collection
#Author           : ARG
#Email            : 
###################################################################

. run.config

fname=$(basename $EST_SAMPLE_FILE)
name="Sample Collection"
description="This is a sample collection"

data="$(echo {} | jq --raw-output \
  --arg name "$name" \
  --arg description "$description" \
  '. + { "name": $name,
         "description": $description
       }'
)"

echo $data

set -x
curl --progress-bar -X POST -H "Authorization: Bearer  $ESTUARY_TOKEN" -H "Content-Type: application/json" -d "$data" $EST_HOST/collections/create
