#!/bin/bash

###################################################################
#Script Name	  : list-bucket.sh                                                                                             
#Description	  : This is a script to list all buckets.
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

echo $data

set -x
curl --trace - --trace-time --progress-bar -X GET -H "Authorization: Bearer $ESTUARY_TOKEN" -H "Content-Type: application/json" -d "$data" $EST_API_HOST/buckets/list
