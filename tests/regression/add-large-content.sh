#!/bin/bash

###################################################################
#Script Name	: add-content.sh                                                                                             
#Description	: This is a script file that runs a curl command to add a file to the estuary content server.                                                                                       
#Author         : ARG
#Email          : 
###################################################################

. ../data/config/run.config

echo '#####################################'
echo `basename "$0"`
echo '#####################################'

fname=$(basename $EST_SAMPLE_LARGE_FILE)

## Override (use upload, and shuttle hosts)
EST_UPLOAD_HOST=https://upload.estuary.tech

## use a different token if needed.
#ESTUARY_TOKEN=EST_this_entire_token_should_be_replaced_ARY

# We don't want to commit a large file so we're going to generate it only for running this script.
# For this process, we're going to generate the large file before and delete it after.
yes "this is for a large file" | head -n 10000000 > $EST_SAMPLE_LARGE_FILE
# Generate a large file.

set -x
curl --progress-bar -X POST $EST_UPLOAD_HOST/content/add -H "Authorization: Bearer $ESTUARY_TOKEN" -H "Accept: application/json" -H "Content-Type: multipart/form-data" -F "data=@$EST_SAMPLE_LARGE_FILE"

# Convert it back to a small file
yes "this is for a large file" | head -n 100 > $EST_SAMPLE_LARGE_FILE
res=$?
if test "$res" != "0"; then
   echo "add-large-content failed: $res"
fi