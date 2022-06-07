#!/bin/bash

###################################################################
#Script Name	: add-large-content-to-shuttles.sh
#Description	: This is a script file that runs a curl command to add a large file to a bunch of shuttles
#Author         : ARG
#Email          : 
###################################################################

. ../data/config/run.config

echo '#####################################'
echo `basename "$0"`
echo '#####################################'
echo ''

fname=$(basename $EST_SAMPLE_LARGE_FILE)

# We don't want to commit a large file so we're going to generate it only for running this script.
# For this process, we're going to generate the large file before and delete it after.
if [[ $OSTYPE == 'darwin'* ]]; then
  yes "this is for a large file" | head -n 10000 > $EST_SAMPLE_LARGE_FILE
else
  fallocate -l 2G $EST_SAMPLE_LARGE_FILE
fi

# Generate a large file.
for i in "${UPLOAD_URLS[@]}"
do
  set -x
  HOST=$i
  curl --progress-bar -X POST $HOST/content/add -H "Authorization: Bearer $ESTUARY_TOKEN" -H "Accept: application/json" -H "Content-Type: multipart/form-data" -F "data=@$EST_SAMPLE_LARGE_FILE"
done


# Convert it back to a small file
yes "this is for a large file" | head -n 10 > $EST_SAMPLE_LARGE_FILE