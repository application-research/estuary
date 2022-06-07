#!/bin/bash

###################################################################
#Script Name	: add-file.sh
#Description	: This is a script file that runs a curl command to add a file to the estuary content server.
#Author       : ARG
#Email        :
###################################################################

. ../data/config/run.config

echo '#####################################'
echo `basename "$0"`
echo '#####################################'
echo ''

fname=$(basename $EST_SAMPLE_FILE)
EST_UPLOAD_HOST=https://upload.estuary.tech
COLLECTION="185d7ffc-85d8-4b73-86bb-1ca6419bc10d"

echo "Upload to several shuttles: Started"
for i in "${DISABLED_UPLOAD_URLS[@]}"
do
  set -x
  HOST=$i
  curl --trace - --trace-time --progress-bar -X POST $HOST/content/add -H "Authorization: Bearer $ESTUARY_TOKEN" -H "Accept: application/json" -H "Content-Type: multipart/form-data" -F "data=@$EST_SAMPLE_LARGE_FILE"
done
echo "Upload to several shuttles: Done"
res=$?
if test "$res" != "0"; then
   echo "add-file failed: $res"
fi

