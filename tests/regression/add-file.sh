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

set -x
curl --trace - --trace-time --progress-bar -X POST -H "Authorization: Bearer $ESTUARY_TOKEN" -F "data=@$EST_SAMPLE_FILE" -F "filename=$fname" $EST_UPLOAD_HOST/content/add\?collection="$COLLECTION"

