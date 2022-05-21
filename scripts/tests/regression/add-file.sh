#!/bin/bash

###################################################################
#Script Name	: add-file.sh                                                                                             
#Description	: This is a script file that runs a curl command to add a file to the estuary content server.                                                                                       
#Author       : ARG
#Email        : 
###################################################################

echo '#####################################'
echo `basename "$0"`
echo '#####################################'

. run.config

fname=$(basename $EST_SAMPLE_FILE)
EST_HOST=https://upload.estuary.tech
COLLECTION="185d7ffc-85d8-4b73-86bb-1ca6419bc10d"

set -x
curl --progress-bar -X POST -H "Authorization: Bearer $ESTUARY_TOKEN" -F "data=@$EST_SAMPLE_FILE" -F "name=$fname" $EST_HOST/content/add\?collection="$COLLECTION"

