#!/bin/bash

###################################################################
#Script Name	: add-file.sh                                                                                             
#Description	: This is a script file that runs a curl command to add a file to the estuary content server.                                                                                       
#Author       : ARG
#Email        : 
###################################################################

. run.config

fname=$(basename $EST_SAMPLE_FILE)
COLLECTION=""

set -x
curl --progress-bar -X POST -H "Authorization: Bearer $ESTUARY_TOKEN" -F "data=@$EST_SAMPLE_FILE" -F "name=$fname" $EST_HOST/content/add\?collection="$COLLECTION"

