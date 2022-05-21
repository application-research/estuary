#!/bin/bash

###################################################################
#Script Name	: download-large-files.sh
#Description	: This is a script file that runs a curl command to download a file
#Author       : ARG
#Email        : 
###################################################################

echo '#####################################'
echo `basename "$0"`
echo '#####################################'

. run.config

set -x
for i in "${MULTIPLE_GATEAWAYS[@]}"
do
  set -x
  HOST=$i
  curl --progress-bar -X GET $HOST/$CID
done

