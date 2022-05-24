#!/bin/bash

###################################################################
#Script Name	: download-large-files.sh
#Description	: This is a script file that runs a curl command to download a file
#Author       : ARG
#Email        : 
###################################################################

. ../data/config/run.config

echo '#####################################'
echo `basename "$0"`
echo '#####################################'

set -x
curl --progress-bar -X GET $DWEB_HOST/$CID

