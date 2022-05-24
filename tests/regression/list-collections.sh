#!/bin/bash

###################################################################
#Script Name	  : list-collection.sh                                                                                             
#Description	  : This is a script to list all collections.
#Author         : ARG
#Email          : 
###################################################################

. ../data/config/run.config

echo '#####################################'
echo `basename "$0"`
echo '#####################################'

fname=$(basename $EST_SAMPLE_FILE)
name="Sample Collection"
description="This is a sample collection"

echo $data

set -x
curl --progress-bar -X GET -H "Authorization: Bearer $ESTUARY_TOKEN" -H "Content-Type: application/json" -d "$data" $EST_API_HOST/collections/list
