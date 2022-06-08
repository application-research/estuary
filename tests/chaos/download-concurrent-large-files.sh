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
echo ''

for i in {0..3}
do
  nohup ./download-large-files.sh > logs/$i-download-large-files.log &
done

