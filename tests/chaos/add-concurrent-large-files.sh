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

for i in {0..3}
do
  nohup ./add-large-content-to-shuttles.sh > ../data/logs/$i-add-large-file.log &
done

