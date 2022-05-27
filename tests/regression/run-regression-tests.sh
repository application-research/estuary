#!/bin/bash

###################################################################
#Script Name	: run-regression-tests.sh
#Description	: This is a script to run all scripts.
#Author         : ARG
#Email          : 
###################################################################

source ../data/config/config.sh

specificScript=$1
# create reports folder.
if [ -z "$specificScript" ]; then
  echo "no specific script specified";
else
  bash "$specificScript";
  exit;
fi
for f in *.sh; do
    if [ "$f" != "run-regression-tests.sh" ]; then
        bash "$f" || break  # execute successfully or break
        res=$?
        #if test "$res" != "0"; then
           #bash ../reporting/report.sh $f $res
        #fi
    fi
done