#!/bin/bash

###################################################################
#Script Name	: run-chaos.sh
#Description	: This is a script to run all scripts.
#Author         : ARG
#Email          : 
###################################################################

. ../data/config/run.config

specificScript=$1
# create reports folder.
if [ -z "$specificScript" ]; then
  echo "no specific script specified";
else
  bash "$specificScript.sh";
  exit;
fi

for f in *.sh; do
    if [ "$f" != "run-chaos-tests.sh" ]; then
        bash "$f" || break  # execute successfully or break
  # Or more explicitly: if this execution fails, then stop the `for`:
  # if ! bash "$f"; then break; fi
    fi
done