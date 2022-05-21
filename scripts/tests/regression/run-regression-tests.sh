#!/bin/bash

###################################################################
#Script Name	: run-regression-tests.sh
#Description	: This is a script to run all scripts.
#Author         : ARG
#Email          : 
###################################################################

. run.config

for f in *.sh; do
    if [ "$f" != "run-regression-tests.sh" ]; then
        bash "$f" || break  # execute successfully or break
        res=$?
        if test "$res" != "0"; then
           echo "failed: $res"
        fi
  # Or more explicitly: if this execution fails, then stop the `for`:
  # if ! bash "$f"; then break; fi
    fi
done