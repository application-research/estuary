#!/bin/bash

###################################################################
#Script Name	: run.sh                                                                                             
#Description	: This is a script to run all scripts.
#Author         : ARG
#Email          : 
###################################################################

. run.config

for f in *.sh; do
    if [ "$f" != "run.sh" ]; then
        bash "$f" || break  # execute successfully or break
  # Or more explicitly: if this execution fails, then stop the `for`:
  # if ! bash "$f"; then break; fi
    fi
done