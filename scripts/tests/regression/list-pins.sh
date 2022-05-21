#!/bin/bash

###################################################################
#Script Name	  : list-pins.sh                                                                                             
#Description	  : This script is to list all pins
#Author           : ARG
#Email            : 
###################################################################


. run.config

echo '#####################################'
echo `basename "$0"`
echo '#####################################'

qcids=""
qname=""
qstatus=""
qbefore=""
qafter=""
qlimit=""
qreqids=""

if [ -z "$qcids" ]; then
    qcids="&cids=$qcids"
fi

if [ -z "$qname" ]; then
    qname="&name=$qname"
fi

if [ -z "$qstatus" ]; then
    qstatus="&status=$qstatus"
fi

if [ -z "$qbefore" ]; then
    qbefore="&before=$qbefore"
fi

if [ -z "$qafter" ]; then
    qafter="&after=$qafter"
fi

if [ -z "$qlimit" ]; then
    qlimit="&limit=$qlimit"
fi

if [ -z "$qreqids" ]; then
    qreqids="&reqids=$qreqids"
fi

set -x
curl --progress-bar -X GET -H "Authorization: Bearer $ESTUARY_TOKEN" -H "Content-Type: application/json" $EST_HOST/pinning/pins?$qcids$qname$qstatus$qbefore$qafter$qlimit$qreqids
