#!/bin/bash

###################################################################
#Script Name	  : list-pins.sh                                                                                             
#Description	  : This script is to list all pins
#Author           : ARG
#Email            : 
###################################################################


. run.config

qcids=""
qname=""
qstatus=""
qbefore=""
qafter=""
qlimit=""
qreqids=""

if[ $qcids != "" ]; then
    qcids="&cids=$qcids"
fi

if[ $qname != "" ]; then
    qname="&name=$qname"
fi

if[ $qstatus != "" ]; then
    qstatus="&status=$qstatus"
fi

if[ $qbefore != "" ]; then
    qbefore="&before=$qbefore"
fi

if[ $qafter != "" ]; then
    qafter="&after=$qafter"
fi

if[ $qlimit != "" ]; then
    qlimit="&limit=$qlimit"
fi

if[ $qreqids != "" ]; then
    qreqids="&reqids=$qreqids"
fi

set -x
curl --progress-bar -X GET -H "Authorization: Bearer  $ESTUARY_TOKEN" -H "Content-Type: application/json" $EST_HOST/pinning/pins?$qcids$qname$qstatus$qbefore$qafter$qlimit$qreqids
