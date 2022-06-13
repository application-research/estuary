#!/bin/bash

###################################################################
#Script Name	  : peering-add.sh
#Description	  : This is a script to test peering.
#Author         : ARG
#Email          : 
###################################################################

. ../data/config/run.config

echo '#####################################'
echo `basename "$0"`
echo '#####################################'
echo ''

peers='[{"ID": "12D3KooWEGeZ19Q79NdzS6CJBoCwFZwujqi5hoK8BtRcLa48fJdu", "Addrs": ["/ip4/145.40.96.233/tcp/4001"] },{ "ID": "12D3KooWBnmsaeNRP6SCdNbhzaNHihQQBPDhmDvjVGsR1EbswncV", "Addrs":["/ip4/18.1.1.2/tcp/4001", "/ip4/147.75.87.85/tcp/4001"]}]'

set -x
curl --trace - --trace-time --progress-bar -X POST -H "Authorization: Bearer $ESTUARY_TOKEN" -H "Content-Type: application/json" -d "$peers" $EST_API_HOST/admin/peering/peers/add
