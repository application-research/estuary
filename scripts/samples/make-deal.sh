#!/bin/bash

###################################################################
#Script Name	: make-deal.sh                                                                                             
#Description	: This is a script file that runs a curl command to add a file to the estuary content server.                                                                                       
#Author         : ARG
#Email          : 
###################################################################

. run.config

echo '#####################################'
echo `basename "$0"`
echo '#####################################'

miner="$1"
cid="{/:$2}"
price="$3"
duration="$4"

data="{\"Cid\":{\"/\":\"$cid\"},\"Price\":\"$price\",\"Duration\":$duration}"

data="$(echo {} | jq \
  --arg name "$name" \
  --arg description "$description" \
  --argjson cid "$cid" \
  '. + { "Cid": $cid,
         "Price": $price,
         "Duration": $duration
       }'
)"

echo $data
curl -X POST -H "Content-Type: application/json" -d "$data" http://$EST_HOST/deals/make/$miner
