#!/bin/bash

miner="$1"
cid="$2"
price="$3"
duration="$4"

data="{\"Cid\":{\"/\":\"$cid\"},\"Price\":\"$price\",\"Duration\":$duration}"


echo $data

curl -X POST -H "Content-Type: application/json" -d "$data" http://localhost:3004/deals/make/$miner
