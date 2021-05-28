#!/bin/bash

fname="$2"
if [ -z "$fname" ]; then
  fname=$(basename $1)
fi

echo "$1"
echo "$fname"

#ESTUARY_TOKEN="whysaccesstoken3"
#EST_HOST="http://localhost:3004"
EST_HOST="https://api.estuary.tech"

curl -X POST -H "Authorization: Bearer $ESTUARY_TOKEN" -F "data=@$1" -F "name=$fname" $EST_HOST/content/add

