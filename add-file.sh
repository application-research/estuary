#!/bin/bash

fname="$2"
if [ -z "$fname" ]; then
  fname=$(basename $1)
fi

echo "$1"
echo "$fname"

curl -X POST -H "Authorization: Bearer $ESTUARY_TOKEN" -F "data=@$1" -F "name=$fname" http://localhost:3004/content/add

