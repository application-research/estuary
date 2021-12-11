#!/bin/bash

fname="$2"
if [ -z "$fname" ]; then
  fname=$(basename $1)
fi

echo "$1"
echo "$fname"

#ESTUARY_TOKEN="whysaccesstoken3"
EST_HOST="http://localhost:3004"
#EST_HOST="https://api.estuary.tech"
#EST_HOST="https://upload.estuary.tech"
#EST_HOST="https://shuttle-4.estuary.tech"

curl --progress-bar -X POST -H "Authorization: Bearer $ESTUARY_TOKEN" -F "data=@$1" -F "name=$fname" $EST_HOST/content/add\?collection="6b143c45-c4f7-4c42-9306-d7643e179eb2"
#curl --progress-bar -X POST -H "Authorization: Bearer $ESTUARY_TOKEN" -F "data=@$1" -F "name=$fname" $EST_HOST/content/add\?collection="6832faa3-2871-466a-8ee2-3b882119c290"

