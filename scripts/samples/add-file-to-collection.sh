#!/bin/bash

#!/bin/bash

###################################################################
#Script Name	: add-file-to-collection.sh
#Description	: This is a script file that runs a curl command to add a file to the estuary content server.
#Author       : ARG
#Email        :
###################################################################

. run.config

echo '#####################################'
echo `basename "$0"`
echo '#####################################'

fname="$2"

if [ -z "$fname" ]; then
  file=$EST_SAMPLE_FILE
  fname=$(basename $EST_SAMPLE_FILE)
else
  file=$1
  fname=$(basename $1)
fi

echo "$file"
echo "$fname"

#ESTUARY_TOKEN="whysaccesstoken3"
#EST_HOST="http://localhost:3004"
#EST_HOST="https://api.estuary.tech"
#EST_HOST="https://upload.estuary.tech"
EST_HOST="https://shuttle-4.estuary.tech"
#EST_HOST="https://shuttle-5.estuary.tech"

COLLECTION="185d7ffc-85d8-4b73-86bb-1ca6419bc10d"
set -x
curl --progress-bar -X POST -H "Authorization: Bearer $ESTUARY_TOKEN" -F "data=@$file" -F "filename=$fname" $EST_HOST/content/add\?collection="$COLLECTION"

