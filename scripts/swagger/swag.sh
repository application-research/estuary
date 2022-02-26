#!/bin/sh

###################################################################
# Script Name	:   swag.sh                                                                                          
# Description	:   Simple script to generate swagger documentation for estuary.
# Author       	:   ARG
# How to run    :   ./scripts/swagger/swag.sh
# The proper way to run this script is to run the following command:
# `cd estuary (root directory of the project)`
# `make generate-swagger`
###################################################################

# Global Variables
os=$(uname)
swag=''
yq=''
localhost='api.estuary.tech'

# Arguments
host=$1

# Swag
linuxSwag=https://github.com/swaggo/swag/releases/download/v1.7.9-p1/swag_1.7.9-p1_Linux_x86_64.tar.gz
darwinSwag=https://github.com/swaggo/swag/releases/download/v1.7.9-p1/swag_1.7.9-p1_Darwin_x86_64.tar.gz

# YQ
linuxYq=https://github.com/mikefarah/yq/releases/download/v4.20.1/yq_linux_386.tar.gz
darwinYq=https://github.com/mikefarah/yq/releases/download/v4.20.1/yq_darwin_amd64.tar.gz
yqfileName=yq

## Download Swag and put it here
echo "Remove old swag"
rm -Rf ./scripts/swagger/swag.tar.gz
rm -Rf ./scripts/swagger/echo-swag

echo "Downloading Swag"
mkdir ./scripts/swagger/echo-swag

if [ "$os" = "Darwin" ]; then
    echo "Downloading Swag and Yq for Mac"
    swag=$darwinSwag
    yq=$darwinYq
    fileName="${yq##*/}"
    yqfileName="${fileName%.*}"
elif [ "$os" = "Linux" ]; then
    echo "Downloading Swag and Yq for Linux"
    swag=$linuxSwag
    yq=$linuxYq
    fileName="${yq##*/}"
    yqfileName="${fileName%.*}"
else
    echo "Unsupported OS"
    exit 1
fi

if [ "$host" = "" ]; then
    host=$localhost
fi

curl -LJ -o ./scripts/swagger/swag.tar.gz $swag
chmod +x ./scripts/swagger/swag.tar.gz

curl -LJ -o ./scripts/swagger/yq.tar.gz $yq
chmod +x ./scripts/swagger/yq.tar.gz


echo "Extracting Swag"
tar -xvf ./scripts/swagger/swag.tar.gz -C ./scripts/swagger/echo-swag
tar -xvf ./scripts/swagger/yq.tar.gz -C ./scripts/swagger/echo-swag

## Rename Yq
yqfileName=$(echo "$yqfileName" | cut -f 1 -d '.')
mv ./scripts/swagger/echo-swag/$yqfileName ./scripts/swagger/echo-swag/yq

## Run Swag on handler.go
echo "Running Swag"
chmod +x ./scripts/swagger/echo-swag/swag
chmod +x ./scripts/swagger/echo-swag/yq

./scripts/swagger/echo-swag/swag init -g handlers.go

## workaround to add the security and host - this so we can add the Bearer token to the header
## Json 
jq '.host = "'${host}'"' ./docs/swagger.json > ./docs/swagger_host.json
jq '."securityDefinitions"={"bearerAuth":{"type":"apiKey","name":"Authorization","in":"header"}}' ./docs/swagger_host.json > ./docs/swagger_security.json
jq '."security"=[{"bearerAuth":[]}]' ./docs/swagger_security.json > ./docs/swagger_temp.json

## yaml
./scripts/swagger/echo-swag/yq e '.host = "'${host}'"' ./docs/swagger.yaml > ./docs/swagger_host.yaml
./scripts/swagger/echo-swag/yq e '."securityDefinitions"={"bearerAuth":{"type":"apiKey","name":"Authorization","in":"header"}}' ./docs/swagger_host.yaml > ./docs/swagger_security.yaml
./scripts/swagger/echo-swag/yq e '."security"=[{"bearerAuth":[]}]' ./docs/swagger_security.yaml > ./docs/swagger_temp.yaml

##  Move the files back
mv ./docs/swagger_temp.json ./docs/swagger.json
mv ./docs/swagger_temp.yaml ./docs/swagger.yaml


## Clean up. 
echo "Cleaning up"
rm -Rf ./scripts/swagger/swag.tar.gz
rm -Rf ./scripts/swagger/yq.tar.gz
rm -Rf ./scripts/swagger/echo-swag
rm -Rf ./docs/swagger_*