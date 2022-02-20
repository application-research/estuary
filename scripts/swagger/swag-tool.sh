#!/bin/sh

###################################################################
# Script Name	:   swag-tool.sh                                                                                          
# Description	:   Simple script to download swag                                                                                                                                                                     
# Author       	:   ARG
# Running
###################################################################

# Global Variables
os=$(uname)
swag=''
linuxSwag=https://github.com/swaggo/swag/releases/download/v1.7.9-p1/swag_1.7.9-p1_Linux_x86_64.tar.gz
darwinSwag=https://github.com/swaggo/swag/releases/download/v1.7.9-p1/swag_1.7.9-p1_Darwin_x86_64.tar.gz
## Download Swag and put it here

echo "Remove old swag"
rm -Rf ./scripts/swagger/swag.tar.gz
rm -Rf ./scripts/swagger/echo-swag

echo "Downloading Swag"
mkdir ./scripts/swagger/echo-swag

if [ "$os" = "Darwin" ]; then
    echo "Downloading Swag for Mac"
    swag=$darwinSwag
elif [ "$os" = "Linux" ]; then
    echo "Downloading Swag for Linux"
    swag=$linuxSwag
else
    echo "Unsupported OS"
    exit 1
fi

curl -LJ -o ./scripts/swagger/swag.tar.gz $swag
chmod +x ./scripts/swagger/swag.tar.gz

echo "Extracting Swag"
tar -xvf ./scripts/swagger/swag.tar.gz -C ./scripts/swagger/echo-swag
