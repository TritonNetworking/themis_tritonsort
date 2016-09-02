#!/bin/bash

set -e

if [ $# -ge 1 ]
then
    GITHUB_KEY=$1
else
    GITHUB_KEY="NOT_A_FILE"
fi

if [ ! -f $GITHUB_KEY ]
then
    COPY_GITHUB=false
else
    COPY_GITHUB=true
    KEY_BASENAME=`basename $GITHUB_KEY`
    chmod 600 $GITHUB_KEY
    mv $GITHUB_KEY ~/.ssh/
    eval `ssh-agent -s`
    sleep 2
    ssh-add ~/.ssh/${KEY_BASENAME}
fi

# Download and build themis
echo "Buliding themis from HEAD..."
if [ ! -d ~/themis/ ] && [ "$COPY_GITHUB" == true ]
then
    echo "Cloning repo..."
    cd ~
    git clone ${GITHUB_URL} ~/themis
fi

cd ~/themis/src
cmake -D CMAKE_BUILD_TYPE:str=Release .
make clean
make -j`nproc`

# Remove ssh key so we don't store it with the AMI
# An actual Themis instance will install its own ssh keys so we don't have to
# worry about detaching ourselves from the github repo.
if [ "$COPY_GITHUB" == true ]; then
    rm ~/.ssh/$KEY_BASENAME
fi
