#!/bin/bash

set -e

if [ $# -ne 1 ]
then
    echo "Usage: `basename $0` github_key"
    exit $E_BADARGS
fi


GITHUB_KEY=$1

if [ ! -f $GITHUB_KEY ]
then
    echo "Github key $GITHUB_KEY does not exist".
    exit 1
fi

KEY_BASENAME=`basename $GITHUB_KEY`

chmod 600 $GITHUB_KEY
mv $GITHUB_KEY ~/.ssh/

eval `ssh-agent -s`
sleep 2
ssh-add ~/.ssh/${KEY_BASENAME}

# Download and build themis
echo "Buliding themis from HEAD..."
if [ ! -d ~/themis/ ]
then
    echo "Cloning repo..."
    cd ~
    git clone ${GITHUB_URL} ~/themis

    cd ~/themis/src
    cmake -D CMAKE_BUILD_TYPE:str=Release .
    make clean
    make -j`nproc`
fi

# Remove ssh key so we don't store it with the AMI
# An actual Themis instance will install its own ssh keys so we don't have to
# worry about detaching ourselves from the github repo.
rm ~/.ssh/$KEY_BASENAME
