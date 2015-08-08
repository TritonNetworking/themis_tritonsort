#!/bin/bash

set -e

USER=`whoami`

echo "Fetching instance metadata..."
# Parse cluster information out of user-data
USERDATA=`wget -q -O - http://169.254.169.254/latest/user-data`
echo "USERDATA: $USERDATA"

# Userdata is a json dict, so treat it as such.
CLUSTER_ID=`echo $USERDATA | python -c "import json,sys; d=json.load(sys.stdin); print d[\"cluster_ID\"]"`
CLUSTER_NAME=`echo $USERDATA | python -c "import json,sys; d=json.load(sys.stdin); print d[\"cluster_name\"]"`
NODE_TYPE=`echo $USERDATA | python -c "import json,sys; d=json.load(sys.stdin); print d[\"node_type\"]"`
AWS_ACCESS_KEY_ID=`echo $USERDATA | python -c "import json,sys; d=json.load(sys.stdin); print d[\"aws_access_key_id\"]"`
AWS_SECRET_ACCESS_KEY=`echo $USERDATA | python -c "import json,sys; d=json.load(sys.stdin); print d[\"aws_secret_access_key\"]"`
S3_BUCKET=`echo $USERDATA | python -c "import json,sys; d=json.load(sys.stdin); print d[\"s3_bucket\"]"`
DEVICES=`echo $USERDATA | python -c "import json,sys; d=json.load(sys.stdin); print d[\"devices\"]"`

# Get other instance metadata
AVAILABILITY_ZONE=`wget -q -O - http://169.254.169.254/latest/meta-data/placement/availability-zone`
REGION=`echo $AVAILABILITY_ZONE | sed 's/\([0-9]\)[a-z]/\1/'`
INSTANCE_ID=`wget -q -O - http://169.254.169.254/latest/meta-data/instance-id`
INTERNAL_DNS=`wget -q -O - http://169.254.169.254/latest/meta-data/local-hostname`
EXTERNAL_DNS=`wget -q -O - http://169.254.169.254/latest/meta-data/public-hostname`
INTERNAL_IP=`wget -q -O - http://169.254.169.254/latest/meta-data/local-ipv4`
EXTERNAL_IP=`wget -q -O - http://169.254.169.254/latest/meta-data/public-ipv4`
INSTANCE_TYPE=`wget -q -O - http://169.254.169.254/latest/meta-data/instance-type`

echo "Configuring AWS CLI..."
# Set up AWS CLI config
AWS_CONFIG=~/.aws/config
if [ -f $AWS_CONFIG ]
then
    rm $AWS_CONFIG
fi

mkdir -p ~/.aws
echo "[default]" >> $AWS_CONFIG
echo "aws_access_key_id = $AWS_ACCESS_KEY_ID" >> $AWS_CONFIG
echo "aws_secret_access_key = $AWS_SECRET_ACCESS_KEY" >> $AWS_CONFIG
echo "region = $REGION" >> $AWS_CONFIG
echo "[profile themis]" >> $AWS_CONFIG
echo "aws_access_key_id = $AWS_ACCESS_KEY_ID" >> $AWS_CONFIG
echo "aws_secret_access_key = $AWS_SECRET_ACCESS_KEY" >> $AWS_CONFIG
echo "region = $REGION" >> $AWS_CONFIG
sudo chown $USER:$USER $AWS_CONFIG
chmod 600 $AWS_CONFIG

# Set up tags
aws ec2 create-tags --resources $INSTANCE_ID --tags Key=CLUSTER_ID,Value="${CLUSTER_ID}" Key=CLUSTER_NAME,Value="${CLUSTER_NAME}" Key=NODE_TYPE,Value="${NODE_TYPE}" Key=STATUS,Value=Booting

echo "Fetching configuration information from S3..."
# Fetch the remaining configuration information from S3.
aws s3api get-object --bucket "$S3_BUCKET" --key "cluster_${CLUSTER_ID}/cluster.conf" ~/cluster.conf
aws s3api get-object --bucket "$S3_BUCKET" --key "cluster_${CLUSTER_ID}/amazon.conf" ~/amazon.conf

CONFIG_DIRECTORY=`grep config_directory ~/cluster.conf | awk -F= '{print $2}' | tr -d " "`
CONFIG_DIRECTORY=`python -c "import os; print os.path.expanduser(\"$CONFIG_DIRECTORY\")"`
THEMIS_DIRECTORY=`grep themis_directory ~/cluster.conf | awk -F= '{print $2}' | tr -d " "`
THEMIS_DIRECTORY=`python -c "import os; print os.path.expanduser(\"$THEMIS_DIRECTORY\")"`

# sync is **really** dumb and sets its exit status to the number of files synced instead of 0
set +e
aws s3 sync s3://${S3_BUCKET}/cluster_${CLUSTER_ID}/themis_config/ $CONFIG_DIRECTORY
set -e

# Build node config
NODE_CONF=~/node.conf
echo "[node]" > $NODE_CONF
echo "node_type = ${NODE_TYPE}" >> $NODE_CONF
echo "internal_ip = ${INTERNAL_IP}" >> $NODE_CONF
echo "external_ip = ${EXTERNAL_IP}" >> $NODE_CONF
echo "internal_dns = ${INTERNAL_DNS}" >> $NODE_CONF
echo "external_dns = ${EXTERNAL_DNS}" >> $NODE_CONF
echo "devices = ${DEVICES}" >> $NODE_CONF
echo "" >> $NODE_CONF
echo "[amazon]" >> $NODE_CONF
echo "instance_id = ${INSTANCE_ID}" >> $NODE_CONF

# Use ls/for semantics to get all keys because the sync command appears to be asynchronous....
KEYS_LS=`aws s3api list-objects --bucket "${S3_BUCKET}" --prefix "cluster_${CLUSTER_ID}/keys/"`
echo $KEYS_LS
KEYS=`echo $KEYS_LS | python -c "import json,sys; d=json.load(sys.stdin); contents = d[\"Contents\"]; print \",\".join([x[\"Key\"] for x in contents])"`
echo $KEYS

mkdir -p ~/keys
while IFS="," read -ra KEY_LIST
do
    for KEY in "${KEY_LIST[@]}"
    do
        echo $KEY
        BASE=`basename "$KEY"`
        echo $BASE
        aws s3api get-object --bucket "${S3_BUCKET}" --key "$KEY" ~/keys/${BASE}
    done
done <<< "$KEYS"

for key in ~/keys/*
do
    chmod 600 $key
    mv "$key" ~/.ssh/
done
rmdir ~/keys

eval `ssh-agent -s`
sleep 2
ssh-add

PUBLIC_KEY=`grep public_key ~/cluster.conf | awk -F= '{print $2}' | tr -d " "`
cat ~/.ssh/${PUBLIC_KEY} >> ~/.ssh/authorized_keys

echo "Configuring local environment"
# Start netserver if not already running
NETSERVER_RUNNING=`ps aux | grep netserver | grep -v grep | wc -l`
if [[ "$NETSERVER_RUNNING" == "0" ]]
then
    echo "Starting netserver..."
    /usr/local/bin/netserver netserver
fi

# Raise max socket backlog to 1024
sudo bash -c "echo 1024 > /proc/sys/net/core/somaxconn"
sudo bash -c "echo 1024 > /proc/sys/net/ipv4/tcp_max_syn_backlog"

# Set hostname manually
sudo hostname $INTERNAL_DNS

# Download and build themis
echo "Buliding themis from HEAD..."
if [ ! -d $THEMIS_DIRECTORY ]
then
    echo "Cloning repo..."
    cd ~
    git clone ${GITHUB_URL} $THEMIS_DIRECTORY

    cd ${THEMIS_DIRECTORY}/src
    cmake -D CMAKE_BUILD_TYPE:str=Release .
    make clean
    make -j`nproc`
fi

# Start redis if master node
if [[ "$NODE_TYPE" == "master" ]]
then
    # Start redis
    sudo /usr/local/bin/redis-server --daemonize yes

    # Set master hostname on this node only.
    $THEMIS_DIRECTORY/src/scripts/themis/cloud/set_master_hostname.py

    # Start cluster monitor
    tmux new-session -d -s cluster_monitor "tmux new-window '${THEMIS_DIRECTORY}/src/scripts/themis/cluster/cluster_monitor.py'"
fi

# Make log directory
LOG_DIRECTORY=`grep log_directory ~/cluster.conf | awk -F= '{print $2}' | tr -d " "`
eval mkdir -p $LOG_DIRECTORY

# Mount the disks but don't format, but don't throw an error if we can't
# mount them (for example if the disks aren't yet formatted)
set +e
$THEMIS_DIRECTORY/src/scripts/themis/cluster/mount_disks.py
set -e

# Update tags for this instance, which signals we are ready
aws ec2 create-tags --resources $INSTANCE_ID --tags Key=STATUS,Value=Online
