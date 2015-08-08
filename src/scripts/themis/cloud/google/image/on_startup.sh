#!/bin/bash

set -e

USER=`whoami`

echo "Fetching instance metadata..."
# Parse cluster information out of user-data
USERDATA=`curl -s "http://metadata.google.internal/computeMetadata/v1/instance/attributes/userdata" -H "Metadata-Flavor: Google"`
echo "USERDATA: $USERDATA"

# Userdata is a json dict, so treat it as such.
CLUSTER_ID=`echo $USERDATA | python -c "import json,sys; d=json.load(sys.stdin); print d[\"cluster_ID\"]"`
CLUSTER_NAME=`echo $USERDATA | python -c "import json,sys; d=json.load(sys.stdin); print d[\"cluster_name\"]"`
NODE_TYPE=`echo $USERDATA | python -c "import json,sys; d=json.load(sys.stdin); print d[\"node_type\"]"`
STORAGE_BUCKET=`echo $USERDATA | python -c "import json,sys; d=json.load(sys.stdin); print d[\"bucket\"]"`
DEVICES=`echo $USERDATA | python -c "import json,sys; d=json.load(sys.stdin); print d[\"devices\"]"`

echo "Cluster ID $CLUSTER_ID"
echo "Cluster Name $CLUSTER_NAME"
echo "Node type $NODE_TYPE"
echo "Storage bucket $STORAGE_BUCKET"
echo "Devices $DEVICES"

# Get other instance metadata
INSTANCE_ID=`curl -s "http://metadata.google.internal/computeMetadata/v1/instance/id" -H "Metadata-Flavor: Google"`
INTERNAL_DNS=`curl -s "http://metadata.google.internal/computeMetadata/v1/instance/hostname" -H "Metadata-Flavor: Google"`
INSTANCE_NAME=`echo $INTERNAL_DNS | python -c "import sys; print sys.stdin.readline().split('.')[0]"`
INTERNAL_IP=`curl -s "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip" -H "Metadata-Flavor: Google"`
EXTERNAL_IP=`curl -s "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip" -H "Metadata-Flavor: Google"`
# GCE doesn't expose an external hostname, so just use the external IP address.
EXTERNAL_DNS=$EXTERNAL_IP
INSTANCE_TYPE=`curl -s "http://metadata.google.internal/computeMetadata/v1/instance/machine-type" -H "Metadata-Flavor: Google"`
INSTANCE_TYPE=`echo $INSTANCE_TYPE | python -c "import sys; print sys.stdin.readline().split('/')[-1]"`
ZONE=`curl -s "http://metadata.google.internal/computeMetadata/v1/instance/zone" -H "Metadata-Flavor: Google"`
ZONE=`echo $ZONE | python -c "import sys; print sys.stdin.readline().split('/')[-1]"`

echo "Instance name $INSTANCE_NAME"
echo "Instance ID $INSTANCE_ID"
echo "Internal DNS $INTERNAL_DNS"
echo "External DNS $EXTERNAL_DNS"
echo "Internal IP $INTERNAL_IP"
echo "External IP $EXTERNAL_IP"
echo "Instance type $INSTANCE_TYPE"
echo "Zone $ZONE"

# Set up tags
gcloud compute instances add-metadata $INSTANCE_NAME --metadata CLUSTER_ID=${CLUSTER_ID} CLUSTER_NAME=${CLUSTER_NAME} NODE_TYPE=${NODE_TYPE} STATUS=Booting --zone $ZONE

echo "Fetching configuration information from Storage..."
# Fetch the remaining configuration information from Storage
gsutil cp gs://${STORAGE_BUCKET}/cluster_${CLUSTER_ID}/cluster.conf ~/cluster.conf
gsutil cp gs://${STORAGE_BUCKET}/cluster_${CLUSTER_ID}/google.conf ~/google.conf

CONFIG_DIRECTORY=`grep config_directory ~/cluster.conf | awk -F= '{print $2}' | tr -d " "`
CONFIG_DIRECTORY=`python -c "import os; print os.path.expanduser(\"$CONFIG_DIRECTORY\")"`
THEMIS_DIRECTORY=`grep themis_directory ~/cluster.conf | awk -F= '{print $2}' | tr -d " "`
THEMIS_DIRECTORY=`python -c "import os; print os.path.expanduser(\"$THEMIS_DIRECTORY\")"`

echo "Config directory $CONFIG_DIRECTORY"
echo "Themis directory $THEMIS_DIRECTORY"

gsutil cp -R gs://${STORAGE_BUCKET}/cluster_${CLUSTER_ID}/themis_config ~

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
echo "[google]" >> $NODE_CONF
echo "instance_id = ${INSTANCE_ID}" >> $NODE_CONF
echo "instance_name = ${INSTANCE_NAME}" >> $NODE_CONF

gsutil cp -R gs://${STORAGE_BUCKET}/cluster_${CLUSTER_ID}/keys ~

for key in ~/keys/*
do
    chmod 600 $key
    mv "$key" ~/.ssh/
done
rmdir ~/keys

PUBLIC_KEY=`grep public_key ~/cluster.conf | awk -F= '{print $2}' | tr -d " "`
PRIVATE_KEY=`grep private_key ~/cluster.conf | awk -F= '{print $2}' | tr -d " "`

eval `ssh-agent -s`
sleep 2
ssh-add ~/.ssh/${PRIVATE_KEY}

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
$THEMIS_DIRECTORY/src/scripts/themis/cluster/mount_disks.py --partitions
set -e

# Update tags for this instance, which signals we are ready
gcloud compute instances add-metadata $INSTANCE_NAME --metadata STATUS=Online --zone $ZONE
