#! /bin/bash

if [ $# -ne 2 ]
then
    echo "Usage: `basename $0` network interface"
    exit $E_BADARGS
fi

NETWORK=$1
INTERFACE=$2

IP=`ifconfig $INTERFACE | awk '/inet /{print $2}'`
echo "IP is ${IP}"
gcloud compute firewall-rules describe allow-local-access > /dev/null 2>&1

if [ "$?" == "0" ]
then
    # Rule already exists. Update it.
    echo "gcloud compute firewall-rules update allow-local-access --allow tcp:4280 tcp:9090 --source-ranges ${IP}/32"
    gcloud compute firewall-rules update allow-local-access --allow tcp:4280 tcp:9090 --source-ranges ${IP}/32
else
    # Rule doesn't exist. Create it.
    echo "gcloud compute firewall-rules create allow-local-access --allow tcp:4280 tcp:9090 --network $NETWORK --source-ranges ${IP}/32"
    gcloud compute firewall-rules create allow-local-access --allow tcp:4280 tcp:9090 --network $NETWORK --source-ranges ${IP}/32
fi
