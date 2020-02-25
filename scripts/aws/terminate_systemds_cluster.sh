#! /bin/bash
source systemds_cluster.config

aws emr terminate-clusters --cluster-ids $CLUSTER_ID

# Wait for cluster to start
echo "Waiting for cluster terminated state"
aws emr wait cluster-terminated --cluster-id $CLUSTER_ID

echo "Cluster: ${CLUSTER_ID} terminated."
