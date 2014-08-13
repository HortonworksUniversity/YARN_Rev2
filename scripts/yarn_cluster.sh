#!/bin/bash

if [ $# -lt 1 ]
  then
    num_of_nodes=4
else
	num_of_nodes=$1
fi

#Start the NameNode
echo "Starting NameNode..."
CID_namenode=$(docker run -d --privileged --dns 8.8.8.8 -p 50070:50070 -p 8020:8020 -e NODE_TYPE=namenode --name namenode -h namenode -i -t hwxu/hdp_node)
IP_namenode=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" namenode)
echo "NameNode started at $IP_namenode"
echo "Formating NameNode and creating initial folders in HDFS..."
sleep 7

#Start the ResourceManager
echo "Starting ResourceManager..."
CID_resourcemanager=$(docker run -d --privileged --link namenode:namenode -e namenode_ip=$IP_namenode -e NODE_TYPE=resourcemanager --dns 8.8.8.8 -p 8088:8088 -p 8032:8032 -p 50060:50060 -p 8081:8081 -p 8030:8030 -p 8050:8050 -p 8025:8025 -p 8141 -p 19888:19888 -p 45454 -p 10020:10020 -p 22 --name resourcemanager -h resourcemanager -i -t hwxu/hdp_node) 
IP_resourcemanager=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" resourcemanager)
echo "ResourceManager running on $IP_resourcemanager"

#Start the Hive/Oozie Server
echo "Starting a Hive/Oozie server..."
CID_hive=$(docker run -d --privileged --link namenode:namenode -e namenode_ip=$IP_namenode -e NODE_TYPE=hiveserver --dns 8.8.8.8 -p 11000:11000 -p 2181 -p 50111:50111 -p 9083 -p 10000 -p 9999:9999 -p 9933:9933 -p 22  --name hiveserver -h hiveserver -i -t hwxu/hdp_node bash)
IP_hive=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" hiveserver)
echo "Hive/Oozie running on $IP_hive"


#Start the WorkerNodes
echo "Starting $num_of_nodes WorkerNodes..."
for (( i=1; i<=$num_of_nodes; ++i));
do
nodename="node$i"
CID=$(docker run -d --privileged --link namenode:namenode -e namenode_ip=$IP_namenode -e NODE_TYPE=workernode --dns 8.8.8.8 -p 8010 -p 50075 -p 50010 -p 50020 -p 45454 -p 8081 -p 22 --name $nodename -h $nodename -i -t hwxu/hdp_node)
IP_workernode=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" $nodename)
echo "Started $nodename on IP $IP_workernode"
done

#All the Containers are started
echo "Cluster is up and running!"
