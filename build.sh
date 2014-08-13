#!/bin/bash

. /root/dockerfiles/start_scripts/build.sh $@ # && (echo "Parent build.sh failed"; exit 1)

# Rebuild hwxu/hdp_node to make sure students have a fix to the timeline server start script
echo -e "\n*** Building hwxu/hdp_node ***\n"
cd /root/dockerfiles/hdp_node
docker build -t hwxu/hdp_node .
echo -e "\n*** Build of hwxu/hdp_node complete! ***\n"

#If this script is execute multiple times, untagged images get left behind
#This command removes any untagged Docker images
docker rmi -f $(docker images | grep '^<none>' | awk '{print $3}')

cp /root/dockerfiles/hdp_node/configuration_files/core_hadoop/yarn-site.xml /etc/hadoop/conf

#Unzip eclipse
if [[ ! -d "/root/eclipse" ]]; then
	cd /root/
	tar -xvf /root/eclipse.tgz
	chown -R root:root /root/eclipse
fi

mkdir -p /root/yarn/workspace

echo -e "\n*** The lab environment has successfully been built for this classroom VM ***\n"
