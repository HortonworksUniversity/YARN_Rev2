package com.hortonworks.yarnapp;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppClient {
	private static final Logger LOG =
			LoggerFactory.getLogger(AppClient.class);
	private static final String APP_NAME = "YarnApp";
	private YarnConfiguration conf;
	private YarnClient yarnClient;
	private String appJar = "yarnapp.jar";
	private ApplicationId appId;
	
	public static void main(String[] args) {
		AppClient client = new AppClient(args);
		try {
			boolean result = client.run();
		} catch (YarnException | IOException e) {
			e.printStackTrace();
		}
	}
	
	public AppClient(String[] args) {
		conf = new YarnConfiguration();
		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
	}

	public boolean run() throws YarnException, IOException {
		yarnClient.start();
		YarnClientApplication client = yarnClient.createApplication();
		GetNewApplicationResponse appResponse = client.getNewApplicationResponse();
		appId = appResponse.getApplicationId();
		LOG.info("Applicatoin ID = {}", appId);
		int maxMemory =	appResponse.getMaximumResourceCapability().getMemory();
		int maxVCores =	appResponse.getMaximumResourceCapability().getVirtualCores();
		LOG.info("Max memory = {} and max vcores = {}", maxMemory, maxVCores);
		YarnClusterMetrics clusterMetrics =
				yarnClient.getYarnClusterMetrics();
		LOG.info("Number of NodeManagers = {}", clusterMetrics.getNumNodeManagers());

		List<NodeReport> nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
		for (NodeReport node : nodeReports) {
			LOG.info("Node ID = {}, address = {}, containers = {}", node.getNodeId(), node.getHttpAddress(),
					node.getNumContainers());
		}
		List<QueueInfo> queueList = yarnClient.getAllQueues();
		for (QueueInfo queue : queueList) {
			LOG.info("Available queue: {} with capacity {} to {}", queue.getQueueName(), queue.getCapacity(),
					queue.getMaximumCapacity());
		}
		return true;
	}

}
